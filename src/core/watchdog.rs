//! The liveness watchdog.
//!
//! Release builds abort on a panic, so no task dies unnoticed; what is left
//! to catch is a part of the recorder that stops moving: the coordinator, a
//! shard supervisor, the writer, or the whole runtime. The first two beat a
//! [`Heartbeat`] as they work and every second while they wait, the writer
//! included, and the writer's progress counts as a heartbeat of its own. A
//! dedicated OS thread, which a wedged runtime cannot starve, reads them
//! every [`CHECK_INTERVAL`] and aborts the process once any has been still
//! for longer than [`STALE_AFTER`], so `run.sh` restarts it. The checks
//! stop once shutdown begins, which has its own stall exit.

use std::collections::{BTreeMap, HashMap};
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::Poll;
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;

use crate::core::writer::WriterProgress;

/// How long the watchdog thread sleeps between two checks.
const CHECK_INTERVAL: Duration = Duration::from_secs(10);

/// How long a source may go without a sign of moving before the watchdog
/// aborts the process.
const STALE_AFTER: Duration = Duration::from_secs(30);

/// The longest real time between two checks counted in full. A longer gap
/// means the process was paused, by SIGSTOP or a paused VM, or the
/// watchdog ran late; all of it but one [`CHECK_INTERVAL`] is excused.
const MAX_CHECK_GAP: Duration = CHECK_INTERVAL.saturating_mul(2);

/// How often [`Heartbeat::beat_while`] beats.
const WAITING_BEAT_INTERVAL: Duration = Duration::from_secs(1);

/// The source name of the writer's progress.
const WRITER_SOURCE: &str = "writer";

/// The heartbeats the watchdog reads, by source name. Clones share one
/// registry.
#[derive(Debug, Clone, Default)]
pub struct Heartbeats {
    sources: Arc<Mutex<BTreeMap<Arc<str>, Arc<AtomicU64>>>>,
}

impl Heartbeats {
    /// Registers a heartbeat for `source`, a name no other registered
    /// heartbeat has. The watchdog reads it until it is dropped.
    pub fn register(&self, source: &str) -> Heartbeat {
        let source: Arc<str> = source.into();
        let beats = Arc::new(AtomicU64::new(0));
        let previous = self.lock().insert(source.clone(), beats.clone());
        debug_assert!(previous.is_none(), "{source} registered twice");
        Heartbeat {
            source,
            beats,
            registry: self.clone(),
        }
    }

    /// Every registered heartbeat's beats so far.
    pub fn read(&self) -> Vec<Reading> {
        let sources = self.lock();
        let read = |(source, beats): (&Arc<str>, &Arc<AtomicU64>)| Reading {
            source: source.clone(),
            count: beats.load(Ordering::Relaxed),
        };
        sources.iter().map(read).collect()
    }

    fn lock(&self) -> MutexGuard<'_, BTreeMap<Arc<str>, Arc<AtomicU64>>> {
        self.sources.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// One source's heartbeat, read by the watchdog until dropped.
#[derive(Debug)]
pub struct Heartbeat {
    source: Arc<str>,
    beats: Arc<AtomicU64>,
    registry: Heartbeats,
}

impl Heartbeat {
    /// Shows the watchdog that the source is still moving.
    pub fn beat(&self) {
        self.beats.fetch_add(1, Ordering::Relaxed);
    }

    /// Runs `future` to completion, beating when it first has to wait and
    /// every second after, so a source waiting on something slow but
    /// moving, such as the writer, does not look stalled. A future ready at
    /// its first poll creates no timer; one that has to wait gets a timer
    /// for that wait.
    pub async fn beat_while<F: Future>(&self, future: F) -> F::Output {
        let mut future = pin!(future);
        let first = std::future::poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await;
        match first {
            Poll::Ready(output) => output,
            Poll::Pending => self.beat_until(future).await,
        }
    }

    /// Beats at once and every second until `future` completes.
    async fn beat_until<F: Future>(&self, mut future: Pin<&mut F>) -> F::Output {
        let mut beats = tokio::time::interval(WAITING_BEAT_INTERVAL);
        loop {
            tokio::select! {
                biased;
                output = &mut future => return output,
                _ = beats.tick() => self.beat(),
            }
        }
    }
}

impl Drop for Heartbeat {
    fn drop(&mut self) {
        self.registry.lock().remove(&self.source);
    }
}

/// A count of one source's signs of moving, at one moment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reading {
    /// The source's name.
    pub source: Arc<str>,
    /// Its signs of moving so far; any change is a sign.
    pub count: u64,
}

/// Everything one check reads.
#[derive(Debug)]
struct Readings {
    counts: Vec<Reading>,
    shutting_down: bool,
}

/// What a check concludes.
#[derive(Debug, PartialEq, Eq)]
enum Verdict {
    /// Every source has moved recently enough, or the recorder is shutting
    /// down.
    Alive,
    /// `source`, the stillest, has shown no sign of moving for `still_for`,
    /// counted from the first check that read its current count.
    Stalled {
        source: Arc<str>,
        still_for: Duration,
    },
}

/// When each source's count last changed, and when the last check was,
/// kept between checks.
#[derive(Debug, Default)]
struct Liveness {
    sightings: HashMap<Arc<str>, Sighting>,
    last_check: Option<Instant>,
}

/// A source's count and the first check that read it.
#[derive(Debug, Clone, Copy)]
struct Sighting {
    count: u64,
    since: Instant,
}

impl Liveness {
    /// Notes `readings`, taken at `now`, and judges them. A source missing
    /// from them is forgotten, and a new one counts as moving at `now`.
    /// After a gap since the last check longer than [`MAX_CHECK_GAP`], only
    /// one [`CHECK_INTERVAL`] of it counts as stillness: a paused process
    /// gets about 20 s of grace on resuming, while a watchdog that keeps
    /// running late still sees stillness grow by an interval per check.
    /// During shutdown every source is alive.
    fn check(&mut self, readings: Readings, now: Instant) -> Verdict {
        let last = self.last_check.replace(now);
        let gap = last.map(|last| now.saturating_duration_since(last));
        if readings.shutting_down {
            return Verdict::Alive;
        }
        if let Some(gap) = gap.filter(|gap| *gap > MAX_CHECK_GAP) {
            self.excuse(gap - CHECK_INTERVAL);
        }
        let sight = |reading| self.sight(reading, now);
        self.sightings = readings.counts.into_iter().map(sight).collect();
        self.stillest(now)
    }

    /// Counts `excused` of every source's stillness as never having
    /// happened.
    fn excuse(&mut self, excused: Duration) {
        for sighting in self.sightings.values_mut() {
            sighting.since += excused;
        }
    }

    /// `reading`'s sighting at `now`: unchanged since the last check keeps
    /// that check's start, else it starts now.
    fn sight(&self, reading: Reading, now: Instant) -> (Arc<str>, Sighting) {
        let since = match self.sightings.get(&reading.source) {
            Some(seen) if seen.count == reading.count => seen.since,
            _ => now,
        };
        let count = reading.count;
        (reading.source, Sighting { count, since })
    }

    /// Stalled if the source still the longest has been still too long.
    fn stillest(&self, now: Instant) -> Verdict {
        let still_for = |sighting: &Sighting| now.saturating_duration_since(sighting.since);
        let stillest = self
            .sightings
            .iter()
            .map(|(source, seen)| (still_for(seen), source))
            .max();
        match stillest {
            Some((still_for, source)) if still_for > STALE_AFTER => Verdict::Stalled {
                source: source.clone(),
                still_for,
            },
            _ => Verdict::Alive,
        }
    }
}

/// What the watchdog thread watches.
#[derive(Debug)]
pub struct Watched {
    /// The coordinator's and the shard supervisors' heartbeats.
    pub heartbeats: Heartbeats,
    /// The writer's progress, read as the source `writer`.
    pub writer: Arc<WriterProgress>,
    /// Fires when the recorder begins shutting down.
    pub cancel: CancellationToken,
}

impl Watched {
    /// Reads every heartbeat, the writer's movement, and whether shutdown
    /// has begun.
    fn read(&self) -> Readings {
        let mut counts = self.heartbeats.read();
        counts.push(Reading {
            source: WRITER_SOURCE.into(),
            count: self.writer.movement(),
        });
        Readings {
            counts,
            shutting_down: self.cancel.is_cancelled(),
        }
    }
}

/// Starts the watchdog thread on `watched`, detached: nothing joins it. It
/// checks every [`CHECK_INTERVAL`], aborts the process on a stall, and ends
/// at its first check after the cancel token fires.
pub fn spawn_watchdog(watched: Watched) -> std::io::Result<()> {
    std::thread::Builder::new()
        .name("watchdog".into())
        .spawn(move || watch(&watched))
        .map(drop)
}

fn watch(watched: &Watched) {
    let mut liveness = Liveness::default();
    while !watched.cancel.is_cancelled() {
        std::thread::sleep(CHECK_INTERVAL);
        let verdict = liveness.check(watched.read(), Instant::now());
        if let Verdict::Stalled { source, still_for } = verdict {
            tracing::error!(%source, ?still_for, "stopped moving, aborting the process");
            std::process::abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn live(heartbeats: &Heartbeats) -> Readings {
        Readings {
            counts: heartbeats.read(),
            shutting_down: false,
        }
    }

    /// A [`Liveness`] checking `heartbeats`, first at `next`.
    struct Schedule {
        heartbeats: Heartbeats,
        liveness: Liveness,
        next: Instant,
    }

    impl Schedule {
        /// A schedule over the coordinator's and one shard's heartbeats,
        /// returned with them, after its first check.
        fn watching_two() -> (Self, Heartbeat, Heartbeat) {
            let heartbeats = Heartbeats::default();
            let coordinator = heartbeats.register("coordinator");
            let shard = heartbeats.register("shard 0");
            let mut schedule = Self {
                heartbeats,
                liveness: Liveness::default(),
                next: Instant::now(),
            };
            assert_eq!(schedule.check(), Verdict::Alive);
            (schedule, coordinator, shard)
        }

        /// Checks at the next time due, and schedules the one after
        /// [`CHECK_INTERVAL`] later.
        fn check(&mut self) -> Verdict {
            let now = self.next;
            self.next += CHECK_INTERVAL;
            self.liveness.check(live(&self.heartbeats), now)
        }

        /// Runs `before_each` and then a check, `checks` times, and returns
        /// the last verdict.
        fn check_after(&mut self, checks: u32, before_each: impl Fn()) -> Verdict {
            let mut verdict = Verdict::Alive;
            for _ in 0..checks {
                before_each();
                verdict = self.check();
            }
            verdict
        }
    }

    #[test]
    fn sources_that_keep_beating_are_alive() {
        let (mut schedule, coordinator, shard) = Schedule::watching_two();
        let verdict = schedule.check_after(6, || {
            coordinator.beat();
            shard.beat();
        });
        assert_eq!(verdict, Verdict::Alive);
    }

    #[test]
    fn a_source_still_past_the_limit_is_stalled() {
        let (mut schedule, coordinator, _shard) = Schedule::watching_two();
        let verdict = schedule.check_after(4, || coordinator.beat());
        let stalled = Verdict::Stalled {
            source: "shard 0".into(),
            still_for: CHECK_INTERVAL * 4,
        };
        assert_eq!(verdict, stalled);
    }

    #[test]
    fn a_source_still_for_exactly_the_limit_is_alive() {
        let (mut schedule, _coordinator, _shard) = Schedule::watching_two();
        assert_eq!(schedule.check_after(3, || {}), Verdict::Alive);
    }

    #[test]
    fn a_source_still_during_shutdown_is_alive() {
        let (mut schedule, _coordinator, _shard) = Schedule::watching_two();
        schedule.check_after(3, || {});
        let readings = Readings {
            shutting_down: true,
            ..live(&schedule.heartbeats)
        };
        let verdict = schedule.liveness.check(readings, schedule.next);
        assert_eq!(verdict, Verdict::Alive);
    }

    #[test]
    fn a_deregistered_source_is_not_checked() {
        let (mut schedule, coordinator, shard) = Schedule::watching_two();
        drop(shard);
        let verdict = schedule.check_after(4, || coordinator.beat());
        assert_eq!(verdict, Verdict::Alive);
    }

    #[test]
    fn a_pause_longer_than_the_limit_is_excused() {
        let (mut schedule, _coordinator, _shard) = Schedule::watching_two();
        schedule.next += MAX_CHECK_GAP + CHECK_INTERVAL;
        let after_pause = schedule.check();
        let next = schedule.check();
        assert_eq!((after_pause, next), (Verdict::Alive, Verdict::Alive));
    }

    /// The pause counts as one check interval of stillness, so three
    /// checks after it the shard has been still for four intervals.
    #[test]
    fn a_source_still_across_a_pause_counts_one_interval_of_it() {
        let (mut schedule, coordinator, _shard) = Schedule::watching_two();
        schedule.next += MAX_CHECK_GAP;
        schedule.check();
        let verdict = schedule.check_after(3, || coordinator.beat());
        let stalled = Verdict::Stalled {
            source: "shard 0".into(),
            still_for: CHECK_INTERVAL * 4,
        };
        assert_eq!(verdict, stalled);
    }

    /// Each check comes just past the longest counted gap, as from a
    /// watchdog that always runs late, yet stillness still grows.
    #[test]
    fn a_still_source_checked_late_every_time_still_stalls() {
        let (mut schedule, _coordinator, _shard) = Schedule::watching_two();
        let late = MAX_CHECK_GAP + Duration::from_secs(1) - CHECK_INTERVAL;
        let mut verdicts = Vec::new();
        for _ in 0..4 {
            schedule.next += late;
            verdicts.push(schedule.check());
        }
        let alive = verdicts.iter().filter(|v| **v == Verdict::Alive).count();
        assert_eq!(alive, 3, "{verdicts:?}");
    }

    #[test]
    #[should_panic(expected = "registered twice")]
    fn registering_a_name_twice_is_a_bug() {
        let heartbeats = Heartbeats::default();
        let _first = heartbeats.register("shard 0");
        let _second = heartbeats.register("shard 0");
    }

    fn idle_writer(cancel: CancellationToken) -> Watched {
        Watched {
            heartbeats: Heartbeats::default(),
            writer: Arc::new(WriterProgress::default()),
            cancel,
        }
    }

    #[test]
    fn a_writer_without_progress_past_the_limit_is_stalled() {
        let watched = idle_writer(CancellationToken::new());
        let mut liveness = Liveness::default();
        let start = Instant::now();
        let mut verdict = liveness.check(watched.read(), start);
        for check in 1..=4 {
            verdict = liveness.check(watched.read(), start + CHECK_INTERVAL * check);
        }
        let stalled = Verdict::Stalled {
            source: WRITER_SOURCE.into(),
            still_for: CHECK_INTERVAL * 4,
        };
        assert_eq!(verdict, stalled);
    }

    #[test]
    fn a_cancelled_recorder_reads_as_shutting_down() {
        let cancel = CancellationToken::new();
        cancel.cancel();
        assert!(idle_writer(cancel).read().shutting_down);
    }

    #[tokio::test(start_paused = true)]
    async fn a_source_waiting_beats_every_second() {
        let heartbeats = Heartbeats::default();
        let heartbeat = heartbeats.register("shard 0");
        let waiting = tokio::time::sleep(Duration::from_millis(3_500));
        heartbeat.beat_while(waiting).await;
        assert_eq!(heartbeats.read()[0].count, 4);
    }

    #[tokio::test(start_paused = true)]
    async fn a_future_ready_at_once_does_not_beat() {
        let heartbeats = Heartbeats::default();
        let heartbeat = heartbeats.register("shard 0");
        let output = heartbeat.beat_while(async { 7 }).await;
        assert_eq!((output, heartbeats.read()[0].count), (7, 0));
    }
}
