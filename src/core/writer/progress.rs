//! [`WriterProgress`]: what the writer thread has written, and whether it is
//! still moving, readable from other threads while it runs.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::time::Instant;

/// How often [`WriterProgress::stalled_for`] reads the progress.
const STALL_POLL: Duration = Duration::from_millis(100);

/// The line of a thread's `/proc` status file that counts the times the
/// thread blocked and was woken.
const WAKEUPS_FIELD: &str = "voluntary_ctxt_switches:";

/// What the writer thread has written so far, and how often it has woken.
#[derive(Debug, Default)]
pub struct WriterProgress {
    snapshots_written: AtomicU64,
    updates_written: AtomicU64,
    /// The writer thread's `/proc` status file, once the thread has found it.
    thread_status: OnceLock<PathBuf>,
}

/// One reading of every sign that the writer is moving.
#[derive(Debug, PartialEq, Eq)]
struct Activity {
    written: u64,
    wakeups: Option<u64>,
}

impl WriterProgress {
    /// `Snapshot` messages written so far.
    pub fn snapshots_written(&self) -> u64 {
        self.snapshots_written.load(Ordering::Relaxed)
    }

    /// `PriceLevelUpdate` messages written so far.
    pub fn updates_written(&self) -> u64 {
        self.updates_written.load(Ordering::Relaxed)
    }

    /// Messages of both kinds written so far.
    fn written(&self) -> u64 {
        self.snapshots_written() + self.updates_written()
    }

    /// Returns once the writer has shown no sign of moving for `window`,
    /// counting from the call. The signs are read every 100 ms.
    ///
    /// The signs are messages written and, where `/proc` shows the writer
    /// thread, the thread's wakeups. Messages count as written when they
    /// enter the current chunk, so a writer blocked in a chunk write writes
    /// none until the write completes. But each time a reader frees room in
    /// the pipe, the kernel wakes the blocked thread, so a writer behind a
    /// slow reader keeps moving, while one behind a reader that never reads
    /// does not. A writer waiting for messages wakes on its own timers, so it
    /// never looks stalled.
    ///
    /// Without `/proc`, only messages count, and a chunk write that takes
    /// longer than `window` looks like a stall.
    ///
    /// Limits:
    /// - A reader that pauses for longer than `window` between reads leaves
    ///   the writer asleep that long, which counts as a stall.
    /// - Wakeups only separate a slow reader from a dead one on pipe and
    ///   socket outputs. A writer blocked on a regular file whose storage
    ///   hangs (a dead NFS server, a hung device) can keep being woken by the
    ///   kernel's dirty-page throttling, so it may never look stalled; a
    ///   second signal is then the way to stop the process.
    pub async fn stalled_for(&self, window: Duration) {
        let mut last = self.activity();
        let mut quiet_since = Instant::now();
        let mut polls = tokio::time::interval(STALL_POLL);
        loop {
            polls.tick().await;
            let activity = self.activity();
            if activity != last {
                last = activity;
                quiet_since = Instant::now();
            } else if quiet_since.elapsed() >= window {
                return;
            }
        }
    }

    /// A count that changes with every sign of the writer moving that
    /// [`WriterProgress::stalled_for`] reads: messages written plus, where
    /// `/proc` shows them, the writer thread's wakeups.
    pub fn movement(&self) -> u64 {
        let Activity { written, wakeups } = self.activity();
        written + wakeups.unwrap_or(0)
    }

    /// Counts one `Snapshot` written.
    pub(super) fn note_snapshot(&self) {
        self.snapshots_written.fetch_add(1, Ordering::Relaxed);
    }

    /// Counts one `PriceLevelUpdate` written.
    pub(super) fn note_update(&self) {
        self.updates_written.fetch_add(1, Ordering::Relaxed);
    }

    /// Marks the calling thread as the writer thread, whose wakeups then
    /// count as progress. Without `/proc`, logs at debug and leaves only
    /// messages to count.
    pub(super) fn attach_writer_thread(&self) {
        match std::fs::read_link("/proc/thread-self") {
            Ok(task) => {
                let status = Path::new("/proc").join(task).join("status");
                if self.thread_status.set(status).is_err() {
                    tracing::debug!("the writer thread was already attached");
                }
            }
            Err(error) => {
                let detail = "stall checks count messages only";
                tracing::debug!(%error, detail, "no /proc entry for the writer thread");
            }
        }
    }

    fn activity(&self) -> Activity {
        Activity {
            written: self.written(),
            wakeups: self.wakeups(),
        }
    }

    /// How many times the writer thread has blocked and been woken, if
    /// `/proc` says.
    fn wakeups(&self) -> Option<u64> {
        let status = std::fs::read_to_string(self.thread_status.get()?).ok()?;
        let field = status
            .lines()
            .find_map(|line| line.strip_prefix(WAKEUPS_FIELD))?;
        field.trim().parse().ok()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn a_writer_that_writes_nothing_stalls_after_the_window() {
        let progress = WriterProgress::default();
        let started = Instant::now();
        progress.stalled_for(Duration::from_secs(5)).await;
        let took = started.elapsed();
        let window = Duration::from_secs(5);
        assert!(took >= window && took <= window + STALL_POLL, "{took:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn each_write_restarts_the_stall_window() {
        let progress = Arc::new(WriterProgress::default());
        let writing = progress.clone();
        let write_three = async move {
            for _ in 0..3 {
                tokio::time::sleep(Duration::from_secs(1)).await;
                writing.note_update();
            }
        };
        let started = Instant::now();
        tokio::join!(
            write_three,
            progress.stalled_for(Duration::from_millis(1500))
        );
        let took = started.elapsed();
        let quiet_from_third_write = Duration::from_millis(4500);
        let latest = quiet_from_third_write + STALL_POLL * 2;
        assert!(took >= quiet_from_third_write && took <= latest, "{took:?}");
    }

    /// Starts a thread that attaches itself as the writer thread, then runs
    /// `body`, and waits until it has attached.
    fn writer_thread(
        progress: &Arc<WriterProgress>,
        body: impl FnOnce() + Send + 'static,
    ) -> std::thread::JoinHandle<()> {
        let (attached, wait) = std::sync::mpsc::channel();
        let progress = progress.clone();
        let thread = std::thread::spawn(move || {
            progress.attach_writer_thread();
            attached.send(()).unwrap();
            body();
        });
        wait.recv().unwrap();
        thread
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn a_writer_thread_that_keeps_waking_is_not_stalled() {
        let progress = Arc::new(WriterProgress::default());
        let waking = writer_thread(&progress, || {
            for _ in 0..10 {
                std::thread::sleep(Duration::from_millis(100));
            }
        });
        let started = Instant::now();
        progress.stalled_for(Duration::from_millis(400)).await;
        let took = started.elapsed();
        waking.join().unwrap();
        assert!(took >= Duration::from_secs(1), "{took:?}");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn a_writer_thread_blocked_for_good_is_stalled() {
        let progress = Arc::new(WriterProgress::default());
        let (release, blocked) = std::sync::mpsc::channel::<()>();
        let stuck = writer_thread(&progress, move || {
            blocked.recv().ok();
        });
        let started = Instant::now();
        progress.stalled_for(Duration::from_millis(300)).await;
        let took = started.elapsed();
        drop(release);
        stuck.join().unwrap();
        assert!(took < Duration::from_secs(1), "{took:?}");
    }
}
