//! Splitting a shard whose primary keeps overflowing.

use std::sync::atomic::{AtomicU64, Ordering};

use engineering_challenge::core::writer::WriterMessage;
use tokio::sync::watch;

use super::failover::{QUIET, accept_pair, bid_at, paired, raise, stop};
use super::mock_exchange::{MockCatalog, SentCommand};
use super::resync::open_gap;
use super::*;

/// The four markets of the shard [`twice_overflowed`] records.
const FOUR: [&str; 4] = ["A", "B", "C", "D"];

/// `base` with every overflow its own incident, so two overflows in a row
/// split a shard.
fn splitting(base: CoordinatorConfig) -> CoordinatorConfig {
    CoordinatorConfig {
        split: SplitRule {
            incident_gap: Duration::ZERO,
            ..SplitRule::default()
        },
        ..base
    }
}

/// A recorder with one shard over [`FOUR`] on `config`, whose connection
/// has overflowed once and come back with every market, and has just
/// overflowed again.
async fn twice_overflowed(server: &MockServer, config: CoordinatorConfig) -> Recorder {
    let mut recorder = Recorder::start_with(server, config);
    recorder.list_open(&FOUR);
    let second = overflowed_once(server, &recorder).await;
    overflow(second).await;
    recorder
}

/// Records [`FOUR`] on one shard whose first connection overflows, and
/// returns its second, with every market and its books recorded.
async fn overflowed_once(server: &MockServer, recorder: &Recorder) -> Shard {
    let mut first = Shard::accept(server, 1).await;
    first.send_books().await;
    recorder.output.wait_for(4, 0).await;
    overflow(first).await;
    let mut second = Shard::accept(server, 2).await;
    assert_eq!(second.symbols, FOUR.map(str::to_owned));
    second.send_books().await;
    recorder.output.wait_for(8, 0).await;
    second
}

/// Sends an overflow error on `shard`'s connection, which drops it.
async fn overflow(mut shard: Shard) {
    shard.client.send(frames::error(25)).await;
}

/// Accepts the old shard's next connection and the new shard's, in either
/// order, naming them 3 and 4. The old one may have subscribed before it
/// learned of the split, and then removes the moved markets at once.
async fn accept_split(server: &MockServer) -> (Shard, Shard) {
    let one = Shard::accept(server, 3).await;
    let other = Shard::accept(server, 4).await;
    let (mut old, new) = if one.owns("A") {
        (one, other)
    } else {
        (other, one)
    };
    if old.owns("B") {
        let SentCommand { cmd, symbols, .. } = old.client.next_command().await;
        assert_eq!((cmd.as_str(), symbols), ("remove", moved()));
        old.symbols.retain(|symbol| !moved().contains(symbol));
    }
    (old, new)
}

/// The markets a split of [`FOUR`] moves: every other one.
fn moved() -> Vec<String> {
    vec!["B".to_owned(), "D".to_owned()]
}

/// Two overflows split the shard: A and C stay, B and D move to a new
/// shard, whose books replace theirs. The old shard drops a change of B
/// that its socket still delivers.
#[tokio::test]
async fn a_second_overflow_splits_the_shard_into_two_with_disjoint_markets() {
    let server = MockServer::start().await;
    let recorder = twice_overflowed(&server, splitting(config(4))).await;
    let (mut old, mut new) = accept_split(&server).await;
    old.send_books().await;
    new.send_books().await;
    raise(&mut old, "B").await;
    raise(&mut old, "A").await;
    raise(&mut new, "B").await;
    recorder.output.wait_for(12, 2).await;
    let counters = recorder.counters.clone();
    let markets = (old.symbols.clone(), new.symbols.clone());
    let finished = stop(recorder, vec![old, new]).await;
    let kept = vec!["A".to_owned(), "C".to_owned()];
    assert_eq!(markets, (kept, moved()));
    let b = vec![sent_book(), sent_book(), sent_book(), bid_at(110)];
    assert_eq!(finished.of("B"), b);
    let counts = counters.read().monitoring;
    let splits = (counts.overflows, counts.splits, counts.splits_skipped);
    assert_eq!(splits, (2, 1, 0));
}

/// With no room in the connection budget for another shard, the split is
/// skipped and the shard comes back with every market.
#[tokio::test]
async fn a_split_with_no_room_in_the_budget_is_skipped() {
    let server = MockServer::start().await;
    let config = CoordinatorConfig {
        max_connections: 2,
        ..splitting(config(4))
    };
    let recorder = twice_overflowed(&server, config).await;
    let third = Shard::accept(&server, 3).await;
    let another = timeout(QUIET, server.accept()).await;
    let counters = recorder.counters.clone();
    let symbols = third.symbols.clone();
    stop(recorder, vec![third]).await;
    assert!(another.is_err(), "a new shard opened");
    let monitoring = counters.read().monitoring;
    let counts = (monitoring.splits, monitoring.splits_skipped);
    let every = FOUR.map(str::to_owned).to_vec();
    assert_eq!((symbols, counts), (every, (0, 1)));
}

/// Waits briefly for a connection that should not come, failing if one
/// does: the new shard of a split.
async fn assert_no_new_shard(server: &MockServer) {
    let another = timeout(QUIET, server.accept()).await;
    assert!(another.is_err(), "a new shard opened");
}

/// One burst of load overflows the primary, then the standby that took
/// over from it, within seconds: one incident, so no split.
#[tokio::test]
async fn an_overflow_of_both_replicas_in_one_burst_does_not_split() {
    let server = MockServer::start().await;
    let mut recorder = Recorder::start_with(&server, paired(4));
    recorder.list_open(&FOUR);
    let mut pair = accept_pair(&server).await;
    pair.primary.send_books().await;
    recorder.output.wait_for(4, 0).await;
    overflow(pair.primary).await;
    let takeover = pair.standby.client.next_command().await;
    overflow(pair.standby).await;
    let first_back = Shard::accept(&server, 3).await;
    let back = vec![first_back, Shard::accept(&server, 4).await];
    assert_no_new_shard(&server).await;
    let counters = recorder.counters.clone();
    stop(recorder, back).await;
    let counts = counters.read().monitoring;
    let outcome = (takeover.cmd.as_str(), counts.overflows, counts.splits);
    assert_eq!(outcome, ("snapshot", 2, 0));
}

/// The standby overflows twice, each overflow its own incident: only the
/// primary's overflows count, so no split.
#[tokio::test]
async fn a_standbys_overflows_do_not_split() {
    let server = MockServer::start().await;
    let mut recorder = Recorder::start_with(&server, splitting(paired(4)));
    recorder.list_open(&FOUR);
    let mut pair = accept_pair(&server).await;
    pair.primary.send_books().await;
    recorder.output.wait_for(4, 0).await;
    overflow(pair.standby).await;
    overflow(Shard::accept(&server, 3).await).await;
    let standby = Shard::accept(&server, 4).await;
    assert_no_new_shard(&server).await;
    let counters = recorder.counters.clone();
    stop(recorder, vec![pair.primary, standby]).await;
    let counts = counters.read().monitoring;
    assert_eq!((counts.overflows, counts.splits), (2, 0));
}

/// How many messages a [`credited_relay`] may pass on to the writer, and
/// how many it has.
struct Credits {
    allowed: watch::Sender<u64>,
    passed: Arc<AtomicU64>,
}

impl Credits {
    /// Passes nothing more until allowed.
    fn hold(&self) {
        let passed = self.passed.load(Ordering::SeqCst);
        self.allowed.send_replace(passed);
    }

    /// Passes `more` messages beyond those allowed so far.
    fn allow(&self, more: u64) {
        self.allowed.send_modify(|allowed| *allowed += more);
    }

    /// Passes every message from now on.
    fn open(&self) {
        self.allowed.send_replace(u64::MAX);
    }
}

/// Passes messages on to `writer` through a channel of one, as many as
/// the returned credits allow; all of them until held.
fn credited_relay(writer: mpsc::Sender<WriterMessage>) -> (mpsc::Sender<WriterMessage>, Credits) {
    let (sender, mut receiver) = mpsc::channel(1);
    let (allowed, mut credit) = watch::channel(u64::MAX);
    let passed = Arc::new(AtomicU64::new(0));
    let counted = passed.clone();
    tokio::spawn(async move {
        while let Some(message) = receiver.recv().await {
            let sent = counted.load(Ordering::SeqCst);
            credit.wait_for(|&allowed| allowed > sent).await.ok();
            if writer.send(message).await.is_err() {
                return;
            }
            counted.fetch_add(1, Ordering::SeqCst);
        }
    });
    (sender, Credits { allowed, passed })
}

/// A recorder over [`FOUR`] on one shard whose second overflow splits it,
/// recording through a [`credited_relay`], with a resync deadline of
/// 300 ms.
fn credited_recorder(server: &MockServer) -> (Recorder, Credits) {
    let config = CoordinatorConfig {
        resync: ResyncTimings {
            timeout: Duration::from_millis(300),
            ..ResyncTimings::default()
        },
        ..splitting(config(4))
    };
    let (pipeline, credits) = Pipeline::with_link(mock_feed(server), config, |link| {
        let (messages, credits) = credited_relay(link.messages.clone());
        link.messages = messages;
        credits
    });
    let (catalog, pages) = MockCatalog::new();
    let mut recorder = Recorder::run(pipeline, catalog, pages);
    recorder.list_open(&FOUR);
    (recorder, credits)
}

/// Leaves the shard's supervisor waiting on the writer with the next three
/// of its messages: C's book and two changes, after a gap that holds the
/// other markets back.
async fn stall_the_shard(shard: &mut Shard, credits: &Credits) {
    open_gap(shard, "A").await;
    credits.hold();
    let seq = shard.next_seq();
    let book = frames::book(shard.sub, seq, "C", (BIDS, ASKS));
    shard.client.send(book).await;
    raise(shard, "C").await;
    raise(shard, "C").await;
}

/// The old shard is stalled behind the writer when its primary overflows a
/// second time and reconnects, so the overflow, and the new socket's books
/// and changes of B, wait in its queue until its resync deadline drains
/// it. The split comes from that drain. The new shard's book of B must
/// reach the writer after every event of B the old shard forwards, so it
/// is the last message of B recorded.
#[tokio::test]
async fn a_split_during_a_drain_starts_the_new_shard_after_the_old_one_lets_go() {
    let server = MockServer::start().await;
    let (recorder, credits) = credited_recorder(&server);
    let mut second = overflowed_once(&server, &recorder).await;
    stall_the_shard(&mut second, &credits).await;
    overflow(second).await;
    let mut third = Shard::accept(&server, 3).await;
    third.send_books().await;
    raise(&mut third, "B").await;
    raise(&mut third, "B").await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    credits.allow(3);
    let opening = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        credits.open();
    });
    let mut new = Shard::accept(&server, 4).await;
    let seq = new.next_seq();
    let book = frames::book(new.sub, seq, "B", (&[(400_000, 70)], ASKS));
    new.client.send(book).await;
    opening.await.unwrap();
    recorder.output.wait_for(14, 4).await;
    let finished = stop(recorder, vec![third, new]).await;
    let new_book = Shape::Snapshot(vec![(400_000, 70)], ASKS.to_vec());
    assert_eq!(finished.of("B").last(), Some(&new_book));
}
