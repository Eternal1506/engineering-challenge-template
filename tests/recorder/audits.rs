//! Book audits: the primary asked for snapshots of a few markets in turn,
//! each reply compared with the recorded book.

use engineering_challenge::core::writer::AuditCounts;

use super::failover::{
    accept_unnamed, answer, answer_with_bid, book_with_bid, name, stop, wait_until,
};
use super::resync::open_gap;
use super::*;

/// How often [`auditing`] audits, two markets at a time.
const ROUNDS: Duration = Duration::from_millis(300);

/// Shards of `shard_size` that audit two markets every [`ROUNDS`], and
/// never check for silence.
fn auditing(shard_size: usize) -> CoordinatorConfig {
    let audit = BookAudit {
        interval: ROUNDS,
        markets: 2,
    };
    CoordinatorConfig {
        monitor: MonitorConfig {
            audit,
            ..quiet_monitor()
        },
        ..config(shard_size)
    }
}

/// The first round audits A and B: A's reply matches its book and is not
/// written, B's differs and replaces it. The next round goes on from C,
/// wrapping around to A.
#[tokio::test]
async fn audits_write_only_the_replies_that_differ_from_the_book() {
    let server = MockServer::start().await;
    let mut recorder = Recorder::start_with(&server, auditing(3));
    recorder.list_open(&["A", "B", "C"]);
    let mut shard = Shard::accept(&server, 1).await;
    shard.send_books().await;
    recorder.output.wait_for(3, 0).await;
    let first = shard.client.next_command().await;
    answer(&mut shard, &first, "A").await;
    answer_with_bid(&mut shard, &first, "B", 90).await;
    let progress = recorder.output.progress.clone();
    wait_until(|| progress.audits().total() == 2).await;
    let second = shard.client.next_command().await;
    let finished = stop(recorder, vec![shard]).await;
    let rounds = (first.cmd.as_str(), first.symbols, second.symbols);
    let expected = ("snapshot", names(&["A", "B"]), names(&["C", "A"]));
    assert_eq!(rounds, expected);
    let books = (finished.of("A"), finished.of("B"));
    let rewritten = vec![sent_book(), book_with_bid(90)];
    assert_eq!(books, (vec![sent_book()], rewritten));
    let audits = AuditCounts {
        matched: 1,
        mismatched: 1,
    };
    assert_eq!(progress.audits(), audits);
}

/// A gap holds A back after its audit was asked for. A's reply, the same
/// book, releases it as any snapshot does, and is written rather than
/// compared, since the book it would be compared with is stale.
#[tokio::test]
async fn an_audit_reply_that_releases_a_held_market_is_written_as_its_snapshot() {
    let server = MockServer::start().await;
    let mut recorder = Recorder::start_with(&server, auditing(3));
    recorder.list_open(&["A", "B", "C"]);
    let mut shard = Shard::accept(&server, 1).await;
    shard.send_books().await;
    recorder.output.wait_for(3, 0).await;
    let audit = shard.client.next_command().await;
    open_gap(&mut shard, "C").await;
    answer(&mut shard, &audit, "A").await;
    recorder.output.wait_for(4, 0).await;
    let progress = recorder.output.progress.clone();
    let finished = stop(recorder, vec![shard]).await;
    let outcome = (finished.of("A"), progress.audits().total());
    assert_eq!(outcome, (vec![sent_book(), sent_book()], 0));
}

/// The primary drops before the first round and stays down for three:
/// no round passes meanwhile, so the first once it is back starts at A.
#[tokio::test]
async fn no_round_passes_while_the_primary_is_down() {
    let server = MockServer::start().await;
    let five = ["A", "B", "C", "D", "E"];
    let mut recorder = Recorder::start_with(&server, auditing(5));
    recorder.list_open(&five);
    let mut shard = Shard::accept(&server, 1).await;
    shard.client.close().await;
    let (client, subscribe) = accept_unnamed(&server).await;
    tokio::time::sleep(ROUNDS * 3).await;
    let mut back = name(client, subscribe, 2).await;
    back.send_books().await;
    let first = back.client.next_command().await;
    stop(recorder, vec![back]).await;
    assert_eq!(first.symbols, names(&["A", "B"]));
}

fn names(list: &[&str]) -> Vec<String> {
    list.iter().map(|&name| name.to_owned()).collect()
}
