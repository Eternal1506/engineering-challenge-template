//! Markets gone silent on the primary: a busy market's resubscribe and its
//! limit, and a socket taken to be dead when too many go silent at once.

use super::failover::{
    Pair, accept_pair, accept_unnamed, answer, bid_at, name, paired, raise, stop,
};
use super::mock_exchange::SentCommand;
use super::*;

/// How long a busy market may be silent under [`watching`].
const SILENCE: Duration = Duration::from_millis(600);

/// How many changes make a market busy under [`watching`].
const BURST: i64 = 20;

/// Silence checks every 50 ms, under which a burst of changes makes a
/// market busy at once and [`SILENCE`] is the longest it may then be
/// silent, with `tune` applied, and no audits.
fn watching(base: CoordinatorConfig, tune: impl FnOnce(&mut StaleChecks)) -> CoordinatorConfig {
    let mut staleness = StaleChecks {
        interval: Duration::from_millis(50),
        short_window: Duration::from_secs(1),
        long_window: Duration::from_secs(10),
        silence_intervals: 1,
        min_silence: SILENCE,
        ..StaleChecks::default()
    };
    tune(&mut staleness);
    CoordinatorConfig {
        monitor: MonitorConfig {
            staleness,
            ..quiet_monitor()
        },
        ..base
    }
}

/// Raises the 0.40 bid of each of `symbols` by one, [`BURST`] times, a
/// round over every symbol at a time, with no exchange time.
async fn burst(shard: &mut Shard, symbols: &[String]) {
    burst_made_at(shard, symbols, None).await;
}

/// [`burst`], every change made at the exchange at `ts`, in Unix
/// milliseconds, if given.
async fn burst_made_at(shard: &mut Shard, symbols: &[String], ts: Option<i64>) {
    for _ in 0..BURST {
        for symbol in symbols {
            raise_made_at(shard, symbol, ts).await;
        }
    }
}

/// Raises the 0.40 bid of `symbol` by one, made at the exchange at `ts`,
/// in Unix milliseconds, if given.
async fn raise_made_at(shard: &mut Shard, symbol: &str, ts: Option<i64>) {
    let seq = shard.next_seq();
    let change = frames::level(shard.sub, seq, symbol, ("buy", 400_000, 1));
    let frame = ts.map_or_else(|| change.clone(), |ts| frames::stamped(change.clone(), ts));
    shard.client.send(frame).await;
}

/// `count` market names.
fn markets(count: usize) -> Vec<String> {
    (0..count).map(|n| format!("M{n:02}")).collect()
}

/// A recorder over `symbols` on one shard with `config`, its books
/// recorded and each market made busy, then left silent.
async fn silent_after_a_burst(
    server: &MockServer,
    config: CoordinatorConfig,
    symbols: &[String],
) -> (Recorder, Shard) {
    let mut recorder = Recorder::start_with(server, config);
    let names: Vec<&str> = symbols.iter().map(String::as_str).collect();
    recorder.list_open(&names);
    let mut shard = Shard::accept(server, 1).await;
    shard.send_books().await;
    burst(&mut shard, symbols).await;
    let (books, changes) = (symbols.len() as u64, BURST as u64 * symbols.len() as u64);
    recorder.output.wait_for(books, changes).await;
    (recorder, shard)
}

/// A command's name and symbols.
fn named(command: &SentCommand) -> (&str, Vec<&str>) {
    let symbols = command.symbols.iter().map(String::as_str).collect();
    (command.cmd.as_str(), symbols)
}

/// A busy market that goes silent is removed and added again on the
/// primary. A change that comes before the add's snapshot is dropped; the
/// snapshot replaces the book, and the changes after it apply to it.
#[tokio::test]
async fn a_busy_market_gone_silent_is_resubscribed_and_held_until_its_snapshot() {
    let server = MockServer::start().await;
    let config = watching(config(2), |_| {});
    let mut recorder = Recorder::start_with(&server, config);
    recorder.list_open(&["A", "B"]);
    let mut shard = Shard::accept(&server, 1).await;
    shard.send_books().await;
    burst(&mut shard, &["A".to_owned()]).await;
    let removal = shard.client.next_command().await;
    let addition = shard.client.next_command().await;
    shard.send_level("A", ("buy", 400_000, 1)).await;
    let seq = shard.next_seq();
    let fresh = frames::book(shard.sub, seq, "A", (&[(400_000, 50)], ASKS));
    shard.client.send(fresh).await;
    raise(&mut shard, "A").await;
    recorder.output.wait_for(3, BURST as u64 + 1).await;
    let counters = recorder.counters.clone();
    let finished = stop(recorder, vec![shard]).await;
    let commands = (named(&removal), named(&addition));
    assert_eq!(commands, (("remove", vec!["A"]), ("add", vec!["A"])));
    let raised = (1..=BURST).map(|step| bid_at(100 + step));
    let fresh = Shape::Snapshot(vec![(400_000, 50)], ASKS.to_vec());
    let mut expected: Vec<Shape> = std::iter::once(sent_book()).chain(raised).collect();
    expected.extend([fresh, bid_at(60)]);
    assert_eq!(finished.of("A"), expected);
    let counts = counters.read();
    let held = (counts.monitoring.resubscribes, counts.resync_drops);
    assert_eq!(held, (1, 1));
}

/// Twenty-five busy markets go silent, but only twenty are resubscribed
/// within the minute; the rest wait.
#[tokio::test]
async fn at_most_the_limit_of_silent_markets_is_resubscribed_a_minute() {
    let server = MockServer::start().await;
    let symbols = markets(25);
    let never_dead = |checks: &mut StaleChecks| checks.dead_socket_percent = 100;
    let config = watching(config(25), never_dead);
    let (recorder, mut shard) = silent_after_a_burst(&server, config, &symbols).await;
    let mut resubscribed = Vec::new();
    while resubscribed.len() < 20 {
        let removal = shard.client.next_command().await;
        let addition = shard.client.next_command().await;
        let commands = (removal.cmd.as_str(), addition.cmd.as_str());
        assert_eq!(commands, ("remove", "add"));
        assert_eq!(removal.symbols, addition.symbols);
        resubscribed.extend(removal.symbols);
    }
    shard.client.assert_no_command(SILENCE * 3).await;
    let counters = recorder.counters.clone();
    stop(recorder, vec![shard]).await;
    let counted = counters.read().monitoring.resubscribes;
    assert_eq!((resubscribed.len(), counted), (20, 20));
}

/// Checks that only note silence: no resubscribe, and a dead socket once
/// five markets, more than 30% of those watched, are silent at once.
fn escalating(base: CoordinatorConfig) -> CoordinatorConfig {
    watching(base, |checks| checks.resubscribe_limit = 0)
}

/// Every busy market of a lone shard goes silent at once: its socket is
/// taken to be dead, and with no standby, it reconnects and subscribes
/// every market again.
#[tokio::test]
async fn a_lone_primary_silent_on_most_busy_markets_reconnects() {
    let server = MockServer::start().await;
    let symbols = markets(6);
    let config = escalating(config(6));
    let (recorder, dropped) = silent_after_a_burst(&server, config, &symbols).await;
    let reconnected = Shard::accept(&server, 2).await;
    drop(dropped);
    let counters = recorder.counters.clone();
    let resubscribed = reconnected.symbols.clone();
    stop(recorder, vec![reconnected]).await;
    let counts = counters.read().monitoring;
    let escalated = (counts.stale_escalations, counts.resubscribes);
    assert_eq!((resubscribed, escalated), (symbols, (1, 0)));
}

/// Every busy market of a lone shard goes silent, and stays silent on the
/// new socket too: that socket is taken to be dead in turn, once twice the
/// silence has passed.
#[tokio::test]
async fn a_new_socket_as_silent_as_the_dead_one_is_taken_to_be_dead_after_a_backoff() {
    let server = MockServer::start().await;
    let symbols = markets(6);
    let config = escalating(config(6));
    let (recorder, first) = silent_after_a_burst(&server, config, &symbols).await;
    let second = Shard::accept(&server, 2).await;
    let connected = Instant::now();
    drop(first);
    let third = Shard::accept(&server, 3).await;
    let waited = connected.elapsed();
    drop(second);
    let counters = recorder.counters.clone();
    stop(recorder, vec![third]).await;
    let escalations = counters.read().monitoring.stale_escalations;
    assert_eq!(escalations, 2);
    assert!(waited >= SILENCE * 2, "taken to be dead after {waited:?}");
}

/// The time now, in Unix milliseconds.
fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// A recorder over `symbols` on one paired shard that only escalates,
/// both replicas' books sent, and every market made busy on the primary,
/// made at the exchange at the returned time, in Unix milliseconds, and
/// all recorded, then left silent there.
async fn a_pair_after_a_burst(server: &MockServer, symbols: &[String]) -> (Recorder, Pair, i64) {
    let names: Vec<&str> = symbols.iter().map(String::as_str).collect();
    let mut recorder = Recorder::start_with(server, escalating(paired(names.len())));
    recorder.list_open(&names);
    let mut pair = accept_pair(server).await;
    pair.primary.send_books().await;
    pair.standby.send_books().await;
    let made_at = now_ms();
    burst_made_at(&mut pair.primary, symbols, Some(made_at)).await;
    let (books, changes) = (symbols.len() as u64, BURST as u64 * symbols.len() as u64);
    recorder.output.wait_for(books, changes).await;
    (recorder, pair, made_at)
}

/// The primary of a pair goes silent on every busy market at once while
/// the standby keeps seeing them change: the standby takes over, asked for
/// a snapshot of every market, and the old primary reconnects.
#[tokio::test]
async fn a_primary_silent_on_markets_the_standby_sees_change_hands_over_to_it() {
    let server = MockServer::start().await;
    let symbols = markets(6);
    let names: Vec<&str> = symbols.iter().map(String::as_str).collect();
    let (recorder, mut pair, made_at) = a_pair_after_a_burst(&server, &symbols).await;
    for symbol in &names {
        raise_made_at(&mut pair.standby, symbol, Some(made_at + 1000)).await;
    }
    let request = pair.standby.client.next_command().await;
    let reconnected = Shard::accept(&server, 3).await;
    for symbol in &names {
        answer(&mut pair.standby, &request, symbol).await;
    }
    recorder.output.wait_for(12, 0).await;
    let counters = recorder.counters.clone();
    drop(pair.primary);
    stop(recorder, vec![pair.standby, reconnected]).await;
    let counts = counters.read();
    let taken_over = (counts.failovers.stale, counts.monitoring.stale_escalations);
    assert_eq!((named(&request), taken_over), (("snapshot", names), (1, 1)));
}

/// The primary of a pair goes silent on every busy market while the
/// standby, connected, delivers no change at all, so it can tell nothing:
/// silence alone counts, the socket is taken to be dead, and with no
/// standby that saw the markets change, the primary reconnects.
#[tokio::test]
async fn beside_a_standby_that_delivers_nothing_silence_alone_counts() {
    let server = MockServer::start().await;
    let symbols = markets(6);
    let (recorder, pair, _) = a_pair_after_a_burst(&server, &symbols).await;
    let reconnected = Shard::accept(&server, 3).await;
    drop(pair.primary);
    let counters = recorder.counters.clone();
    stop(recorder, vec![pair.standby, reconnected]).await;
    let counts = counters.read();
    let acted = (counts.monitoring.stale_escalations, counts.failovers.stale);
    assert_eq!(acted, (1, 0));
}

/// Both replicas of a pair get the same burst, the standby's arriving
/// last, and then every busy market goes quiet on both: a lull, not a dead
/// socket, so nothing is resubscribed and nothing takes over. The
/// standby's late copies of the primary's changes carry the same exchange
/// times, so they show nothing the primary missed.
#[tokio::test]
async fn a_lull_the_standby_sees_too_is_neither_resubscribed_nor_a_dead_socket() {
    let server = MockServer::start().await;
    let symbols = markets(6);
    let (recorder, mut pair, made_at) = a_pair_after_a_burst(&server, &symbols).await;
    burst_made_at(&mut pair.standby, &symbols, Some(made_at)).await;
    pair.primary.client.assert_no_command(SILENCE * 3).await;
    pair.standby.client.assert_no_command(Duration::ZERO).await;
    let counters = recorder.counters.clone();
    stop(recorder, vec![pair.primary, pair.standby]).await;
    let counts = counters.read();
    let acted = (counts.monitoring.stale_escalations, counts.failovers.stale);
    assert_eq!(acted, (0, 0));
}

/// A busy market that goes silent, on the lone primary, is resubscribed
/// once; staying silent after that, it is only quiet, and not resubscribed
/// again until it changes.
#[tokio::test]
async fn a_quiet_market_is_resubscribed_once_per_quiet_spell() {
    let server = MockServer::start().await;
    let config = watching(config(2), |_| {});
    let symbols = ["A".to_owned()];
    let (recorder, mut shard) = silent_after_a_burst(&server, config, &symbols).await;
    let removal = shard.client.next_command().await;
    shard.client.next_command().await;
    shard.client.assert_no_command(SILENCE * 3).await;
    let counters = recorder.counters.clone();
    stop(recorder, vec![shard]).await;
    let counted = counters.read().monitoring.resubscribes;
    assert_eq!((named(&removal), counted), (("remove", vec!["A"]), 1));
}

/// A busy market goes silent just as the primary drops, and the primary
/// stays down well past its silence: nothing is judged meanwhile, and on
/// the new socket the market's silence counts afresh.
#[tokio::test]
async fn markets_are_not_judged_while_the_primary_is_down() {
    let server = MockServer::start().await;
    let config = watching(config(2), |_| {});
    let mut recorder = Recorder::start_with(&server, config);
    recorder.list_open(&["A", "B"]);
    let mut shard = Shard::accept(&server, 1).await;
    shard.send_books().await;
    burst(&mut shard, &["A".to_owned()]).await;
    recorder.output.wait_for(2, BURST as u64).await;
    shard.client.close().await;
    let (client, subscribe) = accept_unnamed(&server).await;
    tokio::time::sleep(SILENCE * 2).await;
    let back = name(client, subscribe, 2).await;
    let counters = recorder.counters.clone();
    stop(recorder, vec![back]).await;
    assert_eq!(counters.read().monitoring.resubscribes, 0);
}

/// A market that changed once has a mean interval of ten seconds under
/// [`watching`], so a silence of a few times [`SILENCE`] is normal for it.
#[tokio::test]
async fn a_market_may_be_silent_for_its_own_mean_interval() {
    let server = MockServer::start().await;
    let config = watching(config(2), |_| {});
    let mut recorder = Recorder::start_with(&server, config);
    recorder.list_open(&["A", "B"]);
    let mut shard = Shard::accept(&server, 1).await;
    shard.send_books().await;
    raise(&mut shard, "A").await;
    shard.client.assert_no_command(SILENCE * 3).await;
    let counters = recorder.counters.clone();
    stop(recorder, vec![shard]).await;
    assert_eq!(counters.read().monitoring.resubscribes, 0);
}
