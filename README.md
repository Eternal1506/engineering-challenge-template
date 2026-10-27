# Kalshi order book recorder

A Rust service for the [Voluntas engineering challenge](https://voluntas.io/engineering-challenge).

## What it does

It records the order book of every live regular (non-MVE) Kalshi market. It lists the open and paused markets
over REST, spreads them over websocket shards of at most 2,000 markets, and keeps a primary and a standby connection
per shard. It follows new, closed and settled markets through the lifecycle channel, a sweep and a periodic
reconcile. A single writer thread keeps every book and writes `Snapshot` and `PriceLevelUpdate` messages through the
template's SBE helpers, with a full snapshot of each market every minute. Sequence gaps, failovers and resubscribes
are resolved on a fresh snapshot from the socket that will carry the market next, so the recorded book never misses
a change or applies one twice. `verify` checks a recording's invariants. Live findings and measurements are in [NOTES.md](NOTES.md), and
latency findings in [LATENCY.md](LATENCY.md).

## Quick start

### Prerequisites

- Linux. The template's reader uses Unix APIs, and the recorder handles Unix signals. Tested on WSL2 Ubuntu.
- Rust stable (the crate uses edition 2024).
- A Kalshi API key (Ed25519).
- An NTP-synced clock; see [Clock sync](#clock-sync).

### Credentials

Put the key id in `secrets/key_id.txt` and the private key in `secrets/private_key.pem`. Both are gitignored.
Alternatively, set `KALSHI_API_KEY_ID` (the key id itself) and `KALSHI_PRIVATE_KEY_PATH` (a path to the PEM file),
which take precedence over the files. `--secrets-dir` points at another directory.

### Build and run

```sh
cargo build --release
BIN=${CARGO_TARGET_DIR:-target}/release/engineering-challenge

# Record to stdout (the default); logs go to stderr.
$BIN record > kalshi.sbe

# Record to a file; {start} becomes the UTC start time, e.g. 20261026T143005Z.
mkdir -p ~/recordings
$BIN record --output ~/recordings/kalshi-{start}.sbe

# Record with restarts after a crash, into ~/recordings/kalshi-{start}.sbe.
./run.sh

# Check recordings; each path is one run.
$BIN verify ~/recordings/kalshi-20261026T143005Z.sbe ~/recordings/kalshi-20261026T150210Z.sbe
```

`record` is also what runs when no subcommand is given. SIGINT or SIGTERM closes every websocket, drains the writer
and closes the file; a second signal exits at once with status 130. A path with `{start}` must not exist yet, and its
directory must exist.

`run.sh` runs `target/release/engineering-challenge` next to the script (set `RECORDER_BIN` if your build output
is elsewhere, e.g. with `CARGO_TARGET_DIR`) and records into `~/recordings/kalshi-{start}.sbe`, creating the
directory. It restarts the recorder after any other exit, a crash or abort included, with a delay that doubles
from 1 s to 30 s and starts again at 1 s after a run of over 5 minutes. It stops when the recorder exits with
status 0, 2, 126, 127 or 130, or when `run.sh` itself gets SIGINT, SIGTERM, SIGHUP or SIGQUIT, which it forwards
to the recorder. Extra arguments go to `record`.

`probe` is a live check of the feed format (one connection of N markets plus the lifecycle channel); its results are
in NOTES.md.

### Main flags of `record`

| Flag | Default | Meaning |
|---|---|---|
| `--output PATH` | `/dev/stdout` | Where to write SBE output; `{start}` becomes the UTC start time. |
| `--shard-size N` | 2000 | Most markets per websocket connection. |
| `--replicas N` | 2 | Connections per shard: one primary, the rest standbys. |
| `--max-connections N` | 190 | Most websockets at once, lifecycle included. A shard that would exceed it gets one connection. |
| `--standby-delay SECS` | 30 | Time from a primary first connecting to its standbys starting. |
| `--failover-warmup SECS` | 5 | How long a standby must be connected before it may take over. |
| `--flush-interval SECS` | off | Flush the current 64 MiB chunk this often, padding it to full size. |
| `--reconcile-interval SECS` | 600 | Time between catalog reconciles. |
| `--audit-markets N` | 5 | Markets each shard audits per minute; 0 turns audits off. |
| `--secrets-dir PATH` | `secrets` | Directory holding `key_id.txt` and `private_key.pem`. |

`verify` takes `--max-snapshot-gap SECS` (default 65) and `--max-price N` (default 1000000). `RUST_LOG` sets the log
level (default `info`).

### Clock sync

`received_at` is the local clock, so the host clock must be NTP-synced before recording. WSL2 takes its time from the
Windows host, and `systemd-timesyncd` does not start inside WSL. We found the Windows clock 7.36 s slow because
Windows time sync had never run; enabling `w32time` on the host and forcing a resync brought it within 10 ms of NTP
(NOTES.md, "Rates and timing"). The stats line logs `feed_lag_p50_ms` and `feed_lag_p99_ms` every 30 s, so a drifting
clock shows as a lag far from the usual tens of milliseconds, or as a negative one when the local clock is behind.

## Output format

- **Messages.** SBE `Snapshot` and `PriceLevelUpdate` from the template's schema, written by its `AlignedSbeWriter`
  in 64 MiB chunks. Without `--flush-interval`, data reaches the output a chunk at a time and on shutdown.
- **Symbol.** The Kalshi market ticker.
- **Sides.** Kalshi publishes bids on both legs. A YES bid at p is `Side::Buy` at p. A NO bid at q is `Side::Sell`
  at 1 − q. Subscriptions send `use_yes_price: false`, so NO levels arrive as NO prices.
- **Fixed point.** Prices and quantities are parsed exactly from Kalshi's decimal strings and scaled by 1,000,000.
- **Quantities are absolute.** A `PriceLevelUpdate` carries the level's quantity after the change; a level that
  empties is written with quantity 0.
- **Timestamps.**
  - `received_at` (Snapshot) and `backtest_timestamp` (PriceLevelUpdate) are the local receive time, stamped before
    the frame is decoded.
  - `exchange_timestamp` is Kalshi's time: `ts_ms` on a delta and `sending_ts_ms` on a feed snapshot. It is the
    latest exchange time the book has seen, so it never goes backwards; with none, it is the receive time.
- **Snapshots.**
  - A market's first message is always a snapshot: the one Kalshi sends on subscribe.
  - The writer snapshots every book once a minute, staggered over 240 slots of 250 ms.
  - Feed snapshots (subscribe, resync, failover, resubscribe, or an audit that found a difference) are written as
    they arrive.
  - Snapshots the writer makes itself are stamped in stream time, not wall time: 1 s behind the writer's clock, which
    is the latest receive time it has processed. A connection lagging by under 1 s therefore keeps its true times.
- **Monotone per symbol.** Each symbol's receive times never go backwards; a late event is raised to the symbol's
  previous time.
- **Reading it.** Use the template's `AlignedSbeReader` (`src/util/aligned_sbe_reader.rs`), which reads 64 MiB
  chunks; `src/core/verify.rs` is a worked example. `verify` checks that every symbol has a snapshot before its
  first update, snapshots at most 65 s apart, receive times that never go backwards, no negative quantities, prices
  in 1..=1,000,000, sorted snapshot sides with no zero levels, and whole chunks.

## Architecture

```
  Kalshi REST /markets              Kalshi websocket                Kalshi websocket
  (open + paused, mve excluded)     market_lifecycle_v2             orderbook_delta (per shard)
          |                                |                               |
          v                                v                               |
  +------------------------------ Coordinator ----------------------+      |
  | market states: lifecycle events, 10 s sweep, 10 min reconcile   |      |
  | subscription changes batched for 1 s, then one command a shard  |      |
  | registry: <= 2,000 markets a shard; connection budget of 190    |      |
  | overflow log -> shard split; stats line every 30 s              |      |
  +---------+------------------------------+------------------------+      |
            | add / remove / snapshot       |                              |
            v                               v                              |
  +---- Shard supervisor ----+    +---- Shard supervisor ----+   ...       |
  |  primary      standby    |<---|  primary      standby    |<------------+
  |  (Connection) (Connection)    |                          |
  |  failover, health checks,|    |                          |
  |  resync gating, stale    |    |                          |
  |  watch, book audits      |    |                          |
  +------------+-------------+    +------------+-------------+
               | primary's events only, in order, never dropped
               v
  +------------- Writer thread ---------------+      resnapshot requests
  | every Book, AlignedSbeWriter (64 MiB)     |----> coordinator -> shard
  | minute snapshots, audit comparisons       |
  +---------------------+---------------------+
                        v
               stdout or --output file

  Watchdog thread: coordinator, supervisor and writer heartbeats; aborts if any is still for 30 s
```

- **Exchange-agnostic core.** `src/core/` holds the coordinator, shards, connection task, books, writer, verify,
  watchdog and stats, and does not import `exchange::kalshi`. It talks to an exchange through three traits in
  `src/exchange/mod.rs`: `MarketCatalog` (list markets by status, page by page), `FeedEndpoint` (websocket URL and
  fresh handshake headers) and `FeedCodec` (frames to `Decoded` messages, `Command`s to frames). The Kalshi adapter
  in `src/exchange/kalshi/` implements them: Ed25519 request signing, the REST catalog (`mve_filter=exclude`, cursor
  pagination, paced requests with backoff on 429), the codec, and the lifecycle mapping. `main.rs` is where Kalshi is
  wired in. Adding an exchange means implementing the three traits and wiring them in `main.rs`.
- **Coordinator.** Lists open and paused markets at startup, streaming pages straight into shard assignment. A
  lifecycle connection reports created, activated, deactivated, close-time, determined and settled events. A sweep
  every 10 s starts markets whose open time has passed and stops those whose close time has, since neither emits a
  lifecycle event. A reconcile every 10 minutes (and at once when the lifecycle connection may have lost events)
  adds missing markets and removes one only after two walks in a row miss it. Changes collect for 1 s and go to each
  shard as one command. The registry puts a new market on the shard with the most free slots and opens a new shard
  when all are full; live markets are not moved except by a split. The connection budget keeps the total at
  `--max-connections`, below Kalshi's limit of about 200: a shard that would exceed it gets one connection instead
  of two.
- **Shards.** Each shard is a supervisor task with its replicas. Every replica subscribes the same markets; only the
  primary's events reach the writer, and the standby's feed the health checks. Each replica is a `Connection` task
  that signs a fresh handshake, offers permessage-deflate, sets TCP_NODELAY, subscribes in batches of 500 every
  100 ms, sends snapshot requests in batches of 200 every 100 ms, checks `seq` continuity, and reconnects with
  jittered backoff from 250 ms to 10 s on a close, an error, an overflow, or 30 s of silence.
- **Writer.** One OS thread owns every `Book` and the `AlignedSbeWriter`. It takes events from a bounded channel of
  262,144 and never drops one; shards wait for room instead. A change that finds no book, or drives a level
  negative, asks the shard for a fresh snapshot.
- **Verify.** `verify` reads recordings with `AlignedSbeReader` and reports violations by kind with examples.
- **Watchdog.** Release builds set `panic = "abort"`, so no task dies unnoticed. A dedicated OS thread reads the
  heartbeats of the coordinator and every shard supervisor, and the writer's progress, every 10 s, and aborts the
  process if any has been still for 30 s; `run.sh` then restarts it.

## How each requirement and extension is met

| Requirement | Mechanism | Evidence |
|---|---|---|
| All live non-MVE markets | REST listing of open and paused markets with `mve_filter=exclude`; shards of ≤ 2,000 | `tests/recorder/sharding.rs`; live runs recorded about 152,500 markets on 77 shards (NOTES.md, "Replica capacity") |
| Discover new markets, drop ended ones | Lifecycle channel, 10 s sweep of open and close times, 10 min reconcile with a two-walk removal rule | `tests/recorder/lifecycle.rs`, coordinator unit tests; live, the sweep removed 3,036 closed markets at 05:00 UTC (NOTES.md, "Websocket compression") |
| Deltas plus a snapshot every minute, through the template's helpers | Writer thread with `AlignedSbeWriter`, staggered minute snapshots | `tests/writer.rs`, `tests/writer_timing.rs`, `tests/sbe_roundtrip.rs`; `verify` clean on every live recorder run in NOTES.md |
| SIGINT closes the websockets and the file | Cancel token: connections send close frames, shards drain, the writer flushes and closes | `tests/recorder/shutdown.rs`, `tests/recorder/stalls.rs`, `tests/ws_connection/shutdown.rs` |
| Complete and time-accurate | Receive time stamped before decoding; per-subscription `seq` gap check; resync on snapshots; book audits | `tests/ws_connection/messages.rs`, `tests/recorder/resync.rs`; live probe runs: 0 gaps, 0 negative quantities, 0 crossed books (NOTES.md, "Ordering fields", "Book consistency") |
| Multiple websockets | Shards of ≤ 2,000 markets, chosen from the measured feed lag | NOTES.md, "Rates and timing": p99 lag 180 ms at 2,000 markets (699 ms in run E, on a slower network) against 1,052 ms at 20,000 |
| **Ext. 1** Disconnect | A standby connected for 5 s is promoted at once; the old primary reconnects as standby | `tests/recorder/failover.rs`; live R=2 run: 27 failovers, `verify` clean (NOTES.md, "Replica capacity") |
| **Ext. 1** Silent, lagging, stuck primary | Each second, the primary is compared with a warm standby: silent (no level changes over the last 10 s while the standby delivered ≥ 20), lagging (median lag trails by > 500 ms for 3 ticks), or stuck in a resync with a live market; a 60 s cooldown follows | `tests/recorder/health.rs`, `tests/recorder/takeover.rs` |
| **Ext. 1** Fresh snapshot on switch | After any switch, each market's changes are held until a snapshot of it arrives on the new primary; that snapshot is written first | `tests/recorder/failover.rs`, `tests/recorder/resync.rs` |
| **Ext. 1** Process crash | `panic = "abort"`, the watchdog, `run.sh` restarts; each run writes a new `{start}` file that begins with snapshots | `tests/run_sh/check.sh` (10 scenarios, 38 checks), `tests/recorder/heartbeats.rs`, watchdog unit tests |
| **Ext. 2** Per-symbol rates, stale resubscribe | Each market's change rate over 1 min and 1 h; a market with ≥ 2 changes a minute that goes silent for max(3 × its mean interval, 2 min) is removed and re-added on the primary, at most 20 per shard a minute | `tests/recorder/staleness.rs`, `src/core/shard/stale.rs` and `activity.rs` unit tests |
| **Ext. 2** Dead-socket escalation | When ≥ 30% of watched markets (and at least 5) are silent at once, the socket is taken to be dead: a standby takes over or the primary reconnects, with a doubling silence threshold up to 30 min | `tests/recorder/staleness.rs` |
| **Ext. 2** Overflow split | Two overflow incidents of a shard's primary within 10 min split the shard in two, within the connection budget | `tests/recorder/splits.rs` |
| **Ext. 2** Book audit | Each minute every shard asks its primary for snapshots of 5 markets in rotation; the writer compares each with its book and writes it only if it differs | `tests/recorder/audits.rs`, `tests/writer.rs` |
| **Ext. 2** Stats line | Every 30 s: shards, markets, sockets up, events/s, feed lag p50/p99, audits and match rate, writer depth; recovery and market-change counters | `src/core/stats.rs` unit tests |
| **Ext. 2** Periodic reconnect of each socket | Not built | See Future work |
| **Ext. 3** Exchange-agnostic core | `MarketCatalog`, `FeedEndpoint`, `FeedCodec`; Kalshi lives in `src/exchange/kalshi/` | The core's integration tests (`tests/recorder.rs`, `tests/ws_connection.rs`) run against the mock exchange in `tests/mock_exchange/`, which knows no real exchange |
| **Ext. 4** Latency | Shard size from measured lag, permessage-deflate, TCP_NODELAY, paced snapshot requests | [LATENCY.md](LATENCY.md) |

## Correctness argument

Across a resync or a failover, the recorded book never misses a change or applies one twice, without relying on any
clock:

- Kalshi sends no ordering value that spans sockets. The documented `clock` field was absent from every snapshot and
  delta measured, and `seq` is per subscription, contiguous on each socket (NOTES.md, "Ordering fields"). So two
  sockets cannot be lined up message for message.
- A socket delivers its messages in order. So a snapshot that arrives on socket S already includes every change S
  delivered before it.
- After a gap, a failover or a resubscribe, each affected market is held: its changes on the socket that will carry
  it are dropped until a snapshot of it arrives on that socket. The dropped changes are in that snapshot.
- The writer replaces the book with the snapshot and writes it; changes after it are applied in order. Updates carry
  absolute quantities computed from that book, so nothing the old socket delivered can be applied on top of it.
- The old primary's events stop reaching the writer at the switch, and the standby's never did before it.
- What is not recovered is the sequence of individual changes lost in a gap: the snapshot restores the book's state,
  not each missed update.
- `get_snapshot` replies matched the locally maintained book level for level in 79 of 80 live checks (NOTES.md,
  "`get_snapshot`"); the cause of the one mismatch is open, which is why the book audit runs continuously and writes
  any differing reply as a fresh snapshot.

## Testing

Counted from the test attributes in the source; the live tests are `#[ignore]`d and run on demand.

| Suite | Tests |
|---|---|
| Library unit tests (`src/`) | 410 |
| Binary unit tests (`main.rs`, logging, probe) | 6 |
| `tests/recorder.rs`: the whole pipeline against the mock exchange | 108 |
| `tests/ws_connection.rs`: the connection task, compression included | 43 |
| `tests/verify.rs` | 25 |
| `tests/writer.rs` | 20 |
| `tests/writer_timing.rs` | 7 |
| `tests/sbe_roundtrip.rs` | 1 |
| `tests/writer_stdout_pipe.rs` (no harness; output through a pipe) | 1 |
| `tests/kalshi_live.rs` (live, ignored) | 5 |
| `tests/run_sh/check.sh` (bash, against a fake recorder) | 10 scenarios, 38 checks |

- **Mock exchange.** `tests/mock_exchange/` is a localhost websocket server that tests script frame by frame, with
  its own small JSON codec, an endpoint, an in-memory catalog, and a permessage-deflate server. Core tests run over
  real sockets against it, so they depend only on the three exchange traits.
- **Reviews.** Each step had an independent review, and most reviews used mutation testing (breaking the code under
  test and checking that a test fails) to check that the tests catch what they claim to.
- **Live runs.** All in [NOTES.md](NOTES.md): five probe runs of 2,000 and 20,000 markets that established the feed's
  behaviour; a one-hour soak of the base recorder over 155,662 markets (73 million updates, 50 disconnects all
  recovered); two 6-minute full-recorder runs at R=1 and R=2 over about 152,500 markets; four 3-minute runs with
  compression on and off; a 2-minute R=2 run on the final yawc version; and a final 15-minute run of the submitted
  build at its defaults over 147,061 markets (8 million updates, 0 disconnects, 100% of 5,180 book audits matched).
  `verify` was clean on each of these recorder runs.

Run the suites with `cargo test` on Linux (`tests/writer_timing.rs` asserts wall-clock spacing and can fail on a
heavily loaded machine; rerun it alone); the live tests with
`cargo test --test kalshi_live -- --ignored --nocapture` (credentials from `KALSHI_SECRETS_DIR`); the `run.sh`
checks with `bash tests/run_sh/check.sh`.

## Limitations and future work

- **The laptop's link caps out at about 9 MB/s.** Uncompressed, R=2 halves each socket's share, and disconnects rose
  from about 90 to about 460 an hour (NOTES.md, "Replica capacity"). With permessage-deflate the feed takes 5.8× to
  6.2× fewer bytes. The 3-minute runs, compressed or not, had no disconnects, but on a quieter feed; a longer R=2 run
  at a busy hour has not been done (NOTES.md, "Websocket compression").
- **Large snapshot requests to a promoted standby on a saturated link are often followed by a reset,** even in batches
  of 200 every 100 ms (NOTES.md, "Replica capacity"). The resync gating keeps the data correct, at the cost of another
  reconnect.
- **Periodic socket recycling is not built.** Each socket stays up until it fails. The design for it: recycle each
  connection at 6 h ± 20%, the standby first, then promote it once healthy (which writes fresh snapshots), then
  recycle the old primary, never two replicas of one shard within 5 minutes.
- **The watchdog may not catch a writer stuck on hung storage.** It tells a slow reader from a dead one by the writer
  thread's wakeups in `/proc`, which works for pipes and sockets. A writer blocked on a regular file whose storage
  hangs (a dead NFS server, a hung device) can keep being woken by the kernel's dirty-page throttling, so it may never
  look stalled; a second signal stops the process (`src/core/writer/progress.rs`).
- **`verify` checks each run as its own file.** Pass a crashed run and its restart as separate paths. Runs
  concatenated into one file are checked as one stream, so downtime longer than the snapshot gap between them is reported as a gap, which it
  is.
- **Not yet tried:** running near Kalshi's servers, simd-json, per-shard lag histograms and interned symbols; see
  LATENCY.md.
