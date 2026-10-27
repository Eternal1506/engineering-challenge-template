# Notes

## Live feed findings (2026-10-26)

Measured with `engineering-challenge probe` (release build, WSL2 Ubuntu on the laptop) against
`wss://external-api-ws.kalshi.com/trade-api/ws/v2`. The probe subscribes the first N open markets from REST
(`status=open&mve_filter=exclude`) in one `subscribe` command, and listens to `market_lifecycle_v2` on a second socket.
It decodes every frame with `KalshiCodec` and applies snapshots and deltas to a local `Book` per market, the way the
recorder will. Partway through the run it sends the following commands:

- at 30 s, a second `subscribe` for one unopened market;
- at 30 s, a `get_snapshot` to an unknown sid;
- `get_snapshot` rounds for the 10 most active markets;
- at 90 s, `delete_markets` for the unopened market, whose `ok` lists the whole subscription.

| Run | Tickers | Length | Start (UTC) | Notes |
|-----|---------|--------|-------------|-------|
| A | 2,000 | 300 s | 15:31 | Fixtures captured. Feed lag unusable (clock stepped mid-run, see below) |
| B | 20,000 | 120 s | 15:38 | |
| C | 20,000 | 120 s | 15:42 | Adds feed lag after the initial snapshots |
| D | 2,000 | 300 s | 15:44 | One `get_snapshot` round |
| E | 2,000 | 300 s | 15:52 | Four `get_snapshot` rounds, with mismatch diagnostics |

### Connection and subscription

- **Auth:** the signed handshake (`GET /trade-api/ws/v2`) was accepted on every connection, in 170–270 ms (1.0–1.9 s in
  run E).
- **Sids per subscribe:** one. A 2,000-ticker or 20,000-ticker `subscribe` returns one `subscribed` reply with one sid,
  and every snapshot and delta arrives on that sid.
- **A second `subscribe` on the same connection gets no new sid.** It merges into the existing `orderbook_delta` sid.
  The reply is an `ok` (with the command's id), not a `subscribed`, and lists the whole subscription: 2,001 tickers. So
  in batched subscribes only the first batch gets `subscribed`, and the later ones get `ok`.
- **Initial snapshots:** one per ticker, every time (2,000 of 2,000; 20,000 of 20,000). The last one arrived after:
  - 2,000 tickers: 0.24–0.31 s (2.1 s in run E);
  - 20,000 tickers: 2.85 s and 3.24 s.
- **Unopened market:** subscribing one is accepted without an error. It gets an `orderbook_snapshot` with no
  `yes_dollars_fp` and no `no_dollars_fp`, and no deltas after that.
- **Nonexistent ticker:** accepted silently, with no error. Measured with `live_bogus_ticker_probe`
  (`tests/kalshi_live.rs`):
  - one `subscribe` for 10 open tickers plus `KXBOGUS-NOPE` got a normal `subscribed`;
  - a later `add_markets` of `KXBOGUS-NOPE2` got a normal `ok`, whose ticker list (12) includes both bogus tickers;
  - each bogus ticker got an `orderbook_snapshot`, like a real one.

  So the websocket cannot reveal a mistyped or unknown ticker; only the REST catalog can.
- **Unknown sid:** `update_subscription` naming an unknown sid gets
  `{"type":"error","id":4,"msg":{"code":7,"msg":"Unknown subscription ID"}}`. The error has no sid and no seq.
- **Kalshi drops markets from the subscription without notice.** The `ok` replies listed 4–5 fewer of the original
  tickers 30–90 s in, in runs C and D, with no message for the removal. These are presumably markets that closed or were
  determined.
- **Error 25 (buffer overflow):** none in any run, including both 20,000-ticker runs.
- **Pings:** 2–23 server pings per run, across both sockets. Tungstenite answered them automatically, and no connection
  dropped.

### Ordering fields

- **`clock` is absent.** Not one snapshot or delta carried it:
  - 0 of 2,014 / 20,017 / 20,016 / 2,028 / 2,046 snapshots;
  - 0 of 284,337 / 1,036,684 / 1,027,584 / 154,077 / 424,662 deltas.

  The documented field is not live yet, so `seq_hint` is `None` for every Kalshi event. Resync therefore runs without
  `clock`: deltas buffered before a resync snapshot are discarded. Replicas cannot be matched delta-for-delta by clock.
- **`seq` is contiguous per sid,** with 0 gaps in every run:
  - orderbook sids: 286,352, 1,056,703, 1,047,602, 156,107 and 426,710 messages;
  - the lifecycle sid in each run as well.

  `ok` replies, `get_snapshot` replies and lifecycle messages (including `event_lifecycle`) take slots in their sid's
  `seq` sequence. So every `Decoded` message carries its stream position, `Ignored` ones included, and the gap check
  counts them all. `subscribed` and `error` frames carry no `seq`.
- **`sending_ts_ms`** is on every frame of every type.
- **`ts_ms`** is on every delta. The exchange's own delay from the change (`ts_ms`) to sending (`sending_ts_ms`) is
  p50 5–6 ms, p99 9–11 ms.

### Book consistency and side mapping

- **Negative quantities after applying deltas: 0** in every run (about 2.9 million deltas in total).
- **Crossed books: 0.** No snapshot and no locally maintained book ever had best bid ≥ best ask.
- **`use_yes_price: false` gives raw NO-leg prices.** Two observations show it:
  - a captured snapshot has YES 0.87 and NO 0.01, which maps to bid 0.87 / ask 0.99;
  - the zero-crossed-books result above.

  If NO levels arrived in YES-leg prices, the 1 − q mapping would cross books.
- **Deltas before their market's first snapshot: 0.**

### `get_snapshot`

- Replies arrive as ordinary `orderbook_snapshot` frames on the same sid. Each carries the command's `id` and takes the
  next `seq`. There is **no `ok`** for `get_snapshot`.
- Reply time for 10 markets:
  - 21–34 ms in runs A–D;
  - 0.5–0.9 s in run E, when the network was slower (feed lag p50 54 ms, handshakes over 1 s).
- **The subscription is unchanged.** The `delete_markets` `ok` sent 30 s after the first `get_snapshot` listed the
  original set exactly:
  - run B: 20,000 tickers, 0 extra, 0 missing;
  - run E: 2,000 tickers, 0 extra, 0 missing;
  - runs C and D: the only missing tickers were the ones Kalshi had already dropped.
- **Replies against the local book:** 79 of 80 matched level for level. Run A: 10/10. Run B: 10/10. Run C: 10/10.
  Run D: 9/10. Run E: 40/40.
  - The mismatch in run D (`KXTEMPCHIHS-26OCT2612-T60.99`) had 33 local levels against 34 in the reply.
  - Run E added diagnostics: the differing levels, the following deltas, and whether the pre-reply book catches up with
    the reply. Run E had no mismatch to diagnose, so the cause is still open.
  - With no `clock`, a reply that is a few deltas ahead of or behind the stream can't be detected. Treat a resync
    snapshot as authoritative.

### Rates and timing

| | 2,000 tickers (runs A / D / E) | 20,000 tickers (runs B / C) |
|---|---|---|
| Messages/s | 954 / 520 / 1,422 | 8,806 / 8,730 |
| Deltas/s | 948 / 514 / 1,416 | 8,639 / 8,563 |
| Bytes/s | 284 k / 156 k / 421 k | 2.67 M / 2.65 M |
| Peak one-second bytes (initial snapshots) | 1.15 M / 1.07 M / 1.04 M | 6.0 M / 7.4 M |
| Delta decode p50 / p99 | 5.6 / 32.7, 5.6 / 30.6, 5.7 / 22.3 µs | 4.2 / 25.4, 2.4 / 13.5 µs |
| Snapshot decode p50 / p99 | 10.2 / 82.2, 4.4 / 36.0, 5.7 / 65.3 µs | 12.7 / 101.6, 9.3 / 73.7 µs |

- **Rates by subscription size:**
  - 2,000 tickers: the rate depends on which markets are first in the REST order and on the hour.
  - 20,000 tickers: 1 connection carried about 8.7k msgs/s and 2.65 MB/s without error 25.
- **Message sizes:** a delta frame averages 295 bytes, and a snapshot 450–690 bytes.
- **The full-list `ok` reply is large:** 67 KB for 2,000 tickers (0.3–0.7 ms to decode) and 714 KB for 20,000
  (5–6 ms).
- **Feed lag** (`received_at − sending_ts_ms`, after the initial snapshots):

  | Run | p50 | p90 | p99 | max |
  |-----|-----|-----|-----|-----|
  | D, 2,000 tickers | 34 ms | 42 ms | 180 ms | 680 ms |
  | E, 2,000 tickers | 54 ms | 67 ms | 699 ms | 2.1 s |
  | C, 20,000 tickers | 30 ms | 134 ms | 1,052 ms | 1.8 s |

  - Run B's whole-run median is 21 ms.
  - These numbers include the probe's own per-frame work, which runs on a single task. It parses every frame twice:
    once into a `serde_json::Value` and once with the codec.
  - One connection can carry 20,000 markets (no error 25, initial snapshots within about 3 s). But its p99 lag tail is
    long, about 1 s, which supports the planned 2,000 markets per connection.
- **Clock sync:** run A's feed lag is unusable.
  - The Windows host clock was 7.36 s slow (measured against `time.google.com` and `time.cloudflare.com`), because
    Windows time sync had never run.
  - WSL follows the host clock, and `systemd-timesyncd` does not start inside WSL (its unit has
    `ConditionVirtualization=!container`).
  - The fix: enable `w32time` on the host and force a resync. The host has been within 10 ms of NTP since about
    15:34:30 UTC, which was in the middle of run A.
  - Runs B–E started after the fix. Before recording, verify clock sync.

### Lifecycle channel

- **`market_lifecycle_v2` `event_type` values seen across runs:** `created`, `deactivated`, `close_date_updated`,
  `determined`, `settled` and `metadata_updated`.
  - `metadata_updated` decodes as `Ignored`.
  - `activated` and `price_level_structure_updated` did not occur.
- **Fields carried:**
  - `deactivated` carries `is_deactivated: true`. Per the AsyncAPI document, the flag marks both pausing and
    resuming, so `deactivated` with `is_deactivated: false` decodes as `Activated`;
  - `close_date_updated` carries `close_ts`;
  - `created` carries `open_ts`, `close_ts`, `additional_metadata`, `price_level_structure` and `price_ranges`.
- **Other message types:** the channel also carries `event_lifecycle` messages, which decode as `Ignored`. No
  `event_fee_update` appeared.
- **Volume:** 50–400 lifecycle messages per run. The lifecycle sid's `seq` was contiguous.

### Forward compatibility and errors

- **Unknown message types:** none.
- **Unknown fields:** none, beyond the fields the AsyncAPI document lists.
- **Decode failures:** 0 in every run.
- **Error frames:** only the deliberate unknown-sid error (code 7), which decodes to
  `FeedErrorKind::UnknownSubscription`.

### Fixtures

`tests/fixtures/kalshi/` holds one raw frame of each kind from run A, pretty-printed. They are public market data, with
no credentials or headers. The files are:

- `snapshot_both_sides`, `snapshot_one_side_absent`, `snapshot_empty` and `snapshot_reply_to_get_snapshot`;
- `delta_yes` and `delta_no`;
- `subscribed`, `ok` (2,001 tickers) and `error_code_7`;
- `event_lifecycle`;
- `lifecycle_created`, `lifecycle_deactivated`, `lifecycle_close_date_updated`, `lifecycle_determined` and
  `lifecycle_settled`.

## Replica capacity (2026-10-26)

Two 6-minute live runs of the full recorder on the laptop's WSL, one after the other: one connection per shard (R=1),
then a primary and a standby per shard (R=2). Both recorded about 152,500 markets on 77 shards of 2,000. Received bytes
are the VM's `IpExt InOctets` (TLS and TCP included), sampled every 5 s; CPU is the process's share of one core.

- **The link carries about 9 MB/s at most.** Received bytes stay flat when R doubles the subscriptions, so R=2 gives
  each socket about half the bandwidth, and the primaries forward about half the events.
- **CPU is not the limit:** about 82% of one core in both runs, on a 10-core machine.

| | R=1 | R=2 |
|---|---|---|
| Received, steady state | 8.1 MB/s average, 9.3 MB/s peak | 8.8 MB/s average, 10.2 MB/s peak |
| Events forwarded per second | about 25,000 | about 13,000 |
| Book connections | 77 | 154 |
| Disconnects | 9 (about 90 an hour) | 46 (about 460 an hour) |
| Disconnect reasons | 6 overflows, 2 resets, 1 "delivery exceeded 15 seconds" | 37 resets, 5 normal closes, 4 "delivery exceeded 15 seconds" |
| Failovers | 0 | 27 (17 on a disconnect, 10 to a standby that connected first) |
| Peak writer channel depth | 2,524 | 1,680 |
| `verify` | clean | clean |

- **Overflows** came from hot shards whose median feed lag had reached 22–45 s.
- **A large `get_snapshot` to a promoted standby is followed by a reset of that socket.**
  - In an earlier 10-minute R=2 run, each failover asked the standby for all 2,000 markets in one command. 26 of 81
    promoted sockets were reset by the peer within 5 s of that request, 0.5–2.3 s after it. A socket's chance of an
    unrelated reset in any 5 s window was about 0.3% in that run.
  - Asking in batches of 200 markets every 100 ms does not stop it: 11 of 17 promoted sockets were still reset,
    0.5–1.1 s after promotion, while the batches were going out, 3 of them on a write.
  - The standby of a hot shard is itself tens of seconds behind on this link, so a burst of snapshot replies queued
    behind its backlog plausibly pushes the exchange to drop it.
- **A 2,000-market `get_snapshot` is rarely answered within 10 s on this link.** In that run, 88 such requests had
  markets still unanswered after 10 s, up to all 2,000.
- **A shard split does not help overflows caused by our own writer backpressure.** When the writer falls behind, every
  shard waits for it and stops reading its sockets, so the exchange's buffers fill on every connection alike, however
  the markets are spread.
