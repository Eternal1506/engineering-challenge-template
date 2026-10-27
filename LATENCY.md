# Latency findings

Extension 4. Every number here comes from a live measurement recorded in [NOTES.md](NOTES.md), and each finding names
its section. All runs were from a laptop on WSL2 Ubuntu, against `wss://external-api-ws.kalshi.com/trade-api/ws/v2`.
Anything not measured is marked **Not measured**.

Feed lag here is `received_at − sending_ts_ms`: our receive time minus Kalshi's send time, so it includes the network,
TLS, our socket read and the clock offset between the two hosts.

## 1. Markets per socket: 2,000

Source: NOTES.md, "Live feed findings", "Rates and timing".

| Run | Markets on one socket | p50 | p90 | p99 | max |
|---|---|---|---|---|---|
| D | 2,000 | 34 ms | 42 ms | 180 ms | 680 ms |
| E | 2,000 | 54 ms | 67 ms | 699 ms | 2.1 s |
| C | 20,000 | 30 ms | 134 ms | 1,052 ms | 1.8 s |

- The median barely moves with subscription size, but the tail does: one socket of 20,000 markets has a p99 near 1 s,
  against 180 ms at 2,000 in run D, started two minutes after run C (run E was on a slower network, with handshakes
  over 1 s).
- One socket can carry 20,000 markets: about 8.7k messages/s and 2.65 MB/s, with no overflow error and every initial
  snapshot within about 3 s. Its tail lag is the reason for the smaller shards, not its capacity.
- These numbers were taken by `probe`, which parses every frame twice on one task, so they include that work.
- Kalshi's own delay from a change (`ts_ms`) to sending it (`sending_ts_ms`) is p50 5–6 ms and p99 9–11 ms, so most of
  the lag is after Kalshi sends.
- The default `--shard-size` is 2,000 as a result. Sizes between 2,000 and 20,000, and below 2,000, are
  **Not measured**.

## 2. Decode cost per message

Source: NOTES.md, "Rates and timing".

| | 2,000 markets (runs A / D / E) | 20,000 markets (runs B / C) |
|---|---|---|
| Delta decode p50 / p99 | 5.6 / 32.7, 5.6 / 30.6, 5.7 / 22.3 µs | 4.2 / 25.4, 2.4 / 13.5 µs |
| Snapshot decode p50 / p99 | 10.2 / 82.2, 4.4 / 36.0, 5.7 / 65.3 µs | 12.7 / 101.6, 9.3 / 73.7 µs |

- A delta frame averages 295 bytes and a snapshot 450–690 bytes.
- The full-subscription `ok` reply is the expensive one: 67 KB for 2,000 markets (0.3–0.7 ms to decode) and 714 KB for
  20,000 (5–6 ms).
- Decoding costs microseconds against a feed lag of tens of milliseconds, so it is not where latency goes. The whole
  recorder used about 82% of one core of ten at about 25,000 events a second (NOTES.md, "Replica capacity").

## 3. The link, not the CPU, is the limit

Source: NOTES.md, "Replica capacity". Two 6-minute runs of the full recorder over about 152,500 markets on 77 shards,
uncompressed.

| | R=1 | R=2 |
|---|---|---|
| Received, steady state | 8.1 MB/s average, 9.3 MB/s peak | 8.8 MB/s average, 10.2 MB/s peak |
| Events forwarded per second | about 25,000 | about 13,000 |
| Disconnects | 9 (about 90 an hour) | 46 (about 460 an hour) |
| Disconnect reasons | 6 overflows, 2 resets, 1 "delivery exceeded 15 seconds" | 37 resets, 5 normal closes, 4 "delivery exceeded 15 seconds" |
| CPU, share of one core | about 82% | about 82% |

- The laptop's link carries about 9 MB/s at most. Doubling the subscriptions (R=2) left received bytes flat, so each
  socket got about half its R=1 share and the primaries forwarded about half the events.
- Disconnects rose about 5×, most of them (37 of 46) resets by the peer.
- The overflows at R=1 came from hot shards whose median feed lag had reached 22–45 s: on a saturated link, lag builds
  up on the busiest sockets until Kalshi drops them.
- When our own writer falls behind, every shard waits for it and stops reading, so the exchange's buffers fill on every
  connection alike. Splitting a shard does not help that case.

## 4. permessage-deflate

Source: NOTES.md, "Websocket compression". The client offers permessage-deflate through yawc; Kalshi agrees to
`permessage-deflate; client_no_context_takeover`. Four 3-minute runs back to back on yawc 0.4.2, compression on and
off, at R=1 and R=2.

| | R=1 on | R=1 off | R=2 on | R=2 off |
|---|---|---|---|---|
| Sockets that negotiated deflate | 75 of 75 | 0 of 75 | 149 of 149 | 0 of 149 |
| Received, steady state | 0.60 MB/s average, 0.67 peak | 3.45 MB/s average, 4.31 peak | 1.08 MB/s average, 1.86 peak | 6.73 MB/s average, 8.86 peak |
| CPU, share of one core | 54% | 51% | 74% | 89% |
| Events recorded per second | about 10,200 | about 10,800 | about 8,200 | about 10,200 |
| Disconnects | 0 | 0 | 0 | 0 |

- Compression cuts received bytes 5.8× at R=1 and 6.2× at R=2.
- It costs no measurable CPU: within 3 points at R=1, and 15 points lower at R=2.
- A 2-minute R=2 run on the final yawc 0.5.0 negotiated the same terms on 149 of 149 sockets, received 1.38 MB/s at
  about 11,800 events a second, used 74% CPU, had no disconnects, and passed `verify`.
- The feed was quieter in these runs than in the capacity runs (about 10,000 events a second against 25,000). The
  uncompressed R=2 run needed 6.73 MB/s for that. Compression at the busier rate is **Not measured**.
- No run lost a socket, so these runs do not show the R=2 disconnect rate on a busy feed. A longer R=2 run at a busy
  hour is **Not measured**.
- Compression's effect on feed lag is **Not measured**.

Compression is on by default.

## 5. Clock sync

Source: NOTES.md, "Rates and timing".

- Run A's feed lag was unusable: the Windows host clock was 7.36 s slow (checked against `time.google.com` and
  `time.cloudflare.com`) because Windows time sync had never run.
- WSL2 follows the host clock, and `systemd-timesyncd` does not start inside WSL.
- Enabling `w32time` on the host and forcing a resync brought it within 10 ms of NTP.
- Every `received_at` and every lag figure depends on this, so the clock must be synced before recording. The stats
  line's `feed_lag_p50_ms` and `feed_lag_p99_ms` show drift: the lag histogram keeps negative lags apart, so a local
  clock behind Kalshi's still shows (`src/core/stats/lag.rs`).

## 6. TCP_NODELAY

Every websocket's TCP socket has Nagle's algorithm off (`src/core/ws/transport.rs`), so our small commands go out
without waiting to be coalesced. Its effect is **Not measured**: no run compared it on and off.

## 7. Snapshot requests

Source: NOTES.md, "`get_snapshot`" and "Replica capacity".

- A `get_snapshot` for 10 markets was answered in 21–34 ms, and 0.5–0.9 s on a slower network.
- A 2,000-market `get_snapshot` is rarely answered within 10 s on the saturated link: in one run, 88 such requests
  still had markets unanswered after 10 s, up to all 2,000.
- A large `get_snapshot` to a promoted standby is often followed by a reset of that socket. When each failover asked
  for all 2,000 markets in one command, 26 of 81 promoted sockets were reset within 5 s of the request, against a
  0.3% chance of an unrelated reset in any 5 s window.
- The recorder now sends snapshot requests in batches of 200 every 100 ms. On the saturated link that did not stop the
  resets: 11 of 17 promoted sockets were still reset, 0.5–1.1 s after promotion. A standby of a hot shard is itself
  tens of seconds behind on that link, so a burst of replies queued behind its backlog plausibly pushes Kalshi to drop
  it. The resync logic keeps the data correct through these resets; whether compression removes them is
  **Not measured**.

## 8. Other measurements

Source: NOTES.md, "Connection and subscription".

- The signed handshake took 170–270 ms (1.0–1.9 s on the slower network of run E).
- After one subscribe, the last initial snapshot arrived within 0.24–0.31 s for 2,000 markets (2.1 s in run E) and
  2.85–3.24 s for 20,000.

## What we would try next

None of these has been measured.

- **Network placement near Kalshi.** The laptop's link is both the bandwidth limit and most of the feed lag. A host in
  the same region as Kalshi's servers would test how much of the lag is distance and how much is the home link.
- **simd-json.** Decoding is microseconds per message today; with the network fixed, it would become a larger share.
- **Per-shard lag histograms.** The stats line reports one feed lag p50/p99 across all shards. Per-shard histograms
  would show which shards are hot and when a shard starts to fall behind, before it overflows.
- **Interned symbols.** Markets are carried as `Arc<str>` tickers and the writer looks books up by them. Small integer
  ids would cut hashing and reference counting on the hot path.
- Also untried: connecting right after Kalshi's maintenance, the pipeline lag from receive to write, and the time from
  a reconnect to the first snapshot inside the recorder.
