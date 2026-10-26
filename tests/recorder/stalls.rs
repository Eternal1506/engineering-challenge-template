//! Shutting down while the writer is stuck or slow. A stuck writer writes
//! to a FIFO whose read end the test holds: each flush writes a whole
//! chunk, so the writer blocks until the test reads it. A slow writer gets
//! its messages through a relay that passes one on every 200 ms.

use std::io::Read;

use engineering_challenge::core::writer::WriterMessage;
use engineering_challenge::exchange::MarketStatus;
use tokio::net::unix::pipe;

use super::mock_exchange::market;
use super::*;

/// A coordinator recording A, B and C through a writer to a FIFO, before
/// anything has read the FIFO.
struct FifoPipeline {
    coordinator: Coordinator<MockEndpoint, MockCodec>,
    counters: Arc<RecorderCounters>,
    writer: WriterHandle,
    reader: pipe::Receiver,
    _dir: TempDir,
}

/// How the writer and the coordinator are set up for a test.
struct Setup {
    /// The writer's channel capacity.
    channel: usize,
    /// How often the writer flushes, blocking on the FIFO.
    flush: Duration,
    /// The coordinator's stall timeout.
    stall_timeout: Duration,
}

impl FifoPipeline {
    /// Starts the pipeline and serves A, B and C's books.
    async fn start(server: &MockServer, setup: Setup) -> (Self, Shard) {
        let dir = tempfile::tempdir().unwrap();
        let fifo = dir.path().join("out.sbe");
        let reader = unread_fifo(&fifo);
        let (link, writer) = start_writer(&fifo, |writer| {
            writer.channel_capacity = setup.channel;
            writer.flush_interval = Some(setup.flush);
        });
        let config = CoordinatorConfig {
            stall_timeout: setup.stall_timeout,
            ..config(3)
        };
        let mut coordinator = Coordinator::new(mock_feed(server), link, config);
        let markets = ["A", "B", "C"].map(|symbol| market(symbol, MarketStatus::Open));
        coordinator.add_markets(markets.to_vec()).await;
        let mut shard = Shard::accept(server, 1).await;
        shard.send_books().await;
        let counters = coordinator.counters();
        let pipeline = Self {
            coordinator,
            counters,
            writer,
            reader,
            _dir: dir,
        };
        (pipeline, shard)
    }
}

/// Makes a FIFO at `path` and opens its read end without reading it.
fn unread_fifo(path: &Path) -> pipe::Receiver {
    let made = std::process::Command::new("mkfifo").arg(path).status();
    assert!(made.unwrap().success(), "mkfifo failed");
    pipe::OpenOptions::new().open_receiver(path).unwrap()
}

/// Reads `reader` to its end on a blocking thread, which reads a chunk far
/// faster than async reads of the FIFO would.
fn read_to_end(reader: pipe::Receiver) -> JoinHandle<()> {
    read_slowly_then_to_end(reader, Duration::ZERO)
}

/// Reads `reader` on a blocking thread at about 3 MiB/s, 64 KiB every
/// 20 ms, for `slow_for`, then at full speed to its end.
fn read_slowly_then_to_end(reader: pipe::Receiver, slow_for: Duration) -> JoinHandle<()> {
    let mut fifo = std::fs::File::from(reader.into_blocking_fd().unwrap());
    tokio::task::spawn_blocking(move || {
        let mut buffer = vec![0; 64 << 10];
        let slow_until = std::time::Instant::now() + slow_for;
        while std::time::Instant::now() < slow_until {
            if fifo.read(&mut buffer).unwrap() == 0 {
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        std::io::copy(&mut fifo, &mut std::io::sink()).unwrap();
    })
}

/// Sends `count` +1 bid changes for A.
async fn send_changes(shard: &mut Shard, count: usize) {
    for _ in 0..count {
        shard.send_level("A", ("buy", 400_000, 1)).await;
    }
}

/// The longest a shutdown behind a slow reader may take.
const SHUTDOWN_LIMIT: Duration = Duration::from_secs(20);

/// Shuts `coordinator` down, answering the shard's close, and returns how
/// long the shutdown took.
async fn shut_down(coordinator: Coordinator<MockEndpoint, MockCodec>, shard: Shard) -> Duration {
    let close = shard.answer_close();
    let started = Instant::now();
    timeout(SHUTDOWN_LIMIT, coordinator.shutdown())
        .await
        .expect("the shutdown hung");
    let took = started.elapsed();
    closed(vec![close]).await;
    took
}

/// Waits for the writer thread to finish.
async fn join(writer: WriterHandle) -> anyhow::Result<WriterStats> {
    tokio::task::spawn_blocking(move || writer.join())
        .await
        .unwrap()
}

#[tokio::test]
async fn shutdown_abandons_the_events_a_stalled_writer_never_takes() {
    let server = MockServer::start().await;
    let stall_timeout = Duration::from_millis(300);
    let setup = Setup {
        channel: 8,
        flush: Duration::from_millis(200),
        stall_timeout,
    };
    let (pipeline, mut shard) = FifoPipeline::start(&server, setup).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    send_changes(&mut shard, 100).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let took = shut_down(pipeline.coordinator, shard).await;
    drop(pipeline.reader);
    let joined = join(pipeline.writer).await;
    assert!(
        took >= stall_timeout && took < Duration::from_secs(2) && joined.is_err(),
        "{took:?}"
    );
}

#[tokio::test]
async fn the_backlog_queued_at_cancel_still_reaches_the_writer() {
    let server = MockServer::start().await;
    let setup = Setup {
        channel: 8,
        flush: Duration::from_secs(1),
        stall_timeout: Duration::from_secs(2),
    };
    let (pipeline, mut shard) = FifoPipeline::start(&server, setup).await;
    tokio::time::sleep(Duration::from_millis(1300)).await;
    send_changes(&mut shard, 300).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let forwarded_before_cancel = pipeline.counters.read().events_forwarded;
    let stopping = tokio::spawn(shut_down(pipeline.coordinator, shard));
    let reading = read_to_end(pipeline.reader);
    stopping.await.unwrap();
    reading.await.unwrap();
    let stats = join(pipeline.writer).await.unwrap();
    let delivered = 3 + 300;
    assert_eq!(
        (forwarded_before_cancel < delivered, stats.events),
        (true, delivered)
    );
}

/// A writer blocked in a chunk write behind a reader taking about 3 MiB/s
/// for 3 s writes no message in that time, yet keeps moving: shutdown
/// outlasts the stall timeout and abandons nothing.
#[tokio::test]
async fn a_writer_draining_into_a_slow_reader_is_not_abandoned() {
    let server = MockServer::start().await;
    let stall_timeout = Duration::from_secs(1);
    let setup = Setup {
        channel: 8,
        flush: Duration::from_secs(1),
        stall_timeout,
    };
    let (pipeline, mut shard) = FifoPipeline::start(&server, setup).await;
    tokio::time::sleep(Duration::from_millis(1300)).await;
    send_changes(&mut shard, 100).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let reading = read_slowly_then_to_end(pipeline.reader, Duration::from_secs(3));
    let took = shut_down(pipeline.coordinator, shard).await;
    reading.await.unwrap();
    let stats = join(pipeline.writer).await.unwrap();
    assert_eq!(
        (stats.events, took > stall_timeout),
        (103, true),
        "{took:?}"
    );
}

/// Passes each message on to `writer` 200 ms after taking it, through a
/// channel of one, like a writer that is slow but keeps writing.
fn slow_relay(writer: mpsc::Sender<WriterMessage>) -> mpsc::Sender<WriterMessage> {
    let (sender, mut receiver) = mpsc::channel(1);
    tokio::spawn(async move {
        while let Some(message) = receiver.recv().await {
            tokio::time::sleep(Duration::from_millis(200)).await;
            if writer.send(message).await.is_err() {
                return;
            }
        }
    });
    sender
}

/// A writer that writes a message every 200 ms makes progress well within
/// the stall timeout, so shutdown outlasts the timeout yet abandons nothing.
#[tokio::test]
async fn a_slow_but_progressing_writer_is_not_abandoned() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let (mut link, writer) = start_writer(&dir.path().join("out.sbe"), |_| {});
    link.messages = slow_relay(link.messages);
    let stall_timeout = Duration::from_secs(1);
    let config = CoordinatorConfig {
        stall_timeout,
        ..config(3)
    };
    let mut coordinator = Coordinator::new(mock_feed(&server), link, config);
    let markets = ["A", "B", "C"].map(|symbol| market(symbol, MarketStatus::Open));
    coordinator.add_markets(markets.to_vec()).await;
    let mut shard = Shard::accept(&server, 1).await;
    shard.send_books().await;
    send_changes(&mut shard, 12).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let took = shut_down(coordinator, shard).await;
    let stats = join(writer).await.unwrap();
    assert_eq!((stats.events, took > stall_timeout), (15, true), "{took:?}");
}
