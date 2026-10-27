use std::convert::Infallible;
use std::io::IsTerminal;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use engineering_challenge::cli::{Cli, Command, RecordArgs, SecretsArgs, VerifyArgs};
use engineering_challenge::core::coordinator::{
    Coordinator, CoordinatorConfig, DEFAULT_STALL_TIMEOUT, Feed, LifecycleTimings, WriterLink,
};
use engineering_challenge::core::shard::FailoverTimings;
use engineering_challenge::core::verify::{VerifyLimits, verify_files};
use engineering_challenge::core::writer::{
    WriterConfig, WriterHandle, WriterProgress, spawn_writer,
};
use engineering_challenge::exchange::kalshi::{
    KalshiCatalog, KalshiCodec, KalshiCredentials, KalshiEndpoint, RequestSigner,
};
use tokio::signal::unix::{Signal, SignalKind, signal};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::util::SubscriberInitExt;

mod logging;
mod probe;

/// How many resnapshot requests may wait between the writer and the
/// coordinator.
const RESNAPSHOT_QUEUE: usize = 4_096;

/// How often `record` checks that the writer thread is still running.
const WRITER_CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// How long `record`, once cancelled, may go without the writer showing any
/// sign of moving (see [`WriterProgress::stalled_for`]) before it exits
/// with an error, leaving the output unfinished.
const SHUTDOWN_STALL_TIMEOUT: Duration = Duration::from_secs(10);

/// The coordinator abandons the shards' undelivered events before the
/// process gives up, so a stuck writer still gets every event it can take.
const _: () = assert!(DEFAULT_STALL_TIMEOUT.as_millis() < SHUTDOWN_STALL_TIMEOUT.as_millis());

/// The exit status after a second signal, as for a process killed by
/// SIGINT.
const SECOND_SIGNAL_STATUS: i32 = 130;

/// Parses the command line and runs the chosen subcommand. Logs go to stderr,
/// because stdout carries the binary SBE output.
#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> anyhow::Result<()> {
    init_logging();
    match Cli::parse().into_command() {
        Command::Record(args) => record(&args).await,
        Command::Probe(args) => {
            let credentials = load_credentials(&args.secrets)?;
            probe::run(&args, &credentials).await
        }
        Command::Verify(args) => verify(&args),
    }
}

/// Records every open Kalshi market until SIGINT or SIGTERM, then closes the
/// connections, flushes the output and prints the writer's stats to stderr.
/// Fails if the listing or the writer fails.
///
/// A second signal exits at once with status 130, and a shutdown in which
/// the writer shows no sign of moving for [`SHUTDOWN_STALL_TIMEOUT`] exits
/// with status 1; either way the output is left unfinished. A writer
/// draining into a slow reader keeps moving.
async fn record(args: &RecordArgs) -> anyhow::Result<()> {
    let catalog = KalshiCatalog::production()?;
    let (coordinator, writer) = build_recorder(args)?;
    let cancel = CancellationToken::new();
    tokio::spawn(handle_signals(cancel.clone()));
    tokio::spawn(exit_if_shutdown_stalls(cancel.clone(), writer.progress()));
    let recorded = tokio::select! {
        recorded = coordinator.run(catalog, cancel.clone()) => recorded,
        never = cancel_if_writer_stops(&writer, &cancel) => match never {},
    };
    cancel.cancel();
    let stats = tokio::task::spawn_blocking(move || writer.join()).await??;
    eprintln!("writer: {stats:?}");
    recorded
}

/// The Kalshi coordinator and the writer thread it records through.
fn build_recorder(
    args: &RecordArgs,
) -> anyhow::Result<(Coordinator<KalshiEndpoint, KalshiCodec>, WriterHandle)> {
    let credentials = load_credentials(&args.secrets)?;
    let feed = Feed {
        endpoint: kalshi_endpoint(&credentials)?,
        lifecycle_endpoint: Some(kalshi_endpoint(&credentials)?),
        codec: KalshiCodec,
    };
    let (link, writer) = start_writer(args)?;
    let coordinator = Coordinator::new(feed, link, coordinator_config(args));
    Ok((coordinator, writer))
}

/// The production Kalshi websocket, signing handshakes with `credentials`.
fn kalshi_endpoint(credentials: &KalshiCredentials) -> anyhow::Result<KalshiEndpoint> {
    Ok(KalshiEndpoint::production(RequestSigner::new(credentials)?))
}

/// The coordinator's config for `args`.
fn coordinator_config(args: &RecordArgs) -> CoordinatorConfig {
    let defaults = CoordinatorConfig::default();
    CoordinatorConfig {
        shard_size: args.shard_size.get(),
        replicas: args.replicas,
        max_connections: args.max_connections.get(),
        failover: FailoverTimings {
            standby_delay: args.standby_delay,
            warmup: args.failover_warmup,
            reconnect_grace: defaults.connection.liveness_timeout,
            ..defaults.failover.clone()
        },
        lifecycle: LifecycleTimings {
            reconcile_interval: args.reconcile_interval,
            ..defaults.lifecycle.clone()
        },
        ..defaults
    }
}

/// Starts the writer thread on `args.output`, returning the coordinator's
/// link to it and the thread's handle.
fn start_writer(args: &RecordArgs) -> anyhow::Result<(WriterLink, WriterHandle)> {
    let (requests, resnapshot_requests) = mpsc::channel::<Arc<str>>(RESNAPSHOT_QUEUE);
    let config = WriterConfig {
        flush_interval: args.flush_interval,
        ..WriterConfig::new(&args.output, requests)
    };
    let (messages, writer) = spawn_writer(config)?;
    let link = WriterLink {
        messages,
        resnapshot_requests,
        progress: writer.progress(),
    };
    Ok((link, writer))
}

/// Cancels `cancel` on the first SIGINT or SIGTERM, and exits at once on
/// the second.
async fn handle_signals(cancel: CancellationToken) {
    let mut interrupts = listen(SignalKind::interrupt(), "SIGINT");
    let mut terminations = listen(SignalKind::terminate(), "SIGTERM");
    next_signal(&mut interrupts, &mut terminations).await;
    tracing::info!("shutting down; a second signal exits at once");
    cancel.cancel();
    next_signal(&mut interrupts, &mut terminations).await;
    tracing::error!("second signal, exiting without finishing the output");
    std::process::exit(SECOND_SIGNAL_STATUS);
}

/// A stream of `kind` signals, or `None`, logged, if they cannot be caught.
fn listen(kind: SignalKind, name: &str) -> Option<Signal> {
    signal(kind)
        .inspect_err(|error| tracing::error!(%error, signal = name, "cannot listen for signal"))
        .ok()
}

/// Waits for the next signal from either stream.
async fn next_signal(first: &mut Option<Signal>, second: &mut Option<Signal>) {
    tokio::select! {
        () = next_of(first) => {}
        () = next_of(second) => {}
    }
}

/// Waits for the next signal of `signals`; never returns without a stream.
async fn next_of(signals: &mut Option<Signal>) {
    match signals {
        Some(signals) => {
            signals.recv().await;
        }
        None => std::future::pending().await,
    }
}

/// Exits with status 1 if, once `cancel` fires, the writer shows no sign of
/// moving for [`SHUTDOWN_STALL_TIMEOUT`], as when it is stuck on an output
/// nobody reads.
async fn exit_if_shutdown_stalls(cancel: CancellationToken, progress: Arc<WriterProgress>) {
    cancel.cancelled().await;
    progress.stalled_for(SHUTDOWN_STALL_TIMEOUT).await;
    let timeout = SHUTDOWN_STALL_TIMEOUT;
    tracing::error!(?timeout, "the writer stalled during shutdown, exiting");
    std::process::exit(1);
}

/// Cancels `cancel` if the writer thread stops while recording, so the
/// recorder shuts down and reports the writer's error. Never returns.
async fn cancel_if_writer_stops(writer: &WriterHandle, cancel: &CancellationToken) -> Infallible {
    let mut checks = tokio::time::interval(WRITER_CHECK_INTERVAL);
    while !writer.is_finished() {
        checks.tick().await;
    }
    if !cancel.is_cancelled() {
        tracing::error!("the writer thread stopped, shutting down");
        cancel.cancel();
    }
    std::future::pending().await
}

/// Checks the recordings' invariants, prints the report to stderr, and fails
/// if anything was violated.
fn verify(args: &VerifyArgs) -> anyhow::Result<()> {
    let limits = VerifyLimits {
        max_snapshot_gap: args.max_snapshot_gap,
        max_price: args.max_price,
    };
    let report = verify_files(&args.paths, &limits)?;
    eprintln!("{report}");
    anyhow::ensure!(
        report.is_clean(),
        "found {} violations",
        report.violation_count()
    );
    Ok(())
}

fn load_credentials(secrets: &SecretsArgs) -> anyhow::Result<KalshiCredentials> {
    let credentials = KalshiCredentials::load(&secrets.dir)?;
    tracing::info!("loaded Kalshi credentials");
    Ok(credentials)
}

/// Sends `tracing` output to stderr, filtered by `RUST_LOG` (default `info`),
/// with colour only when stderr is a terminal.
fn init_logging() {
    let rust_log = std::env::var("RUST_LOG").unwrap_or_default();
    let ansi = std::io::stderr().is_terminal();
    logging::subscriber(&rust_log, std::io::stderr, ansi).init();
}
