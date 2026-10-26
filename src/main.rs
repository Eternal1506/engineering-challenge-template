use std::io::IsTerminal;

use clap::Parser;
use engineering_challenge::cli::{Cli, Command, SecretsArgs, VerifyArgs};
use engineering_challenge::core::verify::{VerifyLimits, verify_files};
use engineering_challenge::exchange::kalshi::KalshiCredentials;
use tracing_subscriber::util::SubscriberInitExt;

mod logging;
mod probe;

/// Parses the command line and runs the chosen subcommand. Logs go to stderr,
/// because stdout carries the binary SBE output.
#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> anyhow::Result<()> {
    init_logging();
    match Cli::parse().into_command() {
        Command::Record(args) => {
            load_credentials(&args.secrets)?;
            anyhow::bail!("record is not implemented yet")
        }
        Command::Probe(args) => {
            let credentials = load_credentials(&args.secrets)?;
            probe::run(&args, &credentials).await
        }
        Command::Verify(args) => verify(&args),
    }
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
