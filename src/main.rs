use std::io::IsTerminal;

use clap::Parser;
use engineering_challenge::cli::{Cli, Command, SecretsArgs};
use engineering_challenge::exchange::kalshi::KalshiCredentials;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter::LevelFilter;

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
            load_credentials(&args.secrets)?;
            anyhow::bail!("probe is not implemented yet")
        }
        Command::Verify(_) => anyhow::bail!("verify is not implemented yet"),
    }
}

fn load_credentials(secrets: &SecretsArgs) -> anyhow::Result<KalshiCredentials> {
    let credentials = KalshiCredentials::load(&secrets.dir)?;
    tracing::info!("loaded Kalshi credentials");
    Ok(credentials)
}

/// Sends `tracing` output to stderr, filtered by `RUST_LOG` (default `info`),
/// with colour only when stderr is a terminal.
fn init_logging() {
    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_ansi(std::io::stderr().is_terminal())
        .init();
}
