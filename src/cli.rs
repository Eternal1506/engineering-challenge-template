//! Command line interface: `record` (the default), `probe` and `verify`.

use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::time::Duration;

use clap::{Args, Parser, Subcommand};

/// Records the order book of every live Kalshi market as SBE messages.
///
/// Runs `record` when no subcommand is given.
#[derive(Debug, Parser)]
#[command(version, args_conflicts_with_subcommands = true)]
pub struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    #[command(flatten)]
    record: RecordArgs,
}

impl Cli {
    /// The subcommand to run, falling back to `record` with the top-level
    /// options when no subcommand was given.
    pub fn into_command(self) -> Command {
        self.command.unwrap_or(Command::Record(self.record))
    }
}

/// A subcommand of the recorder.
#[derive(Debug, PartialEq, Eq, Subcommand)]
pub enum Command {
    /// Record every live market's order book.
    Record(RecordArgs),
    /// Connect to the live feed and check its format.
    Probe(ProbeArgs),
    /// Read a recording and check its invariants.
    Verify(VerifyArgs),
}

/// Options for `record`.
#[derive(Debug, PartialEq, Eq, Args)]
pub struct RecordArgs {
    /// Where to write SBE output.
    #[arg(long, value_name = "PATH", default_value = "/dev/stdout")]
    pub output: PathBuf,

    /// Maximum number of markets per websocket connection.
    #[arg(long, value_name = "N", default_value = "2000")]
    pub shard_size: NonZeroUsize,

    /// Number of websocket connections per shard.
    #[arg(long, value_name = "N", default_value = "1")]
    pub replicas: NonZeroUsize,

    /// Flush the current output chunk at least this often, in seconds.
    #[arg(long, value_name = "SECS", value_parser = parse_seconds)]
    pub flush_interval: Option<Duration>,

    #[command(flatten)]
    pub secrets: SecretsArgs,
}

/// Options for `probe`.
#[derive(Debug, PartialEq, Eq, Args)]
pub struct ProbeArgs {
    /// How long to watch the live feed, in seconds.
    #[arg(long, value_name = "SECS", default_value = "300", value_parser = parse_seconds)]
    pub duration: Duration,

    /// Number of open markets to subscribe on one connection.
    #[arg(long, value_name = "N", default_value = "2000")]
    pub tickers: NonZeroUsize,

    /// Directory to save one raw frame of each kind seen, as test fixtures.
    #[arg(long, value_name = "PATH")]
    pub fixtures_dir: Option<PathBuf>,

    #[command(flatten)]
    pub secrets: SecretsArgs,
}

/// Options for `verify`.
#[derive(Debug, PartialEq, Eq, Args)]
pub struct VerifyArgs {
    /// The SBE recording to check.
    pub path: PathBuf,
}

/// Where to find the Kalshi API credentials.
#[derive(Debug, PartialEq, Eq, Args)]
pub struct SecretsArgs {
    /// Directory holding `key_id.txt` and `private_key.pem`.
    #[arg(long = "secrets-dir", value_name = "PATH", default_value = "secrets")]
    pub dir: PathBuf,
}

/// Parses a positive whole number of seconds.
fn parse_seconds(value: &str) -> Result<Duration, String> {
    value
        .parse::<NonZeroU64>()
        .map(|seconds| Duration::from_secs(seconds.get()))
        .map_err(|error| format!("expected a positive whole number of seconds: {error}"))
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;
    use clap::error::ErrorKind;

    use super::*;

    fn parse(args: &[&str]) -> Result<Command, clap::Error> {
        let argv = std::iter::once("engineering-challenge").chain(args.iter().copied());
        Cli::try_parse_from(argv).map(Cli::into_command)
    }

    fn default_record_args() -> RecordArgs {
        RecordArgs {
            output: PathBuf::from("/dev/stdout"),
            shard_size: NonZeroUsize::new(2000).unwrap(),
            replicas: NonZeroUsize::new(1).unwrap(),
            flush_interval: None,
            secrets: SecretsArgs {
                dir: PathBuf::from("secrets"),
            },
        }
    }

    #[test]
    fn cli_definition_is_consistent() {
        Cli::command().debug_assert();
    }

    #[test]
    fn no_arguments_records_with_defaults() {
        assert_eq!(parse(&[]).unwrap(), Command::Record(default_record_args()));
    }

    #[test]
    fn record_subcommand_uses_the_same_defaults() {
        assert_eq!(
            parse(&["record"]).unwrap(),
            Command::Record(default_record_args())
        );
    }

    #[test]
    fn top_level_options_apply_to_record() {
        let expected = RecordArgs {
            output: PathBuf::from("out.sbe"),
            flush_interval: Some(Duration::from_secs(5)),
            ..default_record_args()
        };
        let parsed = parse(&["--output", "out.sbe", "--flush-interval", "5"]).unwrap();
        assert_eq!(parsed, Command::Record(expected));
    }

    #[test]
    fn record_rejects_zero_shard_size() {
        assert!(parse(&["record", "--shard-size", "0"]).is_err());
    }

    #[test]
    fn record_rejects_zero_flush_interval() {
        assert!(parse(&["record", "--flush-interval", "0"]).is_err());
    }

    fn default_probe_args() -> ProbeArgs {
        ProbeArgs {
            duration: Duration::from_secs(300),
            tickers: NonZeroUsize::new(2000).unwrap(),
            fixtures_dir: None,
            secrets: SecretsArgs {
                dir: PathBuf::from("secrets"),
            },
        }
    }

    #[test]
    fn probe_defaults_to_five_minutes_and_2000_tickers() {
        assert_eq!(
            parse(&["probe"]).unwrap(),
            Command::Probe(default_probe_args())
        );
    }

    #[test]
    fn probe_takes_tickers_duration_and_fixtures_dir() {
        let expected = ProbeArgs {
            duration: Duration::from_secs(120),
            tickers: NonZeroUsize::new(20_000).unwrap(),
            fixtures_dir: Some(PathBuf::from("tests/fixtures/kalshi")),
            ..default_probe_args()
        };
        let args = [
            "probe",
            "--tickers",
            "20000",
            "--duration",
            "120",
            "--fixtures-dir",
            "tests/fixtures/kalshi",
        ];
        assert_eq!(parse(&args).unwrap(), Command::Probe(expected));
    }

    #[test]
    fn verify_requires_a_path() {
        let error = parse(&["verify"]).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn verify_takes_a_path() {
        let expected = VerifyArgs {
            path: PathBuf::from("run.sbe"),
        };
        assert_eq!(
            parse(&["verify", "run.sbe"]).unwrap(),
            Command::Verify(expected)
        );
    }
}
