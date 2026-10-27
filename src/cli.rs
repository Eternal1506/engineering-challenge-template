//! Command line interface: `record` (the default), `probe` and `verify`.

use std::fs::File;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use chrono::{DateTime, Utc};
use clap::{Args, Parser, Subcommand};

/// The placeholder in `record --output` for the run's start time.
const START_PLACEHOLDER: &str = "{start}";

/// How the start time fills [`START_PLACEHOLDER`].
const START_STAMP_FORMAT: &str = "%Y%m%dT%H%M%SZ";

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
    /// Where to write SBE output. `{start}` in it becomes the UTC start
    /// time, as in `20261026T143005Z`, so each run gets its own file, which
    /// must not exist yet. The directory must exist.
    #[arg(long, value_name = "PATH", default_value = "/dev/stdout")]
    pub output: PathBuf,

    /// Maximum number of markets per websocket connection.
    #[arg(long, value_name = "N", default_value = "2000")]
    pub shard_size: NonZeroUsize,

    /// Number of websocket connections per shard: one primary whose events
    /// are recorded, the rest standbys ready to take over.
    #[arg(long, value_name = "N", default_value = "2")]
    pub replicas: NonZeroUsize,

    /// Most websocket connections at once, the lifecycle connection
    /// included. A shard whose replicas would exceed it gets one.
    #[arg(long, value_name = "N", default_value = "190")]
    pub max_connections: NonZeroUsize,

    /// How long after a shard's primary first connects its standbys start,
    /// in seconds.
    #[arg(long, value_name = "SECS", default_value = "30", value_parser = parse_seconds)]
    pub standby_delay: Duration,

    /// How long a standby must have been connected to take over from a
    /// primary that disconnects, in seconds.
    #[arg(long, value_name = "SECS", default_value = "5", value_parser = parse_seconds)]
    pub failover_warmup: Duration,

    /// Flush the current output chunk this often, in seconds. Each flush
    /// pads the output to a full 64 MiB chunk.
    #[arg(long, value_name = "SECS", value_parser = parse_seconds)]
    pub flush_interval: Option<Duration>,

    /// How often to reconcile the recorded markets with the exchange's
    /// catalog, in seconds.
    #[arg(long, value_name = "SECS", default_value = "600", value_parser = parse_seconds)]
    pub reconcile_interval: Duration,

    /// How many markets each shard audits a minute, comparing a fresh
    /// snapshot from the exchange with the recorded book; 0 turns audits
    /// off.
    #[arg(long, value_name = "N", default_value = "5")]
    pub audit_markets: usize,

    #[command(flatten)]
    pub secrets: SecretsArgs,
}

impl RecordArgs {
    /// The output path for a run started at `start`: every `{start}` in
    /// `output` replaced with `start` as `YYYYMMDDTHHMMSSZ`, which is safe in
    /// file names and sorts in time order. A path that is not UTF-8 is used
    /// as given.
    pub fn output_for(&self, start: DateTime<Utc>) -> PathBuf {
        let Some(output) = self.output.to_str() else {
            return self.output.clone();
        };
        let stamp = start.format(START_STAMP_FORMAT).to_string();
        PathBuf::from(output.replace(START_PLACEHOLDER, &stamp))
    }

    /// [`RecordArgs::output_for`] `start`. An output naming `{start}` is
    /// created empty here and must not exist yet, so a run never writes
    /// over an earlier one's recording when the clock steps back or two
    /// runs start within a second; either fails instead.
    pub fn create_output(&self, start: DateTime<Utc>) -> anyhow::Result<PathBuf> {
        let output = self.output_for(start);
        let stamped = self.output.to_str();
        if stamped.is_some_and(|path| path.contains(START_PLACEHOLDER)) {
            File::create_new(&output)
                .with_context(|| format!("creating {}, which must be new", output.display()))?;
        }
        Ok(output)
    }
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
    /// The SBE recordings to check, in order. Files from consecutive runs are
    /// checked as one recording.
    #[arg(value_name = "PATH", required = true)]
    pub paths: Vec<PathBuf>,

    /// The longest allowed time between two snapshots of one symbol, in
    /// seconds.
    #[arg(long, value_name = "SECS", default_value = "65", value_parser = parse_seconds)]
    pub max_snapshot_gap: Duration,

    /// The highest valid price, in fixed point (1000000 is one whole unit).
    #[arg(long, value_name = "N", default_value = "1000000",
          value_parser = clap::value_parser!(i64).range(0..))]
    pub max_price: i64,
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
            replicas: NonZeroUsize::new(2).unwrap(),
            max_connections: NonZeroUsize::new(190).unwrap(),
            standby_delay: Duration::from_secs(30),
            failover_warmup: Duration::from_secs(5),
            flush_interval: None,
            reconcile_interval: Duration::from_secs(600),
            audit_markets: 5,
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

    #[test]
    fn record_takes_replicas_and_their_budget_and_timings() {
        let expected = RecordArgs {
            replicas: NonZeroUsize::new(3).unwrap(),
            max_connections: NonZeroUsize::new(100).unwrap(),
            standby_delay: Duration::from_secs(10),
            failover_warmup: Duration::from_secs(2),
            ..default_record_args()
        };
        let args = [
            "record",
            "--replicas",
            "3",
            "--max-connections",
            "100",
            "--standby-delay",
            "10",
            "--failover-warmup",
            "2",
        ];
        assert_eq!(parse(&args).unwrap(), Command::Record(expected));
    }

    #[test]
    fn record_rejects_zero_replicas() {
        assert!(parse(&["record", "--replicas", "0"]).is_err());
    }

    #[test]
    fn record_takes_a_reconcile_interval() {
        let expected = RecordArgs {
            reconcile_interval: Duration::from_secs(300),
            ..default_record_args()
        };
        let parsed = parse(&["record", "--reconcile-interval", "300"]).unwrap();
        assert_eq!(parsed, Command::Record(expected));
    }

    #[test]
    fn record_takes_an_audit_size_and_zero_turns_audits_off() {
        let expected = RecordArgs {
            audit_markets: 0,
            ..default_record_args()
        };
        let parsed = parse(&["record", "--audit-markets", "0"]).unwrap();
        assert_eq!(parsed, Command::Record(expected));
    }

    fn output_at(output: &str, start: &str) -> PathBuf {
        let args = RecordArgs {
            output: PathBuf::from(output),
            ..default_record_args()
        };
        args.output_for(start.parse().unwrap())
    }

    #[test]
    fn start_in_the_output_becomes_the_utc_start_time() {
        let output = output_at("/rec/kalshi-{start}.sbe", "2026-10-26T14:30:05.9+02:00");
        assert_eq!(output, PathBuf::from("/rec/kalshi-20261026T123005Z.sbe"));
    }

    #[test]
    fn every_start_in_the_output_is_replaced() {
        let output = output_at("/rec/{start}/run-{start}.sbe", "2026-01-02T03:04:05Z");
        let expected = "/rec/20260102T030405Z/run-20260102T030405Z.sbe";
        assert_eq!(output, PathBuf::from(expected));
    }

    #[test]
    fn an_output_without_start_is_unchanged() {
        let output = output_at("/dev/stdout", "2026-01-02T03:04:05Z");
        assert_eq!(output, PathBuf::from("/dev/stdout"));
    }

    #[test]
    fn start_stamps_sort_in_time_order() {
        let earlier = output_at("{start}", "2026-09-30T23:59:59Z");
        let later = output_at("{start}", "2026-10-01T00:00:00Z");
        assert!(earlier < later, "{earlier:?} {later:?}");
    }

    /// Record args writing to `output` under `dir`.
    fn writing_under(dir: &tempfile::TempDir, output: &str) -> RecordArgs {
        RecordArgs {
            output: dir.path().join(output),
            ..default_record_args()
        }
    }

    const START: &str = "2026-01-02T03:04:05Z";

    #[test]
    fn a_stamped_output_is_created_empty() {
        let dir = tempfile::tempdir().unwrap();
        let args = writing_under(&dir, "kalshi-{start}.sbe");
        let output = args.create_output(START.parse().unwrap()).unwrap();
        let expected = dir.path().join("kalshi-20260102T030405Z.sbe");
        assert_eq!((output, expected.metadata().unwrap().len()), (expected, 0));
    }

    #[test]
    fn a_stamped_output_that_exists_fails_and_is_kept() {
        let dir = tempfile::tempdir().unwrap();
        let earlier = dir.path().join("kalshi-20260102T030405Z.sbe");
        std::fs::write(&earlier, b"earlier run").unwrap();
        let args = writing_under(&dir, "kalshi-{start}.sbe");
        let created = args.create_output(START.parse().unwrap());
        let kept = std::fs::read(&earlier).unwrap();
        assert_eq!((created.is_err(), kept), (true, b"earlier run".to_vec()));
    }

    #[test]
    fn an_unstamped_output_that_exists_is_used_as_given() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("fixed.sbe"), b"old").unwrap();
        let args = writing_under(&dir, "fixed.sbe");
        let output = args.create_output(START.parse().unwrap()).unwrap();
        assert_eq!(output, dir.path().join("fixed.sbe"));
    }

    #[test]
    fn a_stamped_output_in_a_missing_directory_fails() {
        let dir = tempfile::tempdir().unwrap();
        let args = writing_under(&dir, "missing/kalshi-{start}.sbe");
        assert!(args.create_output(START.parse().unwrap()).is_err());
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
    fn verify_takes_a_path_with_default_limits() {
        let expected = VerifyArgs {
            paths: vec![PathBuf::from("run.sbe")],
            max_snapshot_gap: Duration::from_secs(65),
            max_price: 1_000_000,
        };
        assert_eq!(
            parse(&["verify", "run.sbe"]).unwrap(),
            Command::Verify(expected)
        );
    }

    #[test]
    fn verify_takes_paths_in_order_and_limits() {
        let expected = VerifyArgs {
            paths: vec![PathBuf::from("b.sbe"), PathBuf::from("a.sbe")],
            max_snapshot_gap: Duration::from_secs(90),
            max_price: 100,
        };
        let args = [
            "verify",
            "b.sbe",
            "a.sbe",
            "--max-snapshot-gap",
            "90",
            "--max-price",
            "100",
        ];
        assert_eq!(parse(&args).unwrap(), Command::Verify(expected));
    }

    #[test]
    fn verify_rejects_a_negative_max_price() {
        assert!(parse(&["verify", "run.sbe", "--max-price=-1"]).is_err());
    }
}
