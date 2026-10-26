//! Checks a recording's invariants: every symbol starts with a snapshot, is
//! snapshotted often enough, moves forward in time, and has valid prices,
//! quantities and snapshot sides.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Context;
use chrono::{DateTime, TimeDelta, Utc};

use crate::Side;
use crate::core::writer::CHUNK_SIZE;
use crate::util::dtos::challenge_orderbook::{AnyMessage, PriceLevelUpdateDomain, SnapshotDomain};
use crate::util::{AlignedSbeReader, OrderbookSchema};

/// The default longest allowed time between two snapshots of one symbol.
pub const DEFAULT_MAX_SNAPSHOT_GAP: Duration = Duration::from_secs(65);

/// The default highest valid price: one whole unit in fixed point, the
/// payout of a binary contract.
pub const DEFAULT_MAX_PRICE: i64 = 1_000_000;

/// How many examples the report keeps for each kind of violation.
const EXAMPLES_PER_KIND: usize = 5;

/// The bounds a recording must stay within.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VerifyLimits {
    /// The longest allowed time between two snapshots of one symbol.
    pub max_snapshot_gap: Duration,
    /// The highest valid price, in fixed point. Prices must be above 0: a
    /// price of 0 is the signature of a side-mapping bug.
    pub max_price: i64,
}

impl Default for VerifyLimits {
    fn default() -> Self {
        Self {
            max_snapshot_gap: DEFAULT_MAX_SNAPSHOT_GAP,
            max_price: DEFAULT_MAX_PRICE,
        }
    }
}

/// A kind of invariant violation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ViolationKind {
    /// A file's length is not a whole number of chunks, so its tail is
    /// unreadable and files concatenated after it are misaligned.
    PartialChunk,
    /// A `PriceLevelUpdate` came before any snapshot of its symbol in its
    /// file.
    UpdateBeforeSnapshot,
    /// Two snapshots of a symbol were further apart than allowed, or the
    /// symbol was still updating longer than that after its last snapshot.
    SnapshotGap,
    /// A symbol's receive time went backwards.
    TimeWentBackwards,
    /// A level had a negative quantity.
    NegativeQuantity,
    /// A level's price was outside the valid range.
    PriceOutOfRange,
    /// A snapshot's bids were not strictly descending or its asks not
    /// strictly ascending.
    UnsortedSnapshotSide,
    /// A snapshot listed a level with zero quantity.
    ZeroQuantityLevel,
}

impl fmt::Display for ViolationKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::PartialChunk => "partial chunk",
            Self::UpdateBeforeSnapshot => "update before snapshot",
            Self::SnapshotGap => "snapshot gap",
            Self::TimeWentBackwards => "time went backwards",
            Self::NegativeQuantity => "negative quantity",
            Self::PriceOutOfRange => "price out of range",
            Self::UnsortedSnapshotSide => "unsorted snapshot side",
            Self::ZeroQuantityLevel => "zero-quantity snapshot level",
        })
    }
}

/// How often one kind of violation occurred, with the first few examples.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ViolationTally {
    /// How many times it occurred.
    pub count: u64,
    /// Descriptions of the first occurrences.
    pub examples: Vec<String>,
}

/// What [`verify_files`] found.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct VerifyReport {
    /// Files read.
    pub files: usize,
    /// `Snapshot` messages read.
    pub snapshots: u64,
    /// `PriceLevelUpdate` messages read.
    pub updates: u64,
    /// Distinct symbols seen.
    pub symbols: usize,
    /// The earliest receive time of any message.
    pub first_timestamp: Option<DateTime<Utc>>,
    /// The latest receive time of any message.
    pub last_timestamp: Option<DateTime<Utc>>,
    /// Violations by kind. Kinds that never occurred are absent.
    pub violations: BTreeMap<ViolationKind, ViolationTally>,
    /// How many symbols' receive times went backwards at least once. One
    /// message stamped too late makes every later message of its symbol
    /// count as going backwards, so this and the largest step back show how
    /// widespread the problem really is.
    pub symbols_going_backwards: usize,
    /// The largest step back in any symbol's receive time.
    pub largest_step_back: TimeDelta,
}

impl VerifyReport {
    /// Whether no invariant was violated.
    pub fn is_clean(&self) -> bool {
        self.violations.is_empty()
    }

    /// The number of violations of every kind.
    pub fn violation_count(&self) -> u64 {
        self.violations.values().map(|tally| tally.count).sum()
    }

    /// How many violations of `kind` occurred.
    pub fn count(&self, kind: ViolationKind) -> u64 {
        self.violations.get(&kind).map_or(0, |tally| tally.count)
    }

    fn record(&mut self, kind: ViolationKind, example: impl FnOnce() -> String) {
        let tally = self.violations.entry(kind).or_default();
        tally.count += 1;
        if tally.examples.len() < EXAMPLES_PER_KIND {
            tally.examples.push(example());
        }
    }

    fn note_time(&mut self, at: DateTime<Utc>) {
        self.first_timestamp = Some(self.first_timestamp.map_or(at, |first| first.min(at)));
        self.last_timestamp = self.last_timestamp.max(Some(at));
    }

    fn write_span(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.first_timestamp, self.last_timestamp) {
            (Some(first), Some(last)) => {
                let seconds = seconds(last - first);
                writeln!(f, "time span: {first} to {last} ({seconds:.3} s)")
            }
            _ => writeln!(f, "time span: no messages"),
        }
    }

    fn write_examples(&self, f: &mut fmt::Formatter<'_>, kind: ViolationKind) -> fmt::Result {
        let tally = &self.violations[&kind];
        write!(f, "\n  {kind}: {}", tally.count)?;
        if kind == ViolationKind::TimeWentBackwards {
            let symbols = self.symbols_going_backwards;
            let step = seconds(self.largest_step_back);
            write!(f, " ({symbols} symbols, largest step back {step:.3} s)")?;
        }
        for example in &tally.examples {
            write!(f, "\n    {example}")?;
        }
        Ok(())
    }
}

impl fmt::Display for VerifyReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "{} file(s): {} snapshots, {} updates, {} symbols",
            self.files, self.snapshots, self.updates, self.symbols
        )?;
        self.write_span(f)?;
        if self.is_clean() {
            return write!(f, "violations: none");
        }
        write!(f, "violations: {}", self.violation_count())?;
        self.violations
            .keys()
            .try_for_each(|&kind| self.write_examples(f, kind))
    }
}

/// Reads the recordings at `paths` in order and checks their invariants:
///
/// 1. within each file, every symbol has a snapshot before its first update;
/// 2. a symbol's snapshots are at most `max_snapshot_gap` apart. Within each
///    file, the stretch after a symbol's last snapshot runs to the end of the
///    file if the symbol updated in it, and is otherwise not checked, since
///    the market may have closed;
/// 3. each symbol's receive times never go backwards, across files too;
/// 4. quantities are never negative;
/// 5. prices are within `1..=max_price`;
/// 6. snapshot bids are strictly descending, asks strictly ascending, and no
///    snapshot level has zero quantity.
///
/// Each path is one recorder run: invariants 1 and 2 start afresh in each
/// file, so the downtime between consecutive runs is not a gap. Runs
/// concatenated into one file are checked as one stream, so downtime longer
/// than `max_snapshot_gap` between them is reported as a [`SnapshotGap`]:
/// the recording really has no data then. Pass a crashed and a restarted
/// run as separate paths to check each run on its own.
///
/// It also reports a file whose length is not a whole number of chunks.
/// Fails if a file cannot be read or decoded, or if the files hold no
/// messages at all.
///
/// [`SnapshotGap`]: ViolationKind::SnapshotGap
pub fn verify_files(paths: &[PathBuf], limits: &VerifyLimits) -> anyhow::Result<VerifyReport> {
    let mut checker = Checker::new(limits);
    for path in paths {
        checker
            .check_file(path)
            .with_context(|| format!("reading {}", path.display()))?;
    }
    let report = checker.into_report();
    anyhow::ensure!(
        report.snapshots + report.updates > 0,
        "the recordings hold no messages"
    );
    Ok(report)
}

/// The running state of a verification.
struct Checker {
    bounds: Bounds,
    symbols: HashMap<String, SymbolState>,
    file_end: Option<DateTime<Utc>>,
    report: VerifyReport,
}

/// [`VerifyLimits`] in the form the checks compare against.
#[derive(Debug, Clone, Copy)]
struct Bounds {
    max_snapshot_gap: TimeDelta,
    max_price: i64,
}

/// What the checker remembers about one symbol.
#[derive(Debug, Default)]
struct SymbolState {
    last_time: Option<DateTime<Utc>>,
    went_backwards: bool,
    last_snapshot: Option<DateTime<Utc>>,
    updated_since_snapshot: bool,
}

impl Checker {
    fn new(limits: &VerifyLimits) -> Self {
        let bounds = Bounds {
            max_snapshot_gap: TimeDelta::from_std(limits.max_snapshot_gap)
                .unwrap_or(TimeDelta::MAX),
            max_price: limits.max_price,
        };
        Self {
            bounds,
            symbols: HashMap::new(),
            file_end: None,
            report: VerifyReport::default(),
        }
    }

    fn check_file(&mut self, path: &Path) -> anyhow::Result<()> {
        self.report.files += 1;
        self.check_whole_chunks(path)?;
        let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path)?;
        while let Some(message) = reader.read_msg()? {
            match message {
                AnyMessage::Snapshot(decoder) => {
                    self.check_snapshot(&SnapshotDomain::try_from_decoder(decoder)?);
                }
                AnyMessage::PriceLevelUpdate(decoder) => {
                    self.check_update(&PriceLevelUpdateDomain::try_from_decoder(decoder)?);
                }
                _ => anyhow::bail!("unknown SBE message"),
            }
        }
        self.end_file();
        Ok(())
    }

    fn check_whole_chunks(&mut self, path: &Path) -> anyhow::Result<()> {
        let length = std::fs::metadata(path)?.len();
        let tail = length % CHUNK_SIZE as u64;
        if tail != 0 {
            self.report.record(ViolationKind::PartialChunk, || {
                format!(
                    "{}: the last {tail} bytes are not a whole chunk",
                    path.display()
                )
            });
        }
        Ok(())
    }

    fn check_snapshot(&mut self, snapshot: &SnapshotDomain) {
        self.report.snapshots += 1;
        self.extend_file(snapshot.received_at);
        let state = self.symbols.entry(snapshot.symbol.clone()).or_default();
        let mut findings = Findings {
            report: &mut self.report,
            bounds: self.bounds,
            symbol: &snapshot.symbol,
            at: snapshot.received_at,
        };
        findings.check_time(state);
        findings.check_gap(state);
        let bids = snapshot.bids.iter().map(|e| (e.price_level, e.quantity));
        let asks = snapshot.asks.iter().map(|e| (e.price_level, e.quantity));
        findings.check_side(Side::Buy, &bids.collect::<Vec<_>>());
        findings.check_side(Side::Sell, &asks.collect::<Vec<_>>());
        state.last_snapshot = Some(snapshot.received_at);
        state.updated_since_snapshot = false;
    }

    fn check_update(&mut self, update: &PriceLevelUpdateDomain) {
        self.report.updates += 1;
        self.extend_file(update.backtest_timestamp);
        let state = self.symbols.entry(update.symbol.clone()).or_default();
        let mut findings = Findings {
            report: &mut self.report,
            bounds: self.bounds,
            symbol: &update.symbol,
            at: update.backtest_timestamp,
        };
        findings.check_time(state);
        if state.last_snapshot.is_none() {
            findings.record(ViolationKind::UpdateBeforeSnapshot, || {
                "update before the symbol's first snapshot in this file".to_owned()
            });
        }
        findings.check_level(update.side, update.price_level, update.quantity);
        state.updated_since_snapshot = true;
    }

    /// Checks each symbol's final stretch and forgets its snapshots, so the
    /// next file must start each symbol with a snapshot again.
    fn end_file(&mut self) {
        let Some(file_end) = self.file_end.take() else {
            return;
        };
        for (symbol, state) in &mut self.symbols {
            if let Some(last) = state.last_snapshot
                && state.updated_since_snapshot
                && file_end - last > self.bounds.max_snapshot_gap
            {
                self.report.record(ViolationKind::SnapshotGap, || {
                    let gap = seconds(file_end - last);
                    format!("{symbol} at {file_end}: {gap:.3} s from its last snapshot to the end of the file")
                });
            }
            state.last_snapshot = None;
            state.updated_since_snapshot = false;
        }
    }

    fn extend_file(&mut self, at: DateTime<Utc>) {
        self.report.note_time(at);
        self.file_end = self.file_end.max(Some(at));
    }

    fn into_report(mut self) -> VerifyReport {
        self.report.symbols = self.symbols.len();
        self.report
    }
}

/// Checks one message, for `symbol` at `at`, and records what it violates.
/// It borrows only the report, so the symbol's state can be updated
/// alongside it.
struct Findings<'a> {
    report: &'a mut VerifyReport,
    bounds: Bounds,
    symbol: &'a str,
    at: DateTime<Utc>,
}

impl Findings<'_> {
    fn record(&mut self, kind: ViolationKind, detail: impl FnOnce() -> String) {
        let (symbol, at) = (self.symbol, self.at);
        self.report
            .record(kind, || format!("{symbol} at {at}: {}", detail()));
    }

    /// Checks the message is not earlier than the symbol's latest one so
    /// far, and tracks how many symbols go backwards and by how much.
    fn check_time(&mut self, state: &mut SymbolState) {
        let previous = state.last_time;
        state.last_time = previous.max(Some(self.at));
        let Some(previous) = previous.filter(|&previous| self.at < previous) else {
            return;
        };
        let step = previous - self.at;
        self.report.largest_step_back = self.report.largest_step_back.max(step);
        if !state.went_backwards {
            state.went_backwards = true;
            self.report.symbols_going_backwards += 1;
        }
        let step = seconds(step);
        self.record(ViolationKind::TimeWentBackwards, || {
            format!("{step:.3} s before the symbol's latest message, at {previous}")
        });
    }

    fn check_gap(&mut self, state: &SymbolState) {
        let Some(last) = state.last_snapshot else {
            return;
        };
        let gap = self.at - last;
        if gap > self.bounds.max_snapshot_gap {
            self.record(ViolationKind::SnapshotGap, || {
                format!("{:.3} s since the previous snapshot", seconds(gap))
            });
        }
    }

    /// Checks a snapshot side's levels, given best first: each level, the
    /// strict ordering, and that no level is empty.
    fn check_side(&mut self, side: Side, levels: &[(i64, i64)]) {
        for &(price, quantity) in levels {
            self.check_level(side, price, quantity);
            if quantity == 0 {
                self.record(ViolationKind::ZeroQuantityLevel, || {
                    format!("{side} level at {price} has zero quantity")
                });
            }
        }
        for pair in levels.windows(2) {
            let (better, worse) = (pair[0].0, pair[1].0);
            if !is_strictly_better(side, better, worse) {
                self.record(ViolationKind::UnsortedSnapshotSide, || {
                    format!("{side} level at {better} is listed before {worse}")
                });
            }
        }
    }

    fn check_level(&mut self, side: Side, price: i64, quantity: i64) {
        if quantity < 0 {
            self.record(ViolationKind::NegativeQuantity, || {
                format!("{side} level at {price} has quantity {quantity}")
            });
        }
        let max_price = self.bounds.max_price;
        if !(1..=max_price).contains(&price) {
            self.record(ViolationKind::PriceOutOfRange, || {
                format!("{side} price {price} is outside 1..={max_price}")
            });
        }
    }
}

/// Whether `better` is a strictly better price than `worse` on `side`:
/// higher for bids, lower for asks.
fn is_strictly_better(side: Side, better: i64, worse: i64) -> bool {
    match side {
        Side::Buy => better > worse,
        Side::Sell => better < worse,
    }
}

fn seconds(delta: TimeDelta) -> f64 {
    delta
        .num_microseconds()
        .map_or(f64::INFINITY, |micros| micros as f64 / 1e6)
}
