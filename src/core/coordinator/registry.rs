//! [`Registry`]: which shard records which market, how full each shard is,
//! and what changed since the shards were last told.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use crate::core::shard::ShardId;
use crate::core::writer::{DEFAULT_SNAPSHOT_TICK, FORGOTTEN_RETENTION};

/// How much the writer's lag behind the registry may shrink between a
/// departed market's `Forget` and the market's return. The writer starts
/// the retention when it processes the `Forget`, and meets the returning
/// market when it processes the new subscription's snapshot. Measured
/// against the returning market's events, the writer's retention runs out
/// later than the registry's by as much as its lag shrank in between. The
/// shrink is at most the lag at the `Forget`: how long the writer takes to
/// drain what is queued ahead of it, which depends on how fast it drains.
/// 30 s is about a full writer channel at the expected event rate.
const WRITER_LAG_ALLOWANCE: Duration = Duration::from_secs(30);

/// The longest snapshot tick allowed for. The writer prunes the symbols it
/// has forgotten on every tick.
const PRUNE_TICK_ALLOWANCE: Duration = Duration::from_secs(1);

/// How long past [`FORGOTTEN_RETENTION`] the registry remembers a departed
/// market. Measured against the returning market's events, the writer's
/// retention runs out later than the registry's by as much as its lag
/// shrank between the market's `Forget` and its return, up to
/// [`WRITER_LAG_ALLOWANCE`], and the writer drops the market up to a tick
/// past the retention. The grace exceeds both together, so a market
/// returning after the registry forgot it reaches a writer that has
/// stopped dropping its events, and needs no `Track`. A lag shrinking even
/// further is caught by the writer itself: a level change that finds no
/// book asks for a fresh snapshot.
const DEPARTED_GRACE: Duration = Duration::from_secs(60);

const _: () = assert!(
    DEPARTED_GRACE.as_millis()
        > WRITER_LAG_ALLOWANCE.as_millis() + PRUNE_TICK_ALLOWANCE.as_millis()
);
const _: () = assert!(DEFAULT_SNAPSHOT_TICK.as_millis() <= PRUNE_TICK_ALLOWANCE.as_millis());

/// The markets a coordinator records and the shard each one is on.
///
/// A market removed and placed again before the next take goes back to its
/// shard: slots freed since the last take stay reserved until it, so the
/// old subscription's last messages and the new subscription's snapshot
/// arrive on one connection, in order. A market that departed at an earlier
/// take goes back to its last shard if that shard is still open and has
/// room. Any other new market goes to the shard with the most free slots,
/// the oldest first on a tie, and a new shard opens when every shard is
/// full.
///
/// Changes collect until [`Registry::take_changes`], which reports their net
/// effect per shard. A shard whose last market is removed stays open, and
/// can take markets again, until then.
///
/// Departed markets are remembered for [`FORGOTTEN_RETENTION`] plus
/// [`DEPARTED_GRACE`]. Once [`Registry::expire_departed`] drops one, it
/// comes back as a new market, which the writer has stopped dropping by
/// then unless its lag shrank by more than [`WRITER_LAG_ALLOWANCE`].
#[derive(Debug)]
pub(super) struct Registry {
    shard_size: usize,
    owners: HashMap<Arc<str>, ShardId>,
    shards: BTreeMap<ShardId, BTreeSet<Arc<str>>>,
    next_id: u64,
    /// Markets changed since the last take, each with the shard it was on
    /// before its first change.
    pending: BTreeMap<Arc<str>, Option<ShardId>>,
    /// Slots of markets that were on each shard at the last take and have
    /// been removed since, reserved for their return until the next take.
    freed: BTreeMap<ShardId, usize>,
    /// Shards opened since the last take.
    opened: BTreeSet<ShardId>,
    /// Markets removed by an earlier take: the shard each was last on, and
    /// when.
    departed: HashMap<Arc<str>, (ShardId, Instant)>,
}

/// The net effect of the registry changes since the last take.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct Changes {
    /// New shards to start, each with its markets.
    pub(super) opened: Vec<(ShardId, BTreeSet<Arc<str>>)>,
    /// Running shards whose markets changed.
    pub(super) updated: BTreeMap<ShardId, Update>,
    /// Running shards with no markets left, to close.
    pub(super) closed: Vec<ShardId>,
    /// Markets no longer recorded.
    pub(super) departed: Vec<Arc<str>>,
    /// Markets recorded again after an earlier take reported them departed.
    pub(super) returned: Vec<Arc<str>>,
}

/// One market's net move since the last take: the shard it was on before
/// and the one it is on now.
struct Move {
    symbol: Arc<str>,
    before: Option<ShardId>,
    after: Option<ShardId>,
}

/// The change to one running shard's markets.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct Update {
    /// Markets to unsubscribe.
    pub(super) removed: Vec<Arc<str>>,
    /// Markets to subscribe.
    pub(super) added: Vec<Arc<str>>,
}

impl Registry {
    /// An empty registry whose shards hold at most `shard_size` markets.
    pub(super) fn new(shard_size: usize) -> Self {
        Self {
            shard_size: shard_size.max(1),
            owners: HashMap::new(),
            shards: BTreeMap::new(),
            next_id: 0,
            pending: BTreeMap::new(),
            freed: BTreeMap::new(),
            opened: BTreeSet::new(),
            departed: HashMap::new(),
        }
    }

    /// Places `symbol` on a shard; false if it was already registered.
    pub(super) fn place(&mut self, symbol: &Arc<str>) -> bool {
        if self.owners.contains_key(symbol) {
            return false;
        }
        let shard = self.shard_for(symbol);
        if self.reserved_shard(symbol) == Some(shard) {
            self.change_freed(shard, -1);
        }
        self.note_pending(symbol, None);
        self.owners.insert(symbol.clone(), shard);
        self.shards.entry(shard).or_default().insert(symbol.clone());
        true
    }

    /// Unregisters `symbol`; false if it was not registered.
    pub(super) fn remove(&mut self, symbol: &Arc<str>) -> bool {
        let Some(shard) = self.owners.remove(symbol) else {
            return false;
        };
        self.note_pending(symbol, Some(shard));
        if self.reserved_shard(symbol) == Some(shard) {
            self.change_freed(shard, 1);
        }
        if let Some(symbols) = self.shards.get_mut(&shard) {
            symbols.remove(symbol);
        }
        true
    }

    /// The net effect of every change since the last take, made at `now`,
    /// which empty shards close, and starts collecting again.
    pub(super) fn take_changes(&mut self, now: Instant) -> Changes {
        let opened = std::mem::take(&mut self.opened);
        self.freed.clear();
        let mut changes = Changes::default();
        for (symbol, before) in std::mem::take(&mut self.pending) {
            let after = self.owners.get(&symbol).copied();
            let moved = Move {
                symbol,
                before,
                after,
            };
            self.diff_shards(&mut changes, &moved, &opened);
            self.diff_recorded(&mut changes, moved, now);
        }
        self.drop_empty_shards(&mut changes, &opened);
        changes.opened = self.symbols_of(&opened);
        changes
    }

    /// Forgets the markets that departed longer ago than
    /// [`FORGOTTEN_RETENTION`] plus the grace period, at `now`.
    pub(super) fn expire_departed(&mut self, now: Instant) {
        let retention = FORGOTTEN_RETENTION + DEPARTED_GRACE;
        let current = |(_, at): &(ShardId, Instant)| now.duration_since(*at) <= retention;
        self.departed.retain(|_, departure| current(departure));
    }

    /// The shard `symbol` is on, if it is registered.
    pub(super) fn owner(&self, symbol: &str) -> Option<ShardId> {
        self.owners.get(symbol).copied()
    }

    /// How many shards are open.
    pub(super) fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// How many markets are registered.
    pub(super) fn market_count(&self) -> usize {
        self.owners.len()
    }

    /// Remembers the shard `symbol` was on before its first change since
    /// the last take.
    fn note_pending(&mut self, symbol: &Arc<str>, before: Option<ShardId>) {
        self.pending.entry(symbol.clone()).or_insert(before);
    }

    /// The shard whose freed slot is reserved for `symbol`: the one it was
    /// on at the last take, if it has been removed from it since.
    fn reserved_shard(&self, symbol: &str) -> Option<ShardId> {
        self.pending.get(symbol).copied().flatten()
    }

    fn change_freed(&mut self, shard: ShardId, by: isize) {
        let freed = self.freed.entry(shard).or_default();
        *freed = freed.saturating_add_signed(by);
    }

    /// The shard for `symbol`: its reserved slot, else its last shard if
    /// it departed at an earlier take and that shard has room, else the
    /// roomiest shard.
    fn shard_for(&mut self, symbol: &str) -> ShardId {
        if let Some(shard) = self.reserved_shard(symbol) {
            return shard;
        }
        let departed = self.departed.get(symbol).map(|(shard, _)| *shard);
        match departed.filter(|&shard| self.has_room(shard)) {
            Some(shard) => shard,
            None => self.shard_with_room(),
        }
    }

    /// The slots of `shard` taken, counting those reserved for markets
    /// removed since the last take.
    fn load(&self, shard: ShardId, symbols: &BTreeSet<Arc<str>>) -> usize {
        symbols.len() + self.freed.get(&shard).copied().unwrap_or(0)
    }

    fn has_room(&self, shard: ShardId) -> bool {
        let symbols = self.shards.get(&shard);
        symbols.is_some_and(|symbols| self.load(shard, symbols) < self.shard_size)
    }

    /// The shard with the most free slots, or a new one if every shard is
    /// full.
    fn shard_with_room(&mut self) -> ShardId {
        let free = |(id, symbols): (&ShardId, &BTreeSet<Arc<str>>)| {
            let load = self.load(*id, symbols);
            (load < self.shard_size).then(|| (self.shard_size - load, Reverse(*id)))
        };
        let roomiest = self.shards.iter().filter_map(free).max();
        match roomiest {
            Some((_, Reverse(id))) => id,
            None => self.open(),
        }
    }

    fn open(&mut self) -> ShardId {
        let id = ShardId(self.next_id);
        self.next_id += 1;
        self.shards.insert(id, BTreeSet::new());
        self.opened.insert(id);
        id
    }

    /// Adds the subscription changes for one market's move. A shard opened
    /// since the last take subscribes its markets when it starts, and a
    /// shard left empty closes, so neither gets an update.
    fn diff_shards(&self, changes: &mut Changes, moved: &Move, opened: &BTreeSet<ShardId>) {
        if moved.before == moved.after {
            return;
        }
        let has_markets = |shard| self.shards.get(&shard).is_some_and(|set| !set.is_empty());
        if let Some(shard) = moved.before.filter(|&shard| has_markets(shard)) {
            let update = changes.updated.entry(shard).or_default();
            update.removed.push(moved.symbol.clone());
        }
        if let Some(shard) = moved.after.filter(|shard| !opened.contains(shard)) {
            let update = changes.updated.entry(shard).or_default();
            update.added.push(moved.symbol.clone());
        }
    }

    /// Notes a market that stopped being recorded, or started again after
    /// an earlier take reported it departed.
    fn diff_recorded(&mut self, changes: &mut Changes, moved: Move, now: Instant) {
        match (moved.before, moved.after) {
            (Some(shard), None) => {
                self.departed.insert(moved.symbol.clone(), (shard, now));
                changes.departed.push(moved.symbol);
            }
            (None, Some(_)) if self.departed.remove(&moved.symbol).is_some() => {
                changes.returned.push(moved.symbol);
            }
            _ => {}
        }
    }

    /// Drops every empty shard, reporting those that were running as
    /// closed.
    fn drop_empty_shards(&mut self, changes: &mut Changes, opened: &BTreeSet<ShardId>) {
        let empty: Vec<ShardId> = self
            .shards
            .iter()
            .filter(|(_, symbols)| symbols.is_empty())
            .map(|(id, _)| *id)
            .collect();
        for shard in empty {
            self.shards.remove(&shard);
            if !opened.contains(&shard) {
                changes.closed.push(shard);
            }
        }
    }

    /// The markets of each of `shards` that is still open.
    fn symbols_of(&self, shards: &BTreeSet<ShardId>) -> Vec<(ShardId, BTreeSet<Arc<str>>)> {
        let open = |id: &ShardId| Some((*id, self.shards.get(id)?.clone()));
        shards.iter().filter_map(open).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::coordinator::LifecycleTimings;
    use crate::core::writer::Forgotten;

    fn symbols(names: &[&str]) -> Vec<Arc<str>> {
        names.iter().map(|&name| Arc::from(name)).collect()
    }

    fn set(names: &[&str]) -> BTreeSet<Arc<str>> {
        symbols(names).into_iter().collect()
    }

    fn place_all(registry: &mut Registry, names: &[&str]) {
        for symbol in symbols(names) {
            registry.place(&symbol);
        }
    }

    fn remove(registry: &mut Registry, name: &str) -> bool {
        registry.remove(&Arc::from(name))
    }

    fn take(registry: &mut Registry) -> Changes {
        registry.take_changes(Instant::now())
    }

    /// A registry with `names` placed and their changes taken.
    fn registry_with(shard_size: usize, names: &[&str]) -> Registry {
        let mut registry = Registry::new(shard_size);
        place_all(&mut registry, names);
        take(&mut registry);
        registry
    }

    fn owners(registry: &Registry, names: &[&str]) -> Vec<Option<u64>> {
        let owner = |name: &&str| registry.owner(name).map(|shard| shard.0);
        names.iter().map(owner).collect()
    }

    fn update(removed: &[&str], added: &[&str]) -> Update {
        Update {
            removed: symbols(removed),
            added: symbols(added),
        }
    }

    #[test]
    fn markets_fill_each_shard_before_a_new_one_opens() {
        let registry = registry_with(3, &["A", "B", "C", "D", "E", "F", "G"]);
        let names = ["A", "B", "C", "D", "E", "F", "G"];
        let expected = [0, 0, 0, 1, 1, 1, 2].map(Some);
        assert_eq!(owners(&registry, &names), expected);
    }

    #[test]
    fn new_shards_open_with_all_their_markets() {
        let mut registry = Registry::new(3);
        place_all(&mut registry, &["A", "B", "C", "D"]);
        let expected = vec![
            (ShardId(0), set(&["A", "B", "C"])),
            (ShardId(1), set(&["D"])),
        ];
        assert_eq!(take(&mut registry).opened, expected);
    }

    #[test]
    fn a_new_market_goes_to_the_shard_with_the_most_free_slots() {
        let mut registry = registry_with(3, &["A", "B", "C", "D", "E", "F"]);
        for name in ["A", "D", "E"] {
            remove(&mut registry, name);
        }
        take(&mut registry);
        place_all(&mut registry, &["X"]);
        assert_eq!(registry.owner("X"), Some(ShardId(1)));
    }

    #[test]
    fn a_tie_goes_to_the_oldest_shard() {
        let mut registry = registry_with(3, &["A", "B", "C", "D", "E", "F"]);
        remove(&mut registry, "A");
        remove(&mut registry, "D");
        take(&mut registry);
        place_all(&mut registry, &["X"]);
        assert_eq!(registry.owner("X"), Some(ShardId(0)));
    }

    #[test]
    fn a_returning_market_prefers_its_previous_shard() {
        let mut registry = registry_with(3, &["A", "B", "C", "D"]);
        remove(&mut registry, "A");
        take(&mut registry);
        place_all(&mut registry, &["A"]);
        assert_eq!(registry.owner("A"), Some(ShardId(0)));
    }

    #[test]
    fn a_returning_market_whose_previous_shard_is_full_goes_elsewhere() {
        let mut registry = registry_with(2, &["A", "B", "C"]);
        remove(&mut registry, "A");
        take(&mut registry);
        place_all(&mut registry, &["X", "A"]);
        assert_eq!(owners(&registry, &["X", "A"]), [Some(0), Some(1)]);
    }

    #[test]
    fn a_registered_market_is_not_placed_again() {
        let mut registry = registry_with(3, &["A"]);
        let placed = registry.place(&Arc::from("A"));
        assert_eq!((placed, registry.market_count()), (false, 1));
    }

    #[test]
    fn removing_an_unknown_market_does_nothing() {
        let mut registry = registry_with(3, &["A"]);
        let removed = remove(&mut registry, "Z");
        let changes = take(&mut registry);
        assert_eq!((removed, changes), (false, Changes::default()));
    }

    #[test]
    fn changes_to_a_running_shard_are_one_update() {
        let mut registry = registry_with(4, &["A", "B"]);
        remove(&mut registry, "A");
        place_all(&mut registry, &["X", "Y"]);
        let changes = take(&mut registry);
        let expected = BTreeMap::from([(ShardId(0), update(&["A"], &["X", "Y"]))]);
        assert_eq!(changes.updated, expected);
    }

    #[test]
    fn a_removed_market_departs() {
        let mut registry = registry_with(3, &["A", "B"]);
        remove(&mut registry, "A");
        assert_eq!(take(&mut registry).departed, symbols(&["A"]));
    }

    #[test]
    fn a_market_removed_and_placed_again_before_a_take_changes_nothing() {
        let mut registry = registry_with(3, &["A", "B"]);
        remove(&mut registry, "A");
        place_all(&mut registry, &["A"]);
        assert_eq!(take(&mut registry), Changes::default());
    }

    #[test]
    fn a_market_placed_after_departing_returns() {
        let mut registry = registry_with(3, &["A", "B"]);
        remove(&mut registry, "A");
        take(&mut registry);
        place_all(&mut registry, &["A"]);
        let changes = take(&mut registry);
        let expected = BTreeMap::from([(ShardId(0), update(&[], &["A"]))]);
        assert_eq!(
            (changes.updated, changes.returned),
            (expected, symbols(&["A"]))
        );
    }

    #[test]
    fn removing_the_last_market_of_a_shard_closes_it_at_the_take() {
        let mut registry = registry_with(3, &["A", "B", "C", "D"]);
        remove(&mut registry, "D");
        let changes = take(&mut registry);
        let shards = registry.shard_count();
        assert_eq!(
            (changes.closed, changes.updated.len(), shards),
            (vec![ShardId(1)], 0, 1)
        );
    }

    #[test]
    fn an_emptied_shard_takes_markets_until_the_take() {
        let mut registry = registry_with(3, &["A", "B", "C", "D"]);
        remove(&mut registry, "D");
        place_all(&mut registry, &["D"]);
        assert_eq!(take(&mut registry), Changes::default());
    }

    #[test]
    fn a_closed_shards_id_is_not_reused() {
        let mut registry = registry_with(1, &["A"]);
        remove(&mut registry, "A");
        take(&mut registry);
        place_all(&mut registry, &["B"]);
        assert_eq!(registry.owner("B"), Some(ShardId(1)));
    }

    /// X may not take A's freed slot, so A goes back to its own shard
    /// rather than moving to another before the take.
    #[test]
    fn a_market_removed_and_placed_again_before_a_take_gets_its_slot_back() {
        let mut registry = registry_with(2, &["A", "B", "C"]);
        remove(&mut registry, "A");
        place_all(&mut registry, &["X", "A"]);
        assert_eq!(owners(&registry, &["X", "A"]), [Some(1), Some(0)]);
    }

    #[test]
    fn freed_slots_are_free_again_after_the_take() {
        let mut registry = registry_with(2, &["A", "B", "C"]);
        remove(&mut registry, "A");
        take(&mut registry);
        place_all(&mut registry, &["X"]);
        assert_eq!(registry.owner("X"), Some(ShardId(0)));
    }

    /// The changes when A, which departed from a shard it shared with B,
    /// is placed again after an expiry `after` its departure.
    fn departed_then_expired(after: Duration) -> Changes {
        let mut registry = registry_with(3, &["A", "B"]);
        remove(&mut registry, "A");
        let departed_at = Instant::now();
        registry.take_changes(departed_at);
        registry.expire_departed(departed_at + after);
        place_all(&mut registry, &["A"]);
        take(&mut registry)
    }

    #[test]
    fn a_departed_market_returns_within_the_retention() {
        let retention = FORGOTTEN_RETENTION + DEPARTED_GRACE;
        let changes = departed_then_expired(retention);
        assert_eq!(changes.returned, symbols(&["A"]));
    }

    /// The time step of a replayed departure.
    const STEP: Duration = Duration::from_millis(50);

    /// How many steps `period` spans.
    fn steps(period: Duration) -> u32 {
        u32::try_from(period.as_millis() / STEP.as_millis()).unwrap()
    }

    /// A recurring timer, in steps after a departure.
    #[derive(Clone, Copy)]
    struct Timer {
        period: u32,
        phase: u32,
    }

    impl Timer {
        /// A timer firing every `period` steps, `phase` steps into each.
        fn new(period: u32, phase: u32) -> Self {
            Self { period, phase }
        }

        fn fires_at(self, step: u32) -> bool {
            step % self.period == self.phase
        }
    }

    /// The writer's snapshot ticks, which prune what it has forgotten, and
    /// the registry's sweeps, which expire departed markets.
    struct Timers {
        tick: Timer,
        sweep: Timer,
    }

    /// Market A, departed from the registry, as the registry and the
    /// writer each see it.
    struct Departure {
        registry: Registry,
        writer: Forgotten,
        at: Instant,
    }

    impl Departure {
        /// A departs now, and the writer processes its `Forget` the whole
        /// [`WRITER_LAG_ALLOWANCE`] later. A return then reaching the writer
        /// at once is the most the lag can shrink.
        fn new() -> Self {
            let mut registry = registry_with(3, &["A", "B"]);
            remove(&mut registry, "A");
            let at = Instant::now();
            registry.take_changes(at);
            let mut writer = Forgotten::default();
            writer.insert("A".into(), at + WRITER_LAG_ALLOWANCE);
            Self {
                registry,
                writer,
                at,
            }
        }

        /// Runs whatever `timers` fire at `step`.
        fn advance(&mut self, step: u32, timers: &Timers) {
            let now = self.at + STEP * step;
            if timers.tick.fires_at(step) {
                self.writer.prune(now);
            }
            if timers.sweep.fires_at(step) {
                self.registry.expire_departed(now);
            }
        }

        /// Whether the registry remembers A, if the writer still drops it.
        fn remembered_while_dropped(&self) -> bool {
            self.registry.departed.contains_key("A") || !self.writer.contains("A")
        }
    }

    /// Replays a departure in 50 ms steps, with the writer's real pruning,
    /// until a couple of steps past the first sweep that may expire it: the
    /// registry must remember the market for as long as the writer drops
    /// its events, and forget it in the end.
    fn replay_departure(timers: &Timers) {
        let mut departure = Departure::new();
        let last = steps(FORGOTTEN_RETENTION + DEPARTED_GRACE) + timers.sweep.period + 2;
        let any_fires = |step: &u32| timers.tick.fires_at(*step) || timers.sweep.fires_at(*step);
        for step in (1..=last).filter(any_fires) {
            departure.advance(step, timers);
            let at = STEP * step;
            assert!(departure.remembered_while_dropped(), "at {at:?}");
        }
        assert!(!departure.registry.departed.contains_key("A"));
    }

    /// Replays the departure at every phase, in 50 ms steps, of the
    /// writer's default ticks and the registry's default sweeps.
    #[test]
    fn the_registry_remembers_a_departed_market_while_the_writer_drops_it() {
        let tick = steps(DEFAULT_SNAPSHOT_TICK);
        let sweep = steps(LifecycleTimings::default().sweep_interval);
        for tick_phase in 0..tick {
            for sweep_phase in 0..sweep {
                replay_departure(&Timers {
                    tick: Timer::new(tick, tick_phase),
                    sweep: Timer::new(sweep, sweep_phase),
                });
            }
        }
    }

    #[test]
    fn a_departed_market_comes_back_as_new_after_the_retention() {
        let retention = FORGOTTEN_RETENTION + DEPARTED_GRACE;
        let changes = departed_then_expired(retention + Duration::from_secs(1));
        assert_eq!(changes.returned, Vec::<Arc<str>>::new());
    }

    #[test]
    fn a_shard_opened_and_emptied_before_a_take_is_never_started() {
        let mut registry = registry_with(1, &["A"]);
        place_all(&mut registry, &["B"]);
        remove(&mut registry, "B");
        let changes = take(&mut registry);
        assert_eq!((changes, registry.shard_count()), (Changes::default(), 1));
    }
}
