//! [`Registry`]: which shard records which market, and how full each shard
//! is.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use crate::core::shard::ShardId;

/// The markets a coordinator records and the shard each one is on.
///
/// A new market goes to the shard with the most free slots, the oldest
/// first on a tie, and a new shard opens when every shard is full. Markets
/// never move between shards. A shard whose last market is removed closes.
#[derive(Debug)]
pub(super) struct Registry {
    shard_size: usize,
    owners: HashMap<Arc<str>, ShardId>,
    shards: BTreeMap<ShardId, BTreeSet<Arc<str>>>,
    next_id: u64,
}

/// New markets placed on one shard.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct Placement {
    /// The shard.
    pub(super) shard: ShardId,
    /// Whether the shard is new and has to be started.
    pub(super) opens: bool,
    /// The markets placed on it, in placement order.
    pub(super) symbols: Vec<Arc<str>>,
}

/// What removing a market leaves its shard to do.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Removal {
    /// Unsubscribe the market; the shard has others.
    Unsubscribe(ShardId),
    /// Close the shard; the market was its last.
    Close(ShardId),
}

impl Registry {
    /// An empty registry whose shards hold at most `shard_size` markets.
    pub(super) fn new(shard_size: usize) -> Self {
        Self {
            shard_size: shard_size.max(1),
            owners: HashMap::new(),
            shards: BTreeMap::new(),
            next_id: 0,
        }
    }

    /// Places every market of `symbols` not already registered, and returns
    /// the placements by shard, in shard order.
    pub(super) fn place(&mut self, symbols: impl IntoIterator<Item = Arc<str>>) -> Vec<Placement> {
        let mut placements: BTreeMap<ShardId, Placement> = BTreeMap::new();
        for symbol in symbols {
            if self.owners.contains_key(&symbol) {
                continue;
            }
            let (shard, opens) = self.shard_with_room();
            self.assign(symbol.clone(), shard);
            let placement = placements.entry(shard).or_insert_with(|| Placement {
                shard,
                opens,
                symbols: Vec::new(),
            });
            placement.symbols.push(symbol);
        }
        placements.into_values().collect()
    }

    /// Unregisters `symbol`, saying what its shard must do; `None` if it was
    /// not registered.
    pub(super) fn remove(&mut self, symbol: &str) -> Option<Removal> {
        let shard = self.owners.remove(symbol)?;
        let symbols = self.shards.get_mut(&shard)?;
        symbols.remove(symbol);
        if !symbols.is_empty() {
            return Some(Removal::Unsubscribe(shard));
        }
        self.shards.remove(&shard);
        Some(Removal::Close(shard))
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

    /// The shard with the most free slots, or a new one if every shard is
    /// full; true if it is new.
    fn shard_with_room(&mut self) -> (ShardId, bool) {
        let roomiest = self
            .shards
            .iter()
            .filter(|(_, symbols)| symbols.len() < self.shard_size)
            .max_by_key(|(id, symbols)| (self.shard_size - symbols.len(), Reverse(**id)));
        match roomiest {
            Some((id, _)) => (*id, false),
            None => (self.open(), true),
        }
    }

    fn open(&mut self) -> ShardId {
        let id = ShardId(self.next_id);
        self.next_id += 1;
        self.shards.insert(id, BTreeSet::new());
        id
    }

    fn assign(&mut self, symbol: Arc<str>, shard: ShardId) {
        self.owners.insert(symbol.clone(), shard);
        self.shards.entry(shard).or_default().insert(symbol);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn symbols(names: &[&str]) -> Vec<Arc<str>> {
        names.iter().map(|&name| Arc::from(name)).collect()
    }

    fn placement(shard: u64, opens: bool, names: &[&str]) -> Placement {
        Placement {
            shard: ShardId(shard),
            opens,
            symbols: symbols(names),
        }
    }

    fn registry_with(shard_size: usize, names: &[&str]) -> Registry {
        let mut registry = Registry::new(shard_size);
        registry.place(symbols(names));
        registry
    }

    #[test]
    fn markets_fill_each_shard_before_a_new_one_opens() {
        let mut registry = Registry::new(3);
        let placements = registry.place(symbols(&["A", "B", "C", "D", "E", "F", "G"]));
        let expected = vec![
            placement(0, true, &["A", "B", "C"]),
            placement(1, true, &["D", "E", "F"]),
            placement(2, true, &["G"]),
        ];
        assert_eq!(placements, expected);
    }

    #[test]
    fn a_new_market_goes_to_the_shard_with_the_most_free_slots() {
        let mut registry = registry_with(3, &["A", "B", "C", "D", "E", "F"]);
        registry.remove("A");
        registry.remove("D");
        registry.remove("E");
        let placements = registry.place(symbols(&["X"]));
        assert_eq!(placements, vec![placement(1, false, &["X"])]);
    }

    #[test]
    fn a_tie_goes_to_the_oldest_shard() {
        let mut registry = registry_with(3, &["A", "B", "C", "D", "E", "F"]);
        registry.remove("A");
        registry.remove("D");
        let placements = registry.place(symbols(&["X"]));
        assert_eq!(placements, vec![placement(0, false, &["X"])]);
    }

    #[test]
    fn a_page_can_top_up_one_shard_and_open_another() {
        let mut registry = registry_with(3, &["A", "B"]);
        let placements = registry.place(symbols(&["C", "D"]));
        let expected = vec![placement(0, false, &["C"]), placement(1, true, &["D"])];
        assert_eq!(placements, expected);
    }

    #[test]
    fn a_registered_market_is_not_placed_again() {
        let mut registry = registry_with(3, &["A"]);
        let placements = registry.place(symbols(&["A", "B", "B"]));
        assert_eq!(
            (placements, registry.market_count()),
            (vec![placement(0, false, &["B"])], 2)
        );
    }

    #[test]
    fn removing_a_market_from_a_shard_with_others_unsubscribes_it() {
        let mut registry = registry_with(3, &["A", "B"]);
        assert_eq!(registry.remove("A"), Some(Removal::Unsubscribe(ShardId(0))));
    }

    #[test]
    fn removing_the_last_market_of_a_shard_closes_it() {
        let mut registry = registry_with(3, &["A", "B", "C", "D"]);
        let removal = registry.remove("D");
        assert_eq!(
            (removal, registry.shard_count()),
            (Some(Removal::Close(ShardId(1))), 1)
        );
    }

    #[test]
    fn a_closed_shards_id_is_not_reused() {
        let mut registry = registry_with(1, &["A"]);
        registry.remove("A");
        assert_eq!(
            registry.place(symbols(&["B"])),
            vec![placement(1, true, &["B"])]
        );
    }

    #[test]
    fn removing_an_unknown_market_does_nothing() {
        let mut registry = registry_with(3, &["A"]);
        assert_eq!((registry.remove("Z"), registry.market_count()), (None, 1));
    }

    #[test]
    fn the_owner_is_the_shard_a_market_was_placed_on() {
        let registry = registry_with(2, &["A", "B", "C"]);
        assert_eq!(
            (registry.owner("C"), registry.owner("Z")),
            (Some(ShardId(1)), None)
        );
    }
}
