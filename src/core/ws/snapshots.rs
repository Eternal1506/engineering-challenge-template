//! [`SnapshotQueue`]: snapshot requests waiting to go out in paced batches,
//! so asking for a whole subscription's books never lands on the exchange
//! as one burst. A symbol waits in the queue at most once.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

/// One snapshot command's worth of a request.
#[derive(Debug)]
pub(super) struct SnapshotBatch {
    /// The owner's tag for the request the batch belongs to.
    pub(super) tag: u64,
    /// The symbols to snapshot.
    pub(super) symbols: Vec<Arc<str>>,
}

/// The batches still to send on one socket, and when the next may go.
#[derive(Debug)]
pub(super) struct SnapshotQueue {
    batches: VecDeque<SnapshotBatch>,
    next_at: Instant,
}

impl SnapshotQueue {
    /// An empty queue whose first batch may go at once.
    pub(super) fn new() -> Self {
        Self {
            batches: VecDeque::new(),
            next_at: Instant::now(),
        }
    }

    /// Queues `symbols`, asked for under `tag`, in batches of at most
    /// `size`. Symbols already waiting leave their earlier batches, so the
    /// latest request covers them.
    pub(super) fn push(&mut self, tag: u64, symbols: &[Arc<str>], size: usize) {
        self.forget(symbols);
        for chunk in symbols.chunks(size.max(1)) {
            let symbols = chunk.to_vec();
            self.batches.push_back(SnapshotBatch { tag, symbols });
        }
    }

    /// Takes `symbols` out of the batches waiting, dropping batches left
    /// empty.
    fn forget(&mut self, symbols: &[Arc<str>]) {
        let covered: HashSet<&Arc<str>> = symbols.iter().collect();
        for batch in &mut self.batches {
            batch.symbols.retain(|symbol| !covered.contains(symbol));
        }
        self.batches.retain(|batch| !batch.symbols.is_empty());
    }

    /// When the next batch may go out, while one waits.
    pub(super) fn due_at(&self) -> Option<Instant> {
        (!self.batches.is_empty()).then_some(self.next_at)
    }

    /// Takes the next batch; the one after may go `interval` from now.
    pub(super) fn pop(&mut self, interval: Duration) -> Option<SnapshotBatch> {
        let batch = self.batches.pop_front()?;
        self.next_at = Instant::now() + interval;
        Some(batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn symbols(count: usize) -> Vec<Arc<str>> {
        (0..count).map(|n| Arc::from(n.to_string())).collect()
    }

    #[test]
    fn a_request_is_split_into_batches_of_at_most_the_size() {
        let mut queue = SnapshotQueue::new();
        queue.push(7, &symbols(450), 200);
        let sizes: Vec<_> = std::iter::from_fn(|| queue.pop(Duration::ZERO))
            .map(|batch| (batch.tag, batch.symbols.len()))
            .collect();
        assert_eq!(sizes, [(7, 200), (7, 200), (7, 50)]);
    }

    #[test]
    fn a_new_request_takes_its_symbols_out_of_earlier_batches() {
        let mut queue = SnapshotQueue::new();
        queue.push(1, &symbols(4), 2);
        queue.push(2, &symbols(3), 10);
        let batches: Vec<_> = std::iter::from_fn(|| queue.pop(Duration::ZERO))
            .map(|batch| (batch.tag, batch.symbols))
            .collect();
        assert_eq!(batches, [(1, symbols(4)[3..].to_vec()), (2, symbols(3))]);
    }

    #[test]
    fn an_empty_queue_is_never_due() {
        assert_eq!(SnapshotQueue::new().due_at(), None);
    }

    #[test]
    fn the_first_batch_is_due_at_once_and_the_next_after_the_interval() {
        let mut queue = SnapshotQueue::new();
        queue.push(1, &symbols(2), 1);
        let first = queue.due_at().unwrap();
        let popped_at = Instant::now();
        queue.pop(Duration::from_secs(1));
        let second = queue.due_at().unwrap();
        let on_time = first <= popped_at && second >= popped_at + Duration::from_secs(1);
        assert!(on_time);
    }
}
