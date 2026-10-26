//! Client-side request pacing: a [`TokenBucket`] for the steady rate and
//! [`Backoff`] for retries after the server pushes back.

use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use tokio::time::Instant;

/// Paces requests to a steady rate, allowing a burst of up to `capacity`
/// requests after an idle spell. Callers that arrive while the bucket is empty
/// queue up in order.
///
/// The budget covers only the requests made through this bucket. Every task
/// that shares a rate limit must share one bucket.
pub(crate) struct TokenBucket {
    per_second: f64,
    capacity: f64,
    state: Mutex<BucketState>,
}

struct BucketState {
    tokens: f64,
    refilled_at: Instant,
}

impl TokenBucket {
    /// A full bucket of `capacity` tokens that refills at `per_second` tokens
    /// per second.
    pub(crate) fn new(per_second: u32, capacity: u32) -> Self {
        let capacity = f64::from(capacity);
        Self {
            per_second: f64::from(per_second),
            capacity,
            state: Mutex::new(BucketState {
                tokens: capacity,
                refilled_at: Instant::now(),
            }),
        }
    }

    /// Waits until one more request may be sent.
    pub(crate) async fn acquire(&self) {
        tokio::time::sleep(self.reserve()).await;
    }

    /// Takes a token, borrowing against future refills if the bucket is
    /// empty, and returns how long to wait before the token is valid.
    fn reserve(&self) -> Duration {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let now = Instant::now();
        let refill = now.duration_since(state.refilled_at).as_secs_f64() * self.per_second;
        state.tokens = (state.tokens + refill).min(self.capacity) - 1.0;
        state.refilled_at = now;
        Duration::from_secs_f64((-state.tokens).max(0.0) / self.per_second)
    }
}

/// Exponential backoff: the delay doubles after each failed attempt, up to a
/// cap, and gives up after a fixed number of attempts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Backoff {
    /// Delay after the first failed attempt.
    pub first_delay: Duration,
    /// Longest delay between attempts.
    pub max_delay: Duration,
    /// Attempts made in total, including the first, before giving up.
    pub max_attempts: u32,
}

impl Backoff {
    /// How long to wait after failed attempt number `attempt` (counting from
    /// 1), or `None` when no attempts remain.
    pub(crate) fn delay_after(&self, attempt: u32) -> Option<Duration> {
        if attempt >= self.max_attempts {
            return None;
        }
        let factor = 2u32.saturating_pow(attempt.saturating_sub(1));
        Some(self.first_delay.saturating_mul(factor).min(self.max_delay))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn seconds_to_acquire(bucket: &TokenBucket, count: usize) -> f64 {
        let start = Instant::now();
        for _ in 0..count {
            bucket.acquire().await;
        }
        start.elapsed().as_secs_f64()
    }

    #[tokio::test(start_paused = true)]
    async fn full_bucket_allows_a_burst_of_its_capacity() {
        let bucket = TokenBucket::new(15, 5);
        assert_eq!(seconds_to_acquire(&bucket, 5).await, 0.0);
    }

    #[tokio::test(start_paused = true)]
    async fn empty_bucket_spaces_requests_at_the_refill_rate() {
        let bucket = TokenBucket::new(15, 5);
        let elapsed = seconds_to_acquire(&bucket, 35).await;
        assert!((elapsed - 2.0).abs() < 0.01, "elapsed {elapsed} s");
    }

    #[tokio::test(start_paused = true)]
    async fn idle_time_refills_the_bucket_only_up_to_its_capacity() {
        let bucket = TokenBucket::new(15, 5);
        seconds_to_acquire(&bucket, 5).await;
        tokio::time::advance(Duration::from_secs(10)).await;
        let elapsed = seconds_to_acquire(&bucket, 6).await;
        assert!((elapsed - 1.0 / 15.0).abs() < 0.002, "elapsed {elapsed} s");
    }

    #[test]
    fn backoff_doubles_from_one_second_up_to_the_cap_then_gives_up() {
        let backoff = Backoff {
            first_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
            max_attempts: 8,
        };
        let delays: Vec<_> = (1..=8)
            .map(|attempt| backoff.delay_after(attempt).map(|delay| delay.as_secs()))
            .collect();
        assert_eq!(
            delays,
            [
                Some(1),
                Some(2),
                Some(4),
                Some(8),
                Some(16),
                Some(30),
                Some(30),
                None
            ]
        );
    }
}
