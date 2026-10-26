//! Client-side request pacing: a [`TokenBucket`] for the steady rate and
//! [`Backoff`] for retries after the server pushes back.

use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use tokio::time::Instant;

/// Paces requests to a steady rate, allowing a burst of up to one second's
/// worth. Callers that arrive while the bucket is empty queue up in order.
pub(crate) struct TokenBucket {
    per_second: f64,
    state: Mutex<BucketState>,
}

struct BucketState {
    tokens: f64,
    refilled_at: Instant,
}

impl TokenBucket {
    /// A full bucket that refills at `per_second` tokens per second.
    pub(crate) fn new(per_second: u32) -> Self {
        let per_second = f64::from(per_second);
        Self {
            per_second,
            state: Mutex::new(BucketState {
                tokens: per_second,
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
        state.tokens = (state.tokens + refill).min(self.per_second) - 1.0;
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

    #[tokio::test(start_paused = true)]
    async fn full_bucket_allows_one_second_of_requests_at_once() {
        let bucket = TokenBucket::new(15);
        let start = Instant::now();
        for _ in 0..15 {
            bucket.acquire().await;
        }
        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn empty_bucket_spaces_requests_at_the_refill_rate() {
        let bucket = TokenBucket::new(15);
        let start = Instant::now();
        for _ in 0..45 {
            bucket.acquire().await;
        }
        let elapsed = start.elapsed().as_secs_f64();
        assert!((elapsed - 2.0).abs() < 0.01, "elapsed {elapsed} s");
    }

    #[tokio::test(start_paused = true)]
    async fn idle_time_refills_the_bucket() {
        let bucket = TokenBucket::new(15);
        for _ in 0..15 {
            bucket.acquire().await;
        }
        tokio::time::advance(Duration::from_secs(1)).await;
        let start = Instant::now();
        for _ in 0..15 {
            bucket.acquire().await;
        }
        assert_eq!(start.elapsed(), Duration::ZERO);
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
