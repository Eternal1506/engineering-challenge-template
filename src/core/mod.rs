//! The exchange-agnostic recorder core.

pub mod book;
pub mod coordinator;
pub mod events;
pub mod fixed;
pub mod shard;
pub mod stats;
#[cfg(test)]
mod test_logs;
pub mod verify;
pub mod watchdog;
pub mod writer;
pub mod ws;
