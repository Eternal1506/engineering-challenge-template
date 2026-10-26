//! A mock exchange for testing the core without a real one: a localhost
//! websocket server the test scripts frame by frame, an endpoint that points
//! at it, and a codec for a small JSON wire format.
//!
//! It knows nothing about any real exchange, so a core test that passes
//! against it depends only on the `FeedEndpoint` and `FeedCodec` contracts.

mod codec;
mod server;

pub use codec::{MockCodec, SentCommand, frames};
pub use server::{MockClient, MockServer};
