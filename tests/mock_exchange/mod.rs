//! A mock exchange for testing the core without a real one: a localhost
//! websocket server the test scripts frame by frame, an endpoint that points
//! at it, a codec for a small JSON wire format, and an in-memory market
//! catalog.
//!
//! It knows nothing about any real exchange, so a core test that passes
//! against it depends only on the `MarketCatalog`, `FeedEndpoint` and
//! `FeedCodec` contracts.

#[allow(dead_code, reason = "the connection tests list no markets")]
mod catalog;
#[allow(dead_code, reason = "each test crate uses different frame builders")]
mod codec;
#[allow(dead_code, reason = "only the connection tests compress")]
mod deflate;
#[allow(dead_code, reason = "each test crate scripts the server differently")]
mod server;

#[allow(unused_imports, reason = "the connection tests list no markets")]
pub use catalog::{CatalogPages, CatalogWalks, MockCatalog, market};
pub use codec::{MockCodec, SentCommand, frames};
#[allow(unused_imports, reason = "only the connection tests compress")]
pub use deflate::{DeflateClient, DeflateServer, Terms};
#[allow(unused_imports, reason = "the connection tests build no coordinator")]
pub use server::MockEndpoint;
pub use server::{MockClient, MockServer};
