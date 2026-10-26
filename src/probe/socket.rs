//! Opening a websocket to a [`FeedEndpoint`] and sending commands on it.

use std::time::{Duration, Instant};

use anyhow::Context;
use chrono::Utc;
use engineering_challenge::core::events::Command;
use engineering_challenge::exchange::{FeedCodec, FeedEndpoint};
use futures_util::SinkExt;
use futures_util::stream::SplitSink;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

/// A client websocket over TCP or TLS.
pub type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// The sending half of a [`Socket`].
pub type SocketSender = SplitSink<Socket, Message>;

/// Connects to `endpoint` with fresh handshake headers, returning the socket
/// and how long the handshake took.
pub async fn connect(endpoint: &impl FeedEndpoint) -> anyhow::Result<(Socket, Duration)> {
    let mut request = endpoint.url().into_client_request()?;
    for (name, value) in endpoint.handshake_headers(Utc::now()) {
        let value = HeaderValue::from_str(&value).context("invalid handshake header")?;
        request.headers_mut().insert(name, value);
    }
    let started = Instant::now();
    let (socket, _) = tokio_tungstenite::connect_async(request)
        .await
        .with_context(|| format!("websocket handshake with {} failed", endpoint.url()))?;
    Ok((socket, started.elapsed()))
}

/// Encodes `command` with `codec` and sends it as one text frame.
pub async fn send(
    sender: &mut SocketSender,
    codec: &impl FeedCodec,
    command: &Command,
) -> anyhow::Result<()> {
    let text = codec.encode(command);
    sender
        .send(Message::text(text))
        .await
        .context("failed to send a websocket command")
}
