//! Opening and closing the websocket itself.

use std::time::Duration;

use anyhow::Context;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::{self, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::exchange::FeedEndpoint;

/// A client websocket over TCP or TLS.
pub(super) type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Opens a websocket to `endpoint` with fresh handshake headers and Nagle's
/// algorithm off. Errors name the URL but never the headers.
pub(super) async fn open(endpoint: &impl FeedEndpoint) -> anyhow::Result<Socket> {
    let mut request = endpoint.url().into_client_request()?;
    for (name, value) in endpoint.handshake_headers(Utc::now()) {
        let value =
            HeaderValue::from_str(&value).with_context(|| format!("invalid {name} header"))?;
        request.headers_mut().insert(name, value);
    }
    let (socket, _) = tokio_tungstenite::connect_async_with_config(request, None, true)
        .await
        .with_context(|| format!("websocket handshake with {} failed", endpoint.url()))?;
    Ok(socket)
}

/// The local TCP port of `socket`, which tells one connection's socket from
/// another's to tools outside the process.
pub(super) fn local_port(socket: &Socket) -> Option<u16> {
    let stream = socket.get_ref().get_ref();
    stream.local_addr().ok().map(|address| address.port())
}

/// Sends a close frame and waits for the server's close or the end of the
/// stream, discarding anything else that arrives. Gives up after `timeout`
/// in all, sending included.
pub(super) async fn close(socket: &mut Socket, timeout: Duration) {
    match tokio::time::timeout(timeout, close_and_drain(socket)).await {
        Ok(Ok(())) => tracing::info!("closed the connection"),
        Ok(Err(error)) => tracing::debug!(%error, "could not send a close frame"),
        Err(_) => tracing::warn!(?timeout, "the server did not answer the close in time"),
    }
}

async fn close_and_drain(socket: &mut Socket) -> Result<(), tungstenite::Error> {
    socket.send(Message::Close(None)).await?;
    while let Some(Ok(message)) = socket.next().await {
        if message.is_close() {
            break;
        }
    }
    Ok(())
}
