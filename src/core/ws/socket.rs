//! Opening and closing the websocket itself.

use std::future::poll_fn;
use std::time::Duration;

use anyhow::Context;
use chrono::Utc;
use futures_util::SinkExt;
use hyper_util::rt::TokioIo;
use yawc::close::CloseCode;
use yawc::{Frame, OpCode, Options, Role, WebSocket, WebSocketError};

use super::ConnectionConfig;
use super::handshake::UpgradeRequest;
use super::transport::{self, Target};
use crate::exchange::FeedEndpoint;

/// A client websocket and the local TCP port under it.
pub(super) struct Socket {
    frames: WebSocket<TokioIo<hyper::upgrade::Upgraded>>,
    local_port: Option<u16>,
}

/// Opens a websocket to `endpoint` with fresh handshake headers and Nagle's
/// algorithm off, offering compression if `config` asks for it. Logs the
/// extensions the server agreed to. Errors name the URL but never the
/// headers.
pub(super) async fn open(
    endpoint: &impl FeedEndpoint,
    config: &ConnectionConfig,
) -> anyhow::Result<Socket> {
    let url = endpoint.url();
    connect(endpoint, config)
        .await
        .with_context(|| format!("websocket handshake with {url} failed"))
}

async fn connect(
    endpoint: &impl FeedEndpoint,
    config: &ConnectionConfig,
) -> anyhow::Result<Socket> {
    let target = Target::parse(endpoint.url())?;
    let mut request = UpgradeRequest::new(&target, endpoint.handshake_headers(Utc::now()))?;
    if config.compression {
        request = request.with_compression();
    }
    let (stream, local_port) = transport::dial(&target).await?;
    let upgraded = request.send(stream).await?;
    let extensions = upgraded.deflate.map(|deflate| deflate.to_string());
    let extensions = extensions.as_deref();
    tracing::info!(
        local_port,
        extensions = extensions.unwrap_or("none"),
        "opened a socket"
    );
    let settings = websocket_options(config);
    let frames =
        WebSocket::from_stream_with_extensions(upgraded.io, Role::Client, extensions, settings)?;
    Ok(Socket { frames, local_port })
}

/// The websocket options for `config`: its message size limit, before and
/// after inflating, and permessage-deflate if compression is on. yawc's
/// UTF-8 check stays off: the session checks each text frame once as it
/// reads it.
fn websocket_options(config: &ConnectionConfig) -> Options {
    let limit = config.max_message_bytes;
    let options = Options::default().with_limits(limit, limit);
    if config.compression {
        options.with_balanced_compression()
    } else {
        options
    }
}

impl Socket {
    /// The next whole message or control frame. A ping's pong is queued,
    /// and goes out on the next [`Socket::send_replies`], write, or read
    /// that finds nothing to return; a close from the server is answered
    /// and then returned.
    pub(super) async fn next_frame(&mut self) -> Result<Frame, WebSocketError> {
        self.frames.next_frame().await
    }

    /// Sends `text` as one text message.
    pub(super) async fn send_text(&mut self, text: String) -> Result<(), WebSocketError> {
        self.frames.send(Frame::text(text)).await
    }

    /// Writes out the replies yawc has queued, such as pongs, without
    /// waiting for a read to run dry.
    pub(super) async fn send_replies(&mut self) -> Result<(), WebSocketError> {
        poll_fn(|cx| self.frames.poll_ready_unpin(cx)).await
    }

    /// The local TCP port, which tells one connection's socket from
    /// another's to tools outside the process.
    pub(super) fn local_port(&self) -> Option<u16> {
        self.local_port
    }

    /// Sends a close frame and waits for the server's close or the end of
    /// the stream, discarding anything else that arrives. Gives up after
    /// `timeout` in all, sending included.
    pub(super) async fn close(&mut self, timeout: Duration) {
        match tokio::time::timeout(timeout, self.close_and_drain()).await {
            Ok(Ok(())) => tracing::info!("closed the connection"),
            Ok(Err(error)) => tracing::debug!(%error, "could not send a close frame"),
            Err(_) => tracing::warn!(?timeout, "the server did not answer the close in time"),
        }
    }

    async fn close_and_drain(&mut self) -> Result<(), WebSocketError> {
        self.frames
            .send(Frame::close(CloseCode::Normal, b""))
            .await?;
        while let Ok(frame) = self.frames.next_frame().await {
            if frame.opcode() == OpCode::Close {
                break;
            }
        }
        Ok(())
    }
}

/// The code and reason of a close frame, for a log or a disconnect reason.
pub(super) fn describe_close(frame: &Frame) -> String {
    let reason = frame.close_reason().ok().flatten().unwrap_or_default();
    match frame.close_code() {
        Some(code) => format!("code {} ({code:?}), reason {reason:?}", u16::from(code)),
        None => "no code".to_owned(),
    }
}
