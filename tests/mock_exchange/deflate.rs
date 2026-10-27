//! [`DeflateServer`]: a localhost websocket server that agrees to
//! permessage-deflate, with context takeover, whenever the client offers
//! it. Built on yawc's server side, since tungstenite has no compression.

use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::SinkExt;
use hyper::Request;
use hyper::body::Incoming;
use hyper::header::{HeaderMap, SEC_WEBSOCKET_EXTENSIONS};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use serde_json::Value;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::time::timeout;
use yawc::{Frame, HttpWebSocket, OpCode, Options, UpgradeFut, WebSocket};

use super::{MockEndpoint, SentCommand};

/// How long any single expected step may take before the test fails.
const STEP_TIMEOUT: Duration = Duration::from_secs(5);

/// A compressing websocket server on a free localhost port.
pub struct DeflateServer {
    listener: TcpListener,
    options: Options,
}

/// The `Sec-WebSocket-Extensions` of one handshake.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Terms {
    /// What the client offered.
    pub offered: Option<String>,
    /// What the server agreed to.
    pub agreed: Option<String>,
}

impl DeflateServer {
    /// Starts listening, agreeing to compression with context takeover
    /// both ways.
    pub async fn start() -> Self {
        Self::start_with(Options::default().with_balanced_compression()).await
    }

    /// Starts listening, answering offers as `options` say.
    pub async fn start_with(options: Options) -> Self {
        Self {
            listener: TcpListener::bind("127.0.0.1:0").await.unwrap(),
            options,
        }
    }

    /// An endpoint for this server.
    pub fn endpoint(&self) -> MockEndpoint {
        MockEndpoint::at(&self.listener, Arc::default())
    }

    /// The next connection, after its websocket handshake.
    pub async fn accept(&self) -> DeflateClient {
        timeout(STEP_TIMEOUT, self.accept_now())
            .await
            .expect("no connection in time")
    }

    async fn accept_now(&self) -> DeflateClient {
        let (stream, _) = self.listener.accept().await.unwrap();
        let (upgrade, terms) = serve_upgrade(stream, self.options.clone()).await;
        let socket = upgrade.await.unwrap();
        DeflateClient { socket, terms }
    }
}

/// Serves the HTTP side of `stream` on its own task and answers its upgrade
/// request as `options` say.
async fn serve_upgrade(stream: TcpStream, options: Options) -> (UpgradeFut, Terms) {
    let (sender, receiver) = oneshot::channel();
    let sender = Mutex::new(Some(sender));
    let service = service_fn(move |mut request: Request<Incoming>| {
        let offered = extensions(request.headers());
        let (response, upgrade) =
            WebSocket::upgrade_with_options(&mut request, options.clone()).unwrap();
        let agreed = extensions(response.headers());
        if let Some(sender) = sender.lock().unwrap().take() {
            sender.send((upgrade, Terms { offered, agreed })).ok();
        }
        async { Ok::<_, Infallible>(response) }
    });
    let connection = http1::Builder::new().serve_connection(TokioIo::new(stream), service);
    tokio::spawn(connection.with_upgrades());
    receiver.await.expect("the client made no upgrade request")
}

fn extensions(headers: &HeaderMap) -> Option<String> {
    let value = headers.get(SEC_WEBSOCKET_EXTENSIONS)?;
    Some(value.to_str().unwrap().to_owned())
}

/// The server's side of one compressing connection.
pub struct DeflateClient {
    socket: HttpWebSocket,
    terms: Terms,
}

impl DeflateClient {
    /// The extensions offered and agreed in the handshake.
    pub fn terms(&self) -> &Terms {
        &self.terms
    }

    /// The next command the client sends, skipping control frames.
    pub async fn next_command(&mut self) -> SentCommand {
        let text = timeout(STEP_TIMEOUT, self.next_text())
            .await
            .expect("no command in time");
        serde_json::from_str(&text).expect("a command is JSON")
    }

    /// Sends `frame` unprompted.
    pub async fn send(&mut self, frame: Value) {
        let text = Frame::text(frame.to_string());
        self.socket.send(text).await.unwrap();
    }

    /// Sends `frame` as the reply to `command`.
    pub async fn reply(&mut self, command: &SentCommand, mut frame: Value) {
        frame["reply_to"] = command.id.into();
        self.send(frame).await;
    }

    async fn next_text(&mut self) -> String {
        loop {
            let frame = self
                .socket
                .next_frame()
                .await
                .expect("the client went away");
            if frame.opcode() == OpCode::Text {
                return frame.as_str().to_owned();
            }
        }
    }
}
