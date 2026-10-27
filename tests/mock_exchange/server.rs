//! [`MockServer`]: a localhost websocket server that hands each accepted
//! connection to the test as a [`MockClient`] to script.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};
use engineering_challenge::exchange::FeedEndpoint;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::{self, Message};

use super::SentCommand;

/// The handshake header [`MockEndpoint`] sets, standing in for a signature.
const AUTH_HEADER: &str = "x-mock-auth";

/// How long any single expected step may take before the test fails.
const STEP_TIMEOUT: Duration = Duration::from_secs(5);

/// A websocket server on a free localhost port.
pub struct MockServer {
    listener: TcpListener,
    handshakes: Arc<AtomicUsize>,
}

impl MockServer {
    /// Starts listening.
    pub async fn start() -> Self {
        Self {
            listener: TcpListener::bind("127.0.0.1:0").await.unwrap(),
            handshakes: Arc::default(),
        }
    }

    /// An endpoint for this server whose handshake header is
    /// `attempt-<n>` on its n-th handshake, so tests can tell that each
    /// connection got fresh headers.
    pub fn endpoint(&self) -> MockEndpoint {
        let address = self.listener.local_addr().unwrap();
        MockEndpoint {
            url: format!("ws://{address}"),
            handshakes: self.handshakes.clone(),
        }
    }

    /// How many handshakes the endpoint has started, accepted or not.
    pub fn handshake_attempts(&self) -> usize {
        self.handshakes.load(Ordering::SeqCst)
    }

    /// The next connection, after its websocket handshake.
    pub async fn accept(&self) -> MockClient {
        self.accept_within(STEP_TIMEOUT).await
    }

    /// The next connection, failing if none arrives within `limit`.
    pub async fn accept_within(&self, limit: Duration) -> MockClient {
        timeout(limit, self.accept_now())
            .await
            .expect("no connection in time")
    }

    #[expect(
        clippy::result_large_err,
        reason = "tungstenite fixes the handshake callback's signature"
    )]
    async fn accept_now(&self) -> MockClient {
        let (stream, _) = self.listener.accept().await.unwrap();
        let mut auth = None;
        let callback = |request: &Request, response: Response| {
            let header = request.headers().get(AUTH_HEADER);
            auth = header
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            Ok::<_, ErrorResponse>(response)
        };
        let socket = tokio_tungstenite::accept_hdr_async(stream, callback)
            .await
            .unwrap();
        MockClient { socket, auth }
    }
}

/// A [`FeedEndpoint`] for a [`MockServer`].
pub struct MockEndpoint {
    url: String,
    handshakes: Arc<AtomicUsize>,
}

impl FeedEndpoint for MockEndpoint {
    fn url(&self) -> &str {
        &self.url
    }

    fn handshake_headers(&self, _now: DateTime<Utc>) -> Vec<(&'static str, String)> {
        let attempt = self.handshakes.fetch_add(1, Ordering::SeqCst) + 1;
        vec![(AUTH_HEADER, format!("attempt-{attempt}"))]
    }
}

/// The server's side of one accepted connection.
pub struct MockClient {
    socket: WebSocketStream<TcpStream>,
    auth: Option<String>,
}

impl MockClient {
    /// The auth header the client's handshake carried.
    pub fn auth(&self) -> Option<&str> {
        self.auth.as_deref()
    }

    /// The next command the client sends, skipping control frames.
    pub async fn next_command(&mut self) -> SentCommand {
        let text = timeout(STEP_TIMEOUT, self.next_text())
            .await
            .expect("no command in time");
        serde_json::from_str(&text).expect("a command is JSON")
    }

    /// Fails if the client sends a command within `within`.
    pub async fn assert_no_command(&mut self, within: Duration) {
        if let Ok(text) = timeout(within, self.next_text()).await {
            panic!("unexpected command {text}");
        }
    }

    /// Sends `frame` unprompted.
    pub async fn send(&mut self, frame: Value) {
        self.send_text(&frame.to_string()).await;
    }

    /// Sends `frame` as the reply to `command`.
    pub async fn reply(&mut self, command: &SentCommand, mut frame: Value) {
        frame["reply_to"] = command.id.into();
        self.send(frame).await;
    }

    /// Sends `text` as one text frame, valid or not.
    pub async fn send_text(&mut self, text: &str) {
        self.socket.send(Message::text(text)).await.unwrap();
    }

    /// Sends `frame` unprompted, failing if the client has gone.
    pub async fn try_send(&mut self, frame: Value) -> Result<(), tungstenite::Error> {
        self.socket.send(Message::text(frame.to_string())).await
    }

    /// Waits for the client to drop the connection, with or without a close
    /// frame, skipping whatever it sends before.
    pub async fn expect_gone(&mut self) {
        let gone = async {
            while let Some(Ok(message)) = self.socket.next().await {
                if message.is_close() {
                    break;
                }
            }
        };
        timeout(STEP_TIMEOUT, gone)
            .await
            .expect("the client did not go in time");
    }

    /// Sends a ping; fails once the client has gone.
    pub async fn ping(&mut self) -> Result<(), tungstenite::Error> {
        self.socket.send(Message::Ping(Default::default())).await
    }

    /// Closes the connection from the server's side.
    pub async fn close(&mut self) {
        self.socket.close(None).await.unwrap();
    }

    /// Waits for the client's close frame, then answers it.
    pub async fn expect_close(&mut self) {
        timeout(STEP_TIMEOUT, self.until_close())
            .await
            .expect("no close frame in time");
    }

    async fn until_close(&mut self) {
        loop {
            match self.socket.next().await {
                Some(Ok(Message::Close(_))) => break,
                Some(Ok(_)) => continue,
                other => panic!("the client went away without a close frame: {other:?}"),
            }
        }
        while self.socket.next().await.is_some() {}
    }

    async fn next_text(&mut self) -> String {
        loop {
            match self.socket.next().await {
                Some(Ok(Message::Text(text))) => return text.to_string(),
                Some(Ok(Message::Close(_)) | Err(_)) | None => panic!("the client went away"),
                Some(Ok(_)) => continue,
            }
        }
    }
}
