//! The HTTP/1.1 upgrade that turns a stream into a websocket (RFC 6455,
//! section 4), optionally offering permessage-deflate (RFC 7692).

use anyhow::{Context, ensure};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use http_body_util::Empty;
use hyper::body::Bytes;
use hyper::client::conn::http1;
use hyper::header::{
    CONNECTION, HOST, HeaderMap, HeaderName, HeaderValue, SEC_WEBSOCKET_ACCEPT,
    SEC_WEBSOCKET_EXTENSIONS, SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_VERSION, UPGRADE,
};
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use sha1::{Digest, Sha1};
use tokio::io::{AsyncRead, AsyncWrite};

use super::extensions::{self, Deflate, PERMESSAGE_DEFLATE};
use super::transport::Target;

/// The GUID RFC 6455 appends to the key to make the accept value.
const ACCEPT_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

/// The body of an upgrade request: none.
type NoBody = Empty<Bytes>;

/// A stream the server has switched to websocket.
pub(super) struct Upgraded {
    /// The stream, including any bytes read past the server's answer.
    pub(super) io: TokioIo<hyper::upgrade::Upgraded>,
    /// The permessage-deflate terms the server agreed to, if any.
    pub(super) deflate: Option<Deflate>,
}

/// What the server's answer must match.
#[derive(Debug)]
struct Expected {
    /// The `Sec-WebSocket-Key` sent.
    key: String,
    /// Whether permessage-deflate was offered.
    deflate_offered: bool,
}

/// An upgrade request and what its answer must match.
pub(super) struct UpgradeRequest {
    request: Request<NoBody>,
    expected: Expected,
}

impl UpgradeRequest {
    /// A request to upgrade to a websocket at `target`, carrying `headers`
    /// as well. Errors name a bad header but never its value.
    pub(super) fn new(target: &Target, headers: Vec<(&str, String)>) -> anyhow::Result<Self> {
        let key = BASE64.encode(rand::random::<[u8; 16]>());
        let mut request = Request::get(target.resource.as_str())
            .header(HOST, target.host_header.as_str())
            .header(UPGRADE, "websocket")
            .header(CONNECTION, "Upgrade")
            .header(SEC_WEBSOCKET_KEY, key.as_str())
            .header(SEC_WEBSOCKET_VERSION, "13")
            .body(NoBody::new())?;
        for (name, value) in headers {
            let header = HeaderName::from_bytes(name.as_bytes())
                .with_context(|| format!("invalid header name {name}"))?;
            let value =
                HeaderValue::from_str(&value).with_context(|| format!("invalid {name} header"))?;
            request.headers_mut().insert(header, value);
        }
        let expected = Expected {
            key,
            deflate_offered: false,
        };
        Ok(Self { request, expected })
    }

    /// The same request, offering permessage-deflate with both sides keeping
    /// their context and no window sizes, so the server may not shrink the
    /// window our compressor uses.
    pub(super) fn with_compression(mut self) -> Self {
        let offer = HeaderValue::from_static(PERMESSAGE_DEFLATE);
        self.request
            .headers_mut()
            .insert(SEC_WEBSOCKET_EXTENSIONS, offer);
        self.expected.deflate_offered = true;
        self
    }

    /// Sends the request over `stream` and waits for the server to switch.
    /// The HTTP connection runs alongside, not on a task of its own, so
    /// giving up on the handshake drops the stream.
    pub(super) async fn send<S>(self, stream: S) -> anyhow::Result<Upgraded>
    where
        S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        let Self { request, expected } = self;
        let (mut sender, connection) = http1::handshake(TokioIo::new(stream)).await?;
        let exchange = async {
            let response = sender.send_request(request).await?;
            let deflate = verify(&response, &expected)?;
            let io = TokioIo::new(hyper::upgrade::on(response).await?);
            Ok(Upgraded { io, deflate })
        };
        tokio::select! {
            upgraded = exchange => upgraded,
            error = drive(connection) => Err(error),
        }
    }
}

/// Runs `connection` until it hands its stream over to the upgrade, then
/// waits forever. Returns only if the connection fails.
async fn drive<S>(connection: http1::Connection<TokioIo<S>, NoBody>) -> anyhow::Error
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    match connection.with_upgrades().await {
        Ok(()) => std::future::pending().await,
        Err(error) => anyhow::Error::from(error).context("the HTTP connection failed"),
    }
}

/// Checks that `response` switches to websocket as `expected`, and returns
/// the permessage-deflate terms it agreed to.
fn verify<B>(response: &Response<B>, expected: &Expected) -> anyhow::Result<Option<Deflate>> {
    let status = response.status();
    ensure!(
        status == StatusCode::SWITCHING_PROTOCOLS,
        "the server answered {status} instead of switching protocols"
    );
    let headers = response.headers();
    ensure!(
        has_token(headers, &UPGRADE, "websocket") && has_token(headers, &CONNECTION, "upgrade"),
        "the server did not upgrade the connection to websocket"
    );
    let accepted = headers.get(SEC_WEBSOCKET_ACCEPT);
    ensure!(
        accepted.is_some_and(|value| value.as_bytes() == accept_value(&expected.key).as_bytes()),
        "the server's Sec-WebSocket-Accept does not match the key"
    );
    agreed_extensions(headers, expected.deflate_offered)
}

/// The extensions `headers` agree to, read strictly.
fn agreed_extensions(headers: &HeaderMap, offered: bool) -> anyhow::Result<Option<Deflate>> {
    let lines = headers.get_all(SEC_WEBSOCKET_EXTENSIONS).iter();
    let lines: Result<Vec<&str>, _> = lines.map(HeaderValue::to_str).collect();
    let lines = lines.context("unreadable Sec-WebSocket-Extensions")?;
    extensions::agreed(&lines, offered)
}

/// Whether header `name` lists `token`, ignoring case.
fn has_token(headers: &HeaderMap, name: &HeaderName, token: &str) -> bool {
    headers
        .get_all(name)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|listed| listed.trim().eq_ignore_ascii_case(token))
}

/// The `Sec-WebSocket-Accept` a server must answer `key` with.
fn accept_value(key: &str) -> String {
    BASE64.encode(Sha1::digest(format!("{key}{ACCEPT_GUID}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The key from the RFC 6455 example handshake.
    const KEY: &str = "dGhlIHNhbXBsZSBub25jZQ==";

    /// The accept value RFC 6455 gives for [`KEY`].
    const ACCEPT: &str = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";

    fn expected(deflate_offered: bool) -> Expected {
        Expected {
            key: KEY.to_owned(),
            deflate_offered,
        }
    }

    /// A 101 answer to [`KEY`] with `accept` as its accept value.
    fn switching(accept: &'static str) -> hyper::http::response::Builder {
        Response::builder()
            .status(StatusCode::SWITCHING_PROTOCOLS)
            .header(UPGRADE, "websocket")
            .header(CONNECTION, "Upgrade")
            .header(SEC_WEBSOCKET_ACCEPT, accept)
    }

    #[test]
    fn accept_value_matches_the_rfc_example() {
        assert_eq!(accept_value(KEY), ACCEPT);
    }

    #[test]
    fn a_valid_answer_yields_the_agreed_deflate_terms() {
        let response = switching(ACCEPT)
            .header(
                SEC_WEBSOCKET_EXTENSIONS,
                "permessage-deflate; client_no_context_takeover",
            )
            .body(())
            .unwrap();
        let agreed = verify(&response, &expected(true)).unwrap().unwrap();
        assert_eq!(
            agreed.to_string(),
            "permessage-deflate; client_no_context_takeover"
        );
    }

    #[test]
    fn deflate_agreed_without_an_offer_fails() {
        let response = switching(ACCEPT)
            .header(SEC_WEBSOCKET_EXTENSIONS, PERMESSAGE_DEFLATE)
            .body(())
            .unwrap();
        let error = verify(&response, &expected(false)).unwrap_err();
        assert!(format!("{error:#}").contains("not offered"), "{error:#}");
    }

    #[test]
    fn extension_lines_are_read_as_one_list() {
        let response = switching(ACCEPT)
            .header(SEC_WEBSOCKET_EXTENSIONS, PERMESSAGE_DEFLATE)
            .header(SEC_WEBSOCKET_EXTENSIONS, PERMESSAGE_DEFLATE)
            .body(())
            .unwrap();
        let error = verify(&response, &expected(true)).unwrap_err();
        assert!(format!("{error:#}").contains("more than one"), "{error:#}");
    }

    #[test]
    fn an_answer_without_extensions_agrees_to_none() {
        let response = switching(ACCEPT).body(()).unwrap();
        assert_eq!(verify(&response, &expected(true)).unwrap(), None);
    }

    #[test]
    fn a_wrong_accept_value_fails() {
        let response = switching("bm90IHRoZSByaWdodCBvbmU=").body(()).unwrap();
        let error = verify(&response, &expected(true)).unwrap_err().to_string();
        assert!(error.contains("Sec-WebSocket-Accept"), "{error}");
    }

    #[test]
    fn a_refusal_names_its_status() {
        let response = Response::builder().status(401).body(()).unwrap();
        let error = verify(&response, &expected(true)).unwrap_err().to_string();
        assert!(error.contains("401"), "{error}");
    }

    #[test]
    fn tokens_are_found_in_lists_whatever_their_case() {
        let mut headers = HeaderMap::new();
        headers.insert(CONNECTION, HeaderValue::from_static("keep-alive, Upgrade"));
        assert!(has_token(&headers, &CONNECTION, "upgrade"));
        assert!(!has_token(&headers, &CONNECTION, "close"));
    }
}
