//! The byte stream under a websocket: TCP with Nagle's algorithm off,
//! wrapped in TLS for a `wss` URL.

use std::sync::{Arc, LazyLock};

use anyhow::{Context, bail};
use hyper::Uri;
use rustls::pki_types::ServerName;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;
use tokio_util::either::Either;

/// A connected stream, plain for `ws` and TLS for `wss`.
pub(super) type Stream = Either<TcpStream, TlsStream<TcpStream>>;

/// Where a websocket URL points.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct Target {
    /// The host to connect to, an IPv6 address without its brackets.
    host: String,
    port: u16,
    secure: bool,
    /// The `Host` header: the host, and the port if the URL names one.
    pub(super) host_header: String,
    /// The path and query the handshake asks for.
    pub(super) resource: String,
}

impl Target {
    /// Reads a `ws` or `wss` URL.
    pub(super) fn parse(url: &str) -> anyhow::Result<Self> {
        let uri: Uri = url.parse().context("invalid websocket URL")?;
        let secure = match uri.scheme_str() {
            Some("wss") => true,
            Some("ws") => false,
            _ => bail!("the URL is not ws:// or wss://"),
        };
        let authority = uri.authority().context("the URL names no host")?;
        let host = authority.host();
        let default_port = if secure { 443 } else { 80 };
        Ok(Self {
            host: host
                .trim_start_matches('[')
                .trim_end_matches(']')
                .to_owned(),
            port: authority.port_u16().unwrap_or(default_port),
            secure,
            host_header: authority
                .port()
                .map_or_else(|| host.to_owned(), |port| format!("{host}:{port}")),
            resource: uri
                .path_and_query()
                .map_or("/", |path| path.as_str())
                .to_owned(),
        })
    }
}

/// A stream to `target`, with TLS for a `wss` URL, and its local TCP port.
pub(super) async fn dial(target: &Target) -> anyhow::Result<(Stream, Option<u16>)> {
    let tcp = TcpStream::connect((target.host.as_str(), target.port))
        .await
        .context("could not open a TCP connection")?;
    tcp.set_nodelay(true)?;
    let local_port = tcp.local_addr().ok().map(|address| address.port());
    if !target.secure {
        return Ok((Either::Left(tcp), local_port));
    }
    let name = ServerName::try_from(target.host.clone()).context("invalid TLS server name")?;
    let tls = TLS
        .connect(name, tcp)
        .await
        .context("TLS handshake failed")?;
    Ok((Either::Right(tls), local_port))
}

/// Trusts the Mozilla root certificates, with the aws-lc-rs provider.
static TLS: LazyLock<TlsConnector> = LazyLock::new(|| {
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let roots = rustls::RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("aws-lc-rs supports the default TLS versions")
        .with_root_certificates(roots)
        .with_no_client_auth();
    TlsConnector::from(Arc::new(config))
});

#[cfg(test)]
mod tests {
    use super::*;

    fn target(host: &str, port: u16, secure: bool, host_header: &str, resource: &str) -> Target {
        Target {
            host: host.to_owned(),
            port,
            secure,
            host_header: host_header.to_owned(),
            resource: resource.to_owned(),
        }
    }

    #[test]
    fn wss_defaults_to_port_443_and_keeps_path_and_query() {
        let parsed = Target::parse("wss://example.com/trade-api/ws/v2?x=1").unwrap();
        let expected = target(
            "example.com",
            443,
            true,
            "example.com",
            "/trade-api/ws/v2?x=1",
        );
        assert_eq!(parsed, expected);
    }

    #[test]
    fn ws_defaults_to_port_80_and_path_slash() {
        let parsed = Target::parse("ws://example.com").unwrap();
        assert_eq!(parsed, target("example.com", 80, false, "example.com", "/"));
    }

    #[test]
    fn an_explicit_port_is_used_and_named_in_the_host_header() {
        let parsed = Target::parse("ws://127.0.0.1:9001/feed").unwrap();
        assert_eq!(
            parsed,
            target("127.0.0.1", 9001, false, "127.0.0.1:9001", "/feed")
        );
    }

    #[test]
    fn an_ipv6_host_connects_without_brackets_and_keeps_them_in_the_header() {
        let parsed = Target::parse("wss://[::1]:8443/").unwrap();
        assert_eq!(parsed, target("::1", 8443, true, "[::1]:8443", "/"));
    }

    #[test]
    fn userinfo_stays_out_of_the_host_header() {
        let parsed = Target::parse("wss://user:secret@example.com/ws").unwrap();
        assert_eq!(parsed.host_header, "example.com");
    }

    #[test]
    fn other_schemes_are_rejected() {
        let error = Target::parse("https://example.com").unwrap_err();
        assert!(error.to_string().contains("ws://"), "{error}");
    }
}
