//! [`KalshiEndpoint`]: the Kalshi market-data websocket and its signed
//! handshake.

use anyhow::Context;
use chrono::{DateTime, Utc};
use reqwest::Url;

use super::RequestSigner;
use crate::exchange::FeedEndpoint;

/// The production market-data websocket.
pub const WS_URL: &str = "wss://external-api-ws.kalshi.com/trade-api/ws/v2";

/// A Kalshi websocket URL plus the signer that authenticates each handshake.
#[derive(Debug)]
pub struct KalshiEndpoint {
    url: String,
    path: String,
    signer: RequestSigner,
}

impl KalshiEndpoint {
    /// The production websocket at [`WS_URL`].
    pub fn production(signer: RequestSigner) -> Self {
        Self::new(WS_URL, signer).expect("WS_URL is a valid URL")
    }

    /// The websocket at `url`, signing handshakes with `signer`. Fails if
    /// `url` is not a valid URL.
    pub fn new(url: &str, signer: RequestSigner) -> anyhow::Result<Self> {
        let parsed = Url::parse(url).with_context(|| format!("invalid websocket URL {url:?}"))?;
        Ok(Self {
            url: url.to_owned(),
            path: parsed.path().to_owned(),
            signer,
        })
    }
}

impl FeedEndpoint for KalshiEndpoint {
    fn url(&self) -> &str {
        &self.url
    }

    fn handshake_headers(&self, now: DateTime<Utc>) -> Vec<(&'static str, String)> {
        self.signer.headers("GET", &self.path, now).into()
    }
}

#[cfg(test)]
mod tests {
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD as BASE64;
    use ed25519_dalek::pkcs8::EncodePrivateKey;
    use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
    use ed25519_dalek::{Signature, SigningKey, Verifier};

    use super::*;
    use crate::exchange::kalshi::{ACCESS_SIGNATURE_HEADER, KalshiCredentials};

    const SEED: [u8; 32] = [9; 32];

    /// A signer for a throwaway key, loaded the way production loads one.
    fn test_signer() -> RequestSigner {
        let dir = tempfile::tempdir().unwrap();
        let pem = SigningKey::from_bytes(&SEED)
            .to_pkcs8_pem(LineEnding::LF)
            .unwrap();
        std::fs::write(dir.path().join("key_id.txt"), "test-key").unwrap();
        std::fs::write(dir.path().join("private_key.pem"), pem.as_bytes()).unwrap();
        RequestSigner::new(&KalshiCredentials::load(dir.path()).unwrap()).unwrap()
    }

    fn signature(headers: &[(&'static str, String)]) -> Signature {
        let (_, value) = headers
            .iter()
            .find(|(name, _)| *name == ACCESS_SIGNATURE_HEADER)
            .unwrap();
        Signature::from_slice(&BASE64.decode(value).unwrap()).unwrap()
    }

    #[test]
    fn production_endpoint_uses_the_kalshi_websocket_url() {
        assert_eq!(KalshiEndpoint::production(test_signer()).url(), WS_URL);
    }

    #[test]
    fn handshake_is_signed_over_get_and_the_websocket_path() {
        let endpoint = KalshiEndpoint::production(test_signer());
        let now = DateTime::from_timestamp_millis(1_700_000_000_000).unwrap();
        let headers = endpoint.handshake_headers(now);
        let message = "1700000000000GET/trade-api/ws/v2";
        let public_key = SigningKey::from_bytes(&SEED).verifying_key();
        assert!(
            public_key
                .verify(message.as_bytes(), &signature(&headers))
                .is_ok()
        );
    }

    #[test]
    fn an_invalid_url_is_rejected() {
        assert!(KalshiEndpoint::new("not a url", test_signer()).is_err());
    }
}
