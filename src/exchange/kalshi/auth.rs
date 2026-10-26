//! [`RequestSigner`] signs Kalshi REST and websocket requests with Ed25519.

use std::fmt;

use anyhow::Context;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::{DateTime, Utc};
use ed25519_dalek::pkcs8::DecodePrivateKey;
use ed25519_dalek::{Signer, SigningKey};

use super::KalshiCredentials;

/// Header carrying the API key id.
pub const ACCESS_KEY_HEADER: &str = "KALSHI-ACCESS-KEY";
/// Header carrying the signing time in Unix milliseconds.
pub const ACCESS_TIMESTAMP_HEADER: &str = "KALSHI-ACCESS-TIMESTAMP";
/// Header carrying the base64 signature.
pub const ACCESS_SIGNATURE_HEADER: &str = "KALSHI-ACCESS-SIGNATURE";

/// The three authentication headers, as `(name, value)` pairs.
pub type AuthHeaders = [(&'static str, String); 3];

/// Signs requests with the account's Ed25519 key.
///
/// The PEM is parsed once, when the signer is built. [`Debug`](fmt::Debug)
/// output redacts the key id and never touches the key.
pub struct RequestSigner {
    key_id: String,
    signing_key: SigningKey,
}

impl RequestSigner {
    /// Builds a signer from `credentials`. Fails if the private key is not an
    /// Ed25519 key in PKCS#8 PEM form; the error never includes key material.
    pub fn new(credentials: &KalshiCredentials) -> anyhow::Result<Self> {
        Self::from_parts(credentials.key_id(), credentials.private_key_pem())
    }

    /// The authentication headers for `method` on `path`, signed at `now`.
    ///
    /// `path` is the full request path, such as `/trade-api/ws/v2`. Kalshi
    /// signs the path without its query string, so any `?...` is ignored.
    pub fn headers(&self, method: &str, path: &str, now: DateTime<Utc>) -> AuthHeaders {
        let timestamp = now.timestamp_millis().to_string();
        let signature = self.sign(&signed_message(&timestamp, method, path));
        [
            (ACCESS_KEY_HEADER, self.key_id.clone()),
            (ACCESS_TIMESTAMP_HEADER, timestamp),
            (ACCESS_SIGNATURE_HEADER, signature),
        ]
    }

    fn from_parts(key_id: &str, private_key_pem: &str) -> anyhow::Result<Self> {
        let signing_key = SigningKey::from_pkcs8_pem(private_key_pem)
            .context("Kalshi private key is not an Ed25519 key in PKCS#8 PEM form")?;
        Ok(Self {
            key_id: key_id.to_owned(),
            signing_key,
        })
    }

    fn sign(&self, message: &str) -> String {
        BASE64.encode(self.signing_key.sign(message.as_bytes()).to_bytes())
    }
}

impl fmt::Debug for RequestSigner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RequestSigner")
            .field("key_id", &"<redacted>")
            .finish_non_exhaustive()
    }
}

/// The text Kalshi expects signed: timestamp, method and query-free path,
/// concatenated.
fn signed_message(timestamp: &str, method: &str, path: &str) -> String {
    let path = path.split_once('?').map_or(path, |(path, _)| path);
    format!("{timestamp}{method}{path}")
}

#[cfg(test)]
mod tests {
    use ed25519_dalek::pkcs8::EncodePrivateKey;
    use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
    use ed25519_dalek::{Signature, Verifier};

    use super::*;

    const SEED: [u8; 32] = [7; 32];
    const KEY_ID: &str = "test-key-id";
    const SIGNED_AT_MS: i64 = 1_700_000_000_000;

    fn test_signer() -> RequestSigner {
        let pem = SigningKey::from_bytes(&SEED)
            .to_pkcs8_pem(LineEnding::LF)
            .unwrap();
        RequestSigner::from_parts(KEY_ID, &pem).unwrap()
    }

    fn signed_at() -> DateTime<Utc> {
        DateTime::from_timestamp_millis(SIGNED_AT_MS).unwrap()
    }

    fn header_value<'a>(headers: &'a AuthHeaders, name: &str) -> &'a str {
        let (_, value) = headers.iter().find(|(header, _)| *header == name).unwrap();
        value
    }

    fn verifies(headers: &AuthHeaders, message: &str) -> bool {
        let bytes = BASE64
            .decode(header_value(headers, ACCESS_SIGNATURE_HEADER))
            .unwrap();
        let signature = Signature::from_slice(&bytes).unwrap();
        let public_key = SigningKey::from_bytes(&SEED).verifying_key();
        public_key.verify(message.as_bytes(), &signature).is_ok()
    }

    #[test]
    fn signature_verifies_over_timestamp_method_and_path() {
        let headers = test_signer().headers("GET", "/trade-api/ws/v2", signed_at());
        assert!(verifies(&headers, "1700000000000GET/trade-api/ws/v2"));
    }

    #[test]
    fn headers_have_the_kalshi_names() {
        let headers = test_signer().headers("GET", "/trade-api/ws/v2", signed_at());
        assert_eq!(
            headers.map(|(name, _)| name),
            [
                "KALSHI-ACCESS-KEY",
                "KALSHI-ACCESS-TIMESTAMP",
                "KALSHI-ACCESS-SIGNATURE"
            ]
        );
    }

    #[test]
    fn headers_carry_key_id_and_millisecond_timestamp() {
        let headers = test_signer().headers("GET", "/trade-api/ws/v2", signed_at());
        assert_eq!(
            [
                header_value(&headers, ACCESS_KEY_HEADER),
                header_value(&headers, ACCESS_TIMESTAMP_HEADER)
            ],
            [KEY_ID, "1700000000000"]
        );
    }

    #[test]
    fn query_string_is_left_out_of_the_signed_message() {
        let path = "/trade-api/v2/markets?status=open";
        let headers = test_signer().headers("GET", path, signed_at());
        assert!(verifies(&headers, "1700000000000GET/trade-api/v2/markets"));
    }

    #[test]
    fn malformed_pem_is_rejected_without_echoing_it() {
        let pem = "-----BEGIN PRIVATE KEY-----\nbm90LWEta2V5\n-----END PRIVATE KEY-----\n";
        let error = format!("{:#}", RequestSigner::from_parts(KEY_ID, pem).unwrap_err());
        assert!(error.contains("PKCS#8") && !error.contains("bm90LWEta2V5"));
    }

    #[test]
    fn debug_output_hides_the_key_id() {
        assert!(!format!("{:?}", test_signer()).contains(KEY_ID));
    }
}
