//! CLOB L2 API-key credential types, redaction, and HMAC request-header
//! construction for authenticated reads.

use std::collections::BTreeMap;

use base64::{engine::general_purpose, Engine};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use crate::{Error, Result};

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, Default, Eq, PartialEq, Deserialize)]
pub struct ApiKey {
    pub key: String,
    pub secret: String,
    pub passphrase: String,
}

impl std::fmt::Debug for ApiKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ApiKey")
            .field("key", &redact(&self.key))
            .field("secret", &"<redacted>")
            .field("passphrase", &"<redacted>")
            .finish()
    }
}

impl ApiKey {
    pub fn validate(&self) -> Result<()> {
        if self.key.is_empty() || self.secret.is_empty() || self.passphrase.is_empty() {
            return Err(Error::Invalid(
                "api key, secret, and passphrase are required".into(),
            ));
        }
        Ok(())
    }

    pub fn redacted(&self) -> RedactedApiKey {
        RedactedApiKey {
            key: redact(&self.key),
            secret: "<redacted>".into(),
            passphrase: "<redacted>".into(),
        }
    }
}

#[derive(Clone, Default, Eq, PartialEq)]
pub struct L2Credentials {
    pub address: String,
    pub api_key: ApiKey,
}

impl L2Credentials {
    pub fn validate(&self) -> Result<()> {
        let raw = self.address.strip_prefix("0x").unwrap_or_default();
        if raw.len() != 40 || !raw.chars().all(|character| character.is_ascii_hexdigit()) {
            return Err(Error::Invalid(
                "L2 address must be a 20-byte 0x address".into(),
            ));
        }
        self.api_key.validate()
    }
}

impl std::fmt::Debug for L2Credentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("L2Credentials")
            .field("address", &self.address)
            .field("api_key", &self.api_key)
            .finish()
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct RedactedApiKey {
    pub key: String,
    pub secret: String,
    pub passphrase: String,
}

pub fn build_l2_headers(
    credentials: &L2Credentials,
    timestamp: i64,
    method: &str,
    path: &str,
    body: Option<&str>,
) -> Result<BTreeMap<String, String>> {
    credentials.validate()?;
    let signature = sign_hmac(&credentials.api_key.secret, timestamp, method, path, body);
    Ok(BTreeMap::from([
        ("POLY_ADDRESS".into(), credentials.address.clone()),
        ("POLY_API_KEY".into(), credentials.api_key.key.clone()),
        (
            "POLY_PASSPHRASE".into(),
            credentials.api_key.passphrase.clone(),
        ),
        ("POLY_TIMESTAMP".into(), timestamp.to_string()),
        ("POLY_SIGNATURE".into(), signature),
    ]))
}

pub fn sign_hmac(
    secret: &str,
    timestamp: i64,
    method: &str,
    path: &str,
    body: Option<&str>,
) -> String {
    let key = decode_hmac_secret(secret).unwrap_or_else(|| secret.as_bytes().to_vec());
    let mut mac = HmacSha256::new_from_slice(&key).expect("hmac accepts any key length");
    let mut message = format!("{timestamp}{method}{path}");
    if let Some(body) = body {
        message.push_str(body);
    }
    mac.update(message.as_bytes());
    general_purpose::STANDARD
        .encode(mac.finalize().into_bytes())
        .replace('+', "-")
        .replace('/', "_")
}

pub fn compact_json(raw: &str) -> String {
    match serde_json::from_str::<serde_json::Value>(raw) {
        Ok(value) => serde_json::to_string(&value).unwrap_or_else(|_| raw.into()),
        Err(_) => raw.into(),
    }
}

fn decode_hmac_secret(secret: &str) -> Option<Vec<u8>> {
    [
        general_purpose::URL_SAFE,
        general_purpose::URL_SAFE_NO_PAD,
        general_purpose::STANDARD,
        general_purpose::STANDARD_NO_PAD,
    ]
    .into_iter()
    .find_map(|encoding| encoding.decode(secret).ok())
}

fn redact(value: &str) -> String {
    if value.len() <= 8 {
        return "<redacted>".into();
    }
    format!("{}…{}", &value[..4], &value[value.len() - 4..])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn credentials() -> L2Credentials {
        L2Credentials {
            address: "0x1234567890123456789012345678901234567890".into(),
            api_key: ApiKey {
                key: "abcdefghijkl".into(),
                secret: general_purpose::STANDARD.encode("secret"),
                passphrase: "sensitive-passphrase-value".into(),
            },
        }
    }

    #[test]
    fn l2_headers_include_address_and_never_debug_secrets() {
        let credentials = credentials();
        let headers = build_l2_headers(&credentials, 123, "GET", "/data/trades", None).unwrap();
        assert_eq!(headers["POLY_ADDRESS"], credentials.address);
        assert_eq!(headers["POLY_API_KEY"], credentials.api_key.key);
        assert_eq!(headers["POLY_TIMESTAMP"], "123");
        let debug = format!("{credentials:?}");
        assert!(!debug.contains(&credentials.api_key.secret));
        assert!(!debug.contains(&credentials.api_key.passphrase));
        assert!(debug.contains("<redacted>"));
    }

    #[test]
    fn l2_credentials_reject_invalid_addresses() {
        let mut credentials = credentials();
        credentials.address = "0xshort".into();
        assert!(credentials.validate().is_err());
    }

    #[test]
    fn l2_headers_sign_timestamp_method_path_body() {
        let credentials = credentials();
        let headers = build_l2_headers(&credentials, 123, "GET", "/orders", Some("{}")).unwrap();
        assert_eq!(headers["POLY_API_KEY"], "abcdefghijkl");
        assert_eq!(headers["POLY_PASSPHRASE"], "sensitive-passphrase-value");
        assert_eq!(headers["POLY_TIMESTAMP"], "123");
        assert_eq!(
            headers["POLY_SIGNATURE"],
            sign_hmac(
                &credentials.api_key.secret,
                123,
                "GET",
                "/orders",
                Some("{}")
            )
        );
    }

    #[test]
    fn compact_json_preserves_invalid_and_compacts_valid() {
        assert_eq!(
            compact_json(r#"{ "a": 1, "b": "x y" }"#),
            r#"{"a":1,"b":"x y"}"#
        );
        assert_eq!(compact_json("not-json"), "not-json");
    }

    #[test]
    fn redaction_does_not_expose_secrets() {
        let key = ApiKey {
            key: "abcdefghijkl".into(),
            secret: "secret".into(),
            passphrase: "pass".into(),
        };
        assert_eq!(key.redacted().key, "abcd…ijkl");
        assert_eq!(key.redacted().secret, "<redacted>");
    }
}
