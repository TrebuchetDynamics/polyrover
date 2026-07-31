# Authenticated Historical Query Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Add documented L2-authenticated CLOB trade and order reads without making Polyrover's public client secret-bearing or adding any fund-moving behavior.

**Architecture:** Add a separate transport-only authenticated CLOB client behind the existing `authenticated` feature. Every HTTP method borrows validated L2 credentials for one request, signs the documented canonical path, and returns one typed cursor page. The default `public` feature and MegaBot's public-only dependency remain unchanged.

**Tech Stack:** Rust 2021, Tokio, Reqwest, Serde, HMAC-SHA256, existing feature flags and local HTTP fixtures.

> **Execution correction (2026-07-31):** Read DTOs were completed in authenticated-only `clob_history` rather than exposing the execution-only `clob_orders` module. `clob_orders::OrderRecord` remains compatible through a re-export when `execution` is enabled. Live source and tests supersede any narrower draft snippets below.

## Global Constraints

- Run the public companion plan first: `docs/superpowers/plans/2026-07-31-public-historical-query-parity.md`.
- Authenticated HTTP reads compile only with `features = ["authenticated"]`; `authenticated` continues to imply `public`.
- Credentials are borrowed per request. Do not add credentials to `ClientConfig`, the cloneable unified client, global state, files, CLI flags, environment loading, logs, errors, fixtures, or JSON output.
- `L2Credentials` contains address plus API-key material and has redacted `Debug`; secret and passphrase are never serialized.
- Add no L1 private-key signing, API-key creation/derivation, wallet signer, order creation, cancellation, heartbeat, allowance update, relayer, bridge, or funding method.
- Authenticated CLI commands remain excluded. This slice is library-only.
- Each call returns exactly one upstream cursor page. No automatic traversal or persistence.
- Sign the canonical endpoint path without query parameters, matching the official client; send the full encoded query path over HTTP.
- Ordinary tests use dummy credentials and local servers only. Do not add a live credential canary to this repository.
- MegaBot consumers remain restricted to `default-features = false, features = ["public"]`.
- Every behavior change follows RED → GREEN under `../docs/project/TDD-HARD-RULE.md`.
- Do not commit, push, publish, tag, or release unless separately requested.

## Documented scope

| Surface                    | Endpoint                                                    | Query controls                                                                        | Official source                                                   |
| -------------------------- | ----------------------------------------------------------- | ------------------------------------------------------------------------------------- | ----------------------------------------------------------------- |
| Authenticated trades       | `GET /data/trades`                                          | `id`, `maker_address`, `market`, `asset_id`, `before`, `after`, `next_cursor`         | <https://docs.polymarket.com/api-reference/trade/get-trades>      |
| Authenticated orders page  | `GET /data/orders`                                          | `id`, `market`, `asset_id`, `next_cursor`                                             | <https://docs.polymarket.com/api-reference/trade/get-user-orders> |
| Authenticated order lookup | `GET /data/order/{orderID}`                                 | path order ID                                                                         | <https://docs.polymarket.com/api-reference/trade/get-order>       |
| L2 authentication          | canonical method/path/body plus address and API credentials | `POLY_ADDRESS`, `POLY_SIGNATURE`, `POLY_TIMESTAMP`, `POLY_API_KEY`, `POLY_PASSPHRASE` | <https://docs.polymarket.com/developers/CLOB/authentication>      |

The official Python client is secondary implementation evidence for query names and canonical signing paths: <https://github.com/Polymarket/py-clob-client>.

## File structure

| File                                 | Responsibility after this plan                                                                     |
| ------------------------------------ | -------------------------------------------------------------------------------------------------- |
| `src/capabilities/auth.rs`           | Secret-safe L2 credential value, validation, redaction, and headers.                               |
| `src/api/transport.rs`               | Internal authenticated GET using caller-supplied headers and existing retry/concurrency behavior.  |
| `src/api/authenticated_clob.rs`      | Typed atomic trade/order query paths and signed reads.                                             |
| `src/capabilities/clob_history.rs`   | Authenticated read DTOs and generic cursor page.                                                   |
| `src/capabilities/clob_orders.rs`    | Execution-only DTOs; re-exports `OrderRecord` without exposing execution types to `authenticated`. |
| `src/client.rs`                      | Secret-free authenticated transport field and borrowed-credential delegates.                       |
| `src/lib.rs`                         | Feature-gated module exports.                                                                      |
| `tests/authenticated_client.rs`      | Local HTTP authentication/query/response tests with dummy values.                                  |
| `tests/contracts.rs`                 | Official response examples.                                                                        |
| `tests/fixtures/clob/`               | Synthetic official contract examples only.                                                         |
| `capabilities.json`                  | Authenticated read status and evidence.                                                            |
| `docs/endpoint-capability-matrix.md` | Auth scope, cursor behavior, and explicit exclusions.                                              |
| `README.md`                          | Safe library-only usage and MegaBot exclusion.                                                     |

---

### Task 1: Secret-safe L2 credential contract

**Files:**

- Modify: `src/capabilities/auth.rs`
- Test: `src/capabilities/auth.rs`

**Interfaces:**

- Produces: `auth::L2Credentials { address: String, api_key: ApiKey }`
- Changes: `auth::build_l2_headers(&L2Credentials, timestamp, method, path, body)`
- Preserves: `ApiKey` for user WebSocket subscription payloads

- [x] **Step 1: Replace the existing header test with failing address/redaction tests**

Add to `src/capabilities/auth.rs` tests:

```rust
fn credentials() -> L2Credentials {
    L2Credentials {
        address: "0x1234567890123456789012345678901234567890".into(),
        api_key: ApiKey {
            key: "abcdefghijkl".into(),
            secret: general_purpose::STANDARD.encode("secret"),
            passphrase: "passphrase".into(),
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
```

- [x] **Step 2: Run tests and verify RED**

```bash
cargo test --all-features auth::tests::l2_headers_include_address_and_never_debug_secrets
cargo test --all-features auth::tests::l2_credentials_reject_invalid_addresses
```

Expected: `L2Credentials` is undefined and `build_l2_headers` accepts `ApiKey`.

- [x] **Step 3: Harden `ApiKey` and add `L2Credentials`**

Change `ApiKey` derives to remove secret-revealing `Debug` and `Serialize`:

```rust
#[derive(Clone, Default, Eq, PartialEq, Deserialize)]
pub struct ApiKey {
    pub key: String,
    pub secret: String,
    pub passphrase: String,
}
```

Add:

```rust
impl std::fmt::Debug for ApiKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("ApiKey")
            .field("key", &redact(&self.key))
            .field("secret", &"<redacted>")
            .field("passphrase", &"<redacted>")
            .finish()
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
            return Err(Error::Invalid("L2 address must be a 20-byte 0x address".into()));
        }
        self.api_key.validate()
    }
}

impl std::fmt::Debug for L2Credentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("L2Credentials")
            .field("address", &self.address)
            .field("api_key", &self.api_key)
            .finish()
    }
}
```

- [x] **Step 4: Replace the header builder signature**

Replace `build_l2_headers` with:

```rust
pub fn build_l2_headers(
    credentials: &L2Credentials,
    timestamp: i64,
    method: &str,
    path: &str,
    body: Option<&str>,
) -> Result<BTreeMap<String, String>> {
    credentials.validate()?;
    let signature = sign_hmac(
        &credentials.api_key.secret,
        timestamp,
        method,
        path,
        body,
    );
    Ok(BTreeMap::from([
        ("POLY_ADDRESS".into(), credentials.address.clone()),
        ("POLY_API_KEY".into(), credentials.api_key.key.clone()),
        ("POLY_PASSPHRASE".into(), credentials.api_key.passphrase.clone()),
        ("POLY_TIMESTAMP".into(), timestamp.to_string()),
        ("POLY_SIGNATURE".into(), signature),
    ]))
}
```

Keep `user_subscription_payload(&ApiKey, ...)` unchanged. Update the old header unit test to call `credentials()`.

- [x] **Step 5: Run GREEN and feature regressions**

```bash
cargo test --no-default-features --features authenticated auth::tests
cargo test --all-features auth::tests
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
```

- [x] **Step 6: Skip Task 1 commit (not requested)**

No commit was created; the global no-commit constraint takes precedence.

---

### Task 2: Authenticated GET transport

**Files:**

- Modify: `src/api/transport.rs`
- Test: `src/api/transport.rs`

**Interfaces:**

- Produces crate-internal `transport::Client::get_json_with_headers(path, headers)`
- Reuses existing `execute`, retry policy, timeout, connection pool, and concurrency semaphore

- [x] **Step 1: Write a failing local-server header test**

Add to `src/api/transport.rs` tests:

```rust
#[cfg(feature = "authenticated")]
#[tokio::test]
async fn authenticated_get_sends_caller_headers() {
    use std::{collections::BTreeMap, io::{Read, Write}, net::TcpListener, thread};

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut raw = [0; 4096];
        let length = stream.read(&mut raw).unwrap();
        let request = String::from_utf8_lossy(&raw[..length]);
        assert!(request.contains("poly_address: 0x1234"));
        assert!(request.contains("poly_api_key: key"));
        stream.write_all(
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
        ).unwrap();
    });
    let client = Client::new(Config::new(format!("http://{address}"))).unwrap();
    let _: serde_json::Value = client
        .get_json_with_headers(
            "/data/trades",
            &BTreeMap::from([
                ("POLY_ADDRESS".into(), "0x1234".into()),
                ("POLY_API_KEY".into(), "key".into()),
            ]),
        )
        .await
        .unwrap();
    server.join().unwrap();
}
```

- [x] **Step 2: Run and verify RED**

```bash
cargo test --all-features authenticated_get_sends_caller_headers
```

Expected: `get_json_with_headers` does not exist.

- [x] **Step 3: Implement the minimal internal transport method**

Add:

```rust
#[cfg(feature = "authenticated")]
pub(crate) async fn get_json_with_headers<T: DeserializeOwned>(
    &self,
    path: &str,
    headers: &std::collections::BTreeMap<String, String>,
) -> Result<T> {
    let mut request = self.http.get(self.url(path)?);
    for (name, value) in headers {
        request = request.header(name, value);
    }
    let body = self.execute(request, true).await?;
    Ok(serde_json::from_str(&body)?)
}
```

Do not add a generic public request-builder escape hatch.

- [x] **Step 4: Run GREEN and transport regressions**

```bash
cargo test --all-features authenticated_get_sends_caller_headers
cargo test --all-features transport::tests
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
```

- [x] **Step 5: Skip Task 2 commit (not requested)**

No commit was created; the global no-commit constraint takes precedence.

---

### Task 3: Typed authenticated trade and order pages

**Files:**

- Create: `src/api/authenticated_clob.rs`
- Create: `src/capabilities/clob_history.rs`
- Modify: `src/capabilities/clob_orders.rs`
- Modify: `src/client.rs`
- Modify: `src/lib.rs`
- Create: `tests/authenticated_client.rs`
- Modify: `tests/contracts.rs`
- Create: `tests/fixtures/clob/auth-trades-page.json`
- Create: `tests/fixtures/clob/auth-orders-page.json`
- Modify: `tests/fixtures/provenance.json`

**Interfaces:**

- Produces: `clob_history::CursorPage<T>`, `ClobTradeRecord`, and `OrderRecord`
- Produces: `authenticated_clob::{Client, TradeParams, OrderParams}`
- Produces: `trades_page`, `orders_page`, and `order`
- Produces matching unified `Client` methods under `authenticated`

- [x] **Step 1: Add official response fixtures and provenance**

Create `tests/fixtures/clob/auth-trades-page.json`:

```json
{
  "limit": 100,
  "next_cursor": "MTAw",
  "count": 1,
  "data": [
    {
      "id": "trade-123",
      "taker_order_id": "order-1",
      "market": "market-1",
      "asset_id": "asset-1",
      "side": "BUY",
      "size": "100",
      "fee_rate_bps": "30",
      "price": "0.5",
      "status": "TRADE_STATUS_CONFIRMED",
      "match_time": "1700000000",
      "last_update": "1700000001",
      "transaction_hash": "0xabc",
      "trader_side": "TAKER"
    }
  ]
}
```

Create `tests/fixtures/clob/auth-orders-page.json`:

```json
{
  "limit": 100,
  "next_cursor": "MTAw",
  "count": 1,
  "data": [
    {
      "id": "order-1",
      "status": "ORDER_STATUS_LIVE",
      "market": "market-1",
      "asset_id": "asset-1",
      "side": "BUY",
      "original_size": "100",
      "size_matched": "10",
      "price": "0.5",
      "created_at": 1700000000
    }
  ]
}
```

Append provenance entries using the two official API-reference URLs from the scope table, `retrievedAt: "2026-07-31"`, and `containsLiveData: false`.

- [x] **Step 2: Write failing DTO and local HTTP tests**

In `tests/contracts.rs`, change the post-public-plan fixture count from `11` to `13`, then add:

```rust
#[cfg(feature = "authenticated")]
#[test]
fn authenticated_clob_page_examples_preserve_decimal_text() {
    use polyrover::clob_history::{ClobTradeRecord, CursorPage, OrderRecord};
    let trades: CursorPage<ClobTradeRecord> = serde_json::from_str(include_str!(
        "fixtures/clob/auth-trades-page.json"
    )).unwrap();
    let orders: CursorPage<OrderRecord> = serde_json::from_str(include_str!(
        "fixtures/clob/auth-orders-page.json"
    )).unwrap();
    assert_eq!(trades.data[0].price, "0.5");
    assert_eq!(orders.data[0].original_size, "100");
    assert_eq!(trades.next_cursor, "MTAw");
}
```

Create `tests/authenticated_client.rs` with a local HTTP server and this test:

```rust
#![cfg(feature = "authenticated")]

#[tokio::test]
async fn authenticated_trade_page_signs_canonical_path_and_preserves_cursor() {
    use polyrover::{
        auth::{ApiKey, L2Credentials},
        authenticated_clob::{Client, TradeParams},
    };

    let (base_url, received, server) = serve_json(
        r#"{"limit":100,"next_cursor":"next==","count":0,"data":[]}"#,
    );
    let client = Client::new(base_url).unwrap();
    let credentials = L2Credentials {
        address: "0x1234567890123456789012345678901234567890".into(),
        api_key: ApiKey {
            key: "key".into(),
            secret: "secret".into(),
            passphrase: "pass".into(),
        },
    };
    let page = client.trades_page(&credentials, &TradeParams {
        market: "market-1".into(),
        after: Some(1),
        next_cursor: "cursor==".into(),
        ..Default::default()
    }).await.unwrap();

    assert_eq!(page.next_cursor, "next==");
    let request = received.recv().unwrap();
    assert!(request.starts_with("GET /data/trades?"));
    assert!(request.contains("market=market-1"));
    assert!(request.contains("after=1"));
    assert!(request.contains("next_cursor=cursor%3D%3D"));
    assert!(request.to_ascii_lowercase().contains("poly_address:"));
    assert!(request.to_ascii_lowercase().contains("poly_signature:"));
    server.join().unwrap();
}
```

Use the existing `tests/client.rs::serve_json` implementation verbatim inside the new test file so it is independently runnable.

- [x] **Step 3: Run tests and verify RED**

```bash
cargo test --all-features authenticated_clob_page_examples_preserve_decimal_text
cargo test --all-features --test authenticated_client
```

Expected: authenticated module, page types, and methods are missing.

- [x] **Step 4: Add read DTOs and expose them at the authenticated tier**

Create `src/capabilities/clob_history.rs` with:

```rust
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct CursorPage<T> {
    #[serde(default)]
    pub limit: u32,
    #[serde(default)]
    pub next_cursor: String,
    #[serde(default)]
    pub count: u32,
    #[serde(default)]
    pub data: Vec<T>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct ClobTradeRecord {
    #[serde(default)] pub id: String,
    #[serde(default)] pub taker_order_id: String,
    #[serde(default)] pub market: String,
    #[serde(default)] pub asset_id: String,
    #[serde(default)] pub side: String,
    #[serde(default, deserialize_with = "string_or_number")] pub size: String,
    #[serde(default, deserialize_with = "string_or_number")] pub fee_rate_bps: String,
    #[serde(default, deserialize_with = "string_or_number")] pub price: String,
    #[serde(default)] pub status: String,
    #[serde(default, deserialize_with = "string_or_number")] pub match_time: String,
    #[serde(default, deserialize_with = "string_or_number")] pub last_update: String,
    #[serde(default)] pub transaction_hash: String,
    #[serde(default)] pub trader_side: String,
}
```

Export `clob_history` from `src/lib.rs` only under `authenticated`. Keep `clob_orders` gated by `execution`; move its existing `OrderRecord` definition into `clob_history` and re-export it with `pub use crate::clob_history::OrderRecord;` so execution callers retain the existing path without exposing execution DTOs to authenticated-only builds.

- [x] **Step 5: Implement the authenticated client**

Create `src/api/authenticated_clob.rs`:

```rust
//! L2-authenticated, read-only CLOB trade and order queries.

use chrono::Utc;

use crate::{
    auth::{build_l2_headers, L2Credentials},
    clob_history::{ClobTradeRecord, CursorPage, OrderRecord},
    query::escape,
    transport, Error, Result,
};

pub const DEFAULT_BASE_URL: &str = "https://clob.polymarket.com";

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TradeParams {
    pub id: String,
    pub maker_address: String,
    pub market: String,
    pub asset_id: String,
    pub before: Option<i64>,
    pub after: Option<i64>,
    pub next_cursor: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OrderParams {
    pub id: String,
    pub market: String,
    pub asset_id: String,
    pub next_cursor: String,
}

#[derive(Clone)]
pub struct Client {
    transport: transport::Client,
}

impl Client {
    pub fn new(base_url: impl Into<String>) -> Result<Self> {
        let base_url = match base_url.into() {
            value if value.trim().is_empty() => DEFAULT_BASE_URL.into(),
            value => value,
        };
        Ok(Self { transport: transport::Client::new(transport::Config::new(base_url))? })
    }

    pub(crate) fn from_transport(transport: transport::Client) -> Self {
        Self { transport }
    }

    pub async fn trades_page(
        &self,
        credentials: &L2Credentials,
        params: &TradeParams,
    ) -> Result<CursorPage<ClobTradeRecord>> {
        self.signed_get(credentials, "/data/trades", &trade_path(params)).await
    }

    pub async fn orders_page(
        &self,
        credentials: &L2Credentials,
        params: &OrderParams,
    ) -> Result<CursorPage<OrderRecord>> {
        self.signed_get(credentials, "/data/orders", &order_path(params)).await
    }

    pub async fn order(
        &self,
        credentials: &L2Credentials,
        order_id: &str,
    ) -> Result<OrderRecord> {
        if order_id.trim().is_empty() {
            return Err(Error::Invalid("authenticated order_id is required".into()));
        }
        let path = format!("/data/order/{}", escape(order_id));
        self.signed_get(credentials, &path, &path).await
    }

    async fn signed_get<T: serde::de::DeserializeOwned>(
        &self,
        credentials: &L2Credentials,
        canonical_path: &str,
        request_path: &str,
    ) -> Result<T> {
        let headers = build_l2_headers(
            credentials,
            Utc::now().timestamp(),
            "GET",
            canonical_path,
            None,
        )?;
        self.transport.get_json_with_headers(request_path, &headers).await
    }
}
```

Add these exact query helpers in the same file:

```rust
fn trade_path(params: &TradeParams) -> String {
    query_path(
        "/data/trades",
        vec![
            text_pair("id", &params.id),
            text_pair("maker_address", &params.maker_address),
            text_pair("market", &params.market),
            text_pair("asset_id", &params.asset_id),
            params.before.map(|value| ("before", value.to_string())),
            params.after.map(|value| ("after", value.to_string())),
            text_pair("next_cursor", &params.next_cursor),
        ],
    )
}

fn order_path(params: &OrderParams) -> String {
    query_path(
        "/data/orders",
        vec![
            text_pair("id", &params.id),
            text_pair("market", &params.market),
            text_pair("asset_id", &params.asset_id),
            text_pair("next_cursor", &params.next_cursor),
        ],
    )
}

fn text_pair<'a>(key: &'a str, value: &str) -> Option<(&'a str, String)> {
    (!value.trim().is_empty()).then(|| (key, value.to_string()))
}

fn query_path(base: &str, pairs: Vec<Option<(&str, String)>>) -> String {
    let query = pairs
        .into_iter()
        .flatten()
        .map(|(key, value)| format!("{}={}", escape(key), escape(&value)))
        .collect::<Vec<_>>()
        .join("&");
    if query.is_empty() {
        base.into()
    } else {
        format!("{base}?{query}")
    }
}
```

- [x] **Step 6: Add unified-client delegates without storing credentials**

Import the module only for authenticated builds:

```rust
#[cfg(feature = "authenticated")]
use crate::authenticated_clob;
```

Add this field to unified `Client`:

```rust
#[cfg(feature = "authenticated")]
authenticated_clob: authenticated_clob::Client,
```

Add this initializer inside `Client::new`'s `Self` literal, using the already-created shared transport:

```rust
#[cfg(feature = "authenticated")]
authenticated_clob: authenticated_clob::Client::from_transport(
    transport.with_base_url(config.clob_base_url.clone()),
),
```

Keep the existing public CLOB initializer and use `config.clob_base_url` there after the authenticated initializer. Then add these methods under `#[cfg(feature = "authenticated")]`:

```rust
#[cfg(feature = "authenticated")]
pub async fn authenticated_trades_page(
    &self,
    credentials: &crate::auth::L2Credentials,
    params: &crate::authenticated_clob::TradeParams,
) -> Result<crate::clob_history::CursorPage<crate::clob_history::ClobTradeRecord>> {
    self.authenticated_clob.trades_page(credentials, params).await
}

#[cfg(feature = "authenticated")]
pub async fn authenticated_orders_page(
    &self,
    credentials: &crate::auth::L2Credentials,
    params: &crate::authenticated_clob::OrderParams,
) -> Result<crate::clob_history::CursorPage<crate::clob_history::OrderRecord>> {
    self.authenticated_clob.orders_page(credentials, params).await
}

#[cfg(feature = "authenticated")]
pub async fn authenticated_order(
    &self,
    credentials: &crate::auth::L2Credentials,
    order_id: &str,
) -> Result<crate::clob_history::OrderRecord> {
    self.authenticated_clob.order(credentials, order_id).await
}
```

Export `authenticated_clob` from `src/lib.rs` only under `authenticated`.

- [x] **Step 7: Run GREEN and all feature-tier regressions**

```bash
cargo test --all-features --test authenticated_client
cargo test --all-features authenticated_clob_page_examples_preserve_decimal_text
cargo test --no-default-features --features public
cargo test --no-default-features --features authenticated
cargo test --no-default-features --features execution
cargo test --all-features
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
jq empty tests/fixtures/provenance.json
```

Expected: public-only builds contain no authenticated module; authenticated and execution builds pass; no test contacts Polymarket.

- [x] **Step 8: Skip Task 3 commit (not requested)**

No commit was created; the global no-commit constraint takes precedence.

---

### Task 4: Capability, documentation, and boundary verification

**Files:**

- Modify: `capabilities.json`
- Modify: `src/capabilities/capabilities.rs`
- Modify: `tests/feature_contract.rs`
- Modify: `docs/endpoint-capability-matrix.md`
- Modify: `README.md`
- Modify: `CHANGELOG.md`

**Interfaces:**

- Marks only `clob.trades.list`, `clob.orders.list`, and `clob.order.read` implemented.
- Keeps every mutating, key-creation, wallet, and execution capability unchanged.

- [x] **Step 1: Write the failing feature contract**

Add to `tests/feature_contract.rs`:

```rust
#[test]
fn authenticated_history_reads_are_implemented_without_execution() {
    for id in ["clob.trades.list", "clob.orders.list", "clob.order.read"] {
        assert_eq!(
            CapabilityCatalog::by_id(id).unwrap().status,
            CapabilityStatus::Implemented,
            "{id}"
        );
    }
    for id in [
        "clob.orders.limit.submit",
        "clob.orders.market.submit",
        "clob.order.cancel",
        "clob.apiKey.createOrDerive",
    ] {
        assert_ne!(
            CapabilityCatalog::by_id(id).unwrap().status,
            CapabilityStatus::Implemented,
            "{id}"
        );
    }
}
```

- [x] **Step 2: Run and verify RED**

```bash
cargo test --all-features authenticated_history_reads_are_implemented_without_execution
```

Expected: the three read statuses are planned or DTO-only.

- [x] **Step 3: Update both capability catalogs**

For each read capability, set `status: "implemented"`, list the exact authenticated module and unified method, and use `tests/authenticated_client.rs` plus `tests/contracts.rs` as evidence. Set notes to:

```text
L2-authenticated library-only atomic read; credentials are borrowed per request and redacted; no CLI, key creation, signing key, mutation, traversal, or persistence.
```

Do not change any capability whose operation is `submit`, `cancel`, `update`, or API-key creation/derivation.

- [x] **Step 4: Update the endpoint matrix and README**

Replace the authenticated-read planned row with three implemented rows for `/data/trades`, `/data/orders`, and `/data/order/{orderID}`. Document canonical signing paths, opaque `next_cursor`, per-call credential borrowing, no authenticated CLI, and no live credential canary.

Add this README safety text:

```markdown
Authenticated history is an opt-in library surface. Callers pass
`&L2Credentials` to each read; Polyrover does not load, store, print, or expose
credentials through `ClientConfig` or CLI commands. MegaBot consumers must not
enable this feature and continue to compile only `public`.
```

Add this Rust example using caller-provided variables only:

```rust
use polyrover::{
    auth::L2Credentials,
    authenticated_clob::TradeParams,
    Client,
};

let credentials = L2Credentials {
    address: account_address,
    api_key,
};
let page = client
    .authenticated_trades_page(
        &credentials,
        &TradeParams {
            after: Some(1_700_000_000),
            next_cursor: previous_next_cursor,
            ..Default::default()
        },
    )
    .await?;
```

State immediately below it that `account_address`, `api_key`, and `previous_next_cursor` are supplied by the caller and must not be logged. Add an `Unreleased` changelog bullet without changing package version.

- [x] **Step 5: Run the final safety and compatibility gate**

```bash
cargo check --lib --no-default-features
cargo test --no-default-features --features public
cargo test --no-default-features --features authenticated
cargo test --no-default-features --features execution
cargo test --all-features
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
RUSTDOCFLAGS='-D warnings' cargo doc --all-features --no-deps
cargo test --manifest-path ../rust-crypto-data/Cargo.toml
python3 - <<'PY'
from pathlib import Path
text = Path('src/client.rs').read_text()
assert 'api_secret' not in text
assert 'api_passphrase' not in text
main = Path('src/main.rs').read_text()
assert 'L2Credentials' not in main
assert 'POLY_API_KEY' not in main
PY
jq empty capabilities.json
git diff --check
```

Expected: all tiers pass; the MegaBot public-only consumer passes; no CLI or unified config stores credentials; no live endpoint is contacted.

- [x] **Step 6: Skip Task 4 commit (not requested)**

No commit was created; the global no-commit constraint takes precedence.

## Final self-review

- **Spec coverage:** L2 address/API credential headers, authenticated trade pages, order pages, order lookup, opaque cursors, deterministic contracts, capability evidence, and docs are covered.
- **Safety coverage:** No private key, API-key creation, CLI secret path, mutation, automatic traversal, persistence, or MegaBot authenticated dependency is added.
- **Type consistency:** `L2Credentials`, `CursorPage<T>`, `ClobTradeRecord`, `TradeParams`, and `OrderParams` have one definition and matching use across source/tests/docs.
- **Completeness scan:** Every behavior task has explicit code, RED, GREEN, validation, and commit commands.

## Execution handoff

Execute only after public historical parity is green. Use `executing-plans` in the primary checkout on `main`, one task at a time. Success means authenticated reads work with borrowed credentials under `authenticated`, public-only consumers remain unchanged, and all mutating capabilities remain unimplemented.
