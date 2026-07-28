# Polyoxide-Inspired Public SDK Improvements Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Fresh-subagent tooling is not available in the current Pi harness, so execute inline with a reviewer checkpoint after each task.

**Goal:** Make Polyrover's read-only Rust SDK more reliable and composable by adding bounded HTTP resilience, atomic CLOB price history, contract-drift evidence, operational API guidance, an idiomatic market-event stream, and decimal-safe fill calculations.

**Architecture:** Keep Polyrover stateless and read-only. The shared HTTP transport owns retries and a process-wide concurrency semaphore; typed API adapters remain thin; the unified `Client` remains the primary facade. The consuming server owns polling, scheduling, manifests, persistence, alerts, analytics, and PostgreSQL writes.

**Tech Stack:** Rust 2021, Tokio, Reqwest, Futures, Serde, `rust_decimal` only for the final arithmetic slice, local TCP/WebSocket fixtures, ignored public live canaries.

## Global Constraints

- MegaBot consumers continue to use `default-features = false, features = ["public"]`.
- Add no signing, order submission, cancellation, wallet, relayer, bridge transfer, credential, or private endpoint behavior.
- Retry only `429 Too Many Requests` and `425 Too Early` automatically; do not automatically retry 5xx responses.
- Retry read-only GET requests and explicitly named idempotent read POST requests only.
- Keep live canaries ignored and absent from ordinary CI.
- Preserve upstream decimal strings at serialization boundaries.
- Use RED → GREEN for every behavior change under `docs/project/TDD-HARD-RULE.md`.
- Do not add a downloader, scheduler, database, cache, alert system, or persistence layer.
- Do not commit unless the maintainer explicitly requests it; use `git diff --check` as each task checkpoint.
- Preserve unrelated untracked research artifacts.

---

## Roadmap

| Milestone | Priority | Deliverable | Depends on | Exit gate |
| --- | --- | --- | --- | --- |
| 1. HTTP resilience | P0 | Shared bounded retry/backoff and concurrency across unified public clients | None | Local fixtures prove 429/425 retry, exhaustion, fractional `Retry-After`, and shared semaphore behavior |
| 2. Atomic price history | P0 | Typed single and batch CLOB history methods on `clob::Client` and unified `Client` | Milestone 1 | Local HTTP integration tests prove query/body shape and batch validation |
| 3. Contract-drift evidence | P0 | Provenance-linked fixtures plus ignored public canaries | Milestone 2 | Deterministic contract tests pass; live tests compile but do not run by default |
| 4. Upstream traps | P1 | Rustdoc and capability docs for defaults, cursors, URL ceilings, and batch sizes | Milestones 2–3 | `cargo doc` and link/path checks pass |
| 5. Stream adapter | P1 | Borrowed `Stream<Item = Result<MarketEvent>>` over existing reconnect logic | None | Local WebSocket test consumes an array frame event-by-event and retains stats |
| 6. Decimal simulation | P1 | Exact internal book walking and fee arithmetic with unchanged string output | Milestones 1–5 independent | Regression proves seven `0.1` fills complete exactly; all existing simulation/CLI tests remain unchanged |

Milestones 1–4 form the first release candidate. Milestones 5–6 are independent follow-up slices and should not delay HTTP/history delivery.

---

### Task 1: Bounded HTTP retry and shared concurrency

**Files:**
- Modify: `src/api/transport.rs`
- Modify: `src/error.rs`
- Modify: `src/client.rs`
- Modify: `src/api/gamma.rs`
- Modify: `src/api/clob.rs`
- Modify: `src/api/data.rs`
- Modify: `src/api/crypto_price.rs`
- Test: `src/api/transport.rs`
- Test: `src/error.rs`
- Test: `tests/client.rs`

**Interfaces:**
- Produces: `transport::RetryPolicy { max_retries, base_delay_ms, max_delay_ms }`
- Produces: `transport::Config.max_concurrent_requests: usize`
- Produces: `transport::Client::with_base_url(&self, base_url) -> Self`
- Produces: `transport::Client::post_json_idempotent(path, body)`
- Produces: `Error::is_retriable(&self) -> bool`
- Changes: `ClientConfig` gains `http_timeout_secs`, `http_retry`, and `http_max_concurrent_requests`
- Preserves: direct `gamma::Client::new`, `clob::Client::new`, `data::Client::new`, and `crypto_price::Client::new`

- [x] **Step 1: Write failing retry-policy and error-classification tests**

Add these tests to `src/api/transport.rs`:

```rust
#[test]
fn retry_policy_handles_429_425_fractional_header_and_cap() {
    let policy = RetryPolicy {
        max_retries: 2,
        base_delay_ms: 100,
        max_delay_ms: 500,
    };
    assert_eq!(
        retry_delay(429, 0, Some("0.25"), &policy),
        Some(Duration::from_millis(250))
    );
    assert_eq!(
        retry_delay(425, 1, None, &policy),
        Some(Duration::from_millis(200))
    );
    assert_eq!(
        retry_delay(429, 0, Some("60"), &policy),
        Some(Duration::from_millis(500))
    );
    assert_eq!(retry_delay(500, 0, None, &policy), None);
    assert_eq!(retry_delay(429, 2, None, &policy), None);
}

#[tokio::test]
async fn concurrency_limit_is_shared_across_retargeted_clients() {
    let client = Client::new(Config {
        max_concurrent_requests: 1,
        ..Config::new("https://gamma-api.polymarket.com")
    })
    .unwrap();
    let sibling = client.with_base_url("https://clob.polymarket.com");
    let permit = client.acquire_concurrency().await.unwrap();
    let blocked = tokio::time::timeout(
        Duration::from_millis(20),
        sibling.acquire_concurrency(),
    )
    .await;
    assert!(blocked.is_err());
    drop(permit);
}
```

Add this test to `src/error.rs`:

```rust
#[test]
fn retriable_classification_is_broader_than_automatic_retry() {
    assert!(Error::RateLimited {
        retry_after_secs: None,
    }
    .is_retriable());
    assert!(Error::Api {
        status: 425,
        body: String::new(),
    }
    .is_retriable());
    assert!(Error::Api {
        status: 503,
        body: String::new(),
    }
    .is_retriable());
    assert!(!Error::Api {
        status: 400,
        body: String::new(),
    }
    .is_retriable());
    assert!(!Error::Invalid("bad input".into()).is_retriable());
}
```

- [x] **Step 2: Run the focused tests and verify RED**

Run:

```bash
cargo test --all-features transport::tests::retry_policy_handles_429_425_fractional_header_and_cap
cargo test --all-features error::tests::retriable_classification_is_broader_than_automatic_retry
```

Expected: compilation fails because `RetryPolicy`, `retry_delay`, `acquire_concurrency`, and `Error::is_retriable` do not exist.

- [x] **Step 3: Add the retry policy, shared semaphore, and retriable classification**

In `src/api/transport.rs`, add the imports and policy:

```rust
use std::{sync::Arc, time::Duration};

use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetryPolicy {
    pub max_retries: u32,
    pub base_delay_ms: u64,
    pub max_delay_ms: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            base_delay_ms: 500,
            max_delay_ms: 10_000,
        }
    }
}
```

Extend `transport::Config` and its constructor:

```rust
#[derive(Clone, Debug)]
pub struct Config {
    pub base_url: String,
    pub timeout_secs: u64,
    pub user_agent: String,
    pub retry: RetryPolicy,
    pub max_concurrent_requests: usize,
}

impl Config {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: trim_base(base_url.into()),
            timeout_secs: 30,
            user_agent: "polyrover/0.1".into(),
            retry: RetryPolicy::default(),
            max_concurrent_requests: 8,
        }
    }
}
```

Replace the `transport::Client` fields and constructor with:

```rust
#[derive(Clone)]
pub struct Client {
    base_url: String,
    http: reqwest::Client,
    retry: RetryPolicy,
    concurrency: Arc<Semaphore>,
}

impl Client {
    pub fn new(config: Config) -> Result<Self> {
        if config.max_concurrent_requests == 0 {
            return Err(Error::Invalid(
                "max_concurrent_requests must be greater than zero".into(),
            ));
        }
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.timeout_secs))
            .user_agent(config.user_agent)
            .build()?;
        Ok(Self {
            base_url: config.base_url,
            http,
            retry: config.retry,
            concurrency: Arc::new(Semaphore::new(config.max_concurrent_requests)),
        })
    }

    pub fn with_base_url(&self, base_url: impl Into<String>) -> Self {
        Self {
            base_url: trim_base(base_url.into()),
            ..self.clone()
        }
    }

    async fn acquire_concurrency(&self) -> Result<OwnedSemaphorePermit> {
        self.concurrency
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Http("HTTP concurrency limiter closed".into()))
    }
}
```

Add the policy helper to `src/api/transport.rs`:

```rust
fn retry_delay(
    status: u16,
    attempt: u32,
    retry_after: Option<&str>,
    policy: &RetryPolicy,
) -> Option<Duration> {
    if !matches!(status, 425 | 429) || attempt >= policy.max_retries {
        return None;
    }
    let millis = retry_after
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value >= 0.0)
        .map(|seconds| (seconds * 1_000.0) as u64)
        .unwrap_or_else(|| {
            policy
                .base_delay_ms
                .saturating_mul(1_u64 << attempt.min(16))
        })
        .min(policy.max_delay_ms);
    Some(Duration::from_millis(millis))
}
```

Add to `src/error.rs`:

```rust
impl Error {
    pub fn is_retriable(&self) -> bool {
        match self {
            Self::Http(_) | Self::RateLimited { .. } | Self::ReconnectExhausted { .. } => true,
            Self::Api { status, .. } => *status == 425 || (500..=599).contains(status),
            Self::WebSocket(_) => true,
            Self::Json(_) | Self::Url(_) | Self::Invalid(_) => false,
        }
    }
}
```

- [x] **Step 4: Run the focused tests and verify GREEN**

Run:

```bash
cargo test --all-features transport::tests::retry_policy_handles_429_425_fractional_header_and_cap
cargo test --all-features transport::tests::concurrency_limit_is_shared_across_retargeted_clients
cargo test --all-features error::tests::retriable_classification_is_broader_than_automatic_retry
```

Expected: all three tests pass.

- [x] **Step 5: Write failing local-server tests for retry, exhaustion, and permit release**

Add a `serve_sequence` helper beside `serve_json` in `tests/client.rs`:

```rust
fn serve_sequence(
    responses: Vec<&'static str>,
) -> (String, mpsc::Receiver<String>, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (requests, received) = mpsc::channel();
    let handle = thread::spawn(move || {
        for response in responses {
            let (mut stream, _) = listener.accept().unwrap();
            let mut raw = [0; 4096];
            let length = stream.read(&mut raw).unwrap();
            requests
                .send(String::from_utf8_lossy(&raw[..length]).into_owned())
                .unwrap();
            stream.write_all(response.as_bytes()).unwrap();
        }
    });
    (format!("http://{address}"), received, handle)
}
```

Add these integration tests:

```rust
#[tokio::test]
async fn public_get_retries_429_using_fractional_retry_after() {
    let (clob_base_url, received, server) = serve_sequence(vec![
        "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 0\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 16\r\nConnection: close\r\n\r\n{\"price\":\"0.42\"}",
    ]);
    let client = Client::new(ClientConfig {
        clob_base_url,
        http_retry: polyrover::transport::RetryPolicy {
            max_retries: 1,
            base_delay_ms: 0,
            max_delay_ms: 0,
        },
        ..ClientConfig::default()
    })
    .unwrap();

    assert_eq!(client.price("token-1", "buy").await.unwrap(), "0.42");
    assert_eq!(received.iter().take(2).count(), 2);
    server.join().unwrap();
}

#[tokio::test]
async fn public_get_retries_425() {
    let (clob_base_url, received, server) = serve_sequence(vec![
        "HTTP/1.1 425 Too Early\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 16\r\nConnection: close\r\n\r\n{\"price\":\"0.43\"}",
    ]);
    let client = Client::new(ClientConfig {
        clob_base_url,
        http_retry: polyrover::transport::RetryPolicy {
            max_retries: 1,
            base_delay_ms: 0,
            max_delay_ms: 0,
        },
        ..ClientConfig::default()
    })
    .unwrap();

    assert_eq!(client.price("token-1", "buy").await.unwrap(), "0.43");
    assert_eq!(received.iter().take(2).count(), 2);
    server.join().unwrap();
}
```

- [x] **Step 6: Run the local-server tests and verify RED**

Run:

```bash
cargo test --all-features public_get_retries_429_using_fractional_retry_after
cargo test --all-features public_get_retries_425
```

Expected: compilation fails because `ClientConfig` has no HTTP policy fields, or the request returns the first 429/425 response without retrying.

- [x] **Step 7: Route GET and explicit idempotent POST reads through one retry loop**

Replace the three transport methods and add `execute`:

```rust
pub async fn get_json<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
    let body = self
        .execute(self.http.get(self.url(path)?), true)
        .await?;
    Ok(serde_json::from_str(&body)?)
}

pub async fn post_json<B: Serialize, T: DeserializeOwned>(
    &self,
    path: &str,
    body: &B,
) -> Result<T> {
    let body = self
        .execute(self.http.post(self.url(path)?).json(body), false)
        .await?;
    Ok(serde_json::from_str(&body)?)
}

pub async fn post_json_idempotent<B: Serialize, T: DeserializeOwned>(
    &self,
    path: &str,
    body: &B,
) -> Result<T> {
    let body = self
        .execute(self.http.post(self.url(path)?).json(body), true)
        .await?;
    Ok(serde_json::from_str(&body)?)
}

pub async fn get_raw(&self, path: &str) -> Result<String> {
    self.execute(self.http.get(self.url(path)?), true).await
}

async fn execute(&self, request: reqwest::RequestBuilder, retryable: bool) -> Result<String> {
    let mut attempt = 0;
    loop {
        let request = request
            .try_clone()
            .ok_or_else(|| Error::Invalid("HTTP request body cannot be retried".into()))?;
        let permit = self.acquire_concurrency().await?;
        let response = request.send().await;
        drop(permit);
        let response = response?;
        let status = response.status().as_u16();
        let retry_after = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok());
        if retryable {
            if let Some(delay) = retry_delay(status, attempt, retry_after, &self.retry) {
                attempt += 1;
                tokio::time::sleep(delay).await;
                continue;
            }
        }
        return checked_body(response).await;
    }
}
```

Change `clob::Client::order_books` to call the explicitly retryable read POST:

```rust
self.transport.post_json_idempotent("/books", &params).await
```

Do not change generic `post_json`; its name continues to mean “send once.”

- [x] **Step 8: Share one transport across the unified client**

Add these fields to `ClientConfig` in `src/client.rs`:

```rust
pub http_timeout_secs: u64,
pub http_retry: crate::transport::RetryPolicy,
pub http_max_concurrent_requests: usize,
```

Add these defaults:

```rust
http_timeout_secs: 30,
http_retry: crate::transport::RetryPolicy::default(),
http_max_concurrent_requests: 8,
```

Add this crate-visible constructor to each of `src/api/gamma.rs`, `src/api/clob.rs`, `src/api/data.rs`, and `src/api/crypto_price.rs`:

```rust
pub(crate) fn from_transport(transport: transport::Client) -> Self {
    Self { transport }
}
```

Replace `Client::new` in `src/client.rs` with:

```rust
pub fn new(config: ClientConfig) -> Result<Self> {
    let transport = crate::transport::Client::new(crate::transport::Config {
        base_url: config.gamma_base_url,
        timeout_secs: config.http_timeout_secs,
        user_agent: "polyrover/0.1".into(),
        retry: config.http_retry,
        max_concurrent_requests: config.http_max_concurrent_requests,
    })?;
    Ok(Self {
        gamma: gamma::Client::from_transport(transport.clone()),
        clob: clob::Client::from_transport(transport.with_base_url(config.clob_base_url)),
        data: data::Client::from_transport(transport.with_base_url(config.data_base_url)),
        crypto_price: crypto_price::Client::from_transport(
            transport.with_base_url(config.crypto_price_base_url),
        ),
    })
}
```

This shares the Reqwest pool, retry policy, and semaphore across Gamma, CLOB, Data API, and crypto-reference requests made by one unified client and all its clones.

- [x] **Step 9: Run Task 1 verification**

Run:

```bash
cargo test --all-features transport::tests
cargo test --all-features public_get_retries_429_using_fractional_retry_after
cargo test --all-features public_get_retries_425
cargo test --all-features client_reads_clob_books_in_one_batch
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --check
git diff --check
```

Expected: every command passes. Inspect `git diff -- src/api/transport.rs src/error.rs src/client.rs src/api` and verify no execution transport was added.

---

### Task 2: Typed single and batch CLOB price history

**Files:**
- Modify: `src/models/types.rs`
- Modify: `src/api/clob.rs`
- Modify: `src/client.rs`
- Modify: `capabilities.json`
- Modify: `src/capabilities/capabilities.rs`
- Test: `tests/client.rs`
- Test: `src/api/clob.rs`

**Interfaces:**
- Produces: `clob::PriceHistoryParams`
- Produces: `clob::BatchPriceHistoryParams`
- Produces: `types::ClobPricePoint`
- Produces: `types::ClobPriceHistory`
- Produces: `types::ClobBatchPriceHistory`
- Produces: `clob::Client::{price_history,batch_price_history}`
- Produces: unified `Client::{price_history,batch_price_history}`
- Consumes: `transport::Client::post_json_idempotent` from Task 1

- [x] **Step 1: Write failing unified-client integration tests**

Add imports in `tests/client.rs`:

```rust
use polyrover::clob::{BatchPriceHistoryParams, PriceHistoryParams};
```

Add tests:

```rust
#[tokio::test]
async fn client_reads_typed_clob_price_history() {
    let (clob_base_url, received, server) =
        serve_json(r#"{"history":[{"t":1700000000,"p":0.42}]}"#);
    let client = Client::new(ClientConfig {
        clob_base_url,
        ..ClientConfig::default()
    })
    .unwrap();

    let history = client
        .price_history(&PriceHistoryParams {
            token_id: "token-1".into(),
            start_ts: Some(1_700_000_000),
            end_ts: Some(1_700_003_600),
            interval: Some("1h".into()),
            fidelity: Some(5),
        })
        .await
        .unwrap();

    assert_eq!(history.history[0].timestamp, 1_700_000_000);
    assert_eq!(history.history[0].price, "0.42");
    let request = received.recv().unwrap();
    assert!(request.starts_with("GET /prices-history?"));
    assert!(request.contains("market=token-1"));
    assert!(request.contains("startTs=1700000000"));
    assert!(request.contains("endTs=1700003600"));
    assert!(request.contains("interval=1h"));
    assert!(request.contains("fidelity=5"));
    server.join().unwrap();
}

#[tokio::test]
async fn client_reads_clob_price_history_in_one_batch() {
    let (clob_base_url, received, server) = serve_json(
        r#"{"history":{"token-1":[{"t":1700000000,"p":"0.42"}],"token-2":[]}}"#,
    );
    let client = Client::new(ClientConfig {
        clob_base_url,
        ..ClientConfig::default()
    })
    .unwrap();

    let history = client
        .batch_price_history(&BatchPriceHistoryParams {
            markets: vec!["token-1".into(), "token-2".into()],
            start_ts: Some(1_700_000_000),
            interval: Some("1h".into()),
            fidelity: Some(5),
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(history.history["token-1"][0].price, "0.42");
    let request = received.recv().unwrap();
    assert!(request.starts_with("POST /batch-prices-history "));
    assert!(request.contains(r#""markets":["token-1","token-2"]"#));
    assert!(request.contains(r#""start_ts":1700000000"#));
    assert!(!request.contains("end_ts"));
    server.join().unwrap();
}
```

- [x] **Step 2: Run the tests and verify RED**

Run:

```bash
cargo test --all-features client_reads_typed_clob_price_history
cargo test --all-features client_reads_clob_price_history_in_one_batch
```

Expected: compilation fails because the parameter, response, and client method types do not exist.

- [x] **Step 3: Replace the raw history DTO with typed decimal-preserving DTOs**

In `src/models/types.rs`, import `BTreeMap` and replace `ClobPriceHistory`:

```rust
use std::collections::BTreeMap;

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct ClobPricePoint {
    #[serde(rename = "t")]
    pub timestamp: i64,
    #[serde(rename = "p", default, deserialize_with = "string_or_number")]
    pub price: String,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct ClobPriceHistory {
    #[serde(default)]
    pub history: Vec<ClobPricePoint>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct ClobBatchPriceHistory {
    #[serde(default)]
    pub history: BTreeMap<String, Vec<ClobPricePoint>>,
}
```

This is a deliberate pre-1.0 tightening from `Vec<serde_json::Value>` to typed points while preserving exact price text.

- [x] **Step 4: Add parameter types, validation, and path construction**

In `src/api/clob.rs`, import `ClobBatchPriceHistory` and `ClobPriceHistory`, then add:

```rust
const PRICE_HISTORY_INTERVALS: &[&str] = &["max", "all", "1m", "1w", "1d", "6h", "1h"];
const MAX_BATCH_PRICE_HISTORY_MARKETS: usize = 20;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PriceHistoryParams {
    pub token_id: String,
    pub start_ts: Option<i64>,
    pub end_ts: Option<i64>,
    pub interval: Option<String>,
    pub fidelity: Option<u32>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct BatchPriceHistoryParams {
    pub markets: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_ts: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_ts: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub interval: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fidelity: Option<u32>,
}

fn validate_history_window(
    start_ts: Option<i64>,
    end_ts: Option<i64>,
    interval: Option<&str>,
    fidelity: Option<u32>,
) -> Result<()> {
    if start_ts.zip(end_ts).is_some_and(|(start, end)| start > end) {
        return Err(Error::Invalid(
            "price history start_ts must not exceed end_ts".into(),
        ));
    }
    if interval.is_some_and(|value| !PRICE_HISTORY_INTERVALS.contains(&value)) {
        return Err(Error::Invalid(format!(
            "price history interval must be one of {}",
            PRICE_HISTORY_INTERVALS.join(", ")
        )));
    }
    if fidelity == Some(0) {
        return Err(Error::Invalid(
            "price history fidelity must be greater than zero".into(),
        ));
    }
    Ok(())
}

fn price_history_path(params: &PriceHistoryParams) -> Result<String> {
    if params.token_id.trim().is_empty() {
        return Err(Error::Invalid(
            "price history token_id is required".into(),
        ));
    }
    validate_history_window(
        params.start_ts,
        params.end_ts,
        params.interval.as_deref(),
        params.fidelity,
    )?;
    let mut pairs = vec![("market", params.token_id.clone())];
    if let Some(value) = params.start_ts {
        pairs.push(("startTs", value.to_string()));
    }
    if let Some(value) = params.end_ts {
        pairs.push(("endTs", value.to_string()));
    }
    if let Some(value) = &params.interval {
        pairs.push(("interval", value.clone()));
    }
    if let Some(value) = params.fidelity {
        pairs.push(("fidelity", value.to_string()));
    }
    Ok(format!(
        "/prices-history?{}",
        pairs
            .into_iter()
            .map(|(key, value)| format!("{}={}", escape(key), escape(&value)))
            .collect::<Vec<_>>()
            .join("&")
    ))
}
```

- [x] **Step 5: Add low-level and unified client methods**

Add to `clob::Client`:

```rust
pub async fn price_history(&self, params: &PriceHistoryParams) -> Result<ClobPriceHistory> {
    self.transport.get_json(&price_history_path(params)?).await
}

pub async fn batch_price_history(
    &self,
    params: &BatchPriceHistoryParams,
) -> Result<ClobBatchPriceHistory> {
    if params.markets.is_empty() || params.markets.len() > MAX_BATCH_PRICE_HISTORY_MARKETS {
        return Err(Error::Invalid(format!(
            "batch price history requires 1..={MAX_BATCH_PRICE_HISTORY_MARKETS} markets"
        )));
    }
    if params.markets.iter().any(|market| market.trim().is_empty()) {
        return Err(Error::Invalid(
            "batch price history markets must not contain blank asset IDs".into(),
        ));
    }
    validate_history_window(
        params.start_ts,
        params.end_ts,
        params.interval.as_deref(),
        params.fidelity,
    )?;
    self.transport
        .post_json_idempotent("/batch-prices-history", params)
        .await
}
```

Add imports and facade methods to `src/client.rs`:

```rust
use crate::clob::{BatchPriceHistoryParams, PriceHistoryParams};
use crate::types::{ClobBatchPriceHistory, ClobPriceHistory};

pub async fn price_history(&self, params: &PriceHistoryParams) -> Result<ClobPriceHistory> {
    self.clob.price_history(params).await
}

pub async fn batch_price_history(
    &self,
    params: &BatchPriceHistoryParams,
) -> Result<ClobBatchPriceHistory> {
    self.clob.batch_price_history(params).await
}
```

- [x] **Step 6: Add validation tests**

Add to `src/api/clob.rs` tests:

```rust
#[test]
fn price_history_rejects_invalid_windows_intervals_and_batch_sizes() {
    assert!(price_history_path(&PriceHistoryParams::default()).is_err());
    assert!(price_history_path(&PriceHistoryParams {
        token_id: "token".into(),
        start_ts: Some(2),
        end_ts: Some(1),
        ..Default::default()
    })
    .is_err());
    assert!(price_history_path(&PriceHistoryParams {
        token_id: "token".into(),
        interval: Some("forever".into()),
        ..Default::default()
    })
    .is_err());
}
```

Extend the batch integration test or add a low-level async test proving 21 markets fail before network I/O:

```rust
#[tokio::test]
async fn batch_price_history_rejects_more_than_twenty_markets() {
    let client = polyrover::clob::Client::new("http://127.0.0.1:1").unwrap();
    let error = client
        .batch_price_history(&BatchPriceHistoryParams {
            markets: (0..21).map(|index| format!("token-{index}")).collect(),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(error.to_string().contains("1..=20 markets"));
}
```

- [x] **Step 7: Mark the capability implemented without claiming CLI parity**

Change the `clob.priceHistory.read` record in `capabilities.json` to:

```json
{
  "id": "clob.priceHistory.read",
  "tier": "public",
  "service": "clob",
  "operation": "read",
  "transport": "https",
  "auth": ["none"],
  "signing": "none",
  "mutates": false,
  "cliCommand": ["clob", "price-history"],
  "extension": false,
  "summary": "Clob price history read.",
  "status": "implemented",
  "api": [
    "clob::Client::price_history",
    "clob::Client::batch_price_history",
    "Client::price_history",
    "Client::batch_price_history"
  ],
  "tests": ["tests/client.rs"],
  "guard": null,
  "notes": "SDK methods are implemented; CLI parity remains unimplemented. Batch requests accept at most 20 asset IDs."
}
```

Change only the corresponding `CapabilityStatus::Planned` to `CapabilityStatus::Implemented` in `src/capabilities/capabilities.rs`. The typed catalog does not expose API/test metadata, so no broader generator refactor belongs in this slice.

- [x] **Step 8: Run Task 2 verification**

Run:

```bash
cargo test --all-features client_reads_typed_clob_price_history
cargo test --all-features client_reads_clob_price_history_in_one_batch
cargo test --all-features batch_price_history_rejects_more_than_twenty_markets
cargo test --all-features --test feature_contract
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --check
jq empty capabilities.json
git diff --check
```

Expected: all commands pass and no CLI command was added.

---

### Task 3: Provenance-linked contract fixtures and ignored live canaries

**Files:**
- Create: `tests/fixtures/clob/price-history.json`
- Create: `tests/fixtures/clob/batch-price-history.json`
- Create: `tests/fixtures/provenance.json`
- Create: `tests/contracts.rs`
- Create: `tests/live_public.rs`

**Interfaces:**
- Consumes: typed history DTOs and unified methods from Task 2
- Produces: deterministic schema-drift fixtures with explicit evidence kind
- Produces: manual `POLYROVER_CANARY_TOKEN_ID` public canary
- Preserves: ordinary `cargo test` performs no live network access

- [x] **Step 1: Add contract-example fixtures and provenance**

Create `tests/fixtures/clob/price-history.json`:

```json
{
  "history": [
    {"t": 1700000000, "p": 0.42},
    {"t": 1700000060, "p": "0.425"}
  ]
}
```

Create `tests/fixtures/clob/batch-price-history.json`:

```json
{
  "history": {
    "token-1": [{"t": 1700000000, "p": 0.42}],
    "token-2": []
  }
}
```

Create `tests/fixtures/provenance.json`:

```json
{
  "schemaVersion": 1,
  "fixtures": [
    {
      "path": "clob/price-history.json",
      "kind": "contract-example",
      "source": "https://github.com/dilettante-trading/polyoxide/blob/8c1d8745/docs/specs/clob/markets.md#get-price-history",
      "retrievedAt": "2026-07-28",
      "endpoint": "GET https://clob.polymarket.com/prices-history",
      "containsLiveData": false
    },
    {
      "path": "clob/batch-price-history.json",
      "kind": "contract-example",
      "source": "https://github.com/dilettante-trading/polyoxide/blob/8c1d8745/docs/specs/clob/markets.md#get-batch-price-history",
      "retrievedAt": "2026-07-28",
      "endpoint": "POST https://clob.polymarket.com/batch-prices-history",
      "containsLiveData": false
    }
  ]
}
```

The `kind` and `containsLiveData` fields prevent synthetic documentation examples from being misrepresented as captured live payloads.

- [x] **Step 2: Write deterministic fixture/provenance tests**

Create `tests/contracts.rs`:

```rust
#![cfg(feature = "public")]

use std::{fs, path::Path};

use polyrover::types::{ClobBatchPriceHistory, ClobPriceHistory};
use serde_json::Value;

#[test]
fn clob_history_contract_examples_deserialize_and_preserve_decimal_text() {
    let single: ClobPriceHistory = serde_json::from_str(include_str!(
        "fixtures/clob/price-history.json"
    ))
    .unwrap();
    let batch: ClobBatchPriceHistory = serde_json::from_str(include_str!(
        "fixtures/clob/batch-price-history.json"
    ))
    .unwrap();

    assert_eq!(single.history[0].price, "0.42");
    assert_eq!(single.history[1].price, "0.425");
    assert_eq!(batch.history["token-1"][0].timestamp, 1_700_000_000);
}

#[test]
fn every_contract_fixture_has_explicit_provenance() {
    let manifest: Value = serde_json::from_str(include_str!("fixtures/provenance.json")).unwrap();
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let fixtures = manifest["fixtures"].as_array().unwrap();

    assert_eq!(manifest["schemaVersion"], 1);
    assert_eq!(fixtures.len(), 2);
    for fixture in fixtures {
        let relative = fixture["path"].as_str().unwrap();
        assert!(root.join(relative).is_file(), "missing fixture {relative}");
        assert_eq!(fixture["kind"], "contract-example");
        assert_eq!(fixture["containsLiveData"], false);
        assert!(fixture["source"]
            .as_str()
            .is_some_and(|source| source.starts_with("https://")));
        assert!(!fs::read_to_string(root.join(relative)).unwrap().is_empty());
    }
}
```

- [x] **Step 3: Run deterministic contract tests**

Run:

```bash
cargo test --all-features --test contracts
```

Expected: both tests pass without network access.

- [x] **Step 4: Add ignored public canaries**

Create `tests/live_public.rs`:

```rust
#![cfg(feature = "public")]

use polyrover::{
    clob::{BatchPriceHistoryParams, PriceHistoryParams},
    Client, ClientConfig,
};

fn token_id() -> String {
    std::env::var("POLYROVER_CANARY_TOKEN_ID")
        .expect("set POLYROVER_CANARY_TOKEN_ID to a public CLOB asset ID")
}

#[tokio::test]
#[ignore = "manual public API canary; never run in ordinary CI"]
async fn live_single_price_history_matches_the_typed_contract() {
    let history = Client::new(ClientConfig::default())
        .unwrap()
        .price_history(&PriceHistoryParams {
            token_id: token_id(),
            interval: Some("1d".into()),
            fidelity: Some(60),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(history
        .history
        .iter()
        .all(|point| !point.price.trim().is_empty()));
}

#[tokio::test]
#[ignore = "manual public API canary; never run in ordinary CI"]
async fn live_batch_price_history_matches_the_typed_contract() {
    let token_id = token_id();
    let history = Client::new(ClientConfig::default())
        .unwrap()
        .batch_price_history(&BatchPriceHistoryParams {
            markets: vec![token_id.clone()],
            interval: Some("1d".into()),
            fidelity: Some(60),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(history.history.contains_key(&token_id));
}
```

- [x] **Step 5: Prove live tests compile but remain ignored**

Run:

```bash
cargo test --all-features --test live_public
```

Expected: `2 ignored; 0 failed`. Do not run with `--ignored` during implementation. An operator may later run:

```bash
POLYROVER_CANARY_TOKEN_ID=<PUBLIC_ASSET_ID> \
  cargo test --all-features --test live_public -- --ignored
```

- [x] **Step 6: Run Task 3 verification**

Run:

```bash
cargo test --all-features --test contracts
cargo test --all-features --test live_public
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --check
jq empty tests/fixtures/provenance.json
git diff --check
```

Expected: deterministic tests pass, both live tests are ignored, and no secrets or captured headers exist in fixtures.

---

### Task 4: Document upstream defaults, cursors, URL ceilings, and batch limits

**Files:**
- Modify: `src/api/gamma.rs`
- Modify: `src/api/clob.rs`
- Modify: `docs/endpoint-capability-matrix.md`
- Modify: `README.md`

**Interfaces:**
- Produces: rustdoc guidance on `MarketParams`, `MarketKeysetParams`, `PriceHistoryParams`, and `BatchPriceHistoryParams`
- Produces: explicit distinction between official contract limits and Polyoxide's empirical safe sizes
- Adds no runtime behavior

- [x] **Step 1: Add precise rustdoc to Gamma parameter structs**

Add this rustdoc above `MarketParams`:

```rust
/// Offset-paginated Gamma market query.
///
/// Upstream implicitly behaves as `closed=false` when `closed` is omitted, so
/// identifier lookups can silently exclude closed markets. Set `closed` explicitly
/// when status matters. Polyoxide observed an approximately 8 KiB upstream URL
/// ceiling and recommends conservative chunks of at most 100 slugs, 50 CLOB token
/// IDs, or 60 condition IDs; these are empirical safe sizes, not protocol limits.
```

Add this rustdoc above `MarketKeysetParams`:

```rust
/// Keyset-paginated Gamma market query.
///
/// `after_cursor` is opaque and must be copied unchanged from `next_cursor`.
/// Upstream documents a maximum `limit` of 1000 and implicitly behaves as
/// `closed=false` when `closed` is omitted. Apply the same conservative identifier
/// chunking documented on [`MarketParams`].
```

Add field comments to both structs for `closed`, `clob_token_ids`, and `condition_ids` that point back to these rules. Do not add fixed runtime rejection for empirical URL ceilings.

- [x] **Step 2: Add precise rustdoc to price-history parameters**

Add above `PriceHistoryParams`:

```rust
/// Query for `GET /prices-history`.
///
/// `token_id` is the CLOB asset ID sent upstream as the `market` query parameter.
/// Timestamps are inclusive UNIX seconds. Supported intervals are `max`, `all`,
/// `1m`, `1w`, `1d`, `6h`, and `1h`; `fidelity` is resolution in minutes and
/// defaults upstream to 1.
```

Add above `BatchPriceHistoryParams`:

```rust
/// Body for the idempotent read-only `POST /batch-prices-history` endpoint.
///
/// `markets` contains 1 to 20 CLOB asset IDs. The server—not Polyrover—must split
/// larger jobs, schedule requests, resume failures, and persist returned points.
```

- [x] **Step 3: Update the endpoint matrix**

In `docs/endpoint-capability-matrix.md`:

1. Update `Last verified` to `2026-07-28`.
2. Change the CLOB public row method/event text to “Books, prices, single/batch price history, token fee rates, and market metadata.”
3. Add a “Public API operational notes” section with:

```markdown
## Public API operational notes

- Gamma omits closed markets when `closed` is absent; callers requiring closed
  or mixed-status results must set it explicitly.
- Gamma keyset cursors are opaque. Preserve `next_cursor` byte-for-byte as the
  next `after_cursor`; the documented keyset `limit` maximum is 1000.
- Polyoxide observed an approximately 8 KiB Gamma URL ceiling and uses
  conservative chunks of 100 slugs, 50 CLOB token IDs, and 60 condition IDs.
  These are empirical safe sizes, not upstream protocol guarantees.
- CLOB batch price history accepts at most 20 asset IDs. Polyrover performs one
  atomic request; consumers own splitting, retries across jobs, and persistence.
- Automatic HTTP retries are bounded to `429` and `425`. A server-provided
  numeric `Retry-After` overrides exponential backoff and is clamped to the
  configured maximum delay. 5xx responses are classified as retriable for the
  caller but are not resent automatically.
```

- [x] **Step 4: Update README limits without expanding CLI scope**

In `README.md`:

- Add “Read single/batch CLOB price history” to the Rust-library capability text, not the CLI table.
- Replace “CI uses deterministic local fixtures and has no scheduled public API canary” with “CI uses deterministic provenance-linked fixtures; public API canaries are ignored and operator-run only.”
- Add one Rust example calling `Client::price_history`; do not add a `clob price-history` CLI command.

Use this example:

```rust
use polyrover::clob::PriceHistoryParams;

let history = client
    .price_history(&PriceHistoryParams {
        token_id: "TOKEN_ID".into(),
        interval: Some("1d".into()),
        fidelity: Some(5),
        ..Default::default()
    })
    .await?;
```

- [x] **Step 5: Validate documentation**

Run:

```bash
cargo doc --all-features --no-deps
cargo test --all-features --test contracts
python3 - <<'PY'
from pathlib import Path
assert 'closed=false' in Path('src/api/gamma.rs').read_text()
assert 'batch price history' in Path('src/api/clob.rs').read_text().lower()
assert 'batch price history' in Path('docs/endpoint-capability-matrix.md').read_text().lower()
assert 'ignored and operator-run only' in Path('README.md').read_text()
PY
git diff --check
```

Expected: rustdoc builds with no warnings, fixture tests pass, and the documentation assertions succeed.

---

### Task 5: Borrowed idiomatic market-event stream adapter

**Files:**
- Modify: `src/streaming/stream_client.rs`
- Test: `src/streaming/stream_client.rs`
- Modify: `README.md`

**Interfaces:**
- Produces: `MarketWsClient::events(&mut self) -> impl Stream<Item = Result<MarketEvent>> + '_`
- Consumes: existing `read_events`, reconnect/resubscribe, deduplication, heartbeat, and stats behavior
- Preserves: `read_raw`, `read_events`, and `read_tracked` APIs

- [x] **Step 1: Write a failing stream-adapter test**

Add to `src/streaming/stream_client.rs` tests:

```rust
#[tokio::test]
async fn event_stream_flattens_array_frames_without_bypassing_stats() {
    use futures_util::{pin_mut, SinkExt, StreamExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        let _subscription = socket.next().await.unwrap().unwrap();
        socket
            .send(Message::Text(
                r#"[{"event_type":"new_market","id":"market-1"},{"event_type":"new_market","id":"market-2"}]"#.into(),
            ))
            .await
            .unwrap();
    });
    let mut client = MarketWsClient::connect(Config {
        url: format!("ws://{address}"),
        ..Default::default()
    })
    .await
    .unwrap();
    client.subscribe_assets(&["token-1".into()]).await.unwrap();

    {
        let events = client.events();
        pin_mut!(events);
        let first = events.next().await.unwrap().unwrap();
        let second = events.next().await.unwrap().unwrap();
        assert!(matches!(first, MarketEvent::NewMarket(row) if row.id == "market-1"));
        assert!(matches!(second, MarketEvent::NewMarket(row) if row.id == "market-2"));
    }
    assert_eq!(client.stats().messages_received, 2);
    server.await.unwrap();
}
```

- [x] **Step 2: Run the focused test and verify RED**

Run:

```bash
cargo test --all-features event_stream_flattens_array_frames_without_bypassing_stats
```

Expected: compilation fails because `MarketWsClient::events` does not exist.

- [x] **Step 3: Implement the borrowed adapter over existing behavior**

Add `VecDeque` to imports and add this method to `MarketWsClient`:

```rust
use std::{collections::VecDeque, time::Duration};

pub fn events(&mut self) -> impl futures_util::stream::Stream<Item = Result<MarketEvent>> + '_ {
    futures_util::stream::unfold(
        (self, VecDeque::<MarketEvent>::new()),
        |(client, mut pending)| async move {
            loop {
                if let Some(event) = pending.pop_front() {
                    return Some((Ok(event), (client, pending)));
                }
                let observed_at_ms = chrono::Utc::now().timestamp_millis();
                match client.read_events(observed_at_ms).await {
                    Ok(events) => pending.extend(events),
                    Err(error) => return Some((Err(error), (client, pending))),
                }
            }
        },
    )
}
```

The adapter borrows the existing client, flattens multi-event frames, and delegates all reconnect, heartbeat, deduplication, and stats behavior to `read_events`. It intentionally does not introduce another socket state machine.

- [x] **Step 4: Run stream and reconnect regression tests**

Run:

```bash
cargo test --all-features event_stream_flattens_array_frames_without_bypassing_stats
cargo test --all-features reconnects_and_resubscribes_after_reset
cargo test --all-features reconnect_preserves_dedup_so_replayed_messages_stay_suppressed
```

Expected: all tests pass.

- [x] **Step 5: Document server-side consumption**

Add a compact Rust-library example to `README.md`:

```rust
use futures_util::{pin_mut, StreamExt};

let events = market_client.events();
pin_mut!(events);
while let Some(event) = events.next().await {
    let event = event?;
    // Send the typed event to the server's persistence/analytics pipeline.
}
```

State that callers must stop/drop the adapter to regain mutable access to `MarketWsClient`, and that persistence remains outside Polyrover.

- [x] **Step 6: Run Task 5 verification**

Run:

```bash
cargo test --all-features stream_client
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --check
cargo doc --all-features --no-deps
git diff --check
```

Expected: all commands pass and existing read methods remain public.

---

### Task 6: Decimal-safe fill simulation without output-shape changes

**Files:**
- Modify: `Cargo.toml`
- Modify: `src/research/simulation.rs`
- Test: `src/research/simulation.rs`
- Test: `tests/cli.rs`
- Modify: `README.md`

**Interfaces:**
- Adds dependency: `rust_decimal = "1"`
- Changes only internal arithmetic from `f64` to `Decimal`
- Preserves: `Request`, `FillLevel`, `ResultRow`, `FeeScheduleRow`, JSON field names, and string output precision (six decimals; fees five)
- Preserves: category fee coefficients and aliases

- [x] **Step 1: Write the floating-point completion regression**

Add to `src/research/simulation.rs` tests:

```rust
#[test]
fn seven_tenths_fill_completes_without_binary_residue() {
    let book = ClobOrderBook {
        asset_id: "tok".into(),
        asks: (0..7)
            .map(|_| ClobOrderBookLevel {
                price: "1".into(),
                size: "0.1".into(),
            })
            .collect(),
        ..Default::default()
    };

    let result = simulate_book(
        &book,
        Request {
            token_id: "tok".into(),
            side: "buy".into(),
            amount: "0.7".into(),
            ..Default::default()
        },
    )
    .unwrap();

    assert!(result.complete);
    assert_eq!(result.filled_size, "0.7");
    assert_eq!(result.notional, "0.7");
    assert_eq!(result.unfilled_amount, "0");
}
```

- [x] **Step 2: Run the regression and verify RED**

Run:

```bash
cargo test --all-features seven_tenths_fill_completes_without_binary_residue
```

Expected: the test fails because binary subtraction leaves a positive residue and `complete` is false.

- [x] **Step 3: Add `rust_decimal` and replace internal arithmetic**

Add to `Cargo.toml`:

```toml
rust_decimal = "1"
```

In `src/research/simulation.rs`, add:

```rust
use std::str::FromStr;

use rust_decimal::{Decimal, RoundingStrategy};
```

Replace `simulate_book`, `apply_taker_fee`, `opposing_levels`, `parse_positive`, and the formatting helpers with this complete Decimal implementation. Keep `normalize_side` unchanged:

```rust
pub fn simulate_book(book: &ClobOrderBook, req: Request) -> Result<ResultRow> {
    let side = normalize_side(&req.side)?;
    let amount = parse_positive("--amount", &req.amount)?;
    let limit = if req.limit_price.trim().is_empty() {
        None
    } else {
        Some(parse_positive("--limit-price", &req.limit_price)?)
    };
    let mut levels = opposing_levels(book, side);
    levels.sort_by(|left, right| {
        if side == "buy" {
            left.0.cmp(&right.0)
        } else {
            right.0.cmp(&left.0)
        }
    });

    let best_price = levels.first().map(|level| fmt(level.0)).unwrap_or_default();
    let mut remaining = amount;
    let mut filled_size = Decimal::ZERO;
    let mut notional = Decimal::ZERO;
    let mut fills = Vec::new();
    let mut worst_price = String::new();

    for (price, size) in levels.iter().copied() {
        if limit.is_some_and(|bound| {
            (side == "buy" && price > bound) || (side == "sell" && price < bound)
        }) {
            break;
        }
        let (fill_size, fill_notional) = if side == "buy" {
            let level_notional = size * price;
            if remaining >= level_notional {
                (size, level_notional)
            } else {
                (remaining / price, remaining)
            }
        } else {
            let fill_size = remaining.min(size);
            (fill_size, fill_size * price)
        };
        if fill_size <= Decimal::ZERO {
            continue;
        }
        filled_size += fill_size;
        notional += fill_notional;
        fills.push(FillLevel {
            price: fmt(price),
            available_size: fmt(size),
            filled_size: fmt(fill_size),
            notional: fmt(fill_notional),
        });
        worst_price = fmt(price);
        remaining -= if side == "buy" {
            fill_notional
        } else {
            fill_size
        };
        if remaining <= Decimal::ZERO {
            remaining = Decimal::ZERO;
            break;
        }
    }

    let mut out = ResultRow {
        token_id: if book.asset_id.is_empty() {
            req.token_id
        } else {
            book.asset_id.clone()
        },
        market: book.market.clone(),
        side: side.into(),
        input_amount: fmt(amount),
        input_amount_type: if side == "buy" { "usdc" } else { "shares" }.into(),
        limit_price: limit.map(fmt).unwrap_or_default(),
        complete: remaining == Decimal::ZERO,
        filled_size: fmt(filled_size),
        notional: fmt(notional),
        best_price,
        worst_price,
        unfilled_amount: fmt(remaining),
        book_hash: book.hash.clone(),
        book_timestamp: book.timestamp.clone(),
        levels: fills,
        ..Default::default()
    };
    if filled_size > Decimal::ZERO {
        let average = notional / filled_size;
        out.average_price = fmt(average);
        out.expected_fill_price = out.average_price.clone();
        if let Ok(best) = Decimal::from_str(&out.best_price) {
            let slippage = if side == "buy" {
                average - best
            } else {
                best - average
            };
            out.slippage = fmt(slippage);
            if best > Decimal::ZERO {
                out.slippage_bps = fmt(slippage / best * Decimal::from(10_000));
            }
        }
    }
    Ok(out)
}

pub fn apply_taker_fee(result: &mut ResultRow, category: &str) -> Result<()> {
    let normalized = category.trim().to_ascii_lowercase();
    let canonical = match normalized.as_str() {
        "general" => "other",
        "world events" | "world-events" | "world_events" => "geopolitics",
        value => value,
    };
    let row = FEE_SCHEDULE
        .iter()
        .find(|row| row.category == canonical)
        .ok_or_else(|| Error::Invalid(format!("unsupported fee category {category:?}")))?;
    let rate = Decimal::from_str(&row.taker_fee_rate.to_string())
        .map_err(|_| Error::Invalid("fee schedule contains an invalid rate".into()))?;
    let fee = result
        .levels
        .iter()
        .try_fold(Decimal::ZERO, |total, level| {
            let price = Decimal::from_str(&level.price)
                .map_err(|_| Error::Invalid("simulated fill contains an invalid price".into()))?;
            if !(Decimal::ZERO..=Decimal::ONE).contains(&price) {
                return Err(Error::Invalid(
                    "simulated fill price must be between 0 and 1".into(),
                ));
            }
            let shares = Decimal::from_str(&level.filled_size)
                .map_err(|_| Error::Invalid("simulated fill contains an invalid size".into()))?;
            Ok::<_, Error>(total + shares * rate * price * (Decimal::ONE - price))
        })?;
    result.fee_category = row.category.into();
    result.taker_fee_rate = fmt(rate);
    result.estimated_taker_fee = fmt_fee(fee);
    Ok(())
}

fn opposing_levels(book: &ClobOrderBook, side: &str) -> Vec<(Decimal, Decimal)> {
    let rows = if side == "sell" {
        &book.bids
    } else {
        &book.asks
    };
    rows.iter()
        .filter_map(|level| {
            let price = Decimal::from_str(level.price.trim()).ok()?;
            let size = Decimal::from_str(level.size.trim()).ok()?;
            (price > Decimal::ZERO && size > Decimal::ZERO).then_some((price, size))
        })
        .collect()
}

fn parse_positive(name: &str, value: &str) -> Result<Decimal> {
    if value.contains('/') {
        return Err(Error::Invalid(format!("{name} must be a decimal")));
    }
    let number = Decimal::from_str(value.trim())
        .map_err(|_| Error::Invalid(format!("{name} must be a positive decimal")))?;
    (number > Decimal::ZERO)
        .then_some(number)
        .ok_or_else(|| Error::Invalid(format!("{name} must be a positive decimal")))
}

fn fmt_fee(value: Decimal) -> String {
    format_decimal(value, 5)
}

fn fmt(value: Decimal) -> String {
    format_decimal(value, 6)
}

fn format_decimal(value: Decimal, places: u32) -> String {
    value
        .round_dp_with_strategy(places, RoundingStrategy::MidpointNearestEven)
        .normalize()
        .to_string()
}
```

Delete the old `trim_decimal`, `f64` parsing, finite checks, and `total_cmp` code in the same edit.

- [x] **Step 4: Run simulation and CLI compatibility tests**

Run:

```bash
cargo test --all-features seven_tenths_fill_completes_without_binary_residue
cargo test --all-features simulation::tests
cargo test --all-features --test cli
```

Expected: all tests pass with unchanged serialized field names and existing expected values.

- [x] **Step 5: Update the documented arithmetic limit**

In `README.md`, replace:

```text
Calculations use validated decimal strings converted to `f64`; fill values use six decimal places.
```

with:

```text
Fill and fee calculations use decimal arithmetic internally; serialized fill values retain six decimal places and fee values retain five.
```

Remove “Simulation uses `f64`, not a domain-safe decimal type” from Current limits. Do not claim that all market-data analytics are decimal-safe: `market_data::Liquidity` and `Depth` remain `f64` in this release.

- [x] **Step 6: Run Task 6 verification**

Run:

```bash
cargo test --all-features
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --check
cargo doc --all-features --no-deps
git diff --check
```

Expected: the full suite passes and README claims only simulation/fee decimal safety.

---

## Deferred decision gates

These items are intentionally outside the six implementation tasks:

1. **Decimal `Liquidity` and `Depth` public fields:** changing `f64` fields to `Decimal` is a public API break. Do it only for a `0.2` release if the consuming server stores these derived values and requires exact reproducibility. The acceptance test must cover exact 1¢/2¢/5¢ depth boundaries without epsilon constants.
2. **Additional Gamma filters:** add liquidity, volume, date, tag, sports, or resolution filters only when a named server collector requires them. Extend both offset and keyset structs in the same slice.
3. **Endpoint-specific quota ledger:** add only after production metrics show the shared concurrency cap and bounded retries are insufficient.
4. **CLI price-history parity:** add only when operators need manual inspection. SDK methods are sufficient for server ingestion.
5. **Raw response escape hatch:** keep typed validation and fixture capture as the default; expose raw payloads only for a demonstrated schema-drift incident.

## Final release gate

After all selected milestones, run:

```bash
cargo test --all-features
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --check
cargo doc --all-features --no-deps
jq empty capabilities.json
jq empty tests/fixtures/provenance.json
git diff --check
git status --short
```

Expected:

- all deterministic tests pass;
- live public canaries report ignored unless an operator explicitly supplies a public token ID and `--ignored`;
- no credential, signer, order, wallet, downloader, scheduler, or persistence behavior appears in the diff;
- the only new dependency is `rust_decimal`, introduced in Milestone 6;
- unrelated research artifacts remain untouched.

## Self-review

- **Spec coverage:** HTTP resilience is Tasks 1; atomic history is Task 2; drift fixtures/canaries are Task 3; upstream traps are Task 4; Stream ergonomics are Task 5; decimal simulation is Task 6. Public market-data decimal output has an explicit versioned decision gate.
- **Scope:** every deliverable is stateless, read-only, and composable by the Rust server. Pipeline and database work are excluded.
- **Type consistency:** `PriceHistoryParams`, `BatchPriceHistoryParams`, `ClobPriceHistory`, `ClobBatchPriceHistory`, `RetryPolicy`, and `events()` names match across producers and consumers.
- **Placeholders:** implementation steps contain concrete paths, signatures, tests, commands, and expected results.
