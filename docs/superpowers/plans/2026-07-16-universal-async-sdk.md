# Universal Async Polyrover SDK Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Polyrover's blocking network stack with one async-only, feature-layered SDK and cut MegaBot's public-data collector over without adding any fund-moving capability.

**Architecture:** Polyrover keeps a synchronous feature-free core and exposes layered `public`, `authenticated`, `wallet`, `execution`, and `bridge` capability packages. Existing network method names become async over `reqwest` and `tokio-tungstenite`; `rust-crypto-data` consumes only `public` through supervised Tokio tasks and fail-closed bounded channels.

**Tech Stack:** Rust 2021, Cargo features, Tokio, reqwest 0.12, tokio-tungstenite 0.28, futures-util, Serde, local TCP/WebSocket fixtures.

## Global Constraints

- Follow `docs/superpowers/specs/2026-07-16-universal-async-sdk-design.md` and the repository `AGENTS.md`/`CONTEXT.md` safety boundaries.
- Preserve existing method names and DTO, parsing, reconnect, deduplication, ordering-of-received-events, statistics, and error semantics; network methods become async.
- Keep parsing, DTOs, simulation, HMAC helpers, wallet derivation, and book math synchronous.
- Default Cargo feature is exactly `public`; feature hierarchy is `authenticated → public`, `execution → authenticated + wallet`, and `full → execution + bridge`.
- Cargo features limit compilation and dependency exposure, not authority.
- Add no live order placement/cancellation, private-key signing/storage/discovery, relayer calls, or bridge execution.
- Core market/result identities support arbitrary Polymarket markets and outcomes; crypto Up/Down remains a specialized helper.
- `rust-crypto-data` must declare exactly `default-features = false, features = ["public"]` and must not import authenticated, wallet, execution, or bridge modules.
- Every bounded observation send times out before the heartbeat deadline; saturation is typed, supervised, recorded as queue-pressure health, and never silently dropped or forward-filled.
- REST reconciliation can restore current book state only; missing trades remain an explicit coverage gap.
- Use local fixtures only. Never call live Polymarket endpoints or load credentials in tests.
- Keep unrelated parent worktree and `polygolem` state untouched.
- Commit and push Polyrover before updating the parent repository's submodule pointer.

---

### Task 1: Establish Cargo capability boundaries

**Files:**
- Modify: `Cargo.toml`
- Modify: `src/lib.rs`
- Modify: `src/error.rs`
- Modify: `src/capabilities.rs`
- Modify: `tests/client.rs`
- Modify: `tests/cli.rs`
- Modify: `tests/market_results.rs`
- Create: `tests/feature_contract.rs`

**Interfaces:**
- Consumes: existing module names and current blocking transports.
- Produces: the final feature hierarchy, feature-aware `capabilities::all()`, a core-safe `Error::Http(String)`, and test gating used by every later task.

- [ ] **Step 1: Write the failing feature contract**

Create `tests/feature_contract.rs`:

```rust
use polyrover::capabilities;

fn ids() -> Vec<String> {
    capabilities::all().into_iter().map(|cap| cap.id).collect()
}

#[test]
fn reporting_matches_compiled_features() {
    let ids = ids();
    assert_eq!(ids.contains(&"gamma.markets".into()), cfg!(feature = "public"));
    assert_eq!(ids.contains(&"websocket.user".into()), cfg!(feature = "authenticated"));
    assert_eq!(ids.contains(&"relayer.deposit_wallet".into()), cfg!(feature = "wallet"));
    assert_eq!(ids.contains(&"clob.trading".into()), cfg!(feature = "execution"));
    assert_eq!(ids.contains(&"bridge.funding".into()), cfg!(feature = "bridge"));
}

#[test]
fn reported_capabilities_stay_sorted() {
    let ids = ids();
    assert!(ids.windows(2).all(|pair| pair[0] <= pair[1]));
}
```

Add `#![cfg(feature = "public")]` as the first line of `tests/client.rs`, `tests/cli.rs`, and `tests/market_results.rs` so bridge-only and core builds do not compile public integration tests.

- [ ] **Step 2: Run the contract to verify RED**

Run:

```bash
cargo test --no-default-features --features bridge --test feature_contract
```

Expected: FAIL because Cargo does not yet define the `bridge` feature.

- [ ] **Step 3: Add the layered features without changing transport behavior yet**

Replace the dependency declarations and binary stanza in `Cargo.toml` with this transitional, compiling feature layout:

```toml
[features]
default = ["public"]
public = ["dep:reqwest", "dep:tungstenite"]
authenticated = ["public", "dep:base64", "dep:hmac", "dep:sha2"]
wallet = ["dep:hex", "dep:sha3"]
execution = ["authenticated", "wallet"]
bridge = []
full = ["execution", "bridge"]

[dependencies]
base64 = { version = "0.22", optional = true }
chrono = { version = "0.4", features = ["serde"] }
hex = { version = "0.4", optional = true }
hmac = { version = "0.12", optional = true }
reqwest = { version = "0.12", default-features = false, features = ["blocking", "json", "rustls-tls"], optional = true }
serde = { version = "1", features = ["derive"] }
serde_json = { version = "1", features = ["arbitrary_precision"] }
sha2 = { version = "0.10", optional = true }
sha3 = { version = "0.10", optional = true }
tungstenite = { version = "0.28", default-features = false, features = ["handshake", "rustls-tls-webpki-roots"], optional = true }

[lib]
name = "polyrover"
path = "src/lib.rs"

[[bin]]
name = "polyrover"
path = "src/main.rs"
required-features = ["public"]
```

Gate `src/lib.rs` exports exactly as follows while leaving pure modules available in the feature-free core:

```rust
#[cfg(feature = "authenticated")]
pub mod auth;
#[cfg(feature = "bridge")]
pub mod bridge;
pub mod capabilities;
#[cfg(feature = "public")]
mod client;
#[cfg(feature = "public")]
pub mod clob;
#[cfg(feature = "execution")]
pub mod clob_orders;
pub mod config;
#[cfg(feature = "public")]
pub mod data;
pub mod data_types;
pub mod error;
#[cfg(feature = "public")]
pub mod gamma;
pub mod intel;
pub mod jsonx;
#[cfg(feature = "public")]
pub mod market_data;
#[cfg(feature = "public")]
pub mod market_resolver;
#[cfg(feature = "public")]
pub mod market_results;
pub mod output;
pub mod paper;
pub mod simulation;
#[cfg(feature = "public")]
pub mod stream;
#[cfg(feature = "public")]
pub mod stream_client;
#[cfg(feature = "public")]
pub mod transport;
pub mod types;
#[cfg(feature = "authenticated")]
pub mod user_stream;
#[cfg(feature = "wallet")]
pub mod wallet;

#[cfg(feature = "public")]
pub use client::{Client, ClientConfig, ClientHealth};
pub use error::{Error, Result};
```

- [ ] **Step 4: Make the shared error type independent of reqwest**

Change `Error::Http(reqwest::Error)` to `Error::Http(String)` and gate only the conversion:

```rust
#[cfg(feature = "public")]
impl From<reqwest::Error> for Error {
    fn from(value: reqwest::Error) -> Self {
        Self::Http(value.to_string())
    }
}
```

Keep its display arm as `Self::Http(err) => write!(f, "http error: {err}")` so callers observe the same text and collector pattern matches remain valid.

- [ ] **Step 5: Make capability reporting feature-aware**

Build `caps` incrementally in `capabilities::all()` with one `#[cfg]` block per tier:

```rust
pub fn all() -> Vec<Capability> {
    let mut caps = Vec::new();

    #[cfg(feature = "public")]
    caps.extend([
        cap("clob.public_data", "CLOB API", "Public order books, prices, spreads, tick sizes, and market metadata.", true, false, vec![AuthRequirement::None], WalletMode::None, &["pkg/clob", "pkg/orderbook", "pkg/marketdata"], &["book", "exchange book", "exchange markets", "exchange price-history"]),
        cap("data.positions", "Data API", "Public wallet-level positions, activity, trades, value, holders, leaderboard, and open interest.", true, false, vec![AuthRequirement::None], WalletMode::None, &["pkg/data"], &["analytics positions", "analytics trades", "analytics activity"]),
        cap("gamma.markets", "Gamma API", "Public event, market, tag, series, comment, and search discovery.", true, false, vec![AuthRequirement::None], WalletMode::None, &["pkg/gamma", "pkg/universal"], &["markets search", "markets markets", "markets market"]),
        cap("websocket.market", "CLOB WebSocket", "Public real-time book, price, last-trade, tick-size, best-bid-ask, and lifecycle events.", true, false, vec![AuthRequirement::None], WalletMode::None, &["pkg/stream", "pkg/marketdata"], &["stream market", "stream crypto", "marketdata live"]),
    ]);
    #[cfg(feature = "authenticated")]
    caps.push(cap("websocket.user", "CLOB WebSocket", "Authenticated user order and trade stream for inspection and reconciliation.", false, false, vec![AuthRequirement::L2], WalletMode::DepositWalletOnly, &["pkg/stream"], &["stream user"]));
    #[cfg(feature = "wallet")]
    caps.push(cap("relayer.deposit_wallet", "Relayer V2", "Deposit-wallet deploy, approvals, gasless transactions, CTF redeem, and transaction lookup.", false, true, vec![AuthRequirement::Siwe, AuthRequirement::PrivateKey], WalletMode::DepositWalletOnly, &["pkg/relayer", "pkg/ctf", "pkg/settlement"], &["wallet", "tx transaction"]));
    #[cfg(feature = "execution")]
    caps.push(cap("clob.trading", "CLOB API", "Deposit-wallet CLOB V2 order signing, placement, cancellation, account reads, and builder attribution.", false, true, vec![AuthRequirement::L1, AuthRequirement::L2, AuthRequirement::PrivateKey], WalletMode::DepositWalletOnly, &["pkg/clob"], &["exchange create-order", "exchange market-order", "exchange cancel"]));
    #[cfg(feature = "bridge")]
    caps.push(cap("bridge.funding", "Bridge", "Supported assets, deposit addresses, quotes, and deposit status for pUSD funding.", false, true, vec![AuthRequirement::None], WalletMode::DepositWalletOnly, &["pkg/bridge"], &["bridge assets", "bridge deposit", "bridge status", "bridge quote"]));

    caps.sort_by(|a, b| a.id.cmp(&b.id));
    caps
}
```

Replace the existing capability unit tests with feature-aware expectations:

```rust
#[test]
fn includes_only_compiled_surfaces_and_stays_sorted() {
    let caps = all();
    let ids = caps.iter().map(|cap| cap.id.as_str()).collect::<Vec<_>>();
    #[cfg(feature = "public")]
    for id in ["gamma.markets", "clob.public_data", "data.positions", "websocket.market"] {
        assert!(ids.contains(&id), "missing {id}");
    }
    #[cfg(feature = "authenticated")]
    assert!(ids.contains(&"websocket.user"));
    #[cfg(feature = "wallet")]
    assert!(ids.contains(&"relayer.deposit_wallet"));
    #[cfg(feature = "execution")]
    assert!(ids.contains(&"clob.trading"));
    #[cfg(feature = "bridge")]
    assert!(ids.contains(&"bridge.funding"));
    assert!(caps.windows(2).all(|pair| pair[0].id <= pair[1].id));
}

#[test]
fn read_only_capabilities_exclude_secret_requirements() {
    for cap in all().iter().filter(|cap| cap.read_only) {
        assert!(!cap.mutating);
        assert!(!cap.requires(AuthRequirement::L2));
        assert!(!cap.requires(AuthRequirement::Siwe));
        assert!(!cap.requires(AuthRequirement::PrivateKey));
    }
}

#[cfg(feature = "execution")]
#[test]
fn trading_declares_explicit_auth() {
    let caps = all();
    let trading = caps.iter().find(|cap| cap.id == "clob.trading").unwrap();
    assert!(trading.mutating);
    assert!(trading.requires(AuthRequirement::L1));
    assert!(trading.requires(AuthRequirement::L2));
    assert!(trading.requires(AuthRequirement::PrivateKey));
}
```

- [ ] **Step 6: Verify every meaningful tier**

Run:

```bash
cargo check --lib --no-default-features
cargo test --no-default-features --features public
cargo test --no-default-features --features authenticated
cargo test --no-default-features --features execution
cargo test --no-default-features --features bridge
cargo test --all-features
```

Expected: all PASS; the core check skips the binary and public modules.

- [ ] **Step 7: Commit the feature boundary**

```bash
git add Cargo.toml Cargo.lock src/lib.rs src/error.rs src/capabilities.rs tests/client.rs tests/cli.rs tests/market_results.rs tests/feature_contract.rs
git commit -m "feat: separate SDK capability features"
```

---

### Task 2: Generalize market-result identity beyond Up/Down

**Files:**
- Modify: `src/market_results.rs`
- Modify: `tests/market_results.rs`

**Interfaces:**
- Consumes: Gamma `Market.clob_token_ids` and `Market.outcome_prices`.
- Produces: `MarketRef { condition_id, slug, token_ids }`, where `token_ids` supports any non-empty outcome set.

- [ ] **Step 1: Add a failing three-outcome result test**

Add this unit test inside `src/market_results.rs`:

```rust
#[test]
fn exact_result_supports_arbitrary_outcomes() {
    let closed_at = Utc::now();
    let row = Market {
        condition_id: "condition".into(),
        closed: true,
        closed_time: crate::types::NormalizedTime(Some(closed_at.fixed_offset())),
        clob_token_ids: r#"["red","green","blue"]"#.into(),
        outcome_prices: crate::jsonx::StringOrArray(vec!["0".into(), "1".into(), "0".into()]),
        ..Default::default()
    };
    let market = MarketRef {
        condition_id: "condition".into(),
        slug: "colors".into(),
        token_ids: vec!["red".into(), "green".into(), "blue".into()],
    };
    assert_eq!(exact_gamma_result(&row, &market).unwrap().0, "green");
}
```

- [ ] **Step 2: Run the test to verify RED**

Run:

```bash
cargo test --features public market_results::tests::exact_result_supports_arbitrary_outcomes
```

Expected: FAIL because `MarketRef` still exposes `up_token_id` and `down_token_id`.

- [ ] **Step 3: Replace binary outcome identity with generic token identity**

Change `MarketRef` to:

```rust
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MarketRef {
    pub condition_id: String,
    pub slug: String,
    pub token_ids: Vec<String>,
}
```

Replace `exact_gamma_result` with generic set validation:

```rust
fn exact_gamma_result(row: &Market, market: &MarketRef) -> Option<(String, DateTime<Utc>)> {
    if row.condition_id.trim() != market.condition_id.trim() || !row.closed {
        return None;
    }
    let resolved_at = row.closed_time.0?.with_timezone(&Utc);
    let token_ids = serde_json::from_str::<Vec<String>>(&row.clob_token_ids).ok()?;
    if token_ids.is_empty()
        || token_ids.len() != row.outcome_prices.0.len()
        || market.token_ids.is_empty()
    {
        return None;
    }
    let expected = market
        .token_ids
        .iter()
        .map(|token| token.trim())
        .collect::<std::collections::BTreeSet<_>>();
    let actual = token_ids
        .iter()
        .map(|token| token.trim())
        .collect::<std::collections::BTreeSet<_>>();
    if expected.len() != market.token_ids.len()
        || actual.len() != token_ids.len()
        || expected != actual
    {
        return None;
    }
    let mut winner = None;
    for (index, price) in row.outcome_prices.0.iter().enumerate() {
        match price.trim().parse::<f64>().ok()? {
            1.0 if winner.is_none() => winner = Some(index),
            0.0 => {}
            _ => return None,
        }
    }
    let winner = token_ids.get(winner?)?.clone();
    expected.contains(winner.as_str()).then_some((winner, resolved_at))
}
```

Update `tests/market_results.rs` fixtures to pass `token_ids: vec!["token-up".into(), "token-down".into()]`. Do not add asset, horizon, Up/Down, strategy, sizing, or portfolio fields to `MarketRef`.

- [ ] **Step 4: Verify generic and existing result behavior**

Run:

```bash
cargo test --features public market_results
cargo test --test market_results --features public
```

Expected: PASS for both binary and three-outcome fixtures.

- [ ] **Step 5: Commit generic market identity**

```bash
git add src/market_results.rs tests/market_results.rs
git commit -m "refactor: generalize market result identities"
```

---

### Task 3: Convert HTTP transport and public REST APIs to async

**Files:**
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Modify: `src/transport.rs`
- Modify: `src/gamma.rs`
- Modify: `src/clob.rs`
- Modify: `src/data.rs`
- Modify: `src/client.rs`
- Modify: `src/market_resolver.rs`
- Modify: `src/market_results.rs`
- Modify: `tests/client.rs`
- Modify: `tests/market_results.rs`

**Interfaces:**
- Consumes: feature layout from Task 1 and generic `MarketRef` from Task 2.
- Produces: async methods with existing names; constructors, query builders, parsing, and pure helpers remain synchronous.

- [ ] **Step 1: Make the transport rate-limit test expect async I/O**

Change `transport::tests::rate_limit_preserves_retry_after` to `#[tokio::test] async fn` and call:

```rust
assert!(matches!(
    client.get_raw("/limited").await,
    Err(Error::RateLimited {
        retry_after_secs: Some(3)
    })
));
```

- [ ] **Step 2: Run the transport test to verify RED**

Run:

```bash
cargo test --features public transport::tests::rate_limit_preserves_retry_after
```

Expected: compile failure because the blocking result is not a future.

- [ ] **Step 3: Replace blocking transport dependencies**

Change Cargo dependencies to:

```toml
public = ["dep:futures-util", "dep:reqwest", "dep:tokio", "dep:tokio-tungstenite"]

futures-util = { version = "0.3", optional = true }
reqwest = { version = "0.12", default-features = false, features = ["json", "rustls-tls"], optional = true }
tokio = { version = "1", features = ["macros", "net", "rt-multi-thread", "sync", "time"], optional = true }
tokio-tungstenite = { version = "0.28", default-features = false, features = ["connect", "rustls-tls-webpki-roots"], optional = true }
```

Remove the direct `tungstenite` dependency. Add this test-only Tokio feature without enabling it in production builds:

```toml
[dev-dependencies]
tokio = { version = "1", features = ["test-util"] }
```

- [ ] **Step 4: Replace the shared blocking HTTP client**

Use `reqwest::Client` and make only network methods async:

```rust
#[derive(Clone)]
pub struct Client {
    base_url: String,
    http: reqwest::Client,
}

impl Client {
    pub fn new(config: Config) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(config.timeout_secs))
            .user_agent(config.user_agent)
            .build()?;
        Ok(Self { base_url: config.base_url, http })
    }

    pub async fn get_json<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        let body = checked_body(self.http.get(self.url(path)?).send().await?).await?;
        Ok(serde_json::from_str(&body)?)
    }

    pub async fn post_json<B: Serialize, T: DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T> {
        let body = checked_body(self.http.post(self.url(path)?).json(body).send().await?).await?;
        Ok(serde_json::from_str(&body)?)
    }

    pub async fn get_raw(&self, path: &str) -> Result<String> {
        checked_body(self.http.get(self.url(path)?).send().await?).await
    }
}

async fn checked_body(response: reqwest::Response) -> Result<String> {
    let status = response.status();
    let retry_after_secs = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    let body = response.text().await?;
    if status.as_u16() == 429 {
        return Err(Error::RateLimited { retry_after_secs });
    }
    if !status.is_success() {
        return Err(Error::Api { status: status.as_u16(), body });
    }
    Ok(body)
}
```

Keep `Config::new`, `url`, and `trim_base` synchronous and unchanged.

- [ ] **Step 5: Convert every public REST method without renaming it**

Add `async` and `.await` to all transport-backed methods in:

```text
src/gamma.rs: health_check, active_markets, markets, market_by_id,
              market_by_slug, events, event_by_id, search
src/clob.rs: health, server_time, markets, market, market_by_token,
             market_outcome, order_book, order_books, price, midpoint,
             spread, tick_size, neg_risk, simplified_markets
src/data.rs: health, current_positions, closed_positions, trades,
             market_trades, activity, top_holders, total_value,
             markets_traded, open_interest, trader_leaderboard, live_volume
src/client.rs: search, markets, market_by_slug, order_book, order_books,
               price, current_positions, trades, trader_leaderboard,
               health, simulate
```

For composed calls, await at the ownership boundary. For example:

```rust
pub async fn market_outcome(
    &self,
    condition_id: &str,
    gamma_base_url: &str,
) -> Result<ClobMarketOutcome> {
    let condition_id = condition_id.trim();
    if condition_id.is_empty() {
        return Err(Error::Invalid("clob: condition_id is required".into()));
    }
    match self.market(condition_id).await {
        Ok(market) => Ok(outcome_from_clob_market(condition_id, market)),
        Err(err) if !gamma_base_url.trim().is_empty() => {
            resolve_via_gamma(gamma_base_url, condition_id).await.or(Err(err))
        }
        Err(err) => Err(err),
    }
}

async fn resolve_via_gamma(
    gamma_base_url: &str,
    condition_id: &str,
) -> Result<ClobMarketOutcome> {
    let client = gamma::Client::new(gamma_base_url)?;
    let markets = client
        .markets(&gamma::MarketParams {
            condition_ids: vec![condition_id.into()],
            ..Default::default()
        })
        .await?;
    if markets.into_iter().any(|market| market.closed) {
        return Ok(ClobMarketOutcome {
            status: CLOB_OUTCOME_UNRESOLVED.into(),
            condition_id: condition_id.into(),
            closed: true,
            source: format!("gamma:closed_condition_id={condition_id}"),
            ..Default::default()
        });
    }
    Err(Error::Invalid(format!(
        "gamma: no closed market found for condition_id={condition_id}"
    )))
}
```

`Client::health` remains sequential to preserve simple failure labeling:

```rust
pub async fn health(&self) -> ClientHealth {
    ClientHealth {
        gamma: health_label(self.gamma.health_check().await.is_ok()),
        clob: health_label(self.clob.health().await.is_ok()),
    }
}
```

- [ ] **Step 6: Convert network-backed domain helpers**

Make `market_resolver::discover_window_markets` and
`discover_complete_window_markets` async. Await the batch `client.markets`,
per-slug fallback `client.market_by_slug`, and nested discovery call. Leave all
slug/window/token parsing helpers synchronous.

Make `market_results::Resolver::resolve` and `resolve_at` async. Await Gamma
lookup and CLOB outcome calls; retain `exact_gamma_result` as a synchronous pure
helper.

- [ ] **Step 7: Convert local HTTP integration tests to Tokio tests**

Change network tests in `tests/client.rs`, `tests/market_results.rs`, and the
network-backed tests in `src/market_resolver.rs` to `#[tokio::test] async fn`.
Add `.await` immediately before each result unwrap. Keep the existing local
`TcpListener` fixture threads; they do not run inside Polyrover production code.

Example:

```rust
#[tokio::test]
async fn client_reads_clob_books_through_one_public_interface() {
    let (clob_base_url, received, server) =
        serve_json(r#"{"asset_id":"token-1","bids":[],"asks":[]}"#);
    let client = Client::new(ClientConfig {
        clob_base_url,
        ..ClientConfig::default()
    })
    .unwrap();

    let book = client.order_book("token-1").await.unwrap();

    assert_eq!(book.asset_id, "token-1");
    assert!(received.recv().unwrap().starts_with("GET /book?token_id=token-1 "));
    server.join().unwrap();
}
```

- [ ] **Step 8: Verify HTTP and resolver parity**

Run:

```bash
cargo test --no-default-features --features public transport
cargo test --no-default-features --features public --test client
cargo test --no-default-features --features public --test market_results
cargo test --no-default-features --features public market_resolver
```

Expected: all PASS with the existing request paths, response normalization, rate-limit behavior, and result semantics.

- [ ] **Step 9: Commit native async HTTP**

```bash
git add Cargo.toml Cargo.lock src/transport.rs src/gamma.rs src/clob.rs src/data.rs src/client.rs src/market_resolver.rs src/market_results.rs tests/client.rs tests/market_results.rs
git commit -m "refactor: make public HTTP APIs async"
```

---

### Task 4: Replace the public WebSocket client with Tokio transport

**Files:**
- Modify: `src/stream_client.rs`

**Interfaces:**
- Consumes: `tokio-tungstenite`, `futures-util`, existing `stream::Config`, `Deduplicator`, `Tracker`, and status DTOs.
- Produces: async `MarketWsClient` methods with heartbeat/reconnect deadlines independent of market traffic.

- [ ] **Step 1: Convert one lifecycle test to async RED**

Change `reads_typed_market_events_from_websocket` to use
`#[tokio::test] async fn`, `tokio::net::TcpListener`,
`tokio_tungstenite::accept_async`, and awaited client calls:

```rust
let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
let address = listener.local_addr().unwrap();
let server = tokio::spawn(async move {
    let (stream, _) = listener.accept().await.unwrap();
    let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
    assert!(socket.next().await.unwrap().unwrap().to_string().contains("token-1"));
    socket.send(Message::Text(r#"{"event_type":"new_market","id":"market-1"}"#.into())).await.unwrap();
});
let mut client = MarketWsClient::connect(Config {
    url: format!("ws://{address}"),
    ..Default::default()
}).await.unwrap();
client.subscribe_assets(&["token-1".into()]).await.unwrap();
assert!(matches!(
    client.read_events(1).await.unwrap().as_slice(),
    [crate::stream::MarketEvent::NewMarket(market)] if market.id == "market-1"
));
server.await.unwrap();
```

Import `futures_util::{SinkExt, StreamExt}` in the test module.

- [ ] **Step 2: Run the lifecycle test to verify RED**

Run:

```bash
cargo test --features public stream_client::tests::reads_typed_market_events_from_websocket
```

Expected: compile failure because `MarketWsClient` is still synchronous.

- [ ] **Step 3: Replace socket and timer types**

Use:

```rust
use futures_util::{SinkExt, StreamExt};
use tokio::{net::TcpStream, time::{sleep, sleep_until, Duration, Instant}};
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use tokio_tungstenite::tungstenite::Message;

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
```

Change `socket` to `Socket`; remove `std::thread`, `std::net::TcpStream`, IO
read-timeout handling, and `set_read_timeout`.

- [ ] **Step 4: Make connect, retry, subscribe, ping, and close async**

Use these bodies:

```rust
async fn dial(config: &Config) -> Result<Socket> {
    connect_async(config.url.as_str())
        .await
        .map(|(socket, _)| socket)
        .map_err(ws_err)
}

async fn dial_with_retries(config: &Config) -> Result<Socket> {
    let delays = reconnect_delays(config);
    let mut last_err = None;
    for attempt in 0..=delays.len() {
        match Self::dial(config).await {
            Ok(socket) => return Ok(socket),
            Err(err) => last_err = Some(err),
        }
        if let Some(delay) = delays.get(attempt) {
            sleep(*delay).await;
        }
    }
    Err(Error::ReconnectExhausted {
        attempts: delays.len() as u32 + 1,
        last_error: last_err.map(|error| error.to_string()).unwrap_or_else(|| "websocket connect failed".into()),
    })
}

pub async fn subscribe_assets(&mut self, asset_ids: &[String]) -> Result<()> {
    let payload = market_subscription(asset_ids, &self.config)?;
    self.socket.send(Message::Text(payload.to_string().into())).await.map_err(ws_err)?;
    self.stats.set_subscriptions(asset_ids);
    self.subscriptions = asset_ids.to_vec();
    Ok(())
}

pub async fn ping(&mut self) -> Result<()> {
    self.socket.send(Message::Text("PING".into())).await.map_err(ws_err)?;
    self.last_ping = Instant::now();
    Ok(())
}

pub async fn close(mut self) -> Result<()> {
    self.socket.close(None).await.map_err(ws_err)
}
```

`connect` and `connect_with_retries` await their dial functions and initialize
stats, dedup, tracker, subscriptions, and Tokio instants exactly as today.

- [ ] **Step 5: Make reads deadline-driven and cancellation-safe**

Add a private wake enum and use a scoped mutable socket borrow so the selected
branch releases it before calling another `&mut self` method:

```rust
enum ReadWake {
    Message(Option<std::result::Result<Message, tokio_tungstenite::tungstenite::Error>>),
    Ping,
    PongTimeout,
}

async fn next_wake(&mut self) -> ReadWake {
    let ping_deadline = self.last_ping
        + Duration::from_secs(self.config.ping_interval_secs.max(1));
    let pong_timeout = async {
        if self.config.pong_timeout_secs == 0 {
            std::future::pending::<()>().await;
        } else {
            sleep_until(
                self.last_frame_at + Duration::from_secs(self.config.pong_timeout_secs),
            )
            .await;
        }
    };
    tokio::pin!(pong_timeout);
    let socket = &mut self.socket;
    tokio::select! {
        message = socket.next() => ReadWake::Message(message),
        _ = sleep_until(ping_deadline) => ReadWake::Ping,
        _ = &mut pong_timeout => ReadWake::PongTimeout,
    }
}
```

Refactor `read_raw_with_status` into a loop around `next_wake()`:

- `Ping`: call `ping().await`; reconnect on failure if enabled; continue waiting.
- `PongTimeout`: return the same timeout error when reconnect is disabled;
  otherwise await reconnect/resubscribe, set `reconnected = true`, and continue.
- `Message(Some(Ok(message)))`: set `last_frame_at`, then run the existing
  control-frame, text, dedup, parsing, and stats logic unchanged.
- `Message(Some(Err(error)))`: reconnect and continue when enabled; otherwise
  return `ws_err(error)`.
- `Message(None)`: treat as `Error::WebSocket("websocket stream ended".into())`
  and apply the same reconnect policy.

Make `read_raw`, `read_events`, `read_tracked_with_status`, and `read_tracked`
async and await the nested read. Make `reconnect_and_resubscribe` async, await
`dial_with_retries`, preserve stats/dedup/tracker, and resubscribe only when the
stored list is non-empty.

- [ ] **Step 6: Add deterministic heartbeat and gap semantics tests**

Convert all existing network lifecycle tests in `stream_client.rs` to Tokio
fixtures. Retain pure tests as ordinary `#[test]`.

Add a paused-time silent-peer test:

```rust
#[tokio::test(start_paused = true)]
async fn silent_peer_reconnects_on_deadline_without_inbound_traffic() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (first, _) = listener.accept().await.unwrap();
        let mut first = tokio_tungstenite::accept_async(first).await.unwrap();
        let _subscription = first.next().await.unwrap().unwrap();

        let (replacement, _) = listener.accept().await.unwrap();
        let mut replacement = tokio_tungstenite::accept_async(replacement).await.unwrap();
        let _subscription = replacement.next().await.unwrap().unwrap();
        replacement.send(Message::Text(r#"{"event_type":"new_market","id":"replacement"}"#.into())).await.unwrap();
    });
    let mut client = MarketWsClient::connect(Config {
        url: format!("ws://{address}"),
        ping_interval_secs: 1,
        pong_timeout_secs: 2,
        ..Default::default()
    }).await.unwrap();
    client.subscribe_assets(&["token-1".into()]).await.unwrap();
    let read = client.read_raw_with_status(1).await.unwrap();
    assert!(read.reconnected);
    assert_eq!(read.messages[0].payload["id"], "replacement");
    server.await.unwrap();
}
```

Keep the reconnect-dedup test: replayed received messages remain suppressed,
but do not assert that messages absent during disconnection are recovered.

- [ ] **Step 7: Verify WebSocket parity**

Run:

```bash
cargo test --no-default-features --features public stream_client
```

Expected: PASS for subscription, heartbeat, silent-peer timeout, reconnect,
deduplication, stats preservation, and empty-subscription behavior.

- [ ] **Step 8: Commit async public streaming**

```bash
git add src/stream_client.rs
git commit -m "refactor: make public websocket async"
```

---

### Task 5: Convert authenticated streaming and the CLI

**Files:**
- Modify: `src/user_stream.rs`
- Modify: `src/main.rs`
- Modify: `tests/cli.rs`

**Interfaces:**
- Consumes: async public transport from Tasks 3-4 and existing `ApiKey`/payload parsers.
- Produces: async `UserWsClient`, Tokio CLI entrypoint, and no new authenticated or fund-moving behavior.

- [ ] **Step 1: Add an async authenticated user-stream fixture test**

Add to `user_stream.rs` tests:

```rust
#[tokio::test]
async fn reads_user_event_over_async_websocket() {
    use futures_util::{SinkExt, StreamExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        let subscription = socket.next().await.unwrap().unwrap().to_string();
        assert!(subscription.contains("api-key-1234"));
        socket.send(tokio_tungstenite::tungstenite::Message::Text(
            r#"{"event_type":"order","id":"o1"}"#.into(),
        )).await.unwrap();
    });
    let mut client = UserWsClient::connect(
        Config { url: format!("ws://{address}"), ..Default::default() },
        key(),
    ).await.unwrap();
    client.subscribe_user(&["market-1".into()]).await.unwrap();
    assert!(matches!(client.read_event(1).await.unwrap(), UserEvent::Order(order) if order.id == "o1"));
    server.await.unwrap();
}
```

- [ ] **Step 2: Run the authenticated test to verify RED**

Run:

```bash
cargo test --no-default-features --features authenticated user_stream::tests::reads_user_event_over_async_websocket
```

Expected: compile failure because `UserWsClient` remains synchronous.

- [ ] **Step 3: Convert only user-stream network methods**

Use `WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>`,
`connect_async`, `SinkExt`, and `StreamExt`. Make `connect`, `subscribe_user`,
`read_event`, and `close` async. Keep `user_subscription_payload`,
`redacted_user_subscription_payload`, and `parse_user_event` synchronous and
unchanged.

The read body becomes:

```rust
let message = self
    .socket
    .next()
    .await
    .ok_or_else(|| Error::Invalid("user ws closed".into()))?
    .map_err(ws_err)?;
let text = match message {
    Message::Text(text) => text.to_string(),
    Message::Binary(bytes) => String::from_utf8(bytes.to_vec())
        .map_err(|error| Error::Invalid(format!("user ws binary is not utf8: {error}")))?,
    Message::Close(_) => return Err(Error::Invalid("user ws closed".into())),
    _ => return Ok(UserEvent::Ignored),
};
```

Keep credential validation before connecting and keep redaction tests intact.

- [ ] **Step 4: Convert the CLI entrypoint and command handlers**

Use:

```rust
#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        let body = output::error("polyrover", "error", &err.to_string())
            .unwrap_or_else(|_| format!("error: {err}\n"));
        eprint!("{body}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).filter(|arg| arg != "--json").collect();
    let client = Client::new(ClientConfig::default())?;
    match args.as_slice() {
        [] => { print_help(); Ok(()) }
        [cmd] if cmd == "help" || cmd == "--help" => { print_help(); Ok(()) }
        [cmd] if cmd == "ping" => ping(&client).await,
        [group, cmd, rest @ ..] if group == "gamma" && cmd == "search" => gamma_search(&client, rest).await,
        [group, cmd, rest @ ..] if group == "gamma" && cmd == "markets" => gamma_markets(&client, rest).await,
        [group, cmd, rest @ ..] if group == "clob" && cmd == "book" => clob_book(&client, rest).await,
        [group, cmd, rest @ ..] if group == "clob" && cmd == "price" => clob_price(&client, rest).await,
        [group, cmd, rest @ ..] if group == "clob" && cmd == "simulate" => clob_simulate(&client, rest).await,
        [group, cmd, rest @ ..] if group == "analytics" && cmd == "positions" => data_positions(&client, rest).await,
        [group, cmd, rest @ ..] if group == "analytics" && cmd == "trades" => data_trades(&client, rest).await,
        [group, cmd, rest @ ..] if group == "analytics" && cmd == "leaderboard" => data_leaderboard(&client, rest).await,
        [group, cmd, rest @ ..] if group == "stream" && cmd == "watch" => stream_watch(rest).await,
        [group, cmd, rest @ ..] if group == "sim" && cmd == "reset" => sim_reset(rest),
        [group, cmd, rest @ ..] if group == "sim" && cmd == "buy" => sim_buy(rest),
        [group, cmd, rest @ ..] if group == "sim" && cmd == "sell" => sim_sell(rest),
        _ => { print_help(); Ok(()) }
    }
}
```

Make `ping`, Gamma, CLOB, analytics, and `stream_watch` handlers async and await
SDK network calls. Keep `sim_reset`, `sim_buy`, `sim_sell`, `paper_order`, flag
parsing, output, and help synchronous. `stream_watch` awaits connect,
subscription, reads, and close; wrap normal close in:

```rust
let _ = tokio::time::timeout(Duration::from_secs(1), client.close()).await;
```

Change the help heading from `polyrover read-only Polymarket CLI` to
`polyrover async Polymarket CLI`; update `tests/cli.rs` accordingly.

- [ ] **Step 5: Update the CLI WebSocket fixture import**

Replace `use tungstenite::Message` with:

```rust
use tokio_tungstenite::tungstenite::Message;
```

The test server may remain a local fixture thread using the re-exported
synchronous Tungstenite server helper; production has no synchronous transport.

- [ ] **Step 6: Verify authenticated and CLI behavior**

Run:

```bash
cargo test --no-default-features --features authenticated user_stream
cargo test --no-default-features --features public --test cli
cargo test --all-features
```

Expected: all PASS; credential redaction and unsupported guards are unchanged.

- [ ] **Step 7: Commit authenticated streaming and CLI cutover**

```bash
git add src/user_stream.rs src/main.rs tests/cli.rs
git commit -m "refactor: make user stream and CLI async"
```

---

### Task 6: Record the universal async boundary and capability coverage

**Files:**
- Create: `docs/adr/0001-universal-async-sdk.md`
- Create: `docs/endpoint-capability-matrix.md`
- Modify: `README.md`
- Modify: `PORT_PLAN.md`

**Interfaces:**
- Consumes: actual feature and async behavior from Tasks 1-5.
- Produces: durable Polyrover-owned architecture decision and source-backed coverage inventory.

- [ ] **Step 1: Write the Polyrover ADR**

Create `docs/adr/0001-universal-async-sdk.md` with these decisions:

```markdown
# ADR-0001: Universal Async SDK with Safe Public Default

Status: accepted
Date: 2026-07-16

## Context

Polyrover's canonical MegaBot consumer is Tokio-based, while the crate used
blocking reqwest and Tungstenite behind thread bridges. The crate also retained
authenticated, wallet, execution DTO, and bridge DTO surfaces despite being
described as read-only.

## Decision

Polyrover is a universal standalone Rust interface to Polymarket. Network APIs
are async-only. Pure DTOs, parsing, simulation, HMAC helpers, wallet derivation,
and book math remain synchronous.

Cargo features are layered as `public` (default), `authenticated`, `wallet`,
`execution`, `bridge`, and `full`. Features limit compilation and dependency
exposure; they are not authorization controls.

Core market and outcome identities are generic. Crypto Up/Down discovery is a
specialized helper. Strategy, sizing, and portfolio policy stay outside the SDK.

This migration preserves existing behavior and adds no live order, cancellation,
private-key, relayer, or bridge execution method.

## Consequences

Pre-1.0 callers must await network methods. No blocking facade is maintained.
MegaBot compiles only `public`; Polygolem remains MegaBot's exclusive signing
and execution boundary. Any future fund-moving capability requires a separate
safety design and architecture approval.
```

- [ ] **Step 2: Create the endpoint/capability matrix**

Create `docs/endpoint-capability-matrix.md` beginning with:

```markdown
# Endpoint and Capability Matrix

Matrix schema: 1
Last verified: 2026-07-16

Live source and `Cargo.toml` are authoritative. `implemented` means callable
behavior exists and has a linked contract test; `dto-only` means types exist
without network execution; `unsupported` means a guard deliberately rejects
the operation; `planned` means no current Rust API exists.
```

Use separate sections and these rows, linking each Rust path and test path:

```markdown
## Public

| Surface | Method/event | Endpoint/channel | Transport | Auth level | Cargo feature | Status | Rust API | Test |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Gamma | Search/markets/events | `gamma-api.polymarket.com` | HTTPS | none | `public` | implemented | [`src/gamma.rs`](../src/gamma.rs) | [`tests/client.rs`](../tests/client.rs) |
| CLOB | Books/prices/market metadata | `clob.polymarket.com` | HTTPS | none | `public` | implemented | [`src/clob.rs`](../src/clob.rs) | [`tests/client.rs`](../tests/client.rs) |
| Data API | Positions/trades/activity/analytics | `data-api.polymarket.com` | HTTPS | none | `public` | implemented | [`src/data.rs`](../src/data.rs) | [`tests/client.rs`](../tests/client.rs) |
| Market WSS | Book/price/trade/tick/lifecycle events | `/ws/market` | WSS | none | `public` | implemented | [`src/stream_client.rs`](../src/stream_client.rs) | [`src/stream_client.rs`](../src/stream_client.rs) |
| Resolution | Arbitrary market result | Gamma + CLOB | HTTPS | none | `public` | implemented | [`src/market_results.rs`](../src/market_results.rs) | [`tests/market_results.rs`](../tests/market_results.rs) |
| Crypto resolver | Up/Down 5m windows | Gamma | HTTPS | none | `public` | implemented | [`src/market_resolver.rs`](../src/market_resolver.rs) | [`src/market_resolver.rs`](../src/market_resolver.rs) |

## Authenticated

| Surface | Method/event | Endpoint/channel | Transport | Auth level | Cargo feature | Status | Rust API | Test |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| L2 auth | HMAC header construction | local helper | none | L2 | `authenticated` | implemented | [`src/auth.rs`](../src/auth.rs) | [`src/auth.rs`](../src/auth.rs) |
| User WSS | Order/trade events | `/ws/user` | WSS | L2 | `authenticated` | implemented | [`src/user_stream.rs`](../src/user_stream.rs) | [`src/user_stream.rs`](../src/user_stream.rs) |
| Authenticated CLOB reads | Account/order reads | CLOB | HTTPS | L2 | `authenticated` | planned | — | — |

## Wallet

| Surface | Method/event | Endpoint/channel | Transport | Auth level | Cargo feature | Status | Rust API | Test |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Address derivation | Deposit/proxy/safe addresses | local helper | none | none | `wallet` | implemented | [`src/wallet.rs`](../src/wallet.rs) | [`src/wallet.rs`](../src/wallet.rs) |
| Private-key signing/storage | Transaction/order signing | local | private key | `wallet` | planned | — | — |

## Execution

| Surface | Method/event | Endpoint/channel | Transport | Auth level | Cargo feature | Status | Rust API | Test |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| CLOB orders | Order/cancel records and responses | CLOB | none | L2 + wallet | `execution` | dto-only | [`src/clob_orders.rs`](../src/clob_orders.rs) | [`src/clob_orders.rs`](../src/clob_orders.rs) |
| Live order placement/cancel | Submit/cancel | CLOB | HTTPS | L2 + wallet | `execution` | planned | — | — |

## Bridge

| Surface | Method/event | Endpoint/channel | Transport | Auth level | Cargo feature | Status | Rust API | Test |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Bridge metadata | Asset/deposit/status/quote shapes | Bridge API | none | none | `bridge` | dto-only | [`src/bridge.rs`](../src/bridge.rs) | [`src/bridge.rs`](../src/bridge.rs) |
| Withdrawal dry run | Validation and safety result | local helper | none | none | `bridge` | unsupported | [`src/bridge.rs`](../src/bridge.rs) | [`src/bridge.rs`](../src/bridge.rs) |
| Bridge execution | Deposit/withdraw submit | Bridge API | HTTPS | wallet | `bridge` | planned | — | — |
```

Add this verified references section:

```markdown
## Official references

- [Gamma Markets API overview](https://docs.polymarket.com/developers/gamma-markets-api/overview)
- [CLOB authentication](https://docs.polymarket.com/developers/CLOB/authentication)
- [CLOB market WebSocket channel](https://docs.polymarket.com/developers/CLOB/websocket/market-channel)
```

Leave surfaces without a verified authoritative page unlinked rather than
guessing a URL.

- [ ] **Step 3: Update README and port roadmap with exact boundary wording**

Replace read-only product claims with:

```markdown
Polyrover is an async Rust SDK and CLI for Polymarket. Its safe default build
contains public REST and WebSocket data only; authenticated, wallet, execution
DTO, and bridge DTO surfaces require explicit Cargo features.

Network APIs are async-only. Cargo features control compilation and dependency
exposure, not runtime authority. The current execution and bridge surfaces are
DTO-only or guarded: Polyrover does not yet submit orders, cancel orders, sign
with private keys, invoke relayers, or execute bridge transfers.
```

Document the feature hierarchy and label the async conversion as a breaking
pre-1.0 API change. In `PORT_PLAN.md`, replace the blocking pull-parity note
with async pull-based parity and keep all live fund-moving work in a separately
approved future section.

- [ ] **Step 4: Validate docs and every Polyrover tier**

Run:

```bash
cargo fmt --all -- --check
cargo check --lib --no-default-features
cargo test --no-default-features --features public
cargo test --no-default-features --features authenticated
cargo test --no-default-features --features execution
cargo test --no-default-features --features bridge
cargo test --all-features
cargo clippy --all-targets --all-features -- -D warnings
git diff --check
```

Expected: all PASS; matrix links resolve to checked-in files.

- [ ] **Step 5: Commit and push Polyrover before parent integration**

```bash
git add README.md PORT_PLAN.md docs/adr/0001-universal-async-sdk.md docs/endpoint-capability-matrix.md
git commit -m "docs: define universal async SDK coverage"
git push origin main
```

Expected: the pushed Polyrover commit contains Tasks 1-6 and the approved design; do not update MegaBot's submodule pointer before this succeeds.

---

### Task 7: Atomically cut rust-crypto-data over to async Polyrover

This is one parent-repository commit because source changes that await the new
API cannot compile against the old submodule pointer, while the new submodule
pointer cannot compile against the old blocking caller. Keep all RED/GREEN
steps local, validate the coordinated tree, then commit source and pointer
together.

**Files:**
- Modify: `../rust-crypto-data/Cargo.toml`
- Modify: `../rust-crypto-data/src/collection/clob/config.rs`
- Modify: `../rust-crypto-data/src/collection/clob/universe.rs`
- Modify: `../rust-crypto-data/src/collection/clob/runtime.rs`
- Modify: `../rust-crypto-data/src/collection/clob/pipeline.rs`
- Modify: `../rust-crypto-data/src/sources/polymarket/results.rs`
- Modify: `../rust-crypto-data/tests/polyrover_clob_boundary_test.rs`
- Modify: `../AGENTS.md`
- Modify: `../CONTEXT.md`
- Modify: the `polyrover` gitlink in the MegaBot parent repository

**Interfaces:**
- Consumes: pushed async Polyrover commit from Task 6.
- Produces: public-only dependency declaration, async discovery/results, supervised REST/WSS/refresh tasks, typed saturation, bounded close, and no Polyrover-specific blocking bridge.

- [ ] **Step 1: Add failing parent boundary contracts**

In `../rust-crypto-data/tests/polyrover_clob_boundary_test.rs`, read the manifest and production
sources and add:

```rust
let manifest = fs::read_to_string(root.join("Cargo.toml")).unwrap();
assert!(manifest.contains(
    r#"polyrover = { path = "../polyrover", default-features = false, features = ["public"] }"#
));
assert!(!production.contains("polyrover::auth"));
assert!(!production.contains("polyrover::user_stream"));
assert!(!production.contains("polyrover::wallet"));
assert!(!production.contains("polyrover::clob_orders"));
assert!(!production.contains("polyrover::bridge"));
assert!(!production.contains("spawn_blocking(move ||"));
assert!(!production.contains("run_clob_1s_blocking"));
assert!(!production.contains("spawn_market_refresh_thread"));
```

Do not ban all `spawn_blocking`: viewer password hashing legitimately uses it.

- [ ] **Step 2: Add failing deadline and saturation tests**

Add to `../rust-crypto-data/src/collection/clob/config.rs`:

```rust
#[test]
fn observation_send_timeout_precedes_heartbeat_deadline() {
    let ws = polyrover::stream::Config::default();
    assert!(validate_transport_deadlines(OBSERVATION_SEND_TIMEOUT, &ws).is_ok());
    assert!(validate_transport_deadlines(
        Duration::from_secs(ws.pong_timeout_secs),
        &ws,
    ).is_err());
}
```

Add to `../rust-crypto-data/src/collection/clob/runtime.rs`:

```rust
#[tokio::test(start_paused = true)]
async fn saturated_observation_channel_fails_closed() {
    let (sender, mut receiver) = observation_channel(1, Duration::from_millis(50));
    sender.send(RuntimeObservation::Ws(WsObservation::Connected {
        observed_at: Utc::now(),
    })).await.unwrap();
    let second = tokio::spawn(async move {
        sender.send(RuntimeObservation::Ws(WsObservation::Connected {
            observed_at: Utc::now(),
        })).await
    });
    tokio::time::advance(Duration::from_millis(51)).await;
    assert_eq!(second.await.unwrap(), Err(ObservationSendError::Saturated));
    assert!(receiver.recv().await.is_some());
}
```

- [ ] **Step 3: Run parent tests to verify RED**

From the MegaBot root run:

```bash
cargo test --manifest-path rust-crypto-data/Cargo.toml --test polyrover_clob_boundary_test
cargo test --manifest-path rust-crypto-data/Cargo.toml observation_send_timeout_precedes_heartbeat_deadline
cargo test --manifest-path rust-crypto-data/Cargo.toml saturated_observation_channel_fails_closed
```

Expected: failures for the old manifest, missing deadline validator, and missing async bounded sender.

- [ ] **Step 4: Enforce the public-only dependency and test-only Tokio time control**

Change `../rust-crypto-data/Cargo.toml` to:

```toml
polyrover = { path = "../polyrover", default-features = false, features = ["public"] }
```

Add `test-util` only in dev builds:

```toml
[dev-dependencies]
megabot-rl = { path = "../rust-rl" }
tokio = { version = "1", features = ["test-util"] }
```

- [ ] **Step 5: Define collector-owned channel and deadline constants**

In `../rust-crypto-data/src/collection/clob/config.rs`, add:

```rust
use std::time::Duration;

pub(super) const OBSERVATION_CHANNEL_CAPACITY: usize = 256;
pub(super) const OBSERVATION_SEND_TIMEOUT: Duration = Duration::from_millis(500);
pub(super) const TRANSPORT_CLOSE_TIMEOUT: Duration = Duration::from_secs(1);

pub(super) fn validate_transport_deadlines(
    send_timeout: Duration,
    ws: &polyrover::stream::Config,
) -> anyhow::Result<()> {
    anyhow::ensure!(ws.pong_timeout_secs > 0, "CLOB heartbeat deadline must be enabled");
    anyhow::ensure!(
        send_timeout < Duration::from_secs(ws.pong_timeout_secs),
        "CLOB observation send timeout must precede heartbeat deadline"
    );
    Ok(())
}
```

Call `validate_transport_deadlines` in `stream_clob_1s` immediately after it
constructs the WebSocket config. This is a collector invariant, not a new CLI
configuration knob.

- [ ] **Step 6: Generalize the collector's market-result adapter and await results**

Change `market_ref` in `../rust-crypto-data/src/sources/polymarket/results.rs` to:

```rust
polyrover::market_results::MarketRef {
    condition_id: candidate.condition_id.clone(),
    slug: candidate.slug.clone(),
    token_ids: vec![candidate.up_token_id.clone(), candidate.down_token_id.clone()],
}
```

Make `resolve_market_results_with` accept async resolution without adding a
boxed trait. Keep it synchronous for injected pure unit-test closures, and make
the real batch function an explicit async loop:

```rust
pub async fn resolve_market_results(
    resolver: &polyrover::market_results::Resolver,
    markets: &[crate::store::derived::ClobResultCandidate],
    observed_at: DateTime<Utc>,
) -> anyhow::Result<Vec<polyrover::market_results::MarketResult>> {
    anyhow::ensure!(!markets.is_empty(), "result request cannot be empty");
    anyhow::ensure!(markets.len() <= 256, "result request exceeds 256 markets");
    let mut results = Vec::new();
    for candidate in markets {
        match resolver.resolve_at(&market_ref(candidate), observed_at).await {
            Ok(Some(result)) => results.push(result),
            Ok(None) => {}
            Err(error) if aborts_result_batch(&error) => {
                anyhow::bail!("market-result batch aborted: {}", result_error_kind(&error));
            }
            Err(error) => tracing::warn!(
                condition_id = %candidate.condition_id,
                slug = %candidate.slug,
                error_kind = result_error_kind(&error),
                "market-result resolution skipped"
            ),
        }
    }
    results.sort_by(|left, right| left.condition_id.cmp(&right.condition_id));
    Ok(results)
}
```

In `run_market_result_collector`, delete `spawn_blocking` and call
`resolve_market_results(&resolver, &markets, Utc::now()).await` directly. Keep
the existing pure closure helper for local failure-policy unit tests, updating
its successful result to use `market.token_ids[0].clone()`.

- [ ] **Step 7: Make market discovery async**

Change `active_clob_markets` in `../rust-crypto-data/src/collection/clob/universe.rs` to
`pub(super) async fn`. Await both Polyrover discovery helpers:

```rust
let required = polyrover::market_resolver::discover_complete_window_markets(
    &client,
    &assets,
    start,
    start + ChronoDuration::minutes(10),
).await?;
```

Await the optional +15-minute prefetch similarly. Keep all canonical
six-asset/three-horizon projection rules in `rust-crypto-data`; do not move them
into Polyrover core types.

- [ ] **Step 8: Replace unbounded runtime channels with typed bounded sends**

In `../rust-crypto-data/src/collection/clob/runtime.rs`, replace `std::sync::mpsc` with
`tokio::sync::mpsc` and Tokio instants. Define:

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObservationSendError {
    Closed,
    Saturated,
}

#[derive(Clone)]
pub struct ObservationSender {
    tx: tokio::sync::mpsc::Sender<QueuedRuntimeObservation>,
    timeout: Duration,
}

impl ObservationSender {
    pub async fn send(
        &self,
        observation: RuntimeObservation,
    ) -> Result<(), ObservationSendError> {
        let queued = QueuedRuntimeObservation::new(observation);
        match tokio::time::timeout(self.timeout, self.tx.send(queued)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(ObservationSendError::Closed),
            Err(_) => Err(ObservationSendError::Saturated),
        }
    }
}

pub fn observation_channel(
    capacity: usize,
    timeout: Duration,
) -> (
    ObservationSender,
    tokio::sync::mpsc::Receiver<QueuedRuntimeObservation>,
) {
    let (tx, rx) = tokio::sync::mpsc::channel(capacity);
    (ObservationSender { tx, timeout }, rx)
}

#[derive(Debug)]
pub enum TransportTaskError {
    Saturated(&'static str),
    Polyrover(polyrover::Error),
}
```

Implement `Display` and `std::error::Error` for `TransportTaskError` without a
new dependency. Map `Closed` to normal `Ok(())`; map `Saturated` to
`TransportTaskError::Saturated("books_rest")` or `"market_wss"`.

- [ ] **Step 9: Convert REST and WebSocket workers to supervised Tokio tasks**

Return a concrete worker handle:

```rust
pub struct Worker<C> {
    pub commands: Option<tokio::sync::mpsc::Sender<C>>,
    pub join: Option<tokio::task::JoinHandle<Result<(), TransportTaskError>>>,
}

impl<C> Worker<C> {
    pub async fn shutdown(mut self, timeout: Duration) {
        self.commands.take();
        if let Some(join) = self.join.take() {
            let _ = tokio::time::timeout(timeout, join).await;
        }
    }
}

impl<C> Drop for Worker<C> {
    fn drop(&mut self) {
        if let Some(join) = &self.join {
            join.abort();
        }
    }
}
```

`spawn_rest_reconciler` returns `Worker<RestCommand>` and uses
`tokio::select!` between `command_rx.recv()` and `sleep_until(schedule.next_poll)`.
Await `client.order_books(&tokens)`. Preserve deadline-grid, lag, skipped-deadline,
complete-batch, and rate-limit observations exactly.

`spawn_ws_worker` returns `Worker<WsCommand>` and uses `tokio::select!` between
commands and `client.read_tracked_with_status(...)`. Await connect,
subscription, replacement connection, reads, and bounded observation sends.
Identical subscriptions do nothing; changed subscriptions become the first
frame of a fresh connection as today.

The coordinator sends commands through `worker.commands.as_ref()` and treats a
missing sender as a stopped worker. When the WebSocket command receiver closes,
or before the WebSocket task returns a saturation/network failure, attempt:

```rust
let _ = tokio::time::timeout(TRANSPORT_CLOSE_TIMEOUT, client.close()).await;
```

On a controlled coordinator exit, call `shutdown(TRANSPORT_CLOSE_TIMEOUT)` on
both workers before returning. A canceled coordinator drops the workers, whose
`Drop` implementations abort their tasks and may drop the socket. Never wait on
a full observation channel beyond `OBSERVATION_SEND_TIMEOUT`.

- [ ] **Step 10: Convert the coordinator to async ownership and supervision**

In `../rust-crypto-data/src/collection/clob/pipeline.rs`:

- delete `run_clob_1s_blocking`, the outer `spawn_blocking`, `Handle`, and
  `spawn_market_refresh_thread`;
- make the coordinator loop async;
- create the bounded observation channel with the constants from Step 5;
- spawn market refresh as a Tokio task with a bounded channel of capacity 1 and
  retain its `JoinHandle`;
- retain REST and WebSocket `Worker` values, including their `JoinHandle`s;
- use `tokio::select!` over observation receipt, refresh receipt, and a 100ms
  supervision tick;
- on `TransportTaskError::Saturated(transport)`, persist
  `last_error_kind = "queue_saturated"`, mark adaptive transport failure,
  respawn that worker, and resend the current token set/poll interval;
- treat receiver closure as normal shutdown only when the coordinator is
  stopping; an unexpected worker exit is a fail-closed command error;
- abort remaining handles on forced cancellation through each worker's `Drop`.

Make these coordinator methods async and await their current database, live
state, command-send, and event-batch calls:

```text
poll_market_refresh
install_markets_if_due
sync_rest_reconciliation
handle_observation
on_rest_observation
on_complete_rest_batch
confirm_market_tokens
on_ws_connected
on_ws_failed
on_ws_read
record_rest_error
record_ws_degraded
persist_transport_health
apply_poll_interval
process_clob_1s_event_batch
```

The top-level shape becomes:

```rust
pub async fn stream_clob_1s(/* existing arguments */) -> anyhow::Result<()> {
    validate_clob_stream_config(&config)?;
    validate_clob_feature_output(pool.as_ref(), save_features)?;
    let ws_config = polyrover::stream::Config {
        url: config.wss_url.as_deref().map(str::trim).filter(|url| !url.is_empty())
            .unwrap_or(polyrover::stream::DEFAULT_MARKET_URL).into(),
        ..Default::default()
    };
    validate_transport_deadlines(OBSERVATION_SEND_TIMEOUT, &ws_config)?;
    run_clob_1s_async(pool, config, ws_config, live_state, ws_tx, save_features, publish_features).await
}
```

`process_clob_1s_event_batch` directly awaits `process_clob_1s_event`,
`persist_clob_batch`, partition maintenance, trade upsert, token upsert, and
health upsert. Preserve REST as the sole durable 1Hz book writer and WSS as the
only public-trade source.

- [ ] **Step 11: Update runtime tests for Tokio supervision**

Convert worker tests to `#[tokio::test]`, async local listeners, awaited command
sends, and awaited observation receives. Keep pure schedule tests synchronous.
Add assertions that:

```rust
assert_eq!(
    validate_transport_deadlines(Duration::from_secs(30), &polyrover::stream::Config::default())
        .unwrap_err()
        .to_string(),
    "CLOB observation send timeout must precede heartbeat deadline"
);
```

For saturation, assert the worker `JoinHandle` returns
`TransportTaskError::Saturated`, the coordinator records `queue_saturated`, and
the next REST complete batch restores book health without creating a synthetic
trade. Do not add a forward-fill or replayed-trade fixture.

- [ ] **Step 12: Update root boundary wording**

In `../CONTEXT.md`, replace the Polyrover row with:

```markdown
| `polyrover/` | Universal async Rust Polymarket SDK/CLI; MegaBot uses it for public-data collection and research only. Git submodule. | MegaBot compiles only Polyrover's `public` feature. No MegaBot component may use its authenticated, wallet, execution, or bridge capabilities; Polygolem remains MegaBot's exclusive signing/execution boundary. |
```

In `../AGENTS.md`, replace the Polyrover architecture bullet with:

```markdown
- `polyrover`: universal async Rust Polymarket SDK/CLI; MegaBot consumers enable only its `public` feature.
```

Replace the Rust Polymarket boundary rule with:

```markdown
- Add Rust Polymarket capability to Polyrover, but MegaBot crates may consume only `default-features = false, features = ["public"]` unless a separately approved architecture decision changes execution ownership.
- Polygolem remains MegaBot's only auth/signing/wallet/execution dependency.
```

Do not modify Polygolem files or its dirty submodule state.

- [ ] **Step 13: Run coordinated GREEN validation**

From the MegaBot root run:

```bash
cargo fmt --all --manifest-path polyrover/Cargo.toml -- --check
cargo check --manifest-path polyrover/Cargo.toml --lib --no-default-features
cargo test --manifest-path polyrover/Cargo.toml --no-default-features --features public
cargo test --manifest-path polyrover/Cargo.toml --no-default-features --features authenticated
cargo test --manifest-path polyrover/Cargo.toml --no-default-features --features execution
cargo test --manifest-path polyrover/Cargo.toml --no-default-features --features bridge
cargo test --manifest-path polyrover/Cargo.toml --all-features
cargo clippy --manifest-path polyrover/Cargo.toml --all-targets --all-features -- -D warnings
cargo fmt --all --manifest-path rust-crypto-data/Cargo.toml -- --check
cargo test --manifest-path rust-crypto-data/Cargo.toml
cargo clippy --manifest-path rust-crypto-data/Cargo.toml --all-targets -- -D warnings
git diff --check
```

Expected: all PASS. Also run:

```bash
rg -n "spawn_blocking|std::thread|thread::spawn|thread::sleep|sync::mpsc" \
  rust-crypto-data/src/collection/clob \
  rust-crypto-data/src/sources/polymarket
rg -n "polyrover::(auth|user_stream|wallet|clob_orders|bridge)" rust-crypto-data/src
```

Expected: no matches. The repository-wide viewer-auth `spawn_blocking` remains
outside this scoped reference check.

- [ ] **Step 14: Commit the parent cutover atomically**

From the MegaBot root:

```bash
git add AGENTS.md CONTEXT.md rust-crypto-data/Cargo.toml \
  rust-crypto-data/src/collection/clob/config.rs \
  rust-crypto-data/src/collection/clob/universe.rs \
  rust-crypto-data/src/collection/clob/runtime.rs \
  rust-crypto-data/src/collection/clob/pipeline.rs \
  rust-crypto-data/src/sources/polymarket/results.rs \
  rust-crypto-data/tests/polyrover_clob_boundary_test.rs \
  polyrover
git commit -m "refactor: await Polyrover in CLOB collector"
```

Expected: the commit updates the parent source and submodule pointer together,
references the already-pushed Polyrover commit, and does not stage `polygolem`.

---

## Final verification receipt

Before declaring completion, record:

Run `git -C polyrover rev-parse HEAD` and `git rev-parse HEAD` from the MegaBot
root, then record their exact output with these receipts:

```text
Feature tiers: core/public/authenticated/execution/bridge/full all passed
Polyrover clippy: passed with -D warnings
rust-crypto-data tests: passed
rust-crypto-data clippy: passed with -D warnings
Boundary checks: public-only dependency; no scoped blocking bridges; no private capability imports
Safety: no new order, cancel, key, relayer, or bridge execution path
Unrelated state: polygolem untouched
```

Do not push the parent repository unless the owner separately requests parent delivery.
