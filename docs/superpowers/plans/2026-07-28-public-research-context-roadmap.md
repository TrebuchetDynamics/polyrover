# Public Research Context Improvements Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Each task is independently reviewable and must stop at its commit checkpoint.

**Goal:** Expand Polyrover's read-only public SDK with efficient batch market context, exact order-book analytics, richer Gamma discovery/taxonomy, transparent wallet dossiers, and provenance-aware market-flow summaries without adding prediction or execution behavior.

**Architecture:** Keep public HTTP/WebSocket adapters atomic and typed. Put deterministic derived calculations in pure research modules, keep orchestration bounded inside the unified `Client`, and leave persistence, scheduling, alert delivery, cross-venue joins, news ingestion, and strategy decisions to consumers. The seven tasks below are independent subplans ordered by shared foundations rather than one coupled rewrite.

**Tech Stack:** Rust 2021, Tokio, Reqwest, Futures, Serde, `rust_decimal`, Chrono, deterministic local TCP/WebSocket fixtures, ignored operator-run public canaries.

## Global Constraints

- The default Cargo feature remains `public`; read-only consumers use `default-features = false, features = ["public"]`.
- Add no private-key handling, signing, authentication expansion, order submission, cancellation, relayer calls, bridge transfers, wallet connection, or money movement.
- Add no AI forecasts, news/OSINT scraping, copy trading, cross-venue execution, opportunity ranking, or profitability claims.
- `opportunities.scan`, `intel.wallet.alerts`, and every execution capability remain planned.
- Preserve upstream decimal text at HTTP and WebSocket boundaries; use `Decimal` for new calculations.
- Batch POSTs in this plan are documented read-only operations and must use `transport::Client::post_json_idempotent`.
- Do not invent upstream batch limits. Validate non-empty requests and identifiers; document that callers own chunk sizing until a first-party limit is published or measured.
- Every derived result includes an observation timestamp, row count, source label, and cautious language.
- Wallet and flow outputs describe public observations; they never label a person an insider, skilled trader, manipulator, or coordinated actor.
- Ordinary tests never call public endpoints. Live canaries remain `#[ignore]` and require explicit operator input.
- Every behavior change follows RED → GREEN → regression.
- Do not commit, push, publish crates, create tags, or make a GitHub release unless the maintainer explicitly invokes the corresponding delivery action.

---

## Existing foundations to reuse

Already shipped and not to be rebuilt:

- shared bounded HTTP retry/concurrency in `src/api/transport.rs`;
- single/batch CLOB price history in `src/api/clob.rs`;
- provenance-linked fixtures in `tests/fixtures/`;
- borrowed market-event stream adapter in `src/streaming/stream_client.rs`;
- decimal-safe fill simulation in `src/research/simulation.rs`;
- public Data API positions, closed positions, trades, activity, holders, portfolio value, open interest, volume, and leaderboards;
- capability inventory in `capabilities.json` plus `tests/feature_contract.rs`.

## File structure

| File                                 | Responsibility after this plan                                              |
| ------------------------------------ | --------------------------------------------------------------------------- |
| `src/api/clob.rs`                    | Atomic batch prices, midpoints, spreads, and last-trade reads.              |
| `src/api/gamma.rs`                   | Screening query fields plus tags, series, sports, and team reads.           |
| `src/models/types.rs`                | Tolerant public CLOB/Gamma response DTOs.                                   |
| `src/streaming/market_data.rs`       | Exact Decimal book ordering, spread, liquidity, and depth calculations.     |
| `src/research/wallet_dossier.rs`     | Pure transparent wallet aggregation and its bounded input/output contracts. |
| `src/research/market_flow.rs`        | Bounded rolling per-asset public-flow summaries with source labels.         |
| `src/client.rs`                      | Narrow unified-client facades and wallet-dossier orchestration.             |
| `src/lib.rs`                         | Public module exports only.                                                 |
| `tests/client.rs`                    | Local HTTP integration tests for public client methods and request shapes.  |
| `tests/contracts.rs`                 | Fixture provenance and tolerant response-contract tests.                    |
| `tests/fixtures/`                    | First-party documentation examples with explicit provenance.                |
| `capabilities.json`                  | Operation status, source APIs, tests, and non-execution notes.              |
| `docs/endpoint-capability-matrix.md` | Human-readable endpoint inventory and operational limits.                   |
| `README.md`                          | Short current-main examples and safety/limitation updates.                  |
| `CHANGELOG.md`                       | Curated `0.2.0` release notes.                                              |

## Roadmap

| Task | Deliverable                                     | Depends on                         | Independently shippable     |
| ---- | ----------------------------------------------- | ---------------------------------- | --------------------------- |
| 1    | Batch CLOB market-context primitives            | Existing transport                 | Yes                         |
| 2    | Exact Decimal streaming analytics               | Existing `rust_decimal` dependency | Yes, but public type change |
| 3    | Gamma screening filters                         | Existing query builder             | Yes                         |
| 4    | Gamma tags/series/sports taxonomy reads         | Existing Gamma transport           | Yes                         |
| 5    | Transparent wallet dossier                      | Existing Data API facade           | Yes                         |
| 6    | Provenance-aware market-flow tracker            | Task 2                             | Yes                         |
| 7    | Capability/docs/version integration for `0.2.0` | Tasks selected for release         | No behavior change          |

---

### Task 1: Batch CLOB market-context primitives

**Files:**

- Modify: `src/models/types.rs`
- Modify: `src/api/clob.rs`
- Modify: `src/client.rs`
- Modify: `capabilities.json`
- Modify: `src/capabilities/capabilities.rs`
- Modify: `tests/client.rs`
- Modify: `tests/contracts.rs`
- Create: `tests/fixtures/clob/batch-prices.json`
- Create: `tests/fixtures/clob/batch-midpoints.json`
- Create: `tests/fixtures/clob/batch-spreads.json`
- Create: `tests/fixtures/clob/batch-last-trades.json`
- Modify: `tests/fixtures/provenance.json`

**Interfaces:**

- Produces: `clob::BatchMarketRequest { token_id: String, side: String }`
- Produces: `types::ClobLastTradePrice { token_id, price, side }`
- Produces: `clob::Client::{batch_prices,batch_midpoints,batch_spreads,batch_last_trades}`
- Produces: matching methods on unified `Client`
- Returns decimal values as `String` maps, never `f64`

- [ ] **Step 1: Add first-party contract-example fixtures and provenance entries**

Create `tests/fixtures/clob/batch-prices.json`:

```json
{
  "token-1": { "BUY": 0.45 },
  "token-2": { "SELL": "0.52" }
}
```

Create `tests/fixtures/clob/batch-midpoints.json`:

```json
{ "token-1": "0.45", "token-2": 0.52 }
```

Create `tests/fixtures/clob/batch-spreads.json`:

```json
{ "token-1": "0.02", "token-2": 0.015 }
```

Create `tests/fixtures/clob/batch-last-trades.json`:

```json
[
  { "token_id": "token-1", "price": "0.45", "side": "BUY" },
  { "token_id": "token-2", "price": 0.52, "side": "SELL" }
]
```

Append four entries to `tests/fixtures/provenance.json`, each with `kind: "contract-example"`, `containsLiveData: false`, `retrievedAt: "2026-07-28"`, and these sources/endpoints:

```json
[
  {
    "path": "clob/batch-prices.json",
    "source": "https://docs.polymarket.com/api-reference/market-data/get-market-prices-request-body",
    "endpoint": "POST https://clob.polymarket.com/prices"
  },
  {
    "path": "clob/batch-midpoints.json",
    "source": "https://docs.polymarket.com/api-reference/market-data/get-midpoint-prices-request-body",
    "endpoint": "POST https://clob.polymarket.com/midpoints"
  },
  {
    "path": "clob/batch-spreads.json",
    "source": "https://docs.polymarket.com/api-reference/market-data/get-spreads",
    "endpoint": "POST https://clob.polymarket.com/spreads"
  },
  {
    "path": "clob/batch-last-trades.json",
    "source": "https://docs.polymarket.com/api-reference/market-data/get-last-trade-prices-request-body",
    "endpoint": "POST https://clob.polymarket.com/last-trades-prices"
  }
]
```

Preserve the manifest's existing top-level shape; insert these objects into its existing `fixtures` array rather than nesting a second array. Change the provenance-count assertion in `tests/contracts.rs` from `2` to `6`.

- [ ] **Step 2: Write failing DTO and integration tests**

Add to `tests/contracts.rs`:

```rust
#[test]
fn batch_market_context_examples_preserve_decimal_text() {
    use polyrover::types::ClobLastTradePrice;
    use serde_json::Value;

    let prices: Value = serde_json::from_str(include_str!("fixtures/clob/batch-prices.json")).unwrap();
    let midpoints: Value = serde_json::from_str(include_str!("fixtures/clob/batch-midpoints.json")).unwrap();
    let spreads: Value = serde_json::from_str(include_str!("fixtures/clob/batch-spreads.json")).unwrap();
    let trades: Vec<ClobLastTradePrice> =
        serde_json::from_str(include_str!("fixtures/clob/batch-last-trades.json")).unwrap();

    assert_eq!(prices["token-1"]["BUY"], 0.45);
    assert_eq!(midpoints["token-2"], 0.52);
    assert_eq!(spreads["token-2"], 0.015);
    assert_eq!(trades[1].price, "0.52");
}
```

Add to `tests/client.rs`:

```rust
#[tokio::test]
async fn client_reads_batch_prices_as_decimal_text() {
    use polyrover::clob::BatchMarketRequest;

    let (clob_base_url, received, server) =
        serve_json(r#"{"token-1":{"BUY":0.45},"token-2":{"SELL":"0.52"}}"#);
    let client = Client::new(ClientConfig {
        clob_base_url,
        ..ClientConfig::default()
    })
    .unwrap();

    let rows = client
        .batch_prices(&[
            BatchMarketRequest::new("token-1", "BUY"),
            BatchMarketRequest::new("token-2", "SELL"),
        ])
        .await
        .unwrap();

    assert_eq!(rows["token-1"]["BUY"], "0.45");
    assert_eq!(rows["token-2"]["SELL"], "0.52");
    let request = received.recv().unwrap();
    assert!(request.starts_with("POST /prices "));
    assert!(request.contains(r#""token_id":"token-1""#));
    assert!(request.contains(r#""side":"BUY""#));
    server.join().unwrap();
}

#[tokio::test]
async fn batch_context_rejects_empty_requests_and_invalid_price_sides() {
    use polyrover::clob::BatchMarketRequest;

    let client = polyrover::clob::Client::new("http://127.0.0.1:1").unwrap();
    assert!(client.batch_midpoints(&[]).await.is_err());
    let error = client
        .batch_prices(&[BatchMarketRequest::new("token-1", "hold")])
        .await
        .unwrap_err();
    assert!(error.to_string().contains("BUY or SELL"));
}
```

Add equivalent local-server tests for `/midpoints`, `/spreads`, and `/last-trades-prices`, asserting mixed numeric/string responses normalize to exact text.

- [ ] **Step 3: Run focused tests and verify RED**

Run:

```bash
cargo test --all-features batch_market_context_examples_preserve_decimal_text
cargo test --all-features client_reads_batch_prices_as_decimal_text
cargo test --all-features batch_context_rejects_empty_requests_and_invalid_price_sides
```

Expected: compilation fails because `BatchMarketRequest`, `ClobLastTradePrice`, and batch client methods do not exist.

- [ ] **Step 4: Add tolerant request/response types**

Add to `src/models/types.rs`:

```rust
#[derive(Clone, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
pub struct ClobLastTradePrice {
    #[serde(default)]
    pub token_id: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub price: String,
    #[serde(default)]
    pub side: String,
}
```

Add to `src/api/clob.rs`:

```rust
use std::collections::BTreeMap;
use serde_json::Value;

#[derive(Clone, Debug, Default, Serialize, Eq, PartialEq)]
pub struct BatchMarketRequest {
    pub token_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub side: String,
}

impl BatchMarketRequest {
    pub fn new(token_id: impl Into<String>, side: impl Into<String>) -> Self {
        Self {
            token_id: token_id.into(),
            side: side.into().trim().to_ascii_uppercase(),
        }
    }
}

fn validate_batch_requests(rows: &[BatchMarketRequest], require_side: bool) -> Result<()> {
    if rows.is_empty() {
        return Err(Error::Invalid("batch market request must not be empty".into()));
    }
    for row in rows {
        if row.token_id.trim().is_empty() {
            return Err(Error::Invalid("batch market token_id is required".into()));
        }
        if require_side && !matches!(row.side.as_str(), "BUY" | "SELL") {
            return Err(Error::Invalid("batch market side must be BUY or SELL".into()));
        }
    }
    Ok(())
}

fn scalar_map(raw: BTreeMap<String, Value>) -> BTreeMap<String, String> {
    raw.into_iter()
        .map(|(key, value)| (key, crate::jsonx::scalar_to_string(&value)))
        .collect()
}

fn nested_scalar_map(
    raw: BTreeMap<String, BTreeMap<String, Value>>,
) -> BTreeMap<String, BTreeMap<String, String>> {
    raw.into_iter()
        .map(|(token, sides)| (token, scalar_map(sides)))
        .collect()
}
```

- [ ] **Step 5: Implement atomic batch reads and unified facades**

Add to `clob::Client`:

```rust
pub async fn batch_prices(
    &self,
    rows: &[BatchMarketRequest],
) -> Result<BTreeMap<String, BTreeMap<String, String>>> {
    validate_batch_requests(rows, true)?;
    let raw = self
        .transport
        .post_json_idempotent::<_, BTreeMap<String, BTreeMap<String, Value>>>("/prices", &rows)
        .await?;
    Ok(nested_scalar_map(raw))
}

pub async fn batch_midpoints(
    &self,
    rows: &[BatchMarketRequest],
) -> Result<BTreeMap<String, String>> {
    validate_batch_requests(rows, false)?;
    let raw = self
        .transport
        .post_json_idempotent::<_, BTreeMap<String, Value>>("/midpoints", &rows)
        .await?;
    Ok(scalar_map(raw))
}

pub async fn batch_spreads(
    &self,
    rows: &[BatchMarketRequest],
) -> Result<BTreeMap<String, String>> {
    validate_batch_requests(rows, false)?;
    let raw = self
        .transport
        .post_json_idempotent::<_, BTreeMap<String, Value>>("/spreads", &rows)
        .await?;
    Ok(scalar_map(raw))
}

pub async fn batch_last_trades(
    &self,
    rows: &[BatchMarketRequest],
) -> Result<Vec<ClobLastTradePrice>> {
    validate_batch_requests(rows, false)?;
    self.transport
        .post_json_idempotent("/last-trades-prices", &rows)
        .await
}
```

Add matching methods to `src/client.rs` that delegate directly to `self.clob`. Import `BTreeMap`, `BatchMarketRequest`, and `ClobLastTradePrice`; do not add CLI commands in this task.

- [ ] **Step 6: Run GREEN and regression tests**

Run:

```bash
cargo test --all-features batch_market_context
cargo test --all-features client_reads_batch
cargo test --all-features --test contracts
cargo test --all-features --test client
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
```

Expected: all focused tests and both integration suites pass; no public network access occurs.

- [ ] **Step 7: Update capability evidence**

Mark these capabilities implemented in both catalogs:

```text
clob.prices.batchRead      -> clob::Client::batch_prices, Client::batch_prices
clob.midpoints.batchRead   -> clob::Client::batch_midpoints, Client::batch_midpoints
clob.spreads.batchRead     -> clob::Client::batch_spreads, Client::batch_spreads
clob.lastTrades.batchRead  -> clob::Client::batch_last_trades, Client::batch_last_trades
```

Set notes to: `SDK-only atomic read; callers own chunk sizing and persistence. No upstream batch maximum is asserted.` Add `tests/client.rs` and `tests/contracts.rs` as evidence.

Run:

```bash
cargo test --all-features --test feature_contract
jq empty capabilities.json
jq empty tests/fixtures/provenance.json
git diff --check
```

- [ ] **Step 8: Commit Task 1**

```bash
git add src/models/types.rs src/api/clob.rs src/client.rs capabilities.json \
  src/capabilities/capabilities.rs tests/client.rs tests/contracts.rs tests/fixtures
git commit -m "feat: add batch CLOB market context"
```

---

### Task 2: Exact Decimal order-book analytics

**Files:**

- Modify: `src/streaming/market_data.rs`
- Modify: `src/lib.rs`
- Modify: `README.md`
- Test: `src/streaming/market_data.rs`
- Validate public type export: `tests/market_flow.rs`

**Interfaces:**

- Changes: `Liquidity` and `Depth` numeric fields from `f64` to `rust_decimal::Decimal`
- Produces: `pub use rust_decimal::Decimal` from the crate root for consumers
- Preserves: `Snapshot` wire-facing prices/sizes as `String`
- Produces exact spread, midpoint, ordering, zero-size removal, imbalance, and 1¢/2¢/5¢ depth boundaries
- Release impact: pre-1.0 public API break, included in `0.2.0`

- [ ] **Step 1: Write failing exact-boundary regression tests**

Add to `src/streaming/market_data.rs` tests:

```rust
#[test]
fn decimal_depth_uses_exact_cent_boundaries() {
    use rust_decimal::Decimal;

    let snapshot = Snapshot {
        bids: vec![
            Level { price: "0.50".into(), size: "1.1".into() },
            Level { price: "0.49".into(), size: "2.2".into() },
            Level { price: "0.48".into(), size: "3.3".into() },
            Level { price: "0.45".into(), size: "4.4".into() },
            Level { price: "0.449999".into(), size: "99".into() },
        ],
        asks: vec![
            Level { price: "0.51".into(), size: "1".into() },
            Level { price: "0.52".into(), size: "2".into() },
            Level { price: "0.53".into(), size: "3".into() },
            Level { price: "0.56".into(), size: "4".into() },
        ],
        ..Default::default()
    };

    let depth = snapshot.depth();
    assert_eq!(depth.bid_depth_1c, Decimal::new(33, 1));
    assert_eq!(depth.bid_depth_2c, Decimal::new(66, 1));
    assert_eq!(depth.bid_depth_5c, Decimal::new(110, 1));
    assert_eq!(depth.ask_depth_1c, Decimal::new(3, 0));
    assert_eq!(depth.ask_depth_2c, Decimal::new(6, 0));
    assert_eq!(depth.ask_depth_5c, Decimal::new(10, 0));
}

#[test]
fn decimal_midpoint_spread_and_imbalance_are_exact() {
    use rust_decimal::Decimal;

    let mut tracker = Tracker::new();
    let snapshot = tracker.apply_book(BookMessage {
        asset_id: "token-1".into(),
        bids: vec![PriceLevel { price: "0.1".into(), size: "0.1".into() }],
        asks: vec![PriceLevel { price: "0.3".into(), size: "0.2".into() }],
        ..Default::default()
    });
    assert_eq!(snapshot.midpoint, "0.2");
    assert_eq!(snapshot.spread, "0.2");
    assert_eq!(snapshot.liquidity().imbalance, Decimal::new(1, 0) / Decimal::new(3, 0));
}
```

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
cargo test --all-features decimal_depth_uses_exact_cent_boundaries
cargo test --all-features decimal_midpoint_spread_and_imbalance_are_exact
```

Expected: compilation fails because public metrics are `f64`, or assertions expose epsilon/binary rounding behavior.

- [ ] **Step 3: Replace calculation types and helpers with Decimal**

At the top of `src/streaming/market_data.rs` add:

```rust
use std::{collections::BTreeMap, str::FromStr};
use rust_decimal::Decimal;
```

Change `Liquidity` and `Depth` numeric fields to `Decimal`. Replace numeric helpers with:

```rust
fn parse_number(value: &str) -> Decimal {
    Decimal::from_str(value.trim()).unwrap_or(Decimal::ZERO)
}

fn parse_price(value: &str) -> Option<Decimal> {
    Decimal::from_str(value.trim()).ok()
}

fn add_depth(
    size: Decimal,
    distance: Decimal,
    one: &mut Decimal,
    two: &mut Decimal,
    five: &mut Decimal,
) {
    if (Decimal::ZERO..=Decimal::new(1, 2)).contains(&distance) {
        *one += size;
    }
    if (Decimal::ZERO..=Decimal::new(2, 2)).contains(&distance) {
        *two += size;
    }
    if (Decimal::ZERO..=Decimal::new(5, 2)).contains(&distance) {
        *five += size;
    }
}

fn is_zero_size(size: &str) -> bool {
    Decimal::from_str(size.trim()).is_ok_and(|value| value == Decimal::ZERO)
}

fn format_decimal(value: Decimal) -> String {
    value.normalize().to_string()
}

fn midpoint(bid: &str, ask: &str) -> Option<String> {
    Some(format_decimal(
        (parse_price(bid)? + parse_price(ask)?) / Decimal::new(2, 0),
    ))
}

fn spread(bid: &str, ask: &str) -> Option<String> {
    Some(format_decimal(parse_price(ask)? - parse_price(bid)?))
}
```

Update `Snapshot::liquidity`, `Snapshot::depth`, and `sort_levels` to use Decimal constants/comparison. Remove every epsilon literal and the old `format_float` function.

- [ ] **Step 4: Run GREEN and regressions**

Run:

```bash
cargo test --all-features market_data::tests
cargo test --all-features
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
RUSTDOCFLAGS='-D warnings' cargo doc --all-features --no-deps
```

Expected: exact-boundary tests and the full Polyrover suite pass.

- [ ] **Step 5: Validate the public Decimal export**

Add this crate-root re-export to `src/lib.rs`:

```rust
pub use rust_decimal::Decimal;
```

Run:

```bash
cargo test --all-features --test market_flow
```

Expected: the public integration test compiles through `polyrover::Decimal` and passes.

Update README limitations to state: `Streaming liquidity/depth and local simulation use Decimal internally; upstream wire values remain strings.`

- [ ] **Step 6: Commit Task 2**

```bash
git add src/streaming/market_data.rs src/lib.rs README.md
git commit -m "refactor: make order-book analytics decimal exact"
```

---

### Task 3: Gamma screening filters

**Files:**

- Modify: `src/api/gamma.rs`
- Modify: `tests/client.rs`
- Modify: `docs/endpoint-capability-matrix.md`

**Interfaces:**

- Extends both `MarketParams` and `MarketKeysetParams`
- Adds Decimal liquidity/volume thresholds, ISO date bounds, related/include-tag flags, and sports market types
- Preserves offset and opaque-keyset paging behavior

- [ ] **Step 1: Write failing query-shape tests**

Add to `src/api/gamma.rs` tests:

```rust
#[test]
fn market_screening_filters_encode_for_offset_and_keyset_queries() {
    use rust_decimal::Decimal;

    let params = MarketParams {
        liquidity_num_min: Some(Decimal::new(1000, 0)),
        volume_num_min: Some(Decimal::new(5000, 0)),
        start_date_min: "2026-01-01T00:00:00Z".into(),
        end_date_max: "2026-12-31T23:59:59Z".into(),
        related_tags: Some(true),
        include_tag: Some(true),
        sports_market_types: vec!["moneyline".into(), "spread".into()],
        ..Default::default()
    };
    let path = params.path("/markets");
    assert!(path.contains("liquidity_num_min=1000"));
    assert!(path.contains("volume_num_min=5000"));
    assert!(path.contains("start_date_min=2026-01-01T00%3A00%3A00Z"));
    assert!(path.contains("end_date_max=2026-12-31T23%3A59%3A59Z"));
    assert!(path.contains("related_tags=true"));
    assert!(path.contains("include_tag=true"));
    assert_eq!(path.matches("sports_market_types=").count(), 2);

    let keyset = MarketKeysetParams {
        after_cursor: "opaque==".into(),
        liquidity_num_max: Some(Decimal::new(2500, 0)),
        volume_num_max: Some(Decimal::new(9000, 0)),
        ..Default::default()
    };
    let path = keyset.path("/markets/keyset");
    assert!(path.contains("after_cursor=opaque%3D%3D"));
    assert!(path.contains("liquidity_num_max=2500"));
    assert!(path.contains("volume_num_max=9000"));
}
```

- [ ] **Step 2: Run test and verify RED**

```bash
cargo test --all-features market_screening_filters_encode_for_offset_and_keyset_queries
```

Expected: compile failure because the new fields do not exist.

- [ ] **Step 3: Add fields to both parameter structs**

Add these exact fields to `MarketParams` and `MarketKeysetParams`:

```rust
pub liquidity_num_min: Option<rust_decimal::Decimal>,
pub liquidity_num_max: Option<rust_decimal::Decimal>,
pub volume_num_min: Option<rust_decimal::Decimal>,
pub volume_num_max: Option<rust_decimal::Decimal>,
pub start_date_min: String,
pub start_date_max: String,
pub end_date_min: String,
pub end_date_max: String,
pub related_tags: Option<bool>,
pub include_tag: Option<bool>,
pub sports_market_types: Vec<String>,
```

In both `path` methods add:

```rust
q.opt("liquidity_num_min", self.liquidity_num_min);
q.opt("liquidity_num_max", self.liquidity_num_max);
q.opt("volume_num_min", self.volume_num_min);
q.opt("volume_num_max", self.volume_num_max);
q.opt_str("start_date_min", Some(self.start_date_min.as_str()));
q.opt_str("start_date_max", Some(self.start_date_max.as_str()));
q.opt_str("end_date_min", Some(self.end_date_min.as_str()));
q.opt_str("end_date_max", Some(self.end_date_max.as_str()));
q.opt("related_tags", self.related_tags);
q.opt("include_tag", self.include_tag);
q.list("sports_market_types", &self.sports_market_types);
```

Confirm `Query::opt_str` omits empty strings; if it does not, change only `opt_str` to skip empty values and add a regression test for every existing caller.

- [ ] **Step 4: Run GREEN and integration regressions**

```bash
cargo test --all-features market_screening_filters_encode_for_offset_and_keyset_queries
cargo test --all-features gamma
cargo test --all-features --test client
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
git diff --check
```

Expected: all pass; keyset cursors remain byte-for-byte opaque after decoding/encoding.

- [ ] **Step 5: Document filter semantics and Commit Task 3**

Add rustdoc stating numeric thresholds are Gamma screening metadata, not executable CLOB depth. Date strings are ISO 8601 and are forwarded unchanged. Sports types are repeated query parameters and remain subject to the documented URL ceiling.

```bash
git add src/api/gamma.rs tests/client.rs docs/endpoint-capability-matrix.md
git commit -m "feat: add Gamma market screening filters"
```

---

### Task 4: Gamma tags, series, and sports taxonomy reads

**Files:**

- Modify: `src/models/types.rs`
- Modify: `src/api/gamma.rs`
- Modify: `src/client.rs`
- Modify: `capabilities.json`
- Modify: `src/capabilities/capabilities.rs`
- Modify: `tests/client.rs`
- Modify: `tests/contracts.rs`
- Create: `tests/fixtures/gamma/tags.json`
- Create: `tests/fixtures/gamma/series.json`
- Create: `tests/fixtures/gamma/sports.json`
- Create: `tests/fixtures/gamma/teams.json`
- Modify: `tests/fixtures/provenance.json`

**Interfaces:**

- Produces DTOs: `GammaTag`, `GammaSeries`, `SportMetadata`, `SportsMarketTypes`, `Team`
- Produces params: `TaxonomyParams`, `TeamParams`
- Produces Gamma and unified client methods: `tags`, `tag_by_id`, `tag_by_slug`, `series`, `series_by_id`, `sports`, `sports_market_types`, `teams`
- Adds no comments/news/profile endpoints

- [ ] **Step 1: Add contract fixtures from official docs**

Use these minimal fixture bodies:

```json
// tests/fixtures/gamma/tags.json
[{ "id": "7", "label": "Crypto", "slug": "crypto", "forceShow": true }]
```

```json
// tests/fixtures/gamma/series.json
[
  {
    "id": "12",
    "ticker": "BTC5M",
    "slug": "btc-updown-5m",
    "title": "BTC 5m",
    "active": true,
    "volume": 123.45
  }
]
```

```json
// tests/fixtures/gamma/sports.json
[
  {
    "sport": "nfl",
    "image": "https://example.invalid/nfl.png",
    "resolution": "https://example.invalid/rules",
    "ordering": "home",
    "tags": "1,2",
    "series": "9"
  }
]
```

```json
// tests/fixtures/gamma/teams.json
[
  {
    "id": 123,
    "name": "Example",
    "league": "NFL",
    "record": "1-0",
    "logo": "https://example.invalid/logo.png",
    "abbreviation": "EX",
    "alias": "Example Team"
  }
]
```

Add provenance entries using the corresponding official docs URLs under `/api-reference/tags/list-tags`, `/series/list-series`, `/sports/get-sports-metadata-information`, and `/sports/list-teams`; mark them contract examples with no live data. Change the provenance-count assertion in `tests/contracts.rs` from `6` to `10`.

- [ ] **Step 2: Write failing DTO and local-server tests**

Add to `tests/contracts.rs`:

```rust
#[test]
fn gamma_taxonomy_contract_examples_deserialize() {
    use polyrover::types::{GammaSeries, GammaTag, SportMetadata, Team};

    let tags: Vec<GammaTag> = serde_json::from_str(include_str!("fixtures/gamma/tags.json")).unwrap();
    let series: Vec<GammaSeries> = serde_json::from_str(include_str!("fixtures/gamma/series.json")).unwrap();
    let sports: Vec<SportMetadata> = serde_json::from_str(include_str!("fixtures/gamma/sports.json")).unwrap();
    let teams: Vec<Team> = serde_json::from_str(include_str!("fixtures/gamma/teams.json")).unwrap();

    assert_eq!(tags[0].slug, "crypto");
    assert_eq!(series[0].volume, "123.45");
    assert_eq!(sports[0].sport, "nfl");
    assert_eq!(teams[0].id, 123);
}
```

Add local-server tests in `tests/client.rs` for these exact paths:

```text
GET /tags?limit=10&offset=0
GET /tags/7
GET /tags/slug/crypto
GET /series?limit=10&offset=0
GET /series/12
GET /sports
GET /sports/market-types
GET /teams?limit=10&offset=0&league=NFL
```

Assert each response reaches the unified `Client` and every path segment/query value is escaped.

- [ ] **Step 3: Run tests and verify RED**

```bash
cargo test --all-features gamma_taxonomy_contract_examples_deserialize
cargo test --all-features client_reads_gamma_taxonomy
```

Expected: compile failure because models, params, and methods do not exist.

- [ ] **Step 4: Add tolerant DTOs**

Add to `src/models/types.rs`:

```rust
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct GammaTag {
    #[serde(default, deserialize_with = "string_or_number")]
    pub id: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub label: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub slug: String,
    #[serde(default, rename = "forceShow", deserialize_with = "bool_or_false")]
    pub force_show: bool,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct GammaSeries {
    #[serde(default, deserialize_with = "string_or_number")]
    pub id: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub ticker: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub slug: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub title: String,
    #[serde(default, deserialize_with = "bool_or_false")]
    pub active: bool,
    #[serde(default, deserialize_with = "string_or_number")]
    pub volume: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub liquidity: String,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct SportMetadata {
    #[serde(default, deserialize_with = "string_or_number")] pub sport: String,
    #[serde(default, deserialize_with = "string_or_number")] pub image: String,
    #[serde(default, deserialize_with = "string_or_number")] pub resolution: String,
    #[serde(default, deserialize_with = "string_or_number")] pub ordering: String,
    #[serde(default, deserialize_with = "string_or_number")] pub tags: String,
    #[serde(default, deserialize_with = "string_or_number")] pub series: String,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct SportsMarketTypes {
    #[serde(default, rename = "marketTypes")]
    pub market_types: Vec<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct Team {
    #[serde(default, deserialize_with = "int_or_zero")] pub id: i64,
    #[serde(default, deserialize_with = "string_or_number")] pub name: String,
    #[serde(default, deserialize_with = "string_or_number")] pub league: String,
    #[serde(default, deserialize_with = "string_or_number")] pub record: String,
    #[serde(default, deserialize_with = "string_or_number")] pub logo: String,
    #[serde(default, deserialize_with = "string_or_number")] pub abbreviation: String,
    #[serde(default, deserialize_with = "string_or_number")] pub alias: String,
    #[serde(flatten)] pub extra: Map<String, Value>,
}
```

- [ ] **Step 5: Add query params and methods**

Add to `src/api/gamma.rs`:

```rust
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TaxonomyParams {
    pub limit: Option<u32>,
    pub offset: Option<u32>,
    pub order: String,
    pub ascending: Option<bool>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TeamParams {
    pub page: TaxonomyParams,
    pub leagues: Vec<String>,
    pub names: Vec<String>,
    pub abbreviations: Vec<String>,
}
```

Add the path builders:

```rust
impl TaxonomyParams {
    fn path(&self, base: &str) -> String {
        let mut query = Query::new(base);
        query.opt("limit", self.limit);
        query.opt("offset", self.offset);
        query.opt_str("order", Some(self.order.as_str()));
        query.opt("ascending", self.ascending);
        query.finish()
    }
}

impl TeamParams {
    fn path(&self, base: &str) -> String {
        let mut query = Query::new(base);
        query.opt("limit", self.page.limit);
        query.opt("offset", self.page.offset);
        query.opt_str("order", Some(self.page.order.as_str()));
        query.opt("ascending", self.page.ascending);
        query.list("league", &self.leagues);
        query.list("name", &self.names);
        query.list("abbreviation", &self.abbreviations);
        query.finish()
    }
}
```

Add these complete methods on Gamma `Client`:

```rust
pub async fn tags(&self, params: &TaxonomyParams) -> Result<Vec<GammaTag>> {
    self.transport.get_json(&params.path("/tags")).await
}

pub async fn tag_by_id(&self, id: i64) -> Result<GammaTag> {
    self.transport.get_json(&format!("/tags/{id}")).await
}

pub async fn tag_by_slug(&self, slug: &str) -> Result<GammaTag> {
    if slug.trim().is_empty() {
        return Err(crate::Error::Invalid("tag slug is required".into()));
    }
    self.transport
        .get_json(&format!("/tags/slug/{}", escape(slug)))
        .await
}

pub async fn series(&self, params: &TaxonomyParams) -> Result<Vec<GammaSeries>> {
    self.transport.get_json(&params.path("/series")).await
}

pub async fn series_by_id(&self, id: i64) -> Result<GammaSeries> {
    self.transport.get_json(&format!("/series/{id}")).await
}

pub async fn sports(&self) -> Result<Vec<SportMetadata>> {
    self.transport.get_json("/sports").await
}

pub async fn sports_market_types(&self) -> Result<SportsMarketTypes> {
    self.transport.get_json("/sports/market-types").await
}

pub async fn teams(&self, params: &TeamParams) -> Result<Vec<Team>> {
    self.transport.get_json(&params.path("/teams")).await
}
```

Add direct unified `Client` delegates with the same method names, parameter types, and return types; each body calls the corresponding `self.gamma` method and contains no additional requests.

- [ ] **Step 6: Run GREEN and capability regressions**

```bash
cargo test --all-features gamma_taxonomy
cargo test --all-features --test contracts
cargo test --all-features --test client
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
```

Mark `tags.list`, `tags.get`, `series.list`, `series.get`, `sports.list`, `sports.marketTypes.list`, and `sports.teams.list` implemented with exact APIs/tests. Keep related-tag endpoints planned because they were not requested by a current caller.

```bash
cargo test --all-features --test feature_contract
jq empty capabilities.json
jq empty tests/fixtures/provenance.json
git diff --check
```

- [ ] **Step 7: Commit Task 4**

```bash
git add src/models/types.rs src/api/gamma.rs src/client.rs capabilities.json \
  src/capabilities/capabilities.rs tests/client.rs tests/contracts.rs tests/fixtures
git commit -m "feat: add Gamma research taxonomy reads"
```

---

### Task 5: Transparent wallet dossier

**Files:**

- Create: `src/research/wallet_dossier.rs`
- Modify: `src/lib.rs`
- Modify: `src/client.rs`
- Modify: `capabilities.json`
- Modify: `src/capabilities/capabilities.rs`
- Modify: `tests/client.rs`

**Interfaces:**

- Produces: `wallet_dossier::{WalletDossierParams, WalletDossierInput, WalletDossier}`
- Produces: pure `build_wallet_dossier(input) -> Result<WalletDossier>`
- Produces: async `Client::wallet_dossier(&WalletDossierParams)`
- Uses current positions, closed positions, trades, activity, portfolio value, and markets-traded endpoints
- Produces raw transparent metrics only; does not call `score_wallet` and has no recommendation field

- [ ] **Step 1: Write failing pure aggregation tests**

Create `src/research/wallet_dossier.rs` with tests first:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use crate::data_types::{Activity, ClosedPosition, Position, PortfolioValue, TotalMarketsTraded, Trade};

    #[test]
    fn dossier_separates_realized_unrealized_and_time_windows() {
        let as_of = Utc.timestamp_opt(1_800_000_000, 0).unwrap();
        let input = WalletDossierInput {
            wallet: "0xwallet".into(),
            as_of,
            limit: 100,
            positions: vec![Position {
                size: 10.0,
                current_price: 0.6,
                realized_pnl: 2.0,
                unrealized_pnl: 1.5,
                ..Default::default()
            }],
            closed_positions: vec![ClosedPosition {
                position: Position { realized_pnl: 4.0, ..Default::default() },
                timestamp: (as_of.timestamp() - 5 * 86_400).to_string(),
            }],
            trades: vec![
                Trade { market: "m1".into(), price: 0.5, size: 10.0, created_at: (as_of.timestamp() - 2 * 86_400).to_string(), ..Default::default() },
                Trade { market: "m2".into(), price: 0.4, size: 20.0, created_at: (as_of.timestamp() - 20 * 86_400).to_string(), ..Default::default() },
                Trade { market: "m3".into(), price: 0.3, size: 30.0, created_at: (as_of.timestamp() - 60 * 86_400).to_string(), ..Default::default() },
            ],
            activity: vec![Activity { activity_type: "TRADE".into(), ..Default::default() }],
            portfolio_value: PortfolioValue { value: 6.0, ..Default::default() },
            markets_traded: TotalMarketsTraded { markets_traded: 3, ..Default::default() },
        };

        let dossier = build_wallet_dossier(input).unwrap();
        assert_eq!(dossier.open_realized_pnl, "2");
        assert_eq!(dossier.closed_realized_pnl, "4");
        assert_eq!(dossier.unrealized_pnl, "1.5");
        assert_eq!(dossier.gross_exposure, "6");
        assert_eq!(dossier.trade_notional_7d, "5");
        assert_eq!(dossier.trade_notional_30d, "13");
        assert_eq!(dossier.trade_notional_90d, "22");
        assert_eq!(dossier.unique_markets, 3);
        assert_eq!(dossier.activity_types["TRADE"], 1);
        assert!(dossier.language.contains("not a prediction"));
    }

    #[test]
    fn dossier_rejects_blank_wallet_and_invalid_limit() {
        let mut input = WalletDossierInput::default();
        assert!(build_wallet_dossier(input.clone()).is_err());
        input.wallet = "0xwallet".into();
        input.limit = 501;
        assert!(build_wallet_dossier(input).is_err());
    }
}
```

- [ ] **Step 2: Run tests and verify RED**

```bash
cargo test --all-features wallet_dossier::tests
```

Expected: compile failure because dossier types/functions do not exist.

- [ ] **Step 3: Implement the pure dossier contract**

Define:

```rust
use std::{collections::{BTreeMap, BTreeSet}, str::FromStr};
use chrono::{DateTime, TimeZone, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use crate::{data_types::{Activity, ClosedPosition, PortfolioValue, Position, TotalMarketsTraded, Trade}, Error, Result};

const DOSSIER_LANGUAGE: &str =
    "descriptive public-account context; not a prediction, misconduct finding, or trading recommendation";

#[derive(Clone, Debug, PartialEq)]
pub struct WalletDossierParams {
    pub user: String,
    pub limit: u32,
}

impl WalletDossierParams {
    pub fn new(user: impl Into<String>, limit: u32) -> Self {
        Self { user: user.into(), limit }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct WalletDossierInput {
    pub wallet: String,
    pub as_of: DateTime<Utc>,
    pub limit: u32,
    pub positions: Vec<Position>,
    pub closed_positions: Vec<ClosedPosition>,
    pub trades: Vec<Trade>,
    pub activity: Vec<Activity>,
    pub portfolio_value: PortfolioValue,
    pub markets_traded: TotalMarketsTraded,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct WalletDossier {
    pub wallet: String,
    pub as_of: DateTime<Utc>,
    pub source: String,
    pub source_rows: usize,
    pub possibly_truncated: bool,
    pub portfolio_value: String,
    pub open_realized_pnl: String,
    pub closed_realized_pnl: String,
    pub unrealized_pnl: String,
    pub gross_exposure: String,
    pub largest_position_share: String,
    pub trade_notional_7d: String,
    pub trade_notional_30d: String,
    pub trade_notional_90d: String,
    pub unique_markets: usize,
    pub markets_traded: i64,
    pub activity_types: BTreeMap<String, usize>,
    pub language: String,
}
```

Add these complete helpers and pure builder:

```rust
fn decimal(value: f64) -> Decimal {
    Decimal::from_str(&value.to_string()).unwrap_or(Decimal::ZERO)
}

fn parse_observed_time(raw: &str) -> Option<DateTime<Utc>> {
    let text = raw.trim();
    if let Ok(value) = text.parse::<i64>() {
        return if value.abs() >= 10_000_000_000 {
            Utc.timestamp_millis_opt(value).single()
        } else {
            Utc.timestamp_opt(value, 0).single()
        };
    }
    DateTime::parse_from_rfc3339(text)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

fn fmt(value: Decimal) -> String {
    value.normalize().to_string()
}

pub fn build_wallet_dossier(input: WalletDossierInput) -> Result<WalletDossier> {
    if input.wallet.trim().is_empty() {
        return Err(Error::Invalid("wallet dossier user is required".into()));
    }
    if !(1..=500).contains(&input.limit) {
        return Err(Error::Invalid("wallet dossier limit must be 1..=500".into()));
    }

    let limit = input.limit as usize;
    let possibly_truncated = [
        input.positions.len(),
        input.closed_positions.len(),
        input.trades.len(),
        input.activity.len(),
    ]
    .into_iter()
    .any(|rows| rows == limit);

    let mut open_realized = Decimal::ZERO;
    let mut closed_realized = Decimal::ZERO;
    let mut unrealized = Decimal::ZERO;
    let mut gross_exposure = Decimal::ZERO;
    let mut largest_exposure = Decimal::ZERO;
    let mut markets = BTreeSet::new();

    for position in &input.positions {
        open_realized += decimal(position.realized_pnl);
        unrealized += decimal(position.unrealized_pnl);
        let exposure = (decimal(position.size) * decimal(position.current_price)).abs();
        gross_exposure += exposure;
        largest_exposure = largest_exposure.max(exposure);
        if !position.market_id.trim().is_empty() {
            markets.insert(position.market_id.clone());
        }
    }
    for position in &input.closed_positions {
        closed_realized += decimal(position.position.realized_pnl);
        if !position.position.market_id.trim().is_empty() {
            markets.insert(position.position.market_id.clone());
        }
    }

    let mut notional_7d = Decimal::ZERO;
    let mut notional_30d = Decimal::ZERO;
    let mut notional_90d = Decimal::ZERO;
    for trade in &input.trades {
        if !trade.market.trim().is_empty() {
            markets.insert(trade.market.clone());
        }
        let Some(observed_at) = parse_observed_time(&trade.created_at) else {
            continue;
        };
        let age = input.as_of.signed_duration_since(observed_at);
        if age < chrono::Duration::zero() {
            continue;
        }
        let notional = (decimal(trade.price) * decimal(trade.size)).abs();
        if age <= chrono::Duration::days(90) {
            notional_90d += notional;
        }
        if age <= chrono::Duration::days(30) {
            notional_30d += notional;
        }
        if age <= chrono::Duration::days(7) {
            notional_7d += notional;
        }
    }

    let mut activity_types = BTreeMap::new();
    for row in &input.activity {
        let key = row.activity_type.trim().to_ascii_uppercase();
        if !key.is_empty() {
            *activity_types.entry(key).or_insert(0) += 1;
        }
    }

    let source_rows = input.positions.len()
        + input.closed_positions.len()
        + input.trades.len()
        + input.activity.len();
    let largest_position_share = if gross_exposure == Decimal::ZERO {
        Decimal::ZERO
    } else {
        largest_exposure / gross_exposure
    };

    Ok(WalletDossier {
        wallet: input.wallet,
        as_of: input.as_of,
        source: "polymarket-data-api".into(),
        source_rows,
        possibly_truncated,
        portfolio_value: fmt(decimal(input.portfolio_value.value)),
        open_realized_pnl: fmt(open_realized),
        closed_realized_pnl: fmt(closed_realized),
        unrealized_pnl: fmt(unrealized),
        gross_exposure: fmt(gross_exposure),
        largest_position_share: fmt(largest_position_share),
        trade_notional_7d: fmt(notional_7d),
        trade_notional_30d: fmt(notional_30d),
        trade_notional_90d: fmt(notional_90d),
        unique_markets: markets.len(),
        markets_traded: input
            .markets_traded
            .markets_traded
            .max(input.markets_traded.traded),
        activity_types,
        language: DOSSIER_LANGUAGE.into(),
    })
}
```

- [ ] **Step 4: Add the bounded unified-client orchestration test**

Add this multi-response local server helper to `tests/client.rs`:

```rust
fn serve_by_path(
    responses: Vec<(&'static str, &'static str)>,
) -> (String, mpsc::Receiver<String>, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let count = responses.len();
    let (requests, received) = mpsc::channel();
    let handle = thread::spawn(move || {
        for _ in 0..count {
            let (mut stream, _) = listener.accept().unwrap();
            let mut raw = [0; 8192];
            let length = stream.read(&mut raw).unwrap();
            let request = String::from_utf8_lossy(&raw[..length]).into_owned();
            let target = request
                .lines()
                .next()
                .and_then(|line| line.split_whitespace().nth(1))
                .unwrap_or("");
            let body = responses
                .iter()
                .find(|(prefix, _)| target.starts_with(prefix))
                .map(|(_, body)| *body)
                .unwrap_or("{}");
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).unwrap();
            requests.send(request).unwrap();
        }
    });
    (format!("http://{address}"), received, handle)
}

#[tokio::test]
async fn client_builds_wallet_dossier_from_public_data_only() {
    let responses = vec![
        ("/positions", r#"[{"size":10,"curPrice":0.6,"unrealizedPnl":1.5}]"#),
        ("/closed-positions", r#"[{"realizedPnl":4,"timestamp":"1800000000"}]"#),
        ("/trades", r#"[{"market":"m1","price":0.5,"size":10,"timestamp":"1800000000"}]"#),
        ("/activity", r#"[{"type":"TRADE"}]"#),
        ("/value", r#"{"value":6}"#),
        ("/traded", r#"{"traded":3}"#),
    ];
    let (data_base_url, requests, server) = serve_by_path(responses);
    let client = Client::new(ClientConfig { data_base_url, ..Default::default() }).unwrap();
    let dossier = client
        .wallet_dossier(&polyrover::wallet_dossier::WalletDossierParams::new("0xwallet", 100))
        .await
        .unwrap();
    assert_eq!(dossier.wallet, "0xwallet");
    assert_eq!(requests.iter().take(6).count(), 6);
    server.join().unwrap();
}
```

- [ ] **Step 5: Run the orchestration test and verify RED**

```bash
cargo test --all-features client_builds_wallet_dossier_from_public_data_only
```

Expected: compile failure because the module and client method are missing.

- [ ] **Step 6: Export and orchestrate the dossier**

Add to `src/lib.rs`:

```rust
#[cfg(feature = "public")]
#[path = "research/wallet_dossier.rs"]
pub mod wallet_dossier;
```

Add to `Client`:

```rust
pub async fn wallet_dossier(
    &self,
    params: &crate::wallet_dossier::WalletDossierParams,
) -> Result<crate::wallet_dossier::WalletDossier> {
    if params.user.trim().is_empty() || !(1..=500).contains(&params.limit) {
        return Err(crate::Error::Invalid("wallet dossier requires user and limit 1..=500".into()));
    }
    let user = params.user.as_str();
    let limit = params.limit;
    let (positions, closed_positions, trades, activity, portfolio_value, markets_traded) =
        tokio::try_join!(
            self.data.current_positions(user, limit),
            self.data.closed_positions(user, limit),
            self.data.trades(user, limit),
            self.data.activity(user, limit),
            self.data.total_value(user),
            self.data.markets_traded(user),
        )?;
    crate::wallet_dossier::build_wallet_dossier(
        crate::wallet_dossier::WalletDossierInput {
            wallet: params.user.clone(),
            as_of: chrono::Utc::now(),
            limit,
            positions,
            closed_positions,
            trades,
            activity,
            portfolio_value,
            markets_traded,
        },
    )
}
```

- [ ] **Step 7: Run GREEN, regressions, and capability checks**

```bash
cargo test --all-features wallet_dossier
cargo test --all-features --test client
cargo test --all-features
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
```

Mark `intel.wallet.dossier` implemented with `wallet_dossier::build_wallet_dossier`, `Client::wallet_dossier`, module/unit tests, and `tests/client.rs`. Leave `intel.wallet.alerts` planned.

```bash
cargo test --all-features --test feature_contract
jq empty capabilities.json
git diff --check
```

- [ ] **Step 8: Commit Task 5**

```bash
git add src/research/wallet_dossier.rs src/lib.rs src/client.rs capabilities.json \
  src/capabilities/capabilities.rs tests/client.rs
git commit -m "feat: add transparent wallet dossiers"
```

---

### Task 6: Provenance-aware market-flow tracker

**Files:**

- Create: `src/research/market_flow.rs`
- Modify: `src/lib.rs`
- Modify: `capabilities.json`
- Modify: `src/capabilities/capabilities.rs`
- Modify: `README.md`

**Interfaces:**

- Consumes: `market_data::TrackedEvent` and Decimal `Liquidity`/`Depth` from Task 2
- Produces: `MarketFlowConfig`, `MarketFlowTracker`, `MarketFlowSnapshot`
- Maintains a bounded rolling `VecDeque` per asset
- Uses only explicit `LastTradeMessage.side`; source label states it is market-WSS direction, not on-chain reconstruction
- Emits no alerts and performs no network I/O

- [ ] **Step 1: Write failing rolling-window/provenance tests**

Create `src/research/market_flow.rs` with tests first:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn trade(asset: &str, side: &str, price: &str, size: &str, at: i64) -> FlowObservation {
        FlowObservation {
            asset_id: asset.into(),
            market_id: "market-1".into(),
            observed_at_ms: at,
            side: side.into(),
            price: price.into(),
            size: size.into(),
        }
    }

    #[test]
    fn flow_window_prunes_old_trades_and_labels_direction_source() {
        let mut tracker = MarketFlowTracker::new(MarketFlowConfig {
            window_ms: 60_000,
            large_trade_notional: Decimal::new(10, 0),
            max_trades_per_asset: 3,
        })
        .unwrap();
        tracker.observe_trade(trade("token-1", "BUY", "0.5", "10", 1_000)).unwrap();
        tracker.observe_trade(trade("token-1", "SELL", "0.4", "30", 30_000)).unwrap();
        tracker.observe_trade(trade("token-1", "BUY", "0.6", "20", 70_000)).unwrap();

        let row = tracker.snapshot("token-1", 70_000).unwrap();
        assert_eq!(row.trade_count, 2);
        assert_eq!(row.buy_notional, "12");
        assert_eq!(row.sell_notional, "12");
        assert_eq!(row.large_trade_count, 2);
        assert_eq!(row.direction_source, "polymarket-market-wss.last_trade.side");
        assert!(row.language.contains("descriptive"));
    }

    #[test]
    fn flow_window_rejects_unbounded_configuration_and_invalid_decimals() {
        assert!(MarketFlowTracker::new(MarketFlowConfig {
            window_ms: 0,
            large_trade_notional: Decimal::ZERO,
            max_trades_per_asset: 0,
        }).is_err());
        let mut tracker = MarketFlowTracker::new(MarketFlowConfig::default()).unwrap();
        assert!(tracker.observe_trade(trade("token-1", "BUY", "bad", "1", 1)).is_err());
    }
}
```

- [ ] **Step 2: Run tests and verify RED**

```bash
cargo test --all-features market_flow::tests
```

Expected: compile failure because flow types do not exist.

- [ ] **Step 3: Implement bounded pure flow state**

Define:

```rust
use std::{collections::{BTreeMap, VecDeque}, str::FromStr};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use crate::{Error, Result};

#[derive(Clone, Debug, PartialEq)]
pub struct MarketFlowConfig {
    pub window_ms: i64,
    pub large_trade_notional: Decimal,
    pub max_trades_per_asset: usize,
}

impl Default for MarketFlowConfig {
    fn default() -> Self {
        Self {
            window_ms: 15 * 60 * 1_000,
            large_trade_notional: Decimal::new(1_000, 0),
            max_trades_per_asset: 10_000,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct FlowObservation {
    pub asset_id: String,
    pub market_id: String,
    pub observed_at_ms: i64,
    pub side: String,
    pub price: String,
    pub size: String,
}

#[derive(Clone, Debug)]
struct StoredTrade {
    observed_at_ms: i64,
    side: String,
    price: Decimal,
    size: Decimal,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct MarketFlowSnapshot {
    pub asset_id: String,
    pub market_id: String,
    pub observed_at_ms: i64,
    pub window_ms: i64,
    pub source_rows: usize,
    pub trade_count: usize,
    pub large_trade_count: usize,
    pub buy_notional: String,
    pub sell_notional: String,
    pub unknown_notional: String,
    pub first_trade_price: String,
    pub last_trade_price: String,
    pub price_change: String,
    pub spread: String,
    pub midpoint: String,
    pub bid_depth_1c: String,
    pub ask_depth_1c: String,
    pub imbalance: String,
    pub direction_source: String,
    pub book_source: String,
    pub language: String,
}

#[derive(Clone, Debug, Default)]
struct AssetFlow {
    market_id: String,
    trades: VecDeque<StoredTrade>,
    book: Option<crate::market_data::Snapshot>,
}

#[derive(Clone, Debug)]
pub struct MarketFlowTracker {
    config: MarketFlowConfig,
    assets: BTreeMap<String, AssetFlow>,
}
```

Add these exact labels and implementation:

```rust
const DIRECTION_SOURCE: &str = "polymarket-market-wss.last_trade.side";
const BOOK_SOURCE: &str = "polymarket-market-wss.book";
const FLOW_LANGUAGE: &str =
    "descriptive public-flow context; not proof of intent, coordination, misconduct, or trading edge";

fn fmt(value: Decimal) -> String {
    value.normalize().to_string()
}

fn prune(config: &MarketFlowConfig, flow: &mut AssetFlow, observed_at_ms: i64) {
    let cutoff = observed_at_ms.saturating_sub(config.window_ms);
    while flow
        .trades
        .front()
        .is_some_and(|trade| trade.observed_at_ms < cutoff)
    {
        flow.trades.pop_front();
    }
    while flow.trades.len() > config.max_trades_per_asset {
        flow.trades.pop_front();
    }
}

impl MarketFlowTracker {
    pub fn new(config: MarketFlowConfig) -> Result<Self> {
        if config.window_ms <= 0 {
            return Err(Error::Invalid("market flow window_ms must be positive".into()));
        }
        if config.large_trade_notional <= Decimal::ZERO {
            return Err(Error::Invalid(
                "market flow large_trade_notional must be positive".into(),
            ));
        }
        if config.max_trades_per_asset == 0 {
            return Err(Error::Invalid(
                "market flow max_trades_per_asset must be positive".into(),
            ));
        }
        Ok(Self {
            config,
            assets: BTreeMap::new(),
        })
    }

    pub fn observe_trade(&mut self, row: FlowObservation) -> Result<()> {
        if row.asset_id.trim().is_empty() {
            return Err(Error::Invalid("market flow asset_id is required".into()));
        }
        let price = Decimal::from_str(row.price.trim())
            .map_err(|_| Error::Invalid("market flow price must be decimal".into()))?;
        let size = Decimal::from_str(row.size.trim())
            .map_err(|_| Error::Invalid("market flow size must be decimal".into()))?;
        if price <= Decimal::ZERO || size <= Decimal::ZERO {
            return Err(Error::Invalid(
                "market flow price and size must be positive".into(),
            ));
        }
        let flow = self.assets.entry(row.asset_id).or_default();
        if !row.market_id.trim().is_empty() {
            flow.market_id = row.market_id;
        }
        flow.trades.push_back(StoredTrade {
            observed_at_ms: row.observed_at_ms,
            side: row.side.trim().to_ascii_uppercase(),
            price,
            size,
        });
        prune(&self.config, flow, row.observed_at_ms);
        Ok(())
    }

    pub fn observe_snapshot(&mut self, snapshot: crate::market_data::Snapshot) {
        if snapshot.asset_id.trim().is_empty() {
            return;
        }
        let flow = self.assets.entry(snapshot.asset_id.clone()).or_default();
        if !snapshot.market.trim().is_empty() {
            flow.market_id.clone_from(&snapshot.market);
        }
        flow.book = Some(snapshot);
    }

    pub fn snapshot(
        &mut self,
        asset_id: &str,
        observed_at_ms: i64,
    ) -> Option<MarketFlowSnapshot> {
        let flow = self.assets.get_mut(asset_id)?;
        prune(&self.config, flow, observed_at_ms);

        let mut buy = Decimal::ZERO;
        let mut sell = Decimal::ZERO;
        let mut unknown = Decimal::ZERO;
        let mut large_trade_count = 0;
        for trade in &flow.trades {
            let notional = trade.price * trade.size;
            if notional >= self.config.large_trade_notional {
                large_trade_count += 1;
            }
            match trade.side.as_str() {
                "BUY" => buy += notional,
                "SELL" => sell += notional,
                _ => unknown += notional,
            }
        }
        let first = flow.trades.front().map(|trade| trade.price);
        let last = flow.trades.back().map(|trade| trade.price);
        let price_change = first
            .zip(last)
            .map(|(first, last)| last - first)
            .unwrap_or(Decimal::ZERO);

        let (spread, midpoint, bid_depth_1c, ask_depth_1c, imbalance, book_source) =
            if let Some(book) = &flow.book {
                let depth = book.depth();
                let liquidity = book.liquidity();
                (
                    book.spread.clone(),
                    book.midpoint.clone(),
                    fmt(depth.bid_depth_1c),
                    fmt(depth.ask_depth_1c),
                    fmt(liquidity.imbalance),
                    BOOK_SOURCE.into(),
                )
            } else {
                (String::new(), String::new(), String::new(), String::new(), String::new(), String::new())
            };

        Some(MarketFlowSnapshot {
            asset_id: asset_id.into(),
            market_id: flow.market_id.clone(),
            observed_at_ms,
            window_ms: self.config.window_ms,
            source_rows: flow.trades.len() + usize::from(flow.book.is_some()),
            trade_count: flow.trades.len(),
            large_trade_count,
            buy_notional: fmt(buy),
            sell_notional: fmt(sell),
            unknown_notional: fmt(unknown),
            first_trade_price: first.map(fmt).unwrap_or_default(),
            last_trade_price: last.map(fmt).unwrap_or_default(),
            price_change: fmt(price_change),
            spread,
            midpoint,
            bid_depth_1c,
            ask_depth_1c,
            imbalance,
            direction_source: DIRECTION_SOURCE.into(),
            book_source,
            language: FLOW_LANGUAGE.into(),
        })
    }
}
```

- [ ] **Step 4: Add `TrackedEvent` adapter and test it**

Add:

```rust
pub fn observe_tracked(
    &mut self,
    tracked: &crate::market_data::TrackedEvent,
    observed_at_ms: i64,
) -> Result<()> {
    for snapshot in &tracked.snapshots {
        self.observe_snapshot(snapshot.clone());
    }
    if let crate::market_data::MarketUpdate::LastTrade(row) = &tracked.update {
        self.observe_trade(FlowObservation {
            asset_id: row.asset_id.clone(),
            market_id: row.market.clone(),
            observed_at_ms,
            side: row.side.clone(),
            price: row.price.clone(),
            size: row.size.clone(),
        })?;
    }
    Ok(())
}
```

Add this unit test; do not infer side from price movement or book position:

```rust
#[test]
fn tracked_last_trade_uses_explicit_wss_side() {
    use crate::{
        market_data::{MarketUpdate, TrackedEvent, TradeUpdate},
        stream::MarketEvent,
    };

    let mut tracker = MarketFlowTracker::new(MarketFlowConfig::default()).unwrap();
    let tracked = TrackedEvent {
        event: MarketEvent::Ignored,
        update: MarketUpdate::LastTrade(TradeUpdate {
            market: "market-1".into(),
            asset_id: "token-1".into(),
            price: "0.5".into(),
            side: "SELL".into(),
            size: "20".into(),
            timestamp: "1700000000".into(),
            transaction_hash: "0xhash".into(),
        }),
        snapshots: vec![],
    };
    tracker.observe_tracked(&tracked, 1_700_000_000_000).unwrap();
    let row = tracker.snapshot("token-1", 1_700_000_000_000).unwrap();
    assert_eq!(row.trade_count, 1);
    assert_eq!(row.sell_notional, "10");
    assert_eq!(row.direction_source, DIRECTION_SOURCE);
}
```

- [ ] **Step 5: Run GREEN and regressions**

```bash
cargo test --all-features market_flow
cargo test --all-features market_data
cargo test --all-features stream_client
cargo test --all-features
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
```

- [ ] **Step 6: Export and record capability evidence**

Add to `src/lib.rs`:

```rust
#[cfg(feature = "public")]
#[path = "research/market_flow.rs"]
pub mod market_flow;
```

Change `intel.marketFlow.read` to implemented, transport `wss`, API `market_flow::MarketFlowTracker`, tests `src/research/market_flow.rs`, and notes: `Pure bounded summary over typed public market WSS events; no alerting, persistence, intent inference, or execution.` Keep `intel.wallet.alerts` and `opportunities.scan` planned.

Add one compact README example under a collapsed `Market-flow context` section and repeat the cautious language exactly.

```bash
cargo test --all-features --test feature_contract
python3 /home/xel/.agents/skills/beautify-github-readme/scripts/audit_readme.py README.md
jq empty capabilities.json
git diff --check
```

- [ ] **Step 7: Commit Task 6**

```bash
git add src/research/market_flow.rs src/lib.rs capabilities.json \
  src/capabilities/capabilities.rs README.md
git commit -m "feat: add provenance-aware market flow"
```

---

### Task 7: `0.2.0` contract, documentation, and release readiness

**Files:**

- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Modify: `CHANGELOG.md`
- Modify: `README.md`
- Modify: `docs/endpoint-capability-matrix.md`
- Modify: `capabilities.json`
- Modify: `tests/feature_contract.rs`

**Interfaces:**

- Sets package version to `0.2.0`
- Documents the Decimal public-type change and all selected implemented operations
- Produces a verified package only; publishing/tagging/releasing remain separate owner-authorized actions

- [ ] **Step 1: Write the release-contract test before changing the version**

Add to `tests/feature_contract.rs`:

```rust
#[test]
fn public_research_context_capabilities_are_implemented_for_v020() {
    use polyrover::capabilities::{CapabilityCatalog, CapabilityStatus};

    for id in [
        "clob.prices.batchRead",
        "clob.midpoints.batchRead",
        "clob.spreads.batchRead",
        "clob.lastTrades.batchRead",
        "tags.list",
        "series.list",
        "sports.list",
        "sports.marketTypes.list",
        "sports.teams.list",
        "intel.wallet.dossier",
        "intel.marketFlow.read",
    ] {
        assert_eq!(
            CapabilityCatalog::by_id(id).unwrap().status,
            CapabilityStatus::Implemented,
            "{id}"
        );
    }
    assert_eq!(env!("CARGO_PKG_VERSION"), "0.2.0");
}
```

- [ ] **Step 2: Run test and verify RED**

```bash
cargo test --all-features public_research_context_capabilities_are_implemented_for_v020
```

Expected: fails because package version remains `0.1.0` or a selected capability is still planned.

- [ ] **Step 3: Update version and changelog**

Change `Cargo.toml` version to `0.2.0` and run:

```bash
cargo check --all-features
```

Replace the current unversioned changelog body with this exact ordering, copying the existing three Added bullets and API replacement table unchanged into the `0.1.0` section:

```markdown
## Unreleased

## 0.2.0 - 2026-07-28

### Added

- Atomic batch CLOB prices, midpoints, spreads, and last-trade reads.
- Gamma screening filters plus tags, series, sports metadata, market types, and teams.
- Transparent public wallet dossiers with explicit source/coverage fields.
- Bounded provenance-aware market-flow summaries over public WSS events.

### Changed

- Streaming liquidity, depth, midpoint, spread, ordering, and zero-size calculations use exact decimal arithmetic.
- `market_data::Liquidity` and `market_data::Depth` numeric fields changed from `f64` to `rust_decimal::Decimal`.

### Safety

- No signing, order submission, cancellation, wallet connection, relayer, bridge transfer, alert delivery, prediction, or execution capability was added.

## 0.1.0 - 2026-07-27

### Added

- Gamma keyset pagination through `market_page`, including opaque `after_cursor`/`next_cursor` handling for complete catalogs beyond the offset limit.
- Paginated and filterable Data API queries for closed positions, trades, activity, and trader leaderboards.
- Complete public wallet, trade, activity, and leaderboard DTO fields needed for reproducible wallet research.

### Changed

| Previous API                    | Replacement                       | Reason                           |
| ------------------------------- | --------------------------------- | -------------------------------- |
| `capabilities::all()`           | `CapabilityCatalog::all()`        | Use the operation-level catalog. |
| `capabilities::read_only_ids()` | Filter `CapabilityCatalog::all()` | Remove the coarse helper.        |
```

- [ ] **Step 4: Synchronize docs and capability inventory**

Update the endpoint matrix with exact endpoint paths, auth `none`, transport, local tests, and batch-limit wording. Update README install/library examples to distinguish crates.io `0.1.0` from current `0.2.0` until publication; after publication, a separate authorized release task may switch examples to `0.2.0`.

Run a script that fails on contradictory statuses:

```bash
python3 - <<'PY'
import json
from pathlib import Path
catalog = json.loads(Path('capabilities.json').read_text())['capabilities']
rows = {row['id']: row for row in catalog}
required = [
    'clob.prices.batchRead', 'clob.midpoints.batchRead',
    'clob.spreads.batchRead', 'clob.lastTrades.batchRead',
    'tags.list', 'series.list', 'sports.list',
    'sports.marketTypes.list', 'sports.teams.list',
    'intel.wallet.dossier', 'intel.marketFlow.read',
]
assert all(rows[item]['status'] == 'implemented' for item in required)
assert rows['opportunities.scan']['status'] == 'planned'
assert rows['intel.wallet.alerts']['status'] == 'planned'
PY
```

- [ ] **Step 5: Run the full release-readiness gate**

```bash
cargo test --all-features
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
RUSTDOCFLAGS='-D warnings' cargo doc --all-features --no-deps
jq empty capabilities.json
jq empty tests/fixtures/provenance.json
python3 /home/xel/.agents/skills/beautify-github-readme/scripts/audit_readme.py README.md
cargo package --allow-dirty
git diff --check
git status --short
```

Expected:

- all deterministic tests pass;
- live public canaries are ignored;
- public-only integration tests compile and pass;
- package verification succeeds;
- package contents contain no `.env`, research output, vendored Polyoxide source, credentials, or generated logs;
- the only worktree changes are the selected roadmap files;
- no release/tag/publication occurs.

- [ ] **Step 6: Commit Task 7**

```bash
git add Cargo.toml Cargo.lock CHANGELOG.md README.md \
  docs/endpoint-capability-matrix.md capabilities.json tests/feature_contract.rs
git commit -m "chore: prepare Polyrover 0.2.0"
```

---

## Explicitly deferred

These are not implementation omissions; they preserve Polyrover's responsibility boundary:

1. AI-generated forecasts, sentiment scores, and OSINT/news ingestion.
2. Wallet alerts, notification delivery, watchlist daemons, and background polling.
3. Cross-venue matching, contract-equivalence claims, and arbitrage execution.
4. “Smart money,” “insider,” misconduct, coordination, or profitability labels.
5. `opportunities.scan` and automated market ranking.
6. Persistence, database schemas, caches, schedulers, downloaders, and resume manifests.
7. On-chain trade-direction reconstruction; future work must keep on-chain, Data API, and WSS sources distinct.
8. CLI parity for every SDK method; add commands only for a demonstrated operator need.

## Final self-review

- **Spec coverage:** Task 1 covers batch market context; Task 2 exact microstructure; Tasks 3–4 discovery/taxonomy; Task 5 wallet dossiers; Task 6 market flow/provenance; Task 7 version/docs/package gates.
- **Dependency order:** Only market flow depends on Decimal streaming analytics. All other tasks can be accepted/rejected independently.
- **Type consistency:** CLOB batch values remain strings; streaming derived metrics are Decimal; wallet/flow serialized outputs use normalized decimal strings.
- **Safety:** Every new network operation is public/read-only. Derived modules are pure or bounded and do not persist, alert, predict, or execute.
- **TDD:** Every behavior task includes explicit RED, RED verification, minimal implementation, GREEN, regression, and commit checkpoints.
- **No placeholders:** All paths, type names, methods, endpoints, test commands, status changes, and release gates are specified.

## Execution handoff

Execute with `executing-plans` in this checkout on `main`, one task at a time, stopping after each commit for review. The unavailable subagent-driven workflow must not be emulated in this harness.
