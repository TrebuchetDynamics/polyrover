# Public Historical Query Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Give Polyrover documented public historical-query parity across Polymarket Prediction Markets CLOB, Gamma, and Data APIs, including bounded CLI access and precise operational documentation.

**Architecture:** Keep HTTP adapters atomic and typed. Existing CLOB price-history and Data history calls remain the low-level primitives; add missing Gamma event pagination and builder volume time-series, then expose bounded one-request CLI commands. Callers own traversal, time-window partitioning, retries across jobs, and persistence.

**Tech Stack:** Rust 2021, Tokio, Reqwest, Serde, Chrono, `rust_decimal`, deterministic local HTTP fixtures, ignored public canaries.

> **Execution correction (2026-07-31):** A final audit against the official Gamma OpenAPI found additional documented market/event filters beyond the initial snippets below. The completed source and tests also preserve those ID, tag, liquidity/volume, status, recurrence, hierarchy, partner, expansion, and locale controls, and include the documented aggregated builder leaderboard omitted from the initial inventory. Live source and `tests/client.rs::gamma_history_params_preserve_all_documented_filters` supersede the narrower draft snippets.

## Global Constraints

- Canonical term: **documented historical-query parity**.
- Scope is the documented Polymarket Prediction Markets CLOB, Gamma, and Data APIs; Polymarket US, Perps, and undocumented web hosts are excluded.
- Public historical calls use `default-features = false, features = ["public"]` and require no credentials.
- Every CLI command performs exactly one bounded upstream request; no automatic cursor traversal, downloader, scheduler, resume manifest, cache, or persistence.
- Preserve upstream cursors byte-for-byte and expose upstream offset/time-window controls.
- Do not claim permanent retention or complete archival availability. In particular, Data API `/trades` defaults to roughly three years; `start=1` extends user-scoped requests to available full history, while market/event-scoped requests retain the upstream floor.
- Undocumented `crypto_price` remains best-effort and outside the parity claim.
- Ordinary tests use local fixtures only. Live public canaries remain `#[ignore]`.
- Every behavior change follows RED → GREEN under `../docs/project/TDD-HARD-RULE.md`.
- Do not commit, push, publish, tag, or release unless separately requested.

## Current coverage and exact gaps

| Historical surface                      | Current SDK                     | Gap addressed here                          | Official source                                                                           |
| --------------------------------------- | ------------------------------- | ------------------------------------------- | ----------------------------------------------------------------------------------------- |
| CLOB `GET /prices-history`              | Implemented                     | CLI command and first-party docs provenance | <https://docs.polymarket.com/api-reference/markets/get-prices-history>                    |
| CLOB `POST /batch-prices-history`       | Implemented, maximum 20 assets  | CLI command                                 | <https://docs.polymarket.com/api-reference/markets/get-batch-prices-history>              |
| Gamma `GET /markets`, `/markets/keyset` | Date/status filters implemented | Bounded CLI filters/page command            | <https://docs.polymarket.com/api-reference/markets/list-markets-keyset-pagination>        |
| Gamma `GET /events`                     | Basic offset query implemented  | Historical filters                          | <https://docs.polymarket.com/api-reference/events/list-events>                            |
| Gamma `GET /events/keyset`              | Missing                         | Typed atomic page call and CLI              | <https://docs.polymarket.com/api-reference/events/list-events-keyset-pagination>          |
| Data `GET /trades`                      | Full parameter DTO implemented  | CLI filters and retention documentation     | <https://docs.polymarket.com/api-reference/core/get-trades-for-a-user-or-markets>         |
| Data `GET /activity`                    | Full parameter DTO implemented  | CLI filters and pagination documentation    | <https://docs.polymarket.com/api-reference/core/get-user-activity>                        |
| Data `GET /closed-positions`            | Implemented                     | CLI filters                                 | <https://docs.polymarket.com/api-reference/core/get-closed-positions-for-a-user>          |
| Data `GET /v1/leaderboard`              | Implemented                     | CLI period/category/order/offset flags      | <https://docs.polymarket.com/api-reference/core/get-trader-leaderboard-rankings>          |
| Data `GET /v1/builders/volume`          | Missing                         | Typed SDK and CLI                           | <https://docs.polymarket.com/api-reference/builders/get-daily-builder-volume-time-series> |

## File structure

| File                                 | Responsibility after this plan                                              |
| ------------------------------------ | --------------------------------------------------------------------------- |
| `src/api/gamma.rs`                   | Atomic offset/keyset historical market and event discovery.                 |
| `src/api/data.rs`                    | Atomic Data API history and builder-volume reads.                           |
| `src/models/types.rs`                | Gamma event keyset page DTO.                                                |
| `src/models/data_types.rs`           | Builder daily volume DTO.                                                   |
| `src/client.rs`                      | Narrow unified-client delegates.                                            |
| `src/main.rs`                        | Bounded public historical CLI dispatch and pure flag-to-parameter builders. |
| `tests/client.rs`                    | Local HTTP request/response contract tests.                                 |
| `tests/cli.rs`                       | Command discovery/help tests.                                               |
| `tests/contracts.rs`                 | Official-example fixture deserialization and provenance.                    |
| `tests/live_public.rs`               | Ignored operator-run public canaries.                                       |
| `tests/fixtures/`                    | Official contract examples only, never captured credentials.                |
| `capabilities.json`                  | Operation and CLI parity evidence.                                          |
| `src/capabilities/capabilities.rs`   | Typed capability statuses.                                                  |
| `docs/endpoint-capability-matrix.md` | Human-readable parity and upstream limits.                                  |
| `README.md`                          | SDK/CLI examples and retention limitations.                                 |

---

### Task 1: Typed Gamma event-history pages

**Files:**

- Modify: `src/models/types.rs`
- Modify: `src/api/gamma.rs`
- Modify: `src/client.rs`
- Test: `tests/client.rs`

**Interfaces:**

- Produces: `types::EventPage { events: Vec<Event>, next_cursor: String }`
- Produces: `gamma::EventKeysetParams`
- Produces: `gamma::Client::event_page(&EventKeysetParams) -> Result<EventPage>`
- Produces: `Client::events(&EventParams)` and `Client::event_page(&EventKeysetParams)`

- [x] **Step 1: Write failing local-server and query-shape tests**

Add to `tests/client.rs`:

```rust
#[tokio::test]
async fn client_pages_closed_gamma_events_with_date_filters() {
    use polyrover::gamma::{EventKeysetParams, EventParams};

    let (gamma_base_url, received, server) =
        serve_json(r#"{"events":[{"id":"event-1","closed":true}],"next_cursor":"opaque=="}"#);
    let client = Client::new(ClientConfig {
        gamma_base_url,
        ..ClientConfig::default()
    })
    .unwrap();

    let page = client
        .event_page(&EventKeysetParams {
            limit: Some(20),
            after_cursor: "cursor==".into(),
            closed: Some(true),
            start_date_min: "2026-01-01T00:00:00Z".into(),
            end_date_max: "2026-12-31T23:59:59Z".into(),
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(page.events[0].id, "event-1");
    assert_eq!(page.next_cursor, "opaque==");
    let request = received.recv().unwrap();
    assert!(request.starts_with("GET /events/keyset?"));
    assert!(request.contains("after_cursor=cursor%3D%3D"));
    assert!(request.contains("closed=true"));
    assert!(request.contains("start_date_min=2026-01-01T00%3A00%3A00Z"));
    assert!(request.contains("end_date_max=2026-12-31T23%3A59%3A59Z"));
    server.join().unwrap();

    let offset = EventParams {
        archived: Some(true),
        start_date_max: "2025-12-31T23:59:59Z".into(),
        ..Default::default()
    };
    let path = offset.path("/events");
    assert!(path.contains("archived=true"));
    assert!(path.contains("start_date_max=2025-12-31T23%3A59%3A59Z"));
}
```

- [x] **Step 2: Run the test and verify RED**

Run:

```bash
cargo test --all-features client_pages_closed_gamma_events_with_date_filters
```

Expected: compilation fails because `EventPage`, `EventKeysetParams`, unified `events`, and `event_page` do not exist.

- [x] **Step 3: Add the event page DTO and parameter types**

Add to `src/models/types.rs` beside `MarketPage`:

```rust
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct EventPage {
    #[serde(default)]
    pub events: Vec<Event>,
    #[serde(default)]
    pub next_cursor: String,
}
```

Extend `EventParams` in `src/api/gamma.rs`:

```rust
pub ids: Vec<i64>,
pub archived: Option<bool>,
pub start_date_min: String,
pub start_date_max: String,
pub end_date_min: String,
pub end_date_max: String,
```

Add:

```rust
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EventKeysetParams {
    /// Official maximum: 500.
    pub limit: Option<u32>,
    /// Opaque value copied unchanged from the prior `next_cursor`.
    pub after_cursor: String,
    pub order: Option<String>,
    pub ascending: Option<bool>,
    pub ids: Vec<i64>,
    pub slug: Vec<String>,
    pub closed: Option<bool>,
    pub live: Option<bool>,
    pub start_date_min: String,
    pub start_date_max: String,
    pub end_date_min: String,
    pub end_date_max: String,
    pub start_time_min: String,
    pub start_time_max: String,
}
```

- [x] **Step 4: Implement exact path builders and client methods**

Add the new fields to `EventParams::path` using `q.list`, `q.opt`, and `q.opt_str`, then add:

```rust
impl EventKeysetParams {
    pub fn path(&self, base: &str) -> String {
        let mut q = Query::new(base);
        q.opt("limit", self.limit);
        q.pair("after_cursor", &self.after_cursor);
        q.opt_str("order", self.order.as_deref());
        q.opt("ascending", self.ascending);
        for id in &self.ids {
            q.pair("id", &id.to_string());
        }
        q.list("slug", &self.slug);
        q.opt("closed", self.closed);
        q.opt("live", self.live);
        q.opt_str("start_date_min", Some(&self.start_date_min));
        q.opt_str("start_date_max", Some(&self.start_date_max));
        q.opt_str("end_date_min", Some(&self.end_date_min));
        q.opt_str("end_date_max", Some(&self.end_date_max));
        q.opt_str("start_time_min", Some(&self.start_time_min));
        q.opt_str("start_time_max", Some(&self.start_time_max));
        q.finish()
    }
}
```

Import `EventPage`, then add to `gamma::Client`:

```rust
pub async fn event_page(&self, params: &EventKeysetParams) -> Result<EventPage> {
    self.transport
        .get_json(&params.path("/events/keyset"))
        .await
}
```

Add to unified `Client`:

```rust
pub async fn events(&self, params: &gamma::EventParams) -> Result<Vec<crate::types::Event>> {
    self.gamma.events(params).await
}

pub async fn event_page(
    &self,
    params: &gamma::EventKeysetParams,
) -> Result<crate::types::EventPage> {
    self.gamma.event_page(params).await
}
```

- [x] **Step 5: Run GREEN and regressions**

```bash
cargo test --all-features client_pages_closed_gamma_events_with_date_filters
cargo test --all-features gamma
cargo test --all-features --test client
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
```

Expected: all pass; no public network request occurs.

- [x] **Step 6: Skip Task 1 commit (not requested)**

No commit was created; the global no-commit constraint takes precedence.

---

### Task 2: Builder daily volume time-series

**Files:**

- Modify: `src/models/data_types.rs`
- Modify: `src/api/data.rs`
- Modify: `src/client.rs`
- Modify: `tests/client.rs`
- Modify: `tests/contracts.rs`
- Create: `tests/fixtures/data/builder-volume.json`
- Modify: `tests/fixtures/provenance.json`

**Interfaces:**

- Produces: `data::BuilderVolumeParams { time_period: String }`
- Produces: `data_types::BuilderVolumeRow`
- Produces: `data::Client::builder_volume` and unified `Client::builder_volume`

- [x] **Step 1: Add the official contract fixture and provenance entry**

Create `tests/fixtures/data/builder-volume.json`:

```json
[
  {
    "dt": "2025-11-15T00:00:00Z",
    "builder": "example",
    "builderCode": "code",
    "builderLogo": "https://example.invalid/logo.png",
    "verified": true,
    "volume": 123.45,
    "activeUsers": 7,
    "rank": "1"
  }
]
```

Append this object to `tests/fixtures/provenance.json`'s existing `fixtures` array:

```json
{
  "path": "data/builder-volume.json",
  "kind": "contract-example",
  "source": "https://docs.polymarket.com/api-reference/builders/get-daily-builder-volume-time-series",
  "retrievedAt": "2026-07-31",
  "endpoint": "GET https://data-api.polymarket.com/v1/builders/volume",
  "containsLiveData": false
}
```

- [x] **Step 2: Write failing DTO and HTTP tests**

In `tests/contracts.rs`, change `assert_eq!(fixtures.len(), 10);` to `assert_eq!(fixtures.len(), 11);`, then add:

```rust
#[test]
fn builder_volume_contract_example_preserves_decimal_text() {
    let rows: Vec<polyrover::data_types::BuilderVolumeRow> = serde_json::from_str(
        include_str!("fixtures/data/builder-volume.json"),
    )
    .unwrap();
    assert_eq!(rows[0].dt, "2025-11-15T00:00:00Z");
    assert_eq!(rows[0].volume, "123.45");
    assert_eq!(rows[0].active_users, 7);
}
```

Add to `tests/client.rs`:

```rust
#[tokio::test]
async fn client_reads_builder_volume_time_series() {
    let (data_base_url, received, server) = serve_json(
        r#"[{"dt":"2025-11-15T00:00:00Z","builder":"example","volume":123.45,"activeUsers":7}]"#,
    );
    let client = Client::new(ClientConfig {
        data_base_url,
        ..ClientConfig::default()
    })
    .unwrap();

    let rows = client
        .builder_volume(&polyrover::data::BuilderVolumeParams {
            time_period: "ALL".into(),
        })
        .await
        .unwrap();

    assert_eq!(rows[0].volume, "123.45");
    assert!(received
        .recv()
        .unwrap()
        .starts_with("GET /v1/builders/volume?timePeriod=ALL "));
    server.join().unwrap();
}
```

- [x] **Step 3: Run tests and verify RED**

```bash
cargo test --all-features builder_volume_contract_example_preserves_decimal_text
cargo test --all-features client_reads_builder_volume_time_series
```

Expected: compilation fails because the types and methods do not exist.

- [x] **Step 4: Implement the DTO and atomic read**

Add to `src/models/data_types.rs`:

```rust
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct BuilderVolumeRow {
    #[serde(default)]
    pub dt: String,
    #[serde(default)]
    pub builder: String,
    #[serde(default, rename = "builderCode")]
    pub builder_code: String,
    #[serde(default, rename = "builderLogo")]
    pub builder_logo: String,
    #[serde(default, deserialize_with = "bool_or_false")]
    pub verified: bool,
    #[serde(default, deserialize_with = "string_or_number")]
    pub volume: String,
    #[serde(default, rename = "activeUsers", deserialize_with = "int_or_zero")]
    pub active_users: i64,
    #[serde(default, deserialize_with = "string_or_number")]
    pub rank: String,
}
```

Add to `src/api/data.rs`:

```rust
#[derive(Clone, Debug, Default, PartialEq)]
pub struct BuilderVolumeParams {
    /// `DAY`, `WEEK`, `MONTH`, or `ALL`; upstream defaults to `DAY` when empty.
    pub time_period: String,
}

impl BuilderVolumeParams {
    fn path(&self) -> String {
        path(
            "/v1/builders/volume",
            &[pair("timePeriod", &self.time_period)],
        )
    }
}
```

Import `BuilderVolumeRow` and add:

```rust
pub async fn builder_volume(
    &self,
    params: &BuilderVolumeParams,
) -> Result<Vec<BuilderVolumeRow>> {
    self.transport.get_json(&params.path()).await
}
```

Import `BuilderVolumeRow` in `src/client.rs` and add the unified delegate:

```rust
pub async fn builder_volume(
    &self,
    params: &data::BuilderVolumeParams,
) -> Result<Vec<BuilderVolumeRow>> {
    self.data.builder_volume(params).await
}
```

- [x] **Step 5: Run GREEN, provenance, and regressions**

```bash
cargo test --all-features builder_volume
cargo test --all-features --test contracts
cargo test --all-features --test client
jq empty tests/fixtures/provenance.json
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
```

- [x] **Step 6: Skip Task 2 commit (not requested)**

No commit was created; the global no-commit constraint takes precedence.

---

### Task 3: Bounded CLOB history CLI

**Files:**

- Modify: `src/main.rs`
- Modify: `tests/cli.rs`

**Interfaces:**

- Produces CLI: `clob price-history`
- Produces CLI: `clob batch-price-history`
- Consumes existing `PriceHistoryParams` and `BatchPriceHistoryParams`

- [x] **Step 1: Write failing pure parser and help tests**

Add under `#[cfg(test)]` in `src/main.rs`:

```rust
#[test]
fn clob_history_flags_build_one_atomic_request() {
    let args = vec![
        "--token-id".into(), "token-1".into(),
        "--start-ts".into(), "1".into(),
        "--end-ts".into(), "100".into(),
        "--interval".into(), "1d".into(),
        "--fidelity".into(), "5".into(),
    ];
    let params = clob_history_params(&args);
    assert_eq!(params.token_id, "token-1");
    assert_eq!(params.start_ts, Some(1));
    assert_eq!(params.end_ts, Some(100));
    assert_eq!(params.interval.as_deref(), Some("1d"));
    assert_eq!(params.fidelity, Some(5));

    let batch = vec![
        "--token-id".into(), "token-1".into(),
        "--token-id".into(), "token-2".into(),
    ];
    assert_eq!(batch_history_params(&batch).markets.len(), 2);
}
```

Add `clob price-history` and `clob batch-price-history` to `every_command_has_detailed_help` in `tests/cli.rs`.

- [x] **Step 2: Run tests and verify RED**

```bash
cargo test --all-features clob_history_flags_build_one_atomic_request
cargo test --all-features --test cli every_command_has_detailed_help
```

Expected: parser functions and help targets are missing.

- [x] **Step 3: Implement parameter builders and command handlers**

Import `polyrover::clob::{BatchPriceHistoryParams, PriceHistoryParams}` and add:

```rust
fn clob_history_params(args: &[String]) -> PriceHistoryParams {
    PriceHistoryParams {
        token_id: flag(args, "--token-id").unwrap_or_default(),
        start_ts: flag(args, "--start-ts").and_then(|v| v.parse().ok()),
        end_ts: flag(args, "--end-ts").and_then(|v| v.parse().ok()),
        interval: flag(args, "--interval"),
        fidelity: flag(args, "--fidelity").and_then(|v| v.parse().ok()),
    }
}

fn batch_history_params(args: &[String]) -> BatchPriceHistoryParams {
    BatchPriceHistoryParams {
        markets: flag_values(args, "--token-id"),
        start_ts: flag(args, "--start-ts").and_then(|v| v.parse().ok()),
        end_ts: flag(args, "--end-ts").and_then(|v| v.parse().ok()),
        interval: flag(args, "--interval"),
        fidelity: flag(args, "--fidelity").and_then(|v| v.parse().ok()),
    }
}

async fn clob_price_history(client: &Client, args: &[String]) -> Result<()> {
    print_success(
        "clob price-history",
        client.price_history(&clob_history_params(args)).await?,
    )
}

async fn clob_batch_price_history(client: &Client, args: &[String]) -> Result<()> {
    print_success(
        "clob batch-price-history",
        client.batch_price_history(&batch_history_params(args)).await?,
    )
}
```

Add exact dispatch arms for both commands. Do not loop over pages or token chunks.

- [x] **Step 4: Add complete command help**

Add both commands to top-level/group help and add these usage contracts to `print_command_help`:

```text
clob price-history --token-id <id> [--start-ts <unix>] [--end-ts <unix>] [--interval max|all|1m|1w|1d|6h|1h] [--fidelity <minutes>] [--json]
clob batch-price-history --token-id <id>... [--start-ts <unix>] [--end-ts <unix>] [--interval max|all|1m|1w|1d|6h|1h] [--fidelity <minutes>] [--json]
```

State in each help description that one request is made and batch accepts at most 20 asset IDs.

- [x] **Step 5: Run GREEN and CLI regressions**

```bash
cargo test --all-features clob_history_flags_build_one_atomic_request
cargo test --all-features --test cli
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
```

- [x] **Step 6: Skip Task 3 commit (not requested)**

No commit was created; the global no-commit constraint takes precedence.

---

### Task 4: Bounded Gamma and Data history CLI

**Files:**

- Modify: `src/main.rs`
- Modify: `tests/cli.rs`

**Interfaces:**

- Produces CLI: `gamma market-page`, `gamma events`, `gamma event-page`
- Produces CLI: `analytics closed-positions`, `analytics activity`, `analytics builder-volume`
- Extends CLI: `gamma markets`, `analytics trades`, `analytics leaderboard`

- [x] **Step 1: Write failing parser tests**

Add to `src/main.rs` tests:

```rust
#[test]
fn historical_cli_builders_preserve_windows_offsets_and_cursors() {
    let args = vec![
        "--user".into(), "0xabc".into(),
        "--start".into(), "1".into(),
        "--end".into(), "100".into(),
        "--offset".into(), "200".into(),
    ];
    let trades = trade_params(&args);
    assert_eq!(trades.user, "0xabc");
    assert_eq!(trades.start, Some(1));
    assert_eq!(trades.end, Some(100));
    assert_eq!(trades.offset, Some(200));

    let event_args = vec![
        "--after-cursor".into(), "opaque==".into(),
        "--closed".into(), "true".into(),
        "--start-date-min".into(), "2026-01-01T00:00:00Z".into(),
    ];
    let events = event_keyset_params(&event_args);
    assert_eq!(events.after_cursor, "opaque==");
    assert_eq!(events.closed, Some(true));
    assert_eq!(events.start_date_min, "2026-01-01T00:00:00Z");
}
```

Add all six new command paths to `tests/cli.rs::every_command_has_detailed_help`.

- [x] **Step 2: Run tests and verify RED**

```bash
cargo test --all-features historical_cli_builders_preserve_windows_offsets_and_cursors
cargo test --all-features --test cli every_command_has_detailed_help
```

- [x] **Step 3: Add exact parameter builders**

Add these helpers, using the existing `flag` and `flag_values` functions:

```rust
fn bool_flag(args: &[String], name: &str) -> Option<bool> {
    flag(args, name).and_then(|value| value.parse().ok())
}

fn trade_params(args: &[String]) -> polyrover::data::TradeParams {
    polyrover::data::TradeParams {
        user: flag(args, "--user").unwrap_or_default(),
        markets: flag_values(args, "--market"),
        event_ids: flag_values(args, "--event-id")
            .into_iter().filter_map(|v| v.parse().ok()).collect(),
        side: flag(args, "--side").unwrap_or_default(),
        start: flag(args, "--start").and_then(|v| v.parse().ok()),
        end: flag(args, "--end").and_then(|v| v.parse().ok()),
        taker_only: bool_flag(args, "--taker-only"),
        filter_type: flag(args, "--filter-type").unwrap_or_default(),
        filter_amount: flag(args, "--filter-amount").unwrap_or_default(),
        limit: flag(args, "--limit").and_then(|v| v.parse().ok()),
        offset: flag(args, "--offset").and_then(|v| v.parse().ok()),
    }
}

fn event_keyset_params(args: &[String]) -> gamma::EventKeysetParams {
    gamma::EventKeysetParams {
        limit: flag(args, "--limit").and_then(|v| v.parse().ok()),
        after_cursor: flag(args, "--after-cursor").unwrap_or_default(),
        closed: bool_flag(args, "--closed"),
        live: bool_flag(args, "--live"),
        start_date_min: flag(args, "--start-date-min").unwrap_or_default(),
        start_date_max: flag(args, "--start-date-max").unwrap_or_default(),
        end_date_min: flag(args, "--end-date-min").unwrap_or_default(),
        end_date_max: flag(args, "--end-date-max").unwrap_or_default(),
        ..Default::default()
    }
}
```

Add these remaining builders:

```rust
fn market_params(args: &[String]) -> gamma::MarketParams {
    let mut params = gamma::MarketParams::default();
    params.limit = flag(args, "--limit").and_then(|v| v.parse().ok());
    params.offset = flag(args, "--offset").and_then(|v| v.parse().ok());
    params.active = bool_flag(args, "--active");
    params.closed = bool_flag(args, "--closed");
    params.start_date_min = flag(args, "--start-date-min").unwrap_or_default();
    params.start_date_max = flag(args, "--start-date-max").unwrap_or_default();
    params.end_date_min = flag(args, "--end-date-min").unwrap_or_default();
    params.end_date_max = flag(args, "--end-date-max").unwrap_or_default();
    params
}

fn market_keyset_params(args: &[String]) -> gamma::MarketKeysetParams {
    let mut params = gamma::MarketKeysetParams::default();
    params.limit = flag(args, "--limit").and_then(|v| v.parse().ok());
    params.after_cursor = flag(args, "--after-cursor").unwrap_or_default();
    params.active = bool_flag(args, "--active");
    params.closed = bool_flag(args, "--closed");
    params.start_date_min = flag(args, "--start-date-min").unwrap_or_default();
    params.start_date_max = flag(args, "--start-date-max").unwrap_or_default();
    params.end_date_min = flag(args, "--end-date-min").unwrap_or_default();
    params.end_date_max = flag(args, "--end-date-max").unwrap_or_default();
    params
}

fn event_params(args: &[String]) -> gamma::EventParams {
    gamma::EventParams {
        limit: flag(args, "--limit").and_then(|v| v.parse().ok()),
        offset: flag(args, "--offset").and_then(|v| v.parse().ok()),
        closed: bool_flag(args, "--closed"),
        archived: bool_flag(args, "--archived"),
        start_date_min: flag(args, "--start-date-min").unwrap_or_default(),
        start_date_max: flag(args, "--start-date-max").unwrap_or_default(),
        end_date_min: flag(args, "--end-date-min").unwrap_or_default(),
        end_date_max: flag(args, "--end-date-max").unwrap_or_default(),
        ..Default::default()
    }
}

fn activity_params(args: &[String]) -> polyrover::data::ActivityParams {
    polyrover::data::ActivityParams {
        user: flag(args, "--user").unwrap_or_default(),
        markets: flag_values(args, "--market"),
        event_ids: flag_values(args, "--event-id")
            .into_iter().filter_map(|v| v.parse().ok()).collect(),
        activity_types: flag_values(args, "--type"),
        side: flag(args, "--side").unwrap_or_default(),
        start: flag(args, "--start").and_then(|v| v.parse().ok()),
        end: flag(args, "--end").and_then(|v| v.parse().ok()),
        sort_by: flag(args, "--sort-by").unwrap_or_default(),
        sort_direction: flag(args, "--sort-direction").unwrap_or_default(),
        limit: flag(args, "--limit").and_then(|v| v.parse().ok()),
        offset: flag(args, "--offset").and_then(|v| v.parse().ok()),
    }
}

fn closed_position_params(args: &[String]) -> polyrover::data::ClosedPositionParams {
    polyrover::data::ClosedPositionParams {
        user: flag(args, "--user").unwrap_or_default(),
        markets: flag_values(args, "--market"),
        title: flag(args, "--title").unwrap_or_default(),
        event_ids: flag_values(args, "--event-id")
            .into_iter().filter_map(|v| v.parse().ok()).collect(),
        limit: flag(args, "--limit").and_then(|v| v.parse().ok()),
        offset: flag(args, "--offset").and_then(|v| v.parse().ok()),
        sort_by: flag(args, "--sort-by").unwrap_or_default(),
        sort_direction: flag(args, "--sort-direction").unwrap_or_default(),
    }
}

fn leaderboard_params(args: &[String]) -> polyrover::data::LeaderboardParams {
    polyrover::data::LeaderboardParams {
        category: flag(args, "--category").unwrap_or_default(),
        time_period: flag(args, "--time-period").unwrap_or_default(),
        order_by: flag(args, "--order-by").unwrap_or_default(),
        limit: flag(args, "--limit").and_then(|v| v.parse().ok()),
        offset: flag(args, "--offset").and_then(|v| v.parse().ok()),
        user: flag(args, "--user").unwrap_or_default(),
        user_name: flag(args, "--user-name").unwrap_or_default(),
    }
}
```

- [x] **Step 4: Add one-request handlers and dispatch**

Replace the reduced existing handlers and add the missing handlers with this complete set:

```rust
async fn gamma_markets(client: &Client, args: &[String]) -> Result<()> {
    print_success("gamma markets", client.markets(&market_params(args)).await?)
}

async fn gamma_market_page(client: &Client, args: &[String]) -> Result<()> {
    print_success("gamma market-page", client.market_page(&market_keyset_params(args)).await?)
}

async fn gamma_events(client: &Client, args: &[String]) -> Result<()> {
    print_success("gamma events", client.events(&event_params(args)).await?)
}

async fn gamma_event_page(client: &Client, args: &[String]) -> Result<()> {
    print_success("gamma event-page", client.event_page(&event_keyset_params(args)).await?)
}

async fn data_trades(client: &Client, args: &[String]) -> Result<()> {
    print_success("analytics trades", client.trades_with(&trade_params(args)).await?)
}

async fn data_closed_positions(client: &Client, args: &[String]) -> Result<()> {
    print_success(
        "analytics closed-positions",
        client.closed_positions_with(&closed_position_params(args)).await?,
    )
}

async fn data_activity(client: &Client, args: &[String]) -> Result<()> {
    print_success("analytics activity", client.activity_with(&activity_params(args)).await?)
}

async fn data_leaderboard(client: &Client, args: &[String]) -> Result<()> {
    print_success(
        "analytics leaderboard",
        client.trader_leaderboard_with(&leaderboard_params(args)).await?,
    )
}

async fn data_builder_volume(client: &Client, args: &[String]) -> Result<()> {
    print_success(
        "analytics builder-volume",
        client.builder_volume(&polyrover::data::BuilderVolumeParams {
            time_period: flag(args, "--time-period").unwrap_or_default(),
        }).await?,
    )
}
```

Add these dispatch arms and delete the prior reduced parsing bodies:

```rust
[group, cmd, rest @ ..] if group == "gamma" && cmd == "markets" =>
    gamma_markets(&client, rest).await,
[group, cmd, rest @ ..] if group == "gamma" && cmd == "market-page" =>
    gamma_market_page(&client, rest).await,
[group, cmd, rest @ ..] if group == "gamma" && cmd == "events" =>
    gamma_events(&client, rest).await,
[group, cmd, rest @ ..] if group == "gamma" && cmd == "event-page" =>
    gamma_event_page(&client, rest).await,
[group, cmd, rest @ ..] if group == "analytics" && cmd == "trades" =>
    data_trades(&client, rest).await,
[group, cmd, rest @ ..] if group == "analytics" && cmd == "closed-positions" =>
    data_closed_positions(&client, rest).await,
[group, cmd, rest @ ..] if group == "analytics" && cmd == "activity" =>
    data_activity(&client, rest).await,
[group, cmd, rest @ ..] if group == "analytics" && cmd == "leaderboard" =>
    data_leaderboard(&client, rest).await,
[group, cmd, rest @ ..] if group == "analytics" && cmd == "builder-volume" =>
    data_builder_volume(&client, rest).await,
```

- [x] **Step 5: Add complete help contracts**

Add these exact usage lines to `print_command_help`, with an example for every command:

```text
gamma markets [--limit <n>] [--offset <n>] [--active <bool>] [--closed <bool>] [--start-date-min <iso>] [--start-date-max <iso>] [--end-date-min <iso>] [--end-date-max <iso>] [--json]
gamma market-page [--limit <n>] [--after-cursor <cursor>] [--active <bool>] [--closed <bool>] [--start-date-min <iso>] [--start-date-max <iso>] [--end-date-min <iso>] [--end-date-max <iso>] [--json]
gamma events [--limit <n>] [--offset <n>] [--closed <bool>] [--archived <bool>] [--start-date-min <iso>] [--start-date-max <iso>] [--end-date-min <iso>] [--end-date-max <iso>] [--json]
gamma event-page [--limit <n>] [--after-cursor <cursor>] [--closed <bool>] [--live <bool>] [--start-date-min <iso>] [--start-date-max <iso>] [--end-date-min <iso>] [--end-date-max <iso>] [--json]
analytics trades [--user <wallet>] [--market <condition>...] [--event-id <id>...] [--side BUY|SELL] [--start <unix>] [--end <unix>] [--taker-only <bool>] [--filter-type CASH|TOKENS] [--filter-amount <n>] [--limit <n>] [--offset <n>] [--json]
analytics closed-positions --user <wallet> [--market <condition>...] [--event-id <id>...] [--title <text>] [--sort-by <field>] [--sort-direction ASC|DESC] [--limit <n>] [--offset <n>] [--json]
analytics activity --user <wallet> [--market <condition>...] [--event-id <id>...] [--type <type>...] [--side BUY|SELL] [--start <unix>] [--end <unix>] [--sort-by <field>] [--sort-direction ASC|DESC] [--limit <n>] [--offset <n>] [--json]
analytics leaderboard [--category <category>] [--time-period DAY|WEEK|MONTH|ALL] [--order-by <field>] [--user <wallet>] [--user-name <name>] [--limit <n>] [--offset <n>] [--json]
analytics builder-volume [--time-period DAY|WEEK|MONTH|ALL] [--json]
```

Include these upstream cautions verbatim:

```text
analytics trades: omit --start (or pass 0) for the default recent ~3-year window; pass --start 1 for available full history only when --user scopes the request. Market/event-scoped requests cannot extend beyond the upstream floor.
analytics activity: offsets above 5000 require caller-managed start/end window partitioning.
gamma market-page/event-page: --after-cursor is opaque; copy next_cursor unchanged.
```

State that every command returns one page/request and never persists data.

- [x] **Step 6: Run GREEN and regressions**

```bash
cargo test --all-features historical_cli_builders_preserve_windows_offsets_and_cursors
cargo test --all-features --test cli
cargo test --all-features --test client
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
```

- [x] **Step 7: Skip Task 4 commit (not requested)**

No commit was created; the global no-commit constraint takes precedence.

---

### Task 5: Capability evidence, canaries, and documentation

**Files:**

- Modify: `capabilities.json`
- Modify: `src/capabilities/capabilities.rs`
- Modify: `tests/feature_contract.rs`
- Modify: `tests/live_public.rs`
- Modify: `docs/endpoint-capability-matrix.md`
- Modify: `README.md`
- Modify: `CHANGELOG.md`

**Interfaces:**

- Defines the package claim `documented historical-query parity`.
- Records explicit exclusions: persistence, automatic traversal, undocumented hosts, authenticated CLI, Perps, and permanent-retention guarantees.

- [x] **Step 1: Write the failing capability contract**

Add to `tests/feature_contract.rs`:

```rust
#[test]
fn public_historical_query_capabilities_are_implemented() {
    for id in [
        "clob.priceHistory.read",
        "events.list",
        "markets.list",
        "data.trades.read",
        "data.activity.read",
        "data.closedPositions.read",
        "data.leaderboard.read",
        "data.builderVolume.read",
    ] {
        assert_eq!(
            CapabilityCatalog::by_id(id).unwrap().status,
            CapabilityStatus::Implemented,
            "{id}"
        );
    }
}
```

- [x] **Step 2: Run and verify RED**

```bash
cargo test --all-features public_historical_query_capabilities_are_implemented
```

Expected: `data.builderVolume.read` is still planned.

- [x] **Step 3: Synchronize capability catalogs**

In `tests/fixtures/provenance.json`, replace the two Polyoxide source URLs for `clob/price-history.json` and `clob/batch-price-history.json` with these official sources, leaving their contract-example and no-live-data labels unchanged:

```text
https://docs.polymarket.com/api-reference/markets/get-prices-history
https://docs.polymarket.com/api-reference/markets/get-batch-prices-history
```

Set `data.builderVolume.read` to `implemented` with:

```json
"api": ["data::Client::builder_volume", "Client::builder_volume"],
"tests": ["tests/client.rs", "tests/contracts.rs"],
"notes": "Documented daily builder volume time-series; one atomic request with DAY, WEEK, MONTH, or ALL period."
```

Update `clob.priceHistory.read` notes to state public SDK and CLI parity are implemented. Update `events.list`, `markets.list`, `data.trades.read`, `data.activity.read`, `data.closedPositions.read`, and `data.leaderboard.read` notes with their exact CLI commands and atomic-page boundary. Make matching status changes in `src/capabilities/capabilities.rs`.

- [x] **Step 4: Add one ignored Gamma event-history canary**

Add to `tests/live_public.rs`:

```rust
#[tokio::test]
#[ignore = "manual public API canary; never run in ordinary CI"]
async fn live_closed_event_page_matches_the_typed_contract() {
    let page = Client::new(ClientConfig::default())
        .unwrap()
        .event_page(&polyrover::gamma::EventKeysetParams {
            limit: Some(1),
            closed: Some(true),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(page.events.len() <= 1);
}
```

- [x] **Step 5: Rewrite the matrix history section from live evidence**

Update `Last verified` to `2026-07-31`. Add a `## Documented historical-query parity` section with one row per inventory entry from this plan. For each row document endpoint, auth, Rust method, CLI command, pagination/window controls, official limit, fixture/test, and official source URL.

Include these exact limitations:

- CLOB batch price history: maximum 20 asset IDs.
- Gamma market keyset limit: 1000; event keyset limit: 500; cursors opaque.
- Data trades: limit and offset maximum 10000; partition deeper history by `start`/`end`; only user-scoped positive `start` can extend beyond the default roughly three-year window.
- Data activity: limit maximum 500 and offset maximum 5000; partition deeper history by `start`/`end`.
- Polyrover neither discovers an upstream retention guarantee nor represents these APIs as a permanent archive.

- [x] **Step 6: Update README and changelog**

Add these bounded SDK examples, each labeled “one request/page” and linked to the matrix:

```rust
let prices = client.price_history(&polyrover::clob::PriceHistoryParams {
    token_id: "TOKEN_ID".into(),
    start_ts: Some(1_700_000_000),
    end_ts: Some(1_700_086_400),
    fidelity: Some(5),
    ..Default::default()
}).await?;

let events = client.event_page(&polyrover::gamma::EventKeysetParams {
    limit: Some(100),
    closed: Some(true),
    after_cursor: previous_next_cursor,
    ..Default::default()
}).await?;

let trades = client.trades_with(&polyrover::data::TradeParams {
    user: "PUBLIC_WALLET".into(),
    start: Some(1),
    limit: Some(100),
    ..Default::default()
}).await?;
```

Add these CLI examples:

```bash
polyrover clob price-history --token-id "$TOKEN_ID" --interval 1d --fidelity 5 --json
polyrover gamma event-page --closed true --limit 100 --after-cursor "$CURSOR" --json
polyrover analytics trades --user "$PUBLIC_WALLET" --start 1 --limit 100 --offset 0 --json
```

Under limitations, add:

```markdown
“Documented historical-query parity” means Polyrover exposes the documented
Prediction Markets history requests and pagination controls. It does not mean
Polymarket guarantees permanent retention, and Polyrover does not crawl,
download, resume, or persist complete archives.
```

Add an `Unreleased` changelog bullet for public history SDK/CLI parity without changing the package version.

- [x] **Step 7: Run the full public verification gate**

```bash
cargo test --no-default-features --features public
cargo test --all-features
cargo test --all-features --test live_public
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
RUSTDOCFLAGS='-D warnings' cargo doc --all-features --no-deps
jq empty capabilities.json
jq empty tests/fixtures/provenance.json
python3 /home/xel/.agents/skills/beautify-github-readme/scripts/audit_readme.py README.md
git diff --check
```

Expected: deterministic suites pass; public canaries compile and remain ignored; no network runs in ordinary tests.

- [x] **Step 8: Skip Task 5 commit (not requested)**

No commit was created; the global no-commit constraint takes precedence.

## Final self-review

- **Spec coverage:** CLOB price history, Gamma market/event historical discovery, Data user history, leaderboard periods, builder volume time-series, public CLI parity, cursor/offset controls, provenance, canaries, and documentation all map to tasks.
- **Boundary coverage:** No crawler, persistence, undocumented endpoint, credential handling, Perps support, or retention guarantee is introduced.
- **Type consistency:** `EventPage`, `EventKeysetParams`, `BuilderVolumeParams`, and `BuilderVolumeRow` use the same names in source, tests, CLI, and docs.
- **Completeness scan:** Every behavior task has explicit code, RED, GREEN, validation, and commit commands.

## Execution handoff

Execute this plan before the authenticated companion plan. Use `executing-plans` in the primary checkout on `main`, one task at a time. Success means the public feature passes independently, every CLI command performs one bounded request, and the matrix distinguishes API availability from archival retention.
