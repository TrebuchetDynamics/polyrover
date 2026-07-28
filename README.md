<p align="center">
  <img src="./assets/readme/hero.svg" width="100%" alt="Polyrover turns public Polymarket APIs into typed Rust data, versioned agent JSON, and local fill estimates without fund-moving code">
</p>

<p align="center">
  <a href="#quick-start">Quick start</a> ·
  <a href="#why-polyrover">Why Polyrover</a> ·
  <a href="#use-it-as-a-library">Rust API</a> ·
  <a href="#simulation-contract">Simulation</a> ·
  <a href="#process-boundary">Boundary</a>
</p>

**Polyrover is a safe async Rust toolkit for Polymarket research.** It gives
Rust programs, shell pipelines, and agents one process for public market
discovery, order-book reads, typed streams, local fill estimates, and versioned
JSON output.

- **Discover and stream markets** through Gamma, CLOB, Data API, and market WSS.
- **Estimate executable fills locally** against a captured order-book snapshot.
- **Automate without wallet access** using the default `public` build.

> [!IMPORTANT]
> The default artifact has no private-key signing, order submission,
> cancellation, relayer invocation, or bridge-transfer path.

## Quick start

Install from Git and follow one public-data workflow:

```bash
cargo install --git https://github.com/TrebuchetDynamics/polyrover

# 1. Find markets and their outcome token IDs.
polyrover gamma search --query bitcoin --limit 3 --json

# 2. Inspect executable liquidity for one outcome token.
polyrover clob book --token-id <TOKEN_ID> --json

# 3. Inspect the documented schedule and this token's live base fee.
polyrover clob fees --json
polyrover clob fee-rate --token-id <TOKEN_ID> --json

# 4. Estimate a taker buy, including the documented crypto fee formula.
polyrover clob simulate \
  --token-id <TOKEN_ID> \
  --side buy \
  --amount 100 \
  --limit-price 0.55 \
  --fee-category crypto \
  --json

# 5. Watch bounded public market events.
polyrover stream watch \
  --token-id <TOKEN_ID> \
  --limit 10 \
  --seconds 30 \
  --json
```

A fixture-backed simulation in the test suite produces this result shape
(abbreviated):

```json
{
  "ok": true,
  "version": "1",
  "data": {
    "token_id": "tok",
    "side": "buy",
    "input_amount": "5",
    "input_amount_type": "usdc",
    "complete": true,
    "filled_size": "9",
    "notional": "5",
    "fee_category": "crypto",
    "taker_fee_rate": "0.07",
    "estimated_taker_fee": "0.154",
    "average_price": "0.555556",
    "best_price": "0.5",
    "worst_price": "0.6",
    "unfilled_amount": "0"
  },
  "meta": {
    "command": "clob simulate"
  }
}
```

Successes and failures share the same CLI envelope. `version: "1"` versions the
envelope—not every nested upstream payload. Payload types remain pre-1.0.

## Why Polyrover

Polymarket maintains a broader [official Rust SDK v2](https://github.com/Polymarket/rs-clob-client-v2)
with typed order builders, authentication, order management, public APIs, and
WebSockets. Use it when you need a production trading client.

Polyrover is intentionally narrower. Its niche is a research process you can
run beside agents or data pipelines without installing fund-moving code in the
default artifact:

| Need | Polyrover approach |
| --- | --- |
| Process isolation | Default `public` feature has no signer or execution client |
| Reproducible automation | Every CLI command uses one versioned JSON envelope |
| Local execution research | Book walking, fill estimates, paper state, and result reconciliation |
| Explicit inventory | [`capabilities.json`](capabilities.json) links operations to status, source, and tests |
| Rust integration | One async client over public Gamma, CLOB, Data API, and market WSS |

The [official client documentation](https://docs.polymarket.com/api-reference/clients-sdks)
is the authoritative source for Polymarket-supported SDKs and trading APIs.

## Research surface

- **Discovery** — Gamma search, events, offset/keyset market pagination, and crypto-window helpers.
- **Executable market data** — CLOB books, prices, spreads, tick sizes, metadata, and resolution evidence.
- **Public portfolio research** — positions, trades, activity, holders, volume, and leaderboards.
- **Streaming** — typed market events with heartbeat, reconnect, deduplication, subscription restoration, and tracking.
- **Local analysis** — fill estimation, paper state, wallet scoring, and generic market results.

See the [endpoint capability matrix](docs/endpoint-capability-matrix.md) for the
operation-by-operation source and test inventory.

## Use it as a library

Polyrover is pre-1.0 and its network API is async-only.

```toml
[dependencies]
polyrover = { git = "https://github.com/TrebuchetDynamics/polyrover", default-features = false, features = ["public"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust
use polyrover::{
    simulation::{simulate_book, Request},
    Client, ClientConfig,
};

#[tokio::main]
async fn main() -> polyrover::Result<()> {
    let client = Client::new(ClientConfig::default())?;
    let book = client.order_book("TOKEN_ID").await?;

    let estimate = simulate_book(
        &book,
        Request {
            token_id: book.asset_id.clone(),
            side: "buy".into(),
            amount: "100".into(), // USDC notional for buys
            limit_price: "0.55".into(),
        },
    )?;

    println!(
        "complete={} shares={} average_price={}",
        estimate.complete, estimate.filled_size, estimate.average_price
    );
    Ok(())
}
```

Network clients use async `reqwest` and `tokio-tungstenite`. DTO parsing, book
math, simulation, HMAC helpers, and address derivation are synchronous.

## Orders and fees

Polymarket's [order lifecycle](https://docs.polymarket.com/concepts/order-lifecycle)
defines every order as a signed limit order; a “market order” is an immediately
marketable limit order.

| Type | Behavior |
| --- | --- |
| GTC | Rests until filled or cancelled. |
| GTD | Rests until its expiration. |
| FOK | Fills entirely immediately or cancels. |
| FAK | Fills available liquidity immediately and cancels the remainder. |

Post-only orders rest as makers or are rejected when they would match
immediately. Polyrover's public default documents these types but does not place
or sign orders.

The official [fee schedule](https://docs.polymarket.com/trading/fees) charges
makers zero and calculates taker fees per fill as:

```text
fee = shares × taker_fee_rate × price × (1 - price)
```

The documented coefficient is `0.07` for crypto; `0.05` for sports, economics,
culture, weather, and other/general; `0.04` for finance, politics, mentions, and
tech; and `0` for geopolitics/world events. These are formula coefficients—not
percentage labels. Fees are rounded to five decimal places in USDC.

- `clob fees` prints this schedule, order types, maker rebates, formula, and source links without a network call.
- `clob fee-rate --token-id <id>` reads the CLOB's token-specific `base_fee` value and normalizes it as `base_fee_bps`.
- `clob simulate --fee-category <category>` estimates the taker fee across each consumed price level. Without the flag, output remains fee-free for compatibility.

## Simulation contract

`clob simulate` walks one CLOB snapshot; it does not predict a future fill.

- A **buy** consumes asks from lowest to highest price; `amount` is USDC notional.
- A **sell** consumes bids from highest to lowest price; `amount` is shares.
- Limit prices are inclusive. Ineligible levels stop the walk.
- Invalid, non-positive, or non-finite book levels are ignored.
- Insufficient eligible liquidity returns `complete: false` and `unfilled_amount` in the input unit.
- Results include consumed levels, best/worst price, average price, notional, slippage, book hash, and timestamp when supplied upstream.
- With `--fee-category`, results also include the category, coefficient, and estimated taker fee. The fee is calculated per consumed level and rounded to five decimal places.
- The model does **not** infer a market category or include latency, queue position, tick rounding, minimum order size, or book staleness.
- Calculations currently use validated decimal strings converted to `f64`; fill results use six decimal places and fees use five.

Real execution can differ because the market can move between snapshot capture
and order arrival.

## Process boundary

The current codebase is for observation, analysis, simulation, reconciliation,
and pre-trade research. It contains no live order-placement or cancellation
client, private-key signer, relayer caller, or bridge-transfer operation.

Optional features compile additional research surfaces; they do not grant
runtime authority:

<details>
<summary><strong>Compile-time capability layers</strong></summary>

<p align="center">
  <img src="./assets/readme/capability-map.svg" width="100%" alt="Polyrover capability layers from the public default through optional authenticated, wallet, execution-model, and bridge-model features">
</p>

- **`public` (default)** — public Gamma/CLOB/Data reads, market WSS, and resolution.
- **`authenticated`** — `public`, L2 HMAC helpers, and user WSS.
- **`wallet`** — pure address derivation and readiness helpers.
- **`execution`** — authenticated and wallet features plus order/cancel DTOs; no submission transport.
- **`bridge`** — bridge DTOs and local dry-run validation; no transfer transport.
- **`full`** — compiles every surface above; it is not an authorization mode.

</details>

If you need production orders, approvals, CTF operations, or transfers, use the
official SDK or another explicitly execution-capable boundary.

## Current limits

- No crates.io release yet; installation tracks a Git revision.
- Public simulation inputs are validated strings rather than domain-safe `Decimal`, `Side`, and quantity types.
- The JSON envelope is versioned; nested upstream payload schemas have no separate compatibility version yet.
- CI uses deterministic local fixtures and does not run a scheduled public API canary.
- `stream watch` buffers a bounded result envelope; JSON Lines streaming is not implemented.
- Research and backtest output is not evidence of live execution quality or strategy edge.

<details>
<summary><strong>CLI command reference</strong></summary>

```text
ping --json
gamma search --query <text> [--limit n] --json
gamma markets [--limit n] --json
clob book --token-id <id> --json
clob price --token-id <id> --side buy|sell --json
clob fee-rate --token-id <id> --json
clob fees --json
clob simulate --token-id <id> --side buy|sell --amount <n> [--limit-price p] [--fee-category category] --json
analytics positions --user <wallet> [--limit n] --json
analytics trades --user <wallet> [--limit n] --json
analytics leaderboard [--limit n] --json
stream watch --token-id <id> [--token-id <id> ...] [--url ws://...] [--limit n] [--seconds s] --json
sim reset [--cash n] --json
sim buy --token-id <id> --price <p> --size <n> --json
sim sell --token-id <id> --price <p> --size <n> --json
```

Run `polyrover help <command>` for command-specific options and examples.

</details>

## Build and verify

```bash
git clone https://github.com/TrebuchetDynamics/polyrover
cd polyrover
cargo test --all-features
cargo clippy --all-targets --all-features -- -D warnings
cargo doc --open
```

Tests use local fixtures and require no live credentials.

## Project references

- [Endpoint and capability matrix](docs/endpoint-capability-matrix.md)
- [ADR-0001: async SDK with safe public default](docs/adr/0001-universal-async-sdk.md)
- [Port and parity roadmap](PORT_PLAN.md)

## License

Licensed under the [MIT License](LICENSE).
