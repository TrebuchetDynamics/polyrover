# Endpoint and Capability Matrix

Matrix schema: 1
Last verified: 2026-07-28

Live source and `Cargo.toml` are authoritative. The machine-readable
[`capabilities.json`](../capabilities.json) records operation-level source
support independently of Cargo feature selection. `implemented` means callable
behavior exists with test evidence; `dtoOnly` means types exist without the
network operation; `unsupported` means a tested guard rejects it; and `planned`
means no callable API exists. The names follow Polymarket CLI commit `9b18b5f`.
Taxonomy parity does not imply implementation parity.

## Public

| Surface | Method/event | Endpoint/channel | Transport | Auth level | Cargo feature | Status | Rust API | Test |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Gamma | Search, offset/keyset markets, and events | `gamma-api.polymarket.com` | HTTPS | none | `public` | implemented | [`src/api/gamma.rs`](../src/api/gamma.rs) | [`tests/client.rs`](../tests/client.rs) |
| CLOB | Books, prices, single/batch price history, token fee rates, and market metadata | `clob.polymarket.com` | HTTPS | none | `public` | implemented | [`src/api/clob.rs`](../src/api/clob.rs) | [`tests/client.rs`](../tests/client.rs) |
| Data API | Positions, paginated closed positions/trades/activity, holders, and filtered leaderboards | `data-api.polymarket.com` | HTTPS | none | `public` | implemented | [`src/api/data.rs`](../src/api/data.rs) | [`tests/client.rs`](../tests/client.rs) |
| Market WSS | Book/price/trade/tick/lifecycle events | `/ws/market` | WSS | none | `public` | implemented | [`src/streaming/stream_client.rs`](../src/streaming/stream_client.rs) | [`src/streaming/stream_client.rs`](../src/streaming/stream_client.rs) |
| Resolution | Arbitrary market result | Gamma + CLOB | HTTPS | none | `public` | implemented | [`src/research/market_results.rs`](../src/research/market_results.rs) | [`tests/market_results.rs`](../tests/market_results.rs) |
| Crypto resolver | Up/Down 5m windows | Gamma | HTTPS | none | `public` | implemented | [`src/research/market_resolver.rs`](../src/research/market_resolver.rs) | [`src/research/market_resolver.rs`](../src/research/market_resolver.rs) |

## Public API operational notes

- Gamma omits closed markets when `closed` is absent; callers requiring closed
  or mixed-status results must set it explicitly.
- Gamma keyset cursors are opaque. Preserve `next_cursor` byte-for-byte as the
  next `after_cursor`; the documented keyset `limit` maximum is 1000.
- Offset and keyset market queries accept Gamma liquidity/volume metadata
  thresholds, ISO 8601 date bounds, tag flags, and repeated sports-market types.
  These are discovery filters—not executable CLOB depth—and date values are
  forwarded unchanged. Repeated sports types count toward the URL ceiling.
- Polyoxide observed an approximately 8 KiB Gamma URL ceiling and uses
  conservative chunks of 100 slugs, 50 CLOB token IDs, and 60 condition IDs.
  These are empirical safe sizes, not upstream protocol guarantees.
- CLOB batch price history accepts at most 20 asset IDs. Polyrover performs one
  atomic request; consumers own splitting, retries across jobs, and persistence.
- Automatic HTTP retries are bounded to `429` and `425`. A server-provided
  numeric `Retry-After` overrides exponential backoff and is clamped to the
  configured maximum delay. 5xx responses are classified as retriable for the
  caller but are not resent automatically.

## Authenticated

| Surface | Method/event | Endpoint/channel | Transport | Auth level | Cargo feature | Status | Rust API | Test |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| L2 auth | HMAC header construction | local helper | none | L2 | `authenticated` | implemented | [`src/capabilities/auth.rs`](../src/capabilities/auth.rs) | [`src/capabilities/auth.rs`](../src/capabilities/auth.rs) |
| User WSS | Order/trade events | `/ws/user` | WSS | L2 | `authenticated` | implemented | [`src/streaming/user_stream.rs`](../src/streaming/user_stream.rs) | [`src/streaming/user_stream.rs`](../src/streaming/user_stream.rs) |
| Authenticated CLOB reads | Account/order reads | CLOB | HTTPS | L2 | `authenticated` | planned | — | — |

## Wallet

| Surface | Method/event | Endpoint/channel | Transport | Auth level | Cargo feature | Status | Rust API | Test |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Address derivation | Deposit/proxy/safe addresses | local helper | none | none | `wallet` | implemented | [`src/capabilities/wallet.rs`](../src/capabilities/wallet.rs) | [`src/capabilities/wallet.rs`](../src/capabilities/wallet.rs) |
| Wallet signing | Transaction/order signing | local | wallet signer | `wallet` | planned | — | — |

## Execution

| Surface | Method/event | Endpoint/channel | Transport | Auth level | Cargo feature | Status | Rust API | Test |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| CLOB orders | Order/cancel records and responses | CLOB | none | L2 + wallet | `execution` | dtoOnly | [`src/capabilities/clob_orders.rs`](../src/capabilities/clob_orders.rs) | [`src/capabilities/clob_orders.rs`](../src/capabilities/clob_orders.rs) |
| Live order placement/cancel | Submit/cancel | CLOB | HTTPS | L2 + wallet | `execution` | planned | — | — |

## Bridge

| Surface | Method/event | Endpoint/channel | Transport | Auth level | Cargo feature | Status | Rust API | Test |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Bridge metadata | Asset/deposit/status/quote shapes | Bridge API | none | none | `bridge` | dtoOnly | [`src/capabilities/bridge.rs`](../src/capabilities/bridge.rs) | [`src/capabilities/bridge.rs`](../src/capabilities/bridge.rs) |
| Withdrawal simulation | Validation and safety result | local helper | none | none | `bridge` | implemented | [`src/capabilities/bridge.rs`](../src/capabilities/bridge.rs) | [`src/capabilities/bridge.rs`](../src/capabilities/bridge.rs) |
| Bridge execution | Deposit/withdraw submit | Bridge API | HTTPS | wallet | `bridge` | planned | — | — |

## Official references

- [Gamma keyset market pagination](https://docs.polymarket.com/api-reference/markets/list-markets-keyset-pagination)
- [CLOB authentication](https://docs.polymarket.com/developers/CLOB/authentication)
- [CLOB market WebSocket channel](https://docs.polymarket.com/developers/CLOB/websocket/market-channel)
- [CLOB token fee rate](https://docs.polymarket.com/api-reference/market-data/get-fee-rate)
- [Order lifecycle and order types](https://docs.polymarket.com/concepts/order-lifecycle)
- [Trading fee formula and category schedule](https://docs.polymarket.com/trading/fees)
