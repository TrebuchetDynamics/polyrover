# Endpoint and Capability Matrix

Matrix schema: 1
Last verified: 2026-07-16

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
| CLOB | Books/prices/market metadata | `clob.polymarket.com` | HTTPS | none | `public` | implemented | [`src/api/clob.rs`](../src/api/clob.rs) | [`tests/client.rs`](../tests/client.rs) |
| Data API | Positions, paginated closed positions/trades/activity, holders, and filtered leaderboards | `data-api.polymarket.com` | HTTPS | none | `public` | implemented | [`src/api/data.rs`](../src/api/data.rs) | [`tests/client.rs`](../tests/client.rs) |
| Market WSS | Book/price/trade/tick/lifecycle events | `/ws/market` | WSS | none | `public` | implemented | [`src/streaming/stream_client.rs`](../src/streaming/stream_client.rs) | [`src/streaming/stream_client.rs`](../src/streaming/stream_client.rs) |
| Resolution | Arbitrary market result | Gamma + CLOB | HTTPS | none | `public` | implemented | [`src/research/market_results.rs`](../src/research/market_results.rs) | [`tests/market_results.rs`](../tests/market_results.rs) |
| Crypto resolver | Up/Down 5m windows | Gamma | HTTPS | none | `public` | implemented | [`src/research/market_resolver.rs`](../src/research/market_resolver.rs) | [`src/research/market_resolver.rs`](../src/research/market_resolver.rs) |

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
