# Endpoint and Capability Matrix

Matrix schema: 1
Last verified: 2026-07-16

Live source and `Cargo.toml` are authoritative. `implemented` means callable
behavior exists and has a linked contract test; `dto-only` means types exist
without network execution; `unsupported` means a guard deliberately rejects
the operation; `planned` means no current Rust API exists.

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

## Official references

- [Gamma Markets API overview](https://docs.polymarket.com/developers/gamma-markets-api/overview)
- [CLOB authentication](https://docs.polymarket.com/developers/CLOB/authentication)
- [CLOB market WebSocket channel](https://docs.polymarket.com/developers/CLOB/websocket/market-channel)
