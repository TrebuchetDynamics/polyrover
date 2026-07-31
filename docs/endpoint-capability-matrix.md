# Endpoint and Capability Matrix

Matrix schema: 1
Last verified: 2026-07-31

Live source and `Cargo.toml` are authoritative. The machine-readable
[`capabilities.json`](../capabilities.json) records operation-level source
support independently of Cargo feature selection. `implemented` means callable
behavior exists with test evidence; `dtoOnly` means types exist without the
network operation; `unsupported` means a tested guard rejects it; and `planned`
means no callable API exists. The names follow Polymarket CLI commit `9b18b5f`.
Taxonomy parity does not imply implementation parity.

## Public

| Surface            | Method/event                                                                                             | Endpoint/channel                                                                                                       | Transport   | Auth level | Cargo feature | Status      | Rust API                                                                | Test                                                                                             |
| ------------------ | -------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- | ----------- | ---------- | ------------- | ----------- | ----------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ |
| Gamma              | Search plus offset/keyset markets and events                                                             | `GET /public-search`, `/markets`, `/markets/keyset`, `/events`, `/events/keyset`                                       | HTTPS       | none       | `public`      | implemented | [`src/api/gamma.rs`](../src/api/gamma.rs)                               | [`tests/client.rs`](../tests/client.rs)                                                          |
| Gamma taxonomy     | Tags, series, sports metadata/types, and teams                                                           | `GET /tags`, `/tags/{id}`, `/tags/slug/{slug}`, `/series`, `/series/{id}`, `/sports`, `/sports/market-types`, `/teams` | HTTPS       | none       | `public`      | implemented | [`src/api/gamma.rs`](../src/api/gamma.rs)                               | [`tests/client.rs`](../tests/client.rs), [`tests/contracts.rs`](../tests/contracts.rs)           |
| CLOB               | Books, prices, single/batch price history, token fee rates, and market metadata                          | `clob.polymarket.com`                                                                                                  | HTTPS       | none       | `public`      | implemented | [`src/api/clob.rs`](../src/api/clob.rs)                                 | [`tests/client.rs`](../tests/client.rs)                                                          |
| CLOB batch context | Atomic prices, midpoints, spreads, and last trades                                                       | `POST /prices`, `/midpoints`, `/spreads`, `/last-trades-prices`                                                        | HTTPS       | none       | `public`      | implemented | [`src/api/clob.rs`](../src/api/clob.rs)                                 | [`tests/client.rs`](../tests/client.rs), [`tests/contracts.rs`](../tests/contracts.rs)           |
| Data API           | Positions, paginated closed positions/trades/activity, filtered leaderboards, and builder volume history | `data-api.polymarket.com`                                                                                              | HTTPS       | none       | `public`      | implemented | [`src/api/data.rs`](../src/api/data.rs)                                 | [`tests/client.rs`](../tests/client.rs), [`tests/contracts.rs`](../tests/contracts.rs)           |
| Wallet dossier     | Bounded descriptive public-account aggregate                                                             | `GET /positions`, `/closed-positions`, `/trades`, `/activity`, `/value`, `/traded`                                     | HTTPS       | none       | `public`      | implemented | [`src/research/wallet_dossier.rs`](../src/research/wallet_dossier.rs)   | [`tests/wallet_dossier.rs`](../tests/wallet_dossier.rs), [`tests/client.rs`](../tests/client.rs) |
| Market WSS         | Book/price/trade/tick/lifecycle events                                                                   | `/ws/market`                                                                                                           | WSS         | none       | `public`      | implemented | [`src/streaming/stream_client.rs`](../src/streaming/stream_client.rs)   | [`src/streaming/stream_client.rs`](../src/streaming/stream_client.rs)                            |
| Market flow        | Bounded descriptive per-asset summary                                                                    | typed `/ws/market` events, local aggregation                                                                           | WSS + local | none       | `public`      | implemented | [`src/research/market_flow.rs`](../src/research/market_flow.rs)         | [`tests/market_flow.rs`](../tests/market_flow.rs)                                                |
| Resolution         | Arbitrary market result                                                                                  | Gamma + CLOB                                                                                                           | HTTPS       | none       | `public`      | implemented | [`src/research/market_results.rs`](../src/research/market_results.rs)   | [`tests/market_results.rs`](../tests/market_results.rs)                                          |
| Crypto resolver    | Up/Down 5m windows                                                                                       | Gamma                                                                                                                  | HTTPS       | none       | `public`      | implemented | [`src/research/market_resolver.rs`](../src/research/market_resolver.rs) | [`src/research/market_resolver.rs`](../src/research/market_resolver.rs)                          |

## Documented historical-query parity

Every row is one request/page. Callers own traversal, partitioning, retries across jobs, and persistence.

| Surface                  | Endpoint                       | Auth | Rust method                       | CLI command                     | Pagination/window controls                              | Official limit          | Evidence                                                          | Official source                                                                                              |
| ------------------------ | ------------------------------ | ---- | --------------------------------- | ------------------------------- | ------------------------------------------------------- | ----------------------- | ----------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| CLOB price history       | `GET /prices-history`          | none | `Client::price_history`           | `clob price-history`            | `startTs`, `endTs`, `interval`, `fidelity`              | one asset               | `tests/client.rs`, `tests/fixtures/clob/price-history.json`       | [Prices history](https://docs.polymarket.com/api-reference/markets/get-prices-history)                       |
| CLOB batch price history | `POST /batch-prices-history`   | none | `Client::batch_price_history`     | `clob batch-price-history`      | `startTs`, `endTs`, `interval`, `fidelity`              | 20 assets               | `tests/client.rs`, `tests/fixtures/clob/batch-price-history.json` | [Batch prices history](https://docs.polymarket.com/api-reference/markets/get-batch-prices-history)           |
| Gamma markets            | `GET /markets`                 | none | `Client::markets`                 | `gamma markets`                 | offset, date/status/ID filters                          | upstream endpoint limit | `tests/client.rs`                                                 | [List markets](https://docs.polymarket.com/api-reference/markets/list-markets)                               |
| Gamma market page        | `GET /markets/keyset`          | none | `Client::market_page`             | `gamma market-page`             | opaque cursor, date/status/ID filters                   | 1000                    | `tests/client.rs`                                                 | [Market keyset pagination](https://docs.polymarket.com/api-reference/markets/list-markets-keyset-pagination) |
| Gamma events             | `GET /events`                  | none | `Client::events`                  | `gamma events`                  | offset, date/status/ID filters                          | upstream endpoint limit | `tests/client.rs`                                                 | [List events](https://docs.polymarket.com/api-reference/events/list-events)                                  |
| Gamma event page         | `GET /events/keyset`           | none | `Client::event_page`              | `gamma event-page`              | opaque cursor, date/time/status/ID filters              | 500                     | `tests/client.rs`, ignored `tests/live_public.rs`                 | [Event keyset pagination](https://docs.polymarket.com/api-reference/events/list-events-keyset-pagination)    |
| Data trades              | `GET /trades`                  | none | `Client::trades_with`             | `analytics trades`              | offset, `start`/`end`, wallet/market/event filters      | limit/offset 10000      | `tests/client.rs`                                                 | [Trades](https://docs.polymarket.com/api-reference/core/get-trades-for-a-user-or-markets)                    |
| Data activity            | `GET /activity`                | none | `Client::activity_with`           | `analytics activity`            | offset, `start`/`end`, wallet/market/event/type filters | limit 500; offset 5000  | `tests/client.rs`                                                 | [Activity](https://docs.polymarket.com/api-reference/core/get-user-activity)                                 |
| Data closed positions    | `GET /closed-positions`        | none | `Client::closed_positions_with`   | `analytics closed-positions`    | offset and wallet/market/event/title filters            | upstream endpoint limit | `tests/client.rs`                                                 | [Closed positions](https://docs.polymarket.com/api-reference/core/get-closed-positions-for-a-user)           |
| Data leaderboard         | `GET /v1/leaderboard`          | none | `Client::trader_leaderboard_with` | `analytics leaderboard`         | offset, category, period, ordering, user                | upstream endpoint limit | `tests/client.rs`                                                 | [Trader leaderboard](https://docs.polymarket.com/api-reference/core/get-trader-leaderboard-rankings)         |
| Builder leaderboard      | `GET /v1/builders/leaderboard` | none | `Client::builder_leaderboard`     | `analytics builder-leaderboard` | offset and `timePeriod`                                 | limit 50; offset 1000   | `tests/client.rs`, `tests/fixtures/data/builder-leaderboard.json` | [Builder leaderboard](https://docs.polymarket.com/api-reference/builders/get-aggregated-builder-leaderboard) |
| Builder volume           | `GET /v1/builders/volume`      | none | `Client::builder_volume`          | `analytics builder-volume`      | `timePeriod`                                            | one series request      | `tests/client.rs`, `tests/fixtures/data/builder-volume.json`      | [Builder volume](https://docs.polymarket.com/api-reference/builders/get-daily-builder-volume-time-series)    |

Limits and retention:

- CLOB batch price history accepts at most 20 asset IDs.
- Gamma market keyset limit is 1000; event keyset limit is 500; both cursors are opaque.
- Data trades limit and offset are at most 10000. Partition deeper history by `start`/`end`; only user-scoped positive `start` can extend beyond the default roughly three-year window.
- Data activity limit is at most 500 and offset is at most 5000. Partition deeper history by `start`/`end`.
- Builder leaderboard limit is at most 50 and offset is at most 1000.
- Polyrover neither discovers an upstream retention guarantee nor represents these APIs as a permanent archive.

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
- Batch prices, midpoints, spreads, and last trades are also one atomic request.
  Polyrover validates non-empty token IDs but asserts no unpublished upstream
  batch maximum; consumers own chunk sizing and persistence.
- Automatic HTTP retries are bounded to `429` and `425`. A server-provided
  numeric `Retry-After` overrides exponential backoff and is clamped to the
  configured maximum delay. 5xx responses are classified as retriable for the
  caller but are not resent automatically.

## Authenticated

| Surface                   | Method/event                | Endpoint/channel | Transport | Auth level | Cargo feature   | Status      | Rust API                                                            | Test                                                                                                               |
| ------------------------- | --------------------------- | ---------------- | --------- | ---------- | --------------- | ----------- | ------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| L2 auth                   | HMAC header construction    | local helper     | none      | L2         | `authenticated` | implemented | [`src/capabilities/auth.rs`](../src/capabilities/auth.rs)           | [`src/capabilities/auth.rs`](../src/capabilities/auth.rs)                                                          |
| User WSS                  | Order/trade events          | `/ws/user`       | WSS       | L2         | `authenticated` | implemented | [`src/streaming/user_stream.rs`](../src/streaming/user_stream.rs)   | [`src/streaming/user_stream.rs`](../src/streaming/user_stream.rs)                                                  |
| Authenticated CLOB trades | `GET /data/trades`          | CLOB             | HTTPS     | L2         | `authenticated` | implemented | [`src/api/authenticated_clob.rs`](../src/api/authenticated_clob.rs) | [`tests/authenticated_client.rs`](../tests/authenticated_client.rs), [`tests/contracts.rs`](../tests/contracts.rs) |
| Authenticated CLOB orders | `GET /data/orders`          | CLOB             | HTTPS     | L2         | `authenticated` | implemented | [`src/api/authenticated_clob.rs`](../src/api/authenticated_clob.rs) | [`tests/authenticated_client.rs`](../tests/authenticated_client.rs), [`tests/contracts.rs`](../tests/contracts.rs) |
| Authenticated CLOB order  | `GET /data/order/{orderID}` | CLOB             | HTTPS     | L2         | `authenticated` | implemented | [`src/api/authenticated_clob.rs`](../src/api/authenticated_clob.rs) | [`tests/authenticated_client.rs`](../tests/authenticated_client.rs)                                                |

Authenticated history is library-only. Each method borrows `&L2Credentials`, signs the canonical path without query parameters, and returns one opaque-cursor page or one order. `ClientConfig` and the cloneable `Client` contain no credentials. Polyrover provides no authenticated CLI or live credential canary, and adds no API-key creation, private-key signing, order mutation, traversal, or persistence.

## Wallet

| Surface            | Method/event                 | Endpoint/channel | Transport     | Auth level | Cargo feature | Status      | Rust API                                                      | Test                                                          |
| ------------------ | ---------------------------- | ---------------- | ------------- | ---------- | ------------- | ----------- | ------------------------------------------------------------- | ------------------------------------------------------------- |
| Address derivation | Deposit/proxy/safe addresses | local helper     | none          | none       | `wallet`      | implemented | [`src/capabilities/wallet.rs`](../src/capabilities/wallet.rs) | [`src/capabilities/wallet.rs`](../src/capabilities/wallet.rs) |
| Wallet signing     | Transaction/order signing    | local            | wallet signer | `wallet`   | planned       | —           | —                                                             |

## Execution

| Surface                     | Method/event                       | Endpoint/channel | Transport | Auth level  | Cargo feature | Status  | Rust API                                                                | Test                                                                    |
| --------------------------- | ---------------------------------- | ---------------- | --------- | ----------- | ------------- | ------- | ----------------------------------------------------------------------- | ----------------------------------------------------------------------- |
| CLOB orders                 | Order/cancel records and responses | CLOB             | none      | L2 + wallet | `execution`   | dtoOnly | [`src/capabilities/clob_orders.rs`](../src/capabilities/clob_orders.rs) | [`src/capabilities/clob_orders.rs`](../src/capabilities/clob_orders.rs) |
| Live order placement/cancel | Submit/cancel                      | CLOB             | HTTPS     | L2 + wallet | `execution`   | planned | —                                                                       | —                                                                       |

## Bridge

| Surface               | Method/event                      | Endpoint/channel | Transport | Auth level | Cargo feature | Status      | Rust API                                                      | Test                                                          |
| --------------------- | --------------------------------- | ---------------- | --------- | ---------- | ------------- | ----------- | ------------------------------------------------------------- | ------------------------------------------------------------- |
| Bridge metadata       | Asset/deposit/status/quote shapes | Bridge API       | none      | none       | `bridge`      | dtoOnly     | [`src/capabilities/bridge.rs`](../src/capabilities/bridge.rs) | [`src/capabilities/bridge.rs`](../src/capabilities/bridge.rs) |
| Withdrawal simulation | Validation and safety result      | local helper     | none      | none       | `bridge`      | implemented | [`src/capabilities/bridge.rs`](../src/capabilities/bridge.rs) | [`src/capabilities/bridge.rs`](../src/capabilities/bridge.rs) |
| Bridge execution      | Deposit/withdraw submit           | Bridge API       | HTTPS     | wallet     | `bridge`      | planned     | —                                                             | —                                                             |

## Official references

- [Gamma keyset market pagination](https://docs.polymarket.com/api-reference/markets/list-markets-keyset-pagination)
- [CLOB authentication](https://docs.polymarket.com/developers/CLOB/authentication)
- [Authenticated trades](https://docs.polymarket.com/api-reference/trade/get-trades)
- [Authenticated orders](https://docs.polymarket.com/api-reference/trade/get-user-orders)
- [Authenticated order lookup](https://docs.polymarket.com/api-reference/trade/get-order)
- [CLOB market WebSocket channel](https://docs.polymarket.com/developers/CLOB/websocket/market-channel)
- [CLOB token fee rate](https://docs.polymarket.com/api-reference/market-data/get-fee-rate)
- [Order lifecycle and order types](https://docs.polymarket.com/concepts/order-lifecycle)
- [Trading fee formula and category schedule](https://docs.polymarket.com/trading/fees)
