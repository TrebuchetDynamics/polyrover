//! CLOB REST client: public order books, prices, and market metadata.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    gamma,
    jsonx::string_or_number,
    query::escape,
    transport,
    types::{
        first_price, ClobBatchPriceHistory, ClobFeeRate, ClobLastTradePrice, ClobMarket,
        ClobMarketByTokenResponse, ClobMarketOutcome, ClobNegRiskInfo, ClobOrderBook,
        ClobPaginatedMarkets, ClobPriceHistory, ClobServerTime, ClobTickSize,
        CLOB_OUTCOME_RESOLVED, CLOB_OUTCOME_UNRESOLVED,
    },
    Error, Result,
};

pub const DEFAULT_BASE_URL: &str = "https://clob.polymarket.com";

const PRICE_HISTORY_INTERVALS: &[&str] = &["max", "all", "1m", "1w", "1d", "6h", "1h"];
const MAX_BATCH_PRICE_HISTORY_MARKETS: usize = 20;

/// Query for `GET /prices-history`.
///
/// `token_id` is the CLOB asset ID sent upstream as the `market` query parameter.
/// Timestamps are inclusive UNIX seconds. Supported intervals are `max`, `all`,
/// `1m`, `1w`, `1d`, `6h`, and `1h`; `fidelity` is resolution in minutes and
/// defaults upstream to 1.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PriceHistoryParams {
    pub token_id: String,
    pub start_ts: Option<i64>,
    pub end_ts: Option<i64>,
    pub interval: Option<String>,
    pub fidelity: Option<u32>,
}

/// Body for the idempotent read-only `POST /batch-prices-history` endpoint.
///
/// `markets` contains 1 to 20 CLOB asset IDs. The server—not Polyrover—must split
/// larger jobs, schedule requests, resume failures, and persist returned points.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
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

#[derive(Clone)]
pub struct Client {
    transport: transport::Client,
}

impl Client {
    pub fn new(base_url: impl Into<String>) -> Result<Self> {
        let base = match base_url.into() {
            s if s.is_empty() => DEFAULT_BASE_URL.into(),
            s => s,
        };
        Ok(Self {
            transport: transport::Client::new(transport::Config::new(base))?,
        })
    }

    pub(crate) fn from_transport(transport: transport::Client) -> Self {
        Self { transport }
    }

    pub async fn health(&self) -> Result<()> {
        self.transport.get_raw("/").await.map(|_| ())
    }

    pub async fn server_time(&self) -> Result<ClobServerTime> {
        self.transport.get_json("/time").await
    }

    pub async fn markets(&self, next_cursor: &str) -> Result<ClobPaginatedMarkets> {
        self.transport
            .get_json(&cursor_path("/markets", next_cursor))
            .await
    }

    pub async fn market(&self, condition_id: &str) -> Result<ClobMarket> {
        self.transport
            .get_json(&format!("/markets/{}", escape(condition_id)))
            .await
    }

    pub async fn market_by_token(&self, token_id: &str) -> Result<ClobMarketByTokenResponse> {
        self.transport
            .get_json(&format!("/markets-by-token/{}", escape(token_id)))
            .await
    }

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
                resolve_via_gamma(gamma_base_url, condition_id)
                    .await
                    .or(Err(err))
            }
            Err(err) => Err(err),
        }
    }

    pub async fn order_book(&self, token_id: &str) -> Result<ClobOrderBook> {
        self.transport
            .get_json(&format!("/book?token_id={}", escape(token_id)))
            .await
    }

    pub async fn order_books(&self, token_ids: &[String]) -> Result<Vec<ClobOrderBook>> {
        let params = token_ids
            .iter()
            .filter(|token_id| !token_id.trim().is_empty())
            .map(|token_id| BookParam { token_id })
            .collect::<Vec<_>>();
        if params.is_empty() {
            return Ok(Vec::new());
        }
        self.transport.post_json_idempotent("/books", &params).await
    }

    pub async fn price(&self, token_id: &str, side: &str) -> Result<String> {
        let row: PriceResponse = self
            .transport
            .get_json(&format!(
                "/price?token_id={}&side={}",
                escape(token_id),
                escape(side)
            ))
            .await?;
        Ok(row.price)
    }

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

    pub async fn midpoint(&self, token_id: &str) -> Result<String> {
        let row: MidpointResponse = self
            .transport
            .get_json(&format!("/midpoint?token_id={}", escape(token_id)))
            .await?;
        Ok(first_price(&[&row.mid, &row.mid_price]))
    }

    pub async fn spread(&self, token_id: &str) -> Result<String> {
        let row: SpreadResponse = self
            .transport
            .get_json(&format!("/spread?token_id={}", escape(token_id)))
            .await?;
        Ok(row.spread)
    }

    pub async fn tick_size(&self, token_id: &str) -> Result<ClobTickSize> {
        self.transport
            .get_json(&format!("/tick-size?token_id={}", escape(token_id)))
            .await
    }

    /// Returns the token-specific CLOB base fee in basis points.
    pub async fn fee_rate(&self, token_id: &str) -> Result<ClobFeeRate> {
        self.transport
            .get_json(&format!("/fee-rate?token_id={}", escape(token_id)))
            .await
    }

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

    pub async fn neg_risk(&self, token_id: &str) -> Result<ClobNegRiskInfo> {
        self.transport
            .get_json(&format!("/neg-risk?token_id={}", escape(token_id)))
            .await
    }

    pub async fn simplified_markets(&self, next_cursor: &str) -> Result<ClobPaginatedMarkets> {
        self.transport
            .get_json(&cursor_path("/simplified-markets", next_cursor))
            .await
    }
}

fn validate_batch_requests(rows: &[BatchMarketRequest], require_side: bool) -> Result<()> {
    if rows.is_empty() {
        return Err(Error::Invalid(
            "batch market request must not be empty".into(),
        ));
    }
    for row in rows {
        if row.token_id.trim().is_empty() {
            return Err(Error::Invalid("batch market token_id is required".into()));
        }
        if require_side && !matches!(row.side.as_str(), "BUY" | "SELL") {
            return Err(Error::Invalid(
                "batch market side must be BUY or SELL".into(),
            ));
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

#[derive(Serialize)]
struct BookParam<'a> {
    token_id: &'a str,
}

#[derive(Deserialize)]
struct PriceResponse {
    #[serde(default, deserialize_with = "string_or_number")]
    price: String,
}

#[derive(Deserialize)]
struct MidpointResponse {
    #[serde(default, deserialize_with = "string_or_number")]
    mid: String,
    #[serde(default, deserialize_with = "string_or_number")]
    mid_price: String,
}

#[derive(Deserialize)]
struct SpreadResponse {
    #[serde(default, deserialize_with = "string_or_number")]
    spread: String,
}

fn outcome_from_clob_market(condition_id: &str, market: ClobMarket) -> ClobMarketOutcome {
    let winner = winning_token_id(&market);
    if market.closed && !winner.is_empty() {
        return ClobMarketOutcome {
            status: CLOB_OUTCOME_RESOLVED.into(),
            condition_id: condition_id.into(),
            winning_token_id: winner,
            closed: true,
            source: format!("clob:/markets/{condition_id}"),
        };
    }
    ClobMarketOutcome {
        status: CLOB_OUTCOME_UNRESOLVED.into(),
        condition_id: condition_id.into(),
        closed: market.closed,
        source: format!("clob:/markets/{condition_id}:not_closed_or_no_winner"),
        ..Default::default()
    }
}

fn winning_token_id(market: &ClobMarket) -> String {
    let mut winners = market
        .tokens
        .iter()
        .filter(|t| t.winner && !t.token_id.trim().is_empty());
    let Some(first) = winners.next() else {
        return String::new();
    };
    if winners.next().is_some() {
        String::new()
    } else {
        first.token_id.trim().into()
    }
}

async fn resolve_via_gamma(gamma_base_url: &str, condition_id: &str) -> Result<ClobMarketOutcome> {
    let client = gamma::Client::new(gamma_base_url)?;
    let markets = client
        .markets(&gamma::MarketParams {
            condition_ids: vec![condition_id.into()],
            ..Default::default()
        })
        .await?;
    if markets.into_iter().any(|m| m.closed) {
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
        return Err(Error::Invalid("price history token_id is required".into()));
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

fn cursor_path(base: &str, next_cursor: &str) -> String {
    if next_cursor.is_empty() {
        base.into()
    } else {
        format!("{}?next_cursor={}", base, escape(next_cursor))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_match_go_clob_endpoints() {
        assert_eq!(
            cursor_path("/markets", "abc 123"),
            "/markets?next_cursor=abc%20123"
        );
        assert_eq!(
            format!("/book?token_id={}", escape("tok/1")),
            "/book?token_id=tok%2F1"
        );
    }

    #[test]
    fn price_history_rejects_invalid_windows_intervals_and_fidelity() {
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
        assert!(price_history_path(&PriceHistoryParams {
            token_id: "token".into(),
            fidelity: Some(0),
            ..Default::default()
        })
        .is_err());
    }

    #[test]
    fn midpoint_accepts_both_field_names() {
        let row: MidpointResponse = serde_json::from_str(r#"{"mid_price":0.51}"#).unwrap();
        assert_eq!(first_price(&[&row.mid, &row.mid_price]), "0.51");
    }

    #[test]
    fn market_outcome_requires_exactly_one_closed_winner() {
        let market = ClobMarket {
            closed: true,
            tokens: vec![
                crate::types::ClobToken {
                    token_id: "yes".into(),
                    winner: true,
                    ..Default::default()
                },
                crate::types::ClobToken {
                    token_id: "no".into(),
                    winner: false,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let outcome = outcome_from_clob_market("c", market);
        assert_eq!(outcome.status, CLOB_OUTCOME_RESOLVED);
        assert_eq!(outcome.winning_token_id, "yes");

        let unresolved = outcome_from_clob_market(
            "c",
            ClobMarket {
                closed: false,
                ..Default::default()
            },
        );
        assert_eq!(unresolved.status, CLOB_OUTCOME_UNRESOLVED);
        assert!(winning_token_id(&ClobMarket {
            tokens: vec![
                crate::types::ClobToken {
                    token_id: "a".into(),
                    winner: true,
                    ..Default::default()
                },
                crate::types::ClobToken {
                    token_id: "b".into(),
                    winner: true,
                    ..Default::default()
                }
            ],
            ..Default::default()
        })
        .is_empty());
    }
}
