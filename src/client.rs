//! Unified [`Client`] facade over the Gamma, CLOB, and Data API clients,
//! plus aggregate health reporting.

use std::collections::BTreeMap;

use crate::{
    clob::{self, BatchMarketRequest, BatchPriceHistoryParams, PriceHistoryParams},
    crypto_price,
    data::{self, ActivityParams, ClosedPositionParams, LeaderboardParams, TradeParams},
    data_types::{
        Activity, ClosedPosition, Holder, LeaderboardRow, OpenInterest, PortfolioValue, Position,
        Trade,
    },
    gamma::{self, MarketKeysetParams, MarketParams, SearchParams, TaxonomyParams, TeamParams},
    simulation::{self, Request as SimulationRequest, ResultRow as SimulationResult},
    types::{
        ClobBatchPriceHistory, ClobFeeRate, ClobLastTradePrice, ClobOrderBook, ClobPriceHistory,
        GammaSeries, GammaTag, Market, MarketPage, SearchResponse, SportMetadata,
        SportsMarketTypes, Team,
    },
    Result,
};
use serde::Serialize;

/// Combined reachability for the public Gamma and CLOB endpoints.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ClientHealth {
    pub gamma: String,
    pub clob: String,
}

/// Endpoint configuration for [`Client`].
#[derive(Clone, Debug)]
pub struct ClientConfig {
    pub gamma_base_url: String,
    pub clob_base_url: String,
    pub data_base_url: String,
    pub crypto_price_base_url: String,
    pub http_timeout_secs: u64,
    pub http_retry: crate::transport::RetryPolicy,
    pub http_max_concurrent_requests: usize,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            gamma_base_url: gamma::DEFAULT_BASE_URL.into(),
            clob_base_url: clob::DEFAULT_BASE_URL.into(),
            data_base_url: data::DEFAULT_BASE_URL.into(),
            crypto_price_base_url: crypto_price::DEFAULT_BASE_URL.into(),
            http_timeout_secs: 30,
            http_retry: crate::transport::RetryPolicy::default(),
            http_max_concurrent_requests: 8,
        }
    }
}

/// Unified read-only entry point for Polymarket research operations.
#[derive(Clone)]
pub struct Client {
    gamma: gamma::Client,
    clob: clob::Client,
    data: data::Client,
    crypto_price: crypto_price::Client,
}

impl Client {
    /// Creates a client using the configured Gamma, CLOB, and Data endpoints.
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

    pub async fn search(&self, params: &SearchParams) -> Result<SearchResponse> {
        self.gamma.search(params).await
    }

    pub async fn markets(&self, params: &MarketParams) -> Result<Vec<Market>> {
        self.gamma.markets(params).await
    }

    pub async fn market_page(&self, params: &MarketKeysetParams) -> Result<MarketPage> {
        self.gamma.market_page(params).await
    }

    pub async fn market_by_slug(&self, slug: &str) -> Result<Market> {
        self.gamma.market_by_slug(slug).await
    }

    pub async fn tags(&self, params: &TaxonomyParams) -> Result<Vec<GammaTag>> {
        self.gamma.tags(params).await
    }

    pub async fn tag_by_id(&self, id: i64) -> Result<GammaTag> {
        self.gamma.tag_by_id(id).await
    }

    pub async fn tag_by_slug(&self, slug: &str) -> Result<GammaTag> {
        self.gamma.tag_by_slug(slug).await
    }

    pub async fn series(&self, params: &TaxonomyParams) -> Result<Vec<GammaSeries>> {
        self.gamma.series(params).await
    }

    pub async fn series_by_id(&self, id: i64) -> Result<GammaSeries> {
        self.gamma.series_by_id(id).await
    }

    pub async fn sports(&self) -> Result<Vec<SportMetadata>> {
        self.gamma.sports().await
    }

    pub async fn sports_market_types(&self) -> Result<SportsMarketTypes> {
        self.gamma.sports_market_types().await
    }

    pub async fn teams(&self, params: &TeamParams) -> Result<Vec<Team>> {
        self.gamma.teams(params).await
    }

    pub async fn order_book(&self, token_id: &str) -> Result<ClobOrderBook> {
        self.clob.order_book(token_id).await
    }

    pub async fn order_books(&self, token_ids: &[String]) -> Result<Vec<ClobOrderBook>> {
        self.clob.order_books(token_ids).await
    }

    pub async fn price(&self, token_id: &str, side: &str) -> Result<String> {
        self.clob.price(token_id, side).await
    }

    pub async fn batch_prices(
        &self,
        rows: &[BatchMarketRequest],
    ) -> Result<BTreeMap<String, BTreeMap<String, String>>> {
        self.clob.batch_prices(rows).await
    }

    pub async fn batch_midpoints(
        &self,
        rows: &[BatchMarketRequest],
    ) -> Result<BTreeMap<String, String>> {
        self.clob.batch_midpoints(rows).await
    }

    pub async fn batch_spreads(
        &self,
        rows: &[BatchMarketRequest],
    ) -> Result<BTreeMap<String, String>> {
        self.clob.batch_spreads(rows).await
    }

    pub async fn batch_last_trades(
        &self,
        rows: &[BatchMarketRequest],
    ) -> Result<Vec<ClobLastTradePrice>> {
        self.clob.batch_last_trades(rows).await
    }

    /// Returns the token-specific CLOB base fee in basis points.
    pub async fn fee_rate(&self, token_id: &str) -> Result<ClobFeeRate> {
        self.clob.fee_rate(token_id).await
    }

    pub async fn price_history(&self, params: &PriceHistoryParams) -> Result<ClobPriceHistory> {
        self.clob.price_history(params).await
    }

    pub async fn batch_price_history(
        &self,
        params: &BatchPriceHistoryParams,
    ) -> Result<ClobBatchPriceHistory> {
        self.clob.batch_price_history(params).await
    }

    pub async fn crypto_price(
        &self,
        symbol: &str,
        event_start: chrono::DateTime<chrono::Utc>,
        variant: &str,
        end_date: chrono::DateTime<chrono::Utc>,
    ) -> Result<crypto_price::CryptoPrice> {
        self.crypto_price
            .get(symbol, event_start, variant, end_date)
            .await
    }

    pub async fn current_positions(&self, user: &str, limit: u32) -> Result<Vec<Position>> {
        self.data.current_positions(user, limit).await
    }

    pub async fn closed_positions(&self, user: &str, limit: u32) -> Result<Vec<ClosedPosition>> {
        self.data.closed_positions(user, limit).await
    }

    pub async fn closed_positions_with(
        &self,
        params: &ClosedPositionParams,
    ) -> Result<Vec<ClosedPosition>> {
        self.data.closed_positions_with(params).await
    }

    pub async fn trades(&self, user: &str, limit: u32) -> Result<Vec<Trade>> {
        self.data.trades(user, limit).await
    }

    pub async fn trades_with(&self, params: &TradeParams) -> Result<Vec<Trade>> {
        self.data.trades_with(params).await
    }

    pub async fn market_trades(&self, market: &str, limit: u32) -> Result<Vec<Trade>> {
        self.data.market_trades(market, limit).await
    }

    pub async fn activity(&self, user: &str, limit: u32) -> Result<Vec<Activity>> {
        self.data.activity(user, limit).await
    }

    pub async fn activity_with(&self, params: &ActivityParams) -> Result<Vec<Activity>> {
        self.data.activity_with(params).await
    }

    pub async fn top_holders(&self, market: &str, limit: u32) -> Result<Vec<Holder>> {
        self.data.top_holders(market, limit).await
    }

    pub async fn total_value(&self, user: &str) -> Result<PortfolioValue> {
        self.data.total_value(user).await
    }

    pub async fn open_interest(&self, market: &str) -> Result<OpenInterest> {
        self.data.open_interest(market).await
    }

    pub async fn trader_leaderboard(&self, limit: u32) -> Result<Vec<LeaderboardRow>> {
        self.data.trader_leaderboard(limit).await
    }

    pub async fn trader_leaderboard_with(
        &self,
        params: &LeaderboardParams,
    ) -> Result<Vec<LeaderboardRow>> {
        self.data.trader_leaderboard_with(params).await
    }

    pub async fn health(&self) -> ClientHealth {
        ClientHealth {
            gamma: health_label(self.gamma.health_check().await.is_ok()),
            clob: health_label(self.clob.health().await.is_ok()),
        }
    }

    pub async fn simulate(&self, request: SimulationRequest) -> Result<SimulationResult> {
        let book = self.order_book(&request.token_id).await?;
        simulation::simulate_book(&book, request)
    }

    /// Simulates an immediate taker fill and applies the documented category fee.
    pub async fn simulate_with_fee(
        &self,
        request: SimulationRequest,
        fee_category: &str,
    ) -> Result<SimulationResult> {
        let mut result = self.simulate(request).await?;
        simulation::apply_taker_fee(&mut result, fee_category)?;
        Ok(result)
    }
}

fn health_label(healthy: bool) -> String {
    if healthy { "ok" } else { "error" }.into()
}
