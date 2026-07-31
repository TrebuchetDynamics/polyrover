//! L2-authenticated, read-only CLOB trade and order queries.

use chrono::Utc;

use crate::{
    auth::{build_l2_headers, L2Credentials},
    clob_history::{
        ClobTradeRecord, CursorPage, OrderRecord, RewardsMarketPage, TotalUserEarning, UserEarning,
    },
    query::escape,
    transport, Error, Result,
};

pub const DEFAULT_BASE_URL: &str = "https://clob.polymarket.com";

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TradeParams {
    pub id: String,
    pub maker_address: String,
    pub market: String,
    pub asset_id: String,
    pub before: Option<i64>,
    pub after: Option<i64>,
    pub next_cursor: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OrderParams {
    pub id: String,
    pub market: String,
    pub asset_id: String,
    pub next_cursor: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RewardParams {
    pub date: String,
    pub signature_type: Option<u8>,
    pub maker_address: String,
    pub sponsored: Option<bool>,
    pub next_cursor: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RewardsMarketParams {
    pub date: String,
    pub signature_type: Option<u8>,
    pub maker_address: String,
    pub sponsored: Option<bool>,
    pub next_cursor: String,
    pub page_size: Option<u32>,
    pub q: String,
    pub tag_slug: String,
    pub favorite_markets: Option<bool>,
    pub no_competition: Option<bool>,
    pub only_mergeable: Option<bool>,
    pub only_open_orders: Option<bool>,
    pub only_open_positions: Option<bool>,
    pub order_by: String,
    pub position: String,
}

#[derive(Clone)]
pub struct Client {
    transport: transport::Client,
}

impl Client {
    pub fn new(base_url: impl Into<String>) -> Result<Self> {
        let base_url = match base_url.into() {
            value if value.trim().is_empty() => DEFAULT_BASE_URL.into(),
            value => value,
        };
        Ok(Self {
            transport: transport::Client::new(transport::Config::new(base_url))?,
        })
    }

    pub(crate) fn from_transport(transport: transport::Client) -> Self {
        Self { transport }
    }

    pub async fn trades_page(
        &self,
        credentials: &L2Credentials,
        params: &TradeParams,
    ) -> Result<CursorPage<ClobTradeRecord>> {
        self.signed_get(credentials, "/data/trades", &trade_path(params))
            .await
    }

    pub async fn orders_page(
        &self,
        credentials: &L2Credentials,
        params: &OrderParams,
    ) -> Result<CursorPage<OrderRecord>> {
        self.signed_get(credentials, "/data/orders", &order_path(params))
            .await
    }

    pub async fn rewards_page(
        &self,
        credentials: &L2Credentials,
        params: &RewardParams,
    ) -> Result<CursorPage<UserEarning>> {
        if params.date.trim().is_empty() {
            return Err(Error::Invalid("rewards date is required".into()));
        }
        self.signed_get(
            credentials,
            "/rewards/user",
            &reward_path("/rewards/user", params, true),
        )
        .await
    }

    pub async fn rewards_total(
        &self,
        credentials: &L2Credentials,
        params: &RewardParams,
    ) -> Result<Vec<TotalUserEarning>> {
        if params.date.trim().is_empty() {
            return Err(Error::Invalid("rewards date is required".into()));
        }
        self.signed_get(
            credentials,
            "/rewards/user/total",
            &reward_path("/rewards/user/total", params, false),
        )
        .await
    }

    pub async fn rewards_markets_page(
        &self,
        credentials: &L2Credentials,
        params: &RewardsMarketParams,
    ) -> Result<RewardsMarketPage> {
        self.signed_get(
            credentials,
            "/rewards/user/markets",
            &rewards_market_path(params),
        )
        .await
    }

    pub async fn order(&self, credentials: &L2Credentials, order_id: &str) -> Result<OrderRecord> {
        if order_id.trim().is_empty() {
            return Err(Error::Invalid("authenticated order_id is required".into()));
        }
        let path = format!("/data/order/{}", escape(order_id));
        self.signed_get(credentials, &path, &path).await
    }

    async fn signed_get<T: serde::de::DeserializeOwned>(
        &self,
        credentials: &L2Credentials,
        canonical_path: &str,
        request_path: &str,
    ) -> Result<T> {
        let headers = build_l2_headers(
            credentials,
            Utc::now().timestamp(),
            "GET",
            canonical_path,
            None,
        )?;
        self.transport
            .get_json_with_headers(request_path, &headers)
            .await
    }
}

fn trade_path(params: &TradeParams) -> String {
    query_path(
        "/data/trades",
        vec![
            text_pair("id", &params.id),
            text_pair("maker_address", &params.maker_address),
            text_pair("market", &params.market),
            text_pair("asset_id", &params.asset_id),
            params.before.map(|value| ("before", value.to_string())),
            params.after.map(|value| ("after", value.to_string())),
            text_pair("next_cursor", &params.next_cursor),
        ],
    )
}

fn order_path(params: &OrderParams) -> String {
    query_path(
        "/data/orders",
        vec![
            text_pair("id", &params.id),
            text_pair("market", &params.market),
            text_pair("asset_id", &params.asset_id),
            text_pair("next_cursor", &params.next_cursor),
        ],
    )
}

fn reward_path(base: &str, params: &RewardParams, include_cursor: bool) -> String {
    query_path(
        base,
        vec![
            text_pair("date", &params.date),
            params
                .signature_type
                .map(|value| ("signature_type", value.to_string())),
            text_pair("maker_address", &params.maker_address),
            params
                .sponsored
                .map(|value| ("sponsored", value.to_string())),
            include_cursor
                .then(|| text_pair("next_cursor", &params.next_cursor))
                .flatten(),
        ],
    )
}

fn rewards_market_path(params: &RewardsMarketParams) -> String {
    query_path(
        "/rewards/user/markets",
        vec![
            text_pair("date", &params.date),
            params
                .signature_type
                .map(|value| ("signature_type", value.to_string())),
            text_pair("maker_address", &params.maker_address),
            params
                .sponsored
                .map(|value| ("sponsored", value.to_string())),
            text_pair("next_cursor", &params.next_cursor),
            params
                .page_size
                .map(|value| ("page_size", value.to_string())),
            text_pair("q", &params.q),
            text_pair("tag_slug", &params.tag_slug),
            params
                .favorite_markets
                .map(|value| ("favorite_markets", value.to_string())),
            params
                .no_competition
                .map(|value| ("no_competition", value.to_string())),
            params
                .only_mergeable
                .map(|value| ("only_mergeable", value.to_string())),
            params
                .only_open_orders
                .map(|value| ("only_open_orders", value.to_string())),
            params
                .only_open_positions
                .map(|value| ("only_open_positions", value.to_string())),
            text_pair("order_by", &params.order_by),
            text_pair("position", &params.position),
        ],
    )
}

fn text_pair<'a>(key: &'a str, value: &str) -> Option<(&'a str, String)> {
    (!value.trim().is_empty()).then(|| (key, value.to_string()))
}

fn query_path(base: &str, pairs: Vec<Option<(&str, String)>>) -> String {
    let query = pairs
        .into_iter()
        .flatten()
        .map(|(key, value)| format!("{}={}", escape(key), escape(&value)))
        .collect::<Vec<_>>()
        .join("&");
    if query.is_empty() {
        base.into()
    } else {
        format!("{base}?{query}")
    }
}
