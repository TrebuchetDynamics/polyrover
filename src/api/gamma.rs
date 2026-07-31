//! Gamma API client: market and event discovery queries.

use crate::{
    query::escape,
    transport,
    types::{
        Event, EventPage, GammaSeries, GammaTag, HealthResponse, Market, MarketPage,
        SearchResponse, SportMetadata, SportsMarketTypes, Team,
    },
    Result,
};

pub const DEFAULT_BASE_URL: &str = "https://gamma-api.polymarket.com";

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

    pub async fn health_check(&self) -> Result<HealthResponse> {
        self.transport
            .get_raw("/")
            .await
            .map(|_| HealthResponse { data: "ok".into() })
    }

    pub async fn active_markets(&self) -> Result<Vec<Market>> {
        self.markets(&MarketParams {
            active: Some(true),
            closed: Some(false),
            ..Default::default()
        })
        .await
    }

    pub async fn markets(&self, params: &MarketParams) -> Result<Vec<Market>> {
        self.transport.get_json(&params.path("/markets")).await
    }

    pub async fn market_page(&self, params: &MarketKeysetParams) -> Result<MarketPage> {
        self.transport
            .get_json(&params.path("/markets/keyset"))
            .await
    }

    pub async fn market_by_id(&self, id: &str) -> Result<Market> {
        self.transport
            .get_json(&format!("/markets/{}", escape(id)))
            .await
    }

    pub async fn market_by_slug(&self, slug: &str) -> Result<Market> {
        self.transport
            .get_json(&format!("/markets/slug/{}", escape(slug)))
            .await
    }

    pub async fn events(&self, params: &EventParams) -> Result<Vec<Event>> {
        self.transport.get_json(&params.path("/events")).await
    }

    pub async fn event_page(&self, params: &EventKeysetParams) -> Result<EventPage> {
        self.transport
            .get_json(&params.path("/events/keyset"))
            .await
    }

    pub async fn event_by_id(&self, id: &str) -> Result<Event> {
        self.transport
            .get_json(&format!("/events/{}", escape(id)))
            .await
    }

    pub async fn search(&self, params: &SearchParams) -> Result<SearchResponse> {
        self.transport
            .get_json(&params.path("/public-search"))
            .await
    }

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
}

/// Offset-paginated Gamma market query.
///
/// Upstream implicitly behaves as `closed=false` when `closed` is omitted, so
/// identifier lookups can silently exclude closed markets. Set `closed` explicitly
/// when status matters. Polyoxide observed an approximately 8 KiB upstream URL
/// ceiling and recommends conservative chunks of at most 100 slugs, 50 CLOB token
/// IDs, or 60 condition IDs; these are empirical safe sizes, not protocol limits.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MarketParams {
    pub limit: Option<u32>,
    pub offset: Option<u32>,
    pub order: Option<String>,
    pub ascending: Option<bool>,
    pub ids: Vec<i64>,
    /// Conservatively chunk to at most 100 slugs.
    pub slug: Vec<String>,
    /// Conservatively chunk to at most 60 IDs.
    pub condition_ids: Vec<String>,
    /// Conservatively chunk to at most 50 IDs.
    pub clob_token_ids: Vec<String>,
    pub market_maker_address: String,
    pub active: Option<bool>,
    /// Omission behaves as `false` upstream; set explicitly when status matters.
    pub closed: Option<bool>,
    pub tag_id: Option<i64>,
    /// Gamma metadata threshold, not executable CLOB depth.
    pub liquidity_num_min: Option<rust_decimal::Decimal>,
    /// Gamma metadata threshold, not executable CLOB depth.
    pub liquidity_num_max: Option<rust_decimal::Decimal>,
    /// Gamma metadata threshold, not executable CLOB volume.
    pub volume_num_min: Option<rust_decimal::Decimal>,
    /// Gamma metadata threshold, not executable CLOB volume.
    pub volume_num_max: Option<rust_decimal::Decimal>,
    /// ISO 8601 date bound forwarded unchanged.
    pub start_date_min: String,
    /// ISO 8601 date bound forwarded unchanged.
    pub start_date_max: String,
    /// ISO 8601 date bound forwarded unchanged.
    pub end_date_min: String,
    /// ISO 8601 date bound forwarded unchanged.
    pub end_date_max: String,
    pub related_tags: Option<bool>,
    pub cyom: Option<bool>,
    pub uma_resolution_status: String,
    pub game_id: String,
    pub rewards_min_size: Option<rust_decimal::Decimal>,
    pub question_ids: Vec<String>,
    pub include_tag: Option<bool>,
    /// Repeated query parameters subject to the documented URL ceiling.
    pub sports_market_types: Vec<String>,
}

/// Keyset-paginated Gamma market query.
///
/// `after_cursor` is opaque and must be copied unchanged from `next_cursor`.
/// Upstream documents a maximum `limit` of 1000 and implicitly behaves as
/// `closed=false` when `closed` is omitted. Apply the same conservative identifier
/// chunking documented on [`MarketParams`].
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MarketKeysetParams {
    /// Upstream maximum: 1000.
    pub limit: Option<u32>,
    /// Opaque cursor copied unchanged from the previous page.
    pub after_cursor: String,
    pub order: Option<String>,
    pub ascending: Option<bool>,
    pub ids: Vec<i64>,
    pub slug: Vec<String>,
    pub decimalized: Option<bool>,
    /// Conservatively chunk to at most 60 IDs.
    pub condition_ids: Vec<String>,
    /// Conservatively chunk to at most 50 IDs.
    pub clob_token_ids: Vec<String>,
    pub question_ids: Vec<String>,
    pub market_maker_address: String,
    pub active: Option<bool>,
    /// Omission behaves as `false` upstream; set explicitly when status matters.
    pub closed: Option<bool>,
    pub tag_id: Option<i64>,
    pub tag_ids: Vec<i64>,
    /// Gamma metadata threshold, not executable CLOB depth.
    pub liquidity_num_min: Option<rust_decimal::Decimal>,
    /// Gamma metadata threshold, not executable CLOB depth.
    pub liquidity_num_max: Option<rust_decimal::Decimal>,
    /// Gamma metadata threshold, not executable CLOB volume.
    pub volume_num_min: Option<rust_decimal::Decimal>,
    /// Gamma metadata threshold, not executable CLOB volume.
    pub volume_num_max: Option<rust_decimal::Decimal>,
    /// ISO 8601 date bound forwarded unchanged.
    pub start_date_min: String,
    /// ISO 8601 date bound forwarded unchanged.
    pub start_date_max: String,
    /// ISO 8601 date bound forwarded unchanged.
    pub end_date_min: String,
    /// ISO 8601 date bound forwarded unchanged.
    pub end_date_max: String,
    pub related_tags: Option<bool>,
    pub tag_match: String,
    pub cyom: Option<bool>,
    pub rfq_enabled: Option<bool>,
    pub uma_resolution_status: String,
    pub game_id: String,
    pub include_tag: Option<bool>,
    pub locale: String,
    /// Repeated query parameters subject to the documented URL ceiling.
    pub sports_market_types: Vec<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EventParams {
    pub limit: Option<u32>,
    pub offset: Option<u32>,
    pub order: Option<String>,
    pub ascending: Option<bool>,
    pub ids: Vec<i64>,
    pub slug: Vec<String>,
    pub active: Option<bool>,
    pub closed: Option<bool>,
    pub archived: Option<bool>,
    pub tag_id: Option<i64>,
    pub exclude_tag_ids: Vec<i64>,
    pub tag_slug: String,
    pub related_tags: Option<bool>,
    pub featured: Option<bool>,
    pub cyom: Option<bool>,
    pub include_chat: Option<bool>,
    pub include_template: Option<bool>,
    pub recurrence: String,
    pub liquidity_min: Option<rust_decimal::Decimal>,
    pub liquidity_max: Option<rust_decimal::Decimal>,
    pub volume_min: Option<rust_decimal::Decimal>,
    pub volume_max: Option<rust_decimal::Decimal>,
    pub start_date_min: String,
    pub start_date_max: String,
    pub end_date_min: String,
    pub end_date_max: String,
}

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
    pub featured: Option<bool>,
    pub cyom: Option<bool>,
    pub title_search: String,
    pub liquidity_min: Option<rust_decimal::Decimal>,
    pub liquidity_max: Option<rust_decimal::Decimal>,
    pub volume_min: Option<rust_decimal::Decimal>,
    pub volume_max: Option<rust_decimal::Decimal>,
    pub start_date_min: String,
    pub start_date_max: String,
    pub end_date_min: String,
    pub end_date_max: String,
    pub start_time_min: String,
    pub start_time_max: String,
    pub tag_ids: Vec<i64>,
    pub tag_slug: String,
    pub exclude_tag_ids: Vec<i64>,
    pub related_tags: Option<bool>,
    pub tag_match: String,
    pub series_ids: Vec<i64>,
    pub game_ids: Vec<i64>,
    pub event_date: String,
    pub event_week: Option<i64>,
    pub featured_order: Option<bool>,
    pub recurrence: String,
    pub created_by: Vec<String>,
    pub parent_event_id: Option<i64>,
    pub include_children: Option<bool>,
    pub partner_slug: String,
    pub include_chat: Option<bool>,
    pub include_template: Option<bool>,
    pub include_best_lines: Option<bool>,
    pub locale: String,
}

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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SearchParams {
    pub q: String,
    pub limit_per_type: Option<u32>,
    pub page: Option<u32>,
    pub events_status: Option<String>,
    pub sort: Option<String>,
    pub search_profiles: Option<bool>,
}

impl MarketParams {
    pub fn path(&self, base: &str) -> String {
        let mut q = Query::new(base);
        q.opt("limit", self.limit);
        q.opt("offset", self.offset);
        q.opt_str("order", self.order.as_deref());
        q.opt("ascending", self.ascending);
        q.opt("active", self.active);
        q.opt("closed", self.closed);
        q.opt("tag_id", self.tag_id);
        q.opt("liquidity_num_min", self.liquidity_num_min);
        q.opt("liquidity_num_max", self.liquidity_num_max);
        q.opt("volume_num_min", self.volume_num_min);
        q.opt("volume_num_max", self.volume_num_max);
        q.opt_str("start_date_min", Some(self.start_date_min.as_str()));
        q.opt_str("start_date_max", Some(self.start_date_max.as_str()));
        q.opt_str("end_date_min", Some(self.end_date_min.as_str()));
        q.opt_str("end_date_max", Some(self.end_date_max.as_str()));
        q.opt("related_tags", self.related_tags);
        q.opt("cyom", self.cyom);
        q.pair("uma_resolution_status", &self.uma_resolution_status);
        q.pair("game_id", &self.game_id);
        q.list("sports_market_types", &self.sports_market_types);
        q.opt("rewards_min_size", self.rewards_min_size);
        q.list("question_ids", &self.question_ids);
        q.opt("include_tag", self.include_tag);
        q.ids("id", &self.ids);
        q.list("slug", &self.slug);
        q.list("condition_ids", &self.condition_ids);
        q.list("clob_token_ids", &self.clob_token_ids);
        q.pair("market_maker_address", &self.market_maker_address);
        q.finish()
    }
}

impl MarketKeysetParams {
    pub fn path(&self, base: &str) -> String {
        let mut q = Query::new(base);
        q.opt("limit", self.limit);
        q.pair("after_cursor", &self.after_cursor);
        q.opt_str("order", self.order.as_deref());
        q.opt("ascending", self.ascending);
        q.ids("id", &self.ids);
        q.list("slug", &self.slug);
        q.opt("closed", self.closed);
        q.opt("decimalized", self.decimalized);
        q.list("clob_token_ids", &self.clob_token_ids);
        q.list("condition_ids", &self.condition_ids);
        q.list("question_ids", &self.question_ids);
        q.pair("market_maker_address", &self.market_maker_address);
        q.opt("active", self.active);
        q.opt("tag_id", self.tag_id);
        q.ids("tag_id", &self.tag_ids);
        q.opt("liquidity_num_min", self.liquidity_num_min);
        q.opt("liquidity_num_max", self.liquidity_num_max);
        q.opt("volume_num_min", self.volume_num_min);
        q.opt("volume_num_max", self.volume_num_max);
        q.opt_str("start_date_min", Some(self.start_date_min.as_str()));
        q.opt_str("start_date_max", Some(self.start_date_max.as_str()));
        q.opt_str("end_date_min", Some(self.end_date_min.as_str()));
        q.opt_str("end_date_max", Some(self.end_date_max.as_str()));
        q.opt("related_tags", self.related_tags);
        q.pair("tag_match", &self.tag_match);
        q.opt("cyom", self.cyom);
        q.opt("rfq_enabled", self.rfq_enabled);
        q.pair("uma_resolution_status", &self.uma_resolution_status);
        q.pair("game_id", &self.game_id);
        q.list("sports_market_types", &self.sports_market_types);
        q.opt("include_tag", self.include_tag);
        q.pair("locale", &self.locale);
        q.finish()
    }
}

impl EventParams {
    pub fn path(&self, base: &str) -> String {
        let mut q = Query::new(base);
        q.opt("limit", self.limit);
        q.opt("offset", self.offset);
        q.opt_str("order", self.order.as_deref());
        q.opt("ascending", self.ascending);
        q.ids("id", &self.ids);
        q.opt("tag_id", self.tag_id);
        q.ids("exclude_tag_id", &self.exclude_tag_ids);
        q.list("slug", &self.slug);
        q.pair("tag_slug", &self.tag_slug);
        q.opt("related_tags", self.related_tags);
        q.opt("active", self.active);
        q.opt("archived", self.archived);
        q.opt("featured", self.featured);
        q.opt("cyom", self.cyom);
        q.opt("include_chat", self.include_chat);
        q.opt("include_template", self.include_template);
        q.pair("recurrence", &self.recurrence);
        q.opt("closed", self.closed);
        q.opt("liquidity_min", self.liquidity_min);
        q.opt("liquidity_max", self.liquidity_max);
        q.opt("volume_min", self.volume_min);
        q.opt("volume_max", self.volume_max);
        q.opt_str("start_date_min", Some(&self.start_date_min));
        q.opt_str("start_date_max", Some(&self.start_date_max));
        q.opt_str("end_date_min", Some(&self.end_date_min));
        q.opt_str("end_date_max", Some(&self.end_date_max));
        q.finish()
    }
}

impl EventKeysetParams {
    pub fn path(&self, base: &str) -> String {
        let mut q = Query::new(base);
        q.opt("limit", self.limit);
        q.pair("after_cursor", &self.after_cursor);
        q.opt_str("order", self.order.as_deref());
        q.opt("ascending", self.ascending);
        q.ids("id", &self.ids);
        q.list("slug", &self.slug);
        q.opt("closed", self.closed);
        q.opt("live", self.live);
        q.opt("featured", self.featured);
        q.opt("cyom", self.cyom);
        q.pair("title_search", &self.title_search);
        q.opt("liquidity_min", self.liquidity_min);
        q.opt("liquidity_max", self.liquidity_max);
        q.opt("volume_min", self.volume_min);
        q.opt("volume_max", self.volume_max);
        q.opt_str("start_date_min", Some(&self.start_date_min));
        q.opt_str("start_date_max", Some(&self.start_date_max));
        q.opt_str("end_date_min", Some(&self.end_date_min));
        q.opt_str("end_date_max", Some(&self.end_date_max));
        q.opt_str("start_time_min", Some(&self.start_time_min));
        q.opt_str("start_time_max", Some(&self.start_time_max));
        q.ids("tag_id", &self.tag_ids);
        q.pair("tag_slug", &self.tag_slug);
        q.ids("exclude_tag_id", &self.exclude_tag_ids);
        q.opt("related_tags", self.related_tags);
        q.pair("tag_match", &self.tag_match);
        q.ids("series_id", &self.series_ids);
        q.ids("game_id", &self.game_ids);
        q.pair("event_date", &self.event_date);
        q.opt("event_week", self.event_week);
        q.opt("featured_order", self.featured_order);
        q.pair("recurrence", &self.recurrence);
        q.list("created_by", &self.created_by);
        q.opt("parent_event_id", self.parent_event_id);
        q.opt("include_children", self.include_children);
        q.pair("partner_slug", &self.partner_slug);
        q.opt("include_chat", self.include_chat);
        q.opt("include_template", self.include_template);
        q.opt("include_best_lines", self.include_best_lines);
        q.pair("locale", &self.locale);
        q.finish()
    }
}

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

impl SearchParams {
    pub fn path(&self, base: &str) -> String {
        let mut q = Query::new(base);
        q.pair("q", &self.q);
        q.opt("limit_per_type", self.limit_per_type);
        q.opt("page", self.page);
        q.opt_str("events_status", self.events_status.as_deref());
        q.opt_str("sort", self.sort.as_deref());
        q.opt("search_profiles", self.search_profiles);
        q.finish()
    }
}

struct Query {
    base: String,
    pairs: Vec<(String, String)>,
}

impl Query {
    fn new(base: &str) -> Self {
        Self {
            base: base.into(),
            pairs: vec![],
        }
    }
    fn pair(&mut self, key: &str, value: &str) {
        if !value.is_empty() {
            self.pairs.push((key.into(), value.into()));
        }
    }
    fn opt<T: ToString>(&mut self, key: &str, value: Option<T>) {
        if let Some(value) = value {
            self.pair(key, &value.to_string());
        }
    }
    fn opt_str(&mut self, key: &str, value: Option<&str>) {
        if let Some(value) = value {
            self.pair(key, value);
        }
    }
    fn list(&mut self, key: &str, values: &[String]) {
        for value in values {
            self.pair(key, value);
        }
    }
    fn ids(&mut self, key: &str, values: &[i64]) {
        for value in values {
            self.pair(key, &value.to_string());
        }
    }
    fn finish(self) -> String {
        if self.pairs.is_empty() {
            return self.base;
        }
        let query = self
            .pairs
            .into_iter()
            .map(|(k, v)| format!("{}={}", escape(&k), escape(&v)))
            .collect::<Vec<_>>()
            .join("&");
        format!("{}?{}", self.base, query)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn market_params_match_gamma_query_shape() {
        let path = MarketParams {
            limit: Some(5),
            active: Some(true),
            slug: vec!["will btc".into()],
            ..Default::default()
        }
        .path("/markets");
        assert_eq!(path, "/markets?limit=5&active=true&slug=will%20btc");
    }

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

    #[test]
    fn search_params_use_public_search() {
        assert_eq!(
            SearchParams {
                q: "Will BTC".into(),
                limit_per_type: Some(3),
                ..Default::default()
            }
            .path("/public-search"),
            "/public-search?q=Will%20BTC&limit_per_type=3"
        );
    }
}
