//! Transparent aggregates over bounded public Data API wallet rows.

use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
};

use chrono::{DateTime, TimeZone, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    data_types::{Activity, ClosedPosition, PortfolioValue, Position, TotalMarketsTraded, Trade},
    Error, Result,
};

const DOSSIER_LANGUAGE: &str =
    "descriptive public-account context; not a prediction, misconduct finding, or trading recommendation";

#[derive(Clone, Debug, PartialEq)]
pub struct WalletDossierParams {
    pub user: String,
    pub limit: u32,
}

impl WalletDossierParams {
    pub fn new(user: impl Into<String>, limit: u32) -> Self {
        Self {
            user: user.into(),
            limit,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct WalletDossierInput {
    pub wallet: String,
    pub as_of: DateTime<Utc>,
    pub limit: u32,
    pub positions: Vec<Position>,
    pub closed_positions: Vec<ClosedPosition>,
    pub trades: Vec<Trade>,
    pub activity: Vec<Activity>,
    pub portfolio_value: PortfolioValue,
    pub markets_traded: TotalMarketsTraded,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct WalletDossier {
    pub wallet: String,
    pub as_of: DateTime<Utc>,
    pub source: String,
    pub source_rows: usize,
    pub possibly_truncated: bool,
    pub portfolio_value: String,
    pub open_realized_pnl: String,
    pub closed_realized_pnl: String,
    pub unrealized_pnl: String,
    pub gross_exposure: String,
    pub largest_position_share: String,
    pub trade_notional_7d: String,
    pub trade_notional_30d: String,
    pub trade_notional_90d: String,
    pub unique_markets: usize,
    pub markets_traded: i64,
    pub activity_types: BTreeMap<String, usize>,
    pub language: String,
}

fn decimal(value: f64) -> Decimal {
    Decimal::from_str(&value.to_string()).unwrap_or(Decimal::ZERO)
}

fn parse_observed_time(raw: &str) -> Option<DateTime<Utc>> {
    let text = raw.trim();
    if let Ok(value) = text.parse::<i64>() {
        return if value.abs() >= 10_000_000_000 {
            Utc.timestamp_millis_opt(value).single()
        } else {
            Utc.timestamp_opt(value, 0).single()
        };
    }
    DateTime::parse_from_rfc3339(text)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

fn fmt(value: Decimal) -> String {
    value.normalize().to_string()
}

pub fn build_wallet_dossier(input: WalletDossierInput) -> Result<WalletDossier> {
    if input.wallet.trim().is_empty() {
        return Err(Error::Invalid("wallet dossier user is required".into()));
    }
    if !(1..=500).contains(&input.limit) {
        return Err(Error::Invalid(
            "wallet dossier limit must be 1..=500".into(),
        ));
    }

    let limit = input.limit as usize;
    let possibly_truncated = [
        input.positions.len(),
        input.closed_positions.len(),
        input.trades.len(),
        input.activity.len(),
    ]
    .into_iter()
    .any(|rows| rows == limit);

    let mut open_realized = Decimal::ZERO;
    let mut closed_realized = Decimal::ZERO;
    let mut unrealized = Decimal::ZERO;
    let mut gross_exposure = Decimal::ZERO;
    let mut largest_exposure = Decimal::ZERO;
    let mut markets = BTreeSet::new();

    for position in &input.positions {
        open_realized += decimal(position.realized_pnl);
        unrealized += decimal(position.unrealized_pnl);
        let exposure = (decimal(position.size) * decimal(position.current_price)).abs();
        gross_exposure += exposure;
        largest_exposure = largest_exposure.max(exposure);
        if !position.market_id.trim().is_empty() {
            markets.insert(position.market_id.clone());
        }
    }
    for position in &input.closed_positions {
        closed_realized += decimal(position.position.realized_pnl);
        if !position.position.market_id.trim().is_empty() {
            markets.insert(position.position.market_id.clone());
        }
    }

    let mut notional_7d = Decimal::ZERO;
    let mut notional_30d = Decimal::ZERO;
    let mut notional_90d = Decimal::ZERO;
    for trade in &input.trades {
        if !trade.market.trim().is_empty() {
            markets.insert(trade.market.clone());
        }
        let Some(observed_at) = parse_observed_time(&trade.created_at) else {
            continue;
        };
        let age = input.as_of.signed_duration_since(observed_at);
        if age < chrono::Duration::zero() {
            continue;
        }
        let notional = (decimal(trade.price) * decimal(trade.size)).abs();
        if age <= chrono::Duration::days(90) {
            notional_90d += notional;
        }
        if age <= chrono::Duration::days(30) {
            notional_30d += notional;
        }
        if age <= chrono::Duration::days(7) {
            notional_7d += notional;
        }
    }

    let mut activity_types = BTreeMap::new();
    for row in &input.activity {
        let key = row.activity_type.trim().to_ascii_uppercase();
        if !key.is_empty() {
            *activity_types.entry(key).or_insert(0) += 1;
        }
    }

    let source_rows = input.positions.len()
        + input.closed_positions.len()
        + input.trades.len()
        + input.activity.len();
    let largest_position_share = if gross_exposure == Decimal::ZERO {
        Decimal::ZERO
    } else {
        largest_exposure / gross_exposure
    };

    Ok(WalletDossier {
        wallet: input.wallet,
        as_of: input.as_of,
        source: "polymarket-data-api".into(),
        source_rows,
        possibly_truncated,
        portfolio_value: fmt(decimal(input.portfolio_value.value)),
        open_realized_pnl: fmt(open_realized),
        closed_realized_pnl: fmt(closed_realized),
        unrealized_pnl: fmt(unrealized),
        gross_exposure: fmt(gross_exposure),
        largest_position_share: fmt(largest_position_share),
        trade_notional_7d: fmt(notional_7d),
        trade_notional_30d: fmt(notional_30d),
        trade_notional_90d: fmt(notional_90d),
        unique_markets: markets.len(),
        markets_traded: input
            .markets_traded
            .markets_traded
            .max(input.markets_traded.traded),
        activity_types,
        language: DOSSIER_LANGUAGE.into(),
    })
}
