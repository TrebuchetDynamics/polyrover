//! Bounded descriptive summaries over typed public market WebSocket events.

use std::{
    collections::{BTreeMap, VecDeque},
    str::FromStr,
};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{Error, Result};

const DIRECTION_SOURCE: &str = "polymarket-market-wss.last_trade.side";
const BOOK_SOURCE: &str = "polymarket-market-wss.book";
const FLOW_LANGUAGE: &str =
    "descriptive public-flow context; not proof of intent, coordination, misconduct, or trading edge";

#[derive(Clone, Debug, PartialEq)]
pub struct MarketFlowConfig {
    pub window_ms: i64,
    pub large_trade_notional: Decimal,
    pub max_trades_per_asset: usize,
}

impl Default for MarketFlowConfig {
    fn default() -> Self {
        Self {
            window_ms: 15 * 60 * 1_000,
            large_trade_notional: Decimal::new(1_000, 0),
            max_trades_per_asset: 10_000,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct FlowObservation {
    pub asset_id: String,
    pub market_id: String,
    pub observed_at_ms: i64,
    pub side: String,
    pub price: String,
    pub size: String,
}

#[derive(Clone, Debug)]
struct StoredTrade {
    observed_at_ms: i64,
    side: String,
    price: Decimal,
    size: Decimal,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct MarketFlowSnapshot {
    pub asset_id: String,
    pub market_id: String,
    pub observed_at_ms: i64,
    pub window_ms: i64,
    pub source_rows: usize,
    pub trade_count: usize,
    pub large_trade_count: usize,
    pub buy_notional: String,
    pub sell_notional: String,
    pub unknown_notional: String,
    pub first_trade_price: String,
    pub last_trade_price: String,
    pub price_change: String,
    pub spread: String,
    pub midpoint: String,
    pub bid_depth_1c: String,
    pub ask_depth_1c: String,
    pub imbalance: String,
    pub direction_source: String,
    pub book_source: String,
    pub language: String,
}

#[derive(Clone, Debug, Default)]
struct AssetFlow {
    market_id: String,
    trades: VecDeque<StoredTrade>,
    book: Option<crate::market_data::Snapshot>,
}

#[derive(Clone, Debug)]
pub struct MarketFlowTracker {
    config: MarketFlowConfig,
    assets: BTreeMap<String, AssetFlow>,
}

fn fmt(value: Decimal) -> String {
    value.normalize().to_string()
}

fn prune(config: &MarketFlowConfig, flow: &mut AssetFlow, observed_at_ms: i64) {
    let cutoff = observed_at_ms.saturating_sub(config.window_ms);
    while flow
        .trades
        .front()
        .is_some_and(|trade| trade.observed_at_ms < cutoff)
    {
        flow.trades.pop_front();
    }
    while flow.trades.len() > config.max_trades_per_asset {
        flow.trades.pop_front();
    }
}

impl MarketFlowTracker {
    pub fn new(config: MarketFlowConfig) -> Result<Self> {
        if config.window_ms <= 0 {
            return Err(Error::Invalid(
                "market flow window_ms must be positive".into(),
            ));
        }
        if config.large_trade_notional <= Decimal::ZERO {
            return Err(Error::Invalid(
                "market flow large_trade_notional must be positive".into(),
            ));
        }
        if config.max_trades_per_asset == 0 {
            return Err(Error::Invalid(
                "market flow max_trades_per_asset must be positive".into(),
            ));
        }
        Ok(Self {
            config,
            assets: BTreeMap::new(),
        })
    }

    pub fn observe_trade(&mut self, row: FlowObservation) -> Result<()> {
        if row.asset_id.trim().is_empty() {
            return Err(Error::Invalid("market flow asset_id is required".into()));
        }
        let price = Decimal::from_str(row.price.trim())
            .map_err(|_| Error::Invalid("market flow price must be decimal".into()))?;
        let size = Decimal::from_str(row.size.trim())
            .map_err(|_| Error::Invalid("market flow size must be decimal".into()))?;
        if price <= Decimal::ZERO || size <= Decimal::ZERO {
            return Err(Error::Invalid(
                "market flow price and size must be positive".into(),
            ));
        }
        let flow = self.assets.entry(row.asset_id).or_default();
        if !row.market_id.trim().is_empty() {
            flow.market_id = row.market_id;
        }
        flow.trades.push_back(StoredTrade {
            observed_at_ms: row.observed_at_ms,
            side: row.side.trim().to_ascii_uppercase(),
            price,
            size,
        });
        prune(&self.config, flow, row.observed_at_ms);
        Ok(())
    }

    pub fn observe_snapshot(&mut self, snapshot: crate::market_data::Snapshot) {
        if snapshot.asset_id.trim().is_empty() {
            return;
        }
        let flow = self.assets.entry(snapshot.asset_id.clone()).or_default();
        if !snapshot.market.trim().is_empty() {
            flow.market_id.clone_from(&snapshot.market);
        }
        flow.book = Some(snapshot);
    }

    pub fn observe_tracked(
        &mut self,
        tracked: &crate::market_data::TrackedEvent,
        observed_at_ms: i64,
    ) -> Result<()> {
        for snapshot in &tracked.snapshots {
            self.observe_snapshot(snapshot.clone());
        }
        if let crate::market_data::MarketUpdate::LastTrade(row) = &tracked.update {
            self.observe_trade(FlowObservation {
                asset_id: row.asset_id.clone(),
                market_id: row.market.clone(),
                observed_at_ms,
                side: row.side.clone(),
                price: row.price.clone(),
                size: row.size.clone(),
            })?;
        }
        Ok(())
    }

    pub fn snapshot(&mut self, asset_id: &str, observed_at_ms: i64) -> Option<MarketFlowSnapshot> {
        let flow = self.assets.get_mut(asset_id)?;
        prune(&self.config, flow, observed_at_ms);

        let mut buy = Decimal::ZERO;
        let mut sell = Decimal::ZERO;
        let mut unknown = Decimal::ZERO;
        let mut large_trade_count = 0;
        for trade in &flow.trades {
            let notional = trade.price * trade.size;
            if notional >= self.config.large_trade_notional {
                large_trade_count += 1;
            }
            match trade.side.as_str() {
                "BUY" => buy += notional,
                "SELL" => sell += notional,
                _ => unknown += notional,
            }
        }
        let first = flow.trades.front().map(|trade| trade.price);
        let last = flow.trades.back().map(|trade| trade.price);
        let price_change = first
            .zip(last)
            .map(|(first, last)| last - first)
            .unwrap_or(Decimal::ZERO);

        let (spread, midpoint, bid_depth_1c, ask_depth_1c, imbalance, book_source) =
            if let Some(book) = &flow.book {
                let depth = book.depth();
                let liquidity = book.liquidity();
                (
                    book.spread.clone(),
                    book.midpoint.clone(),
                    fmt(depth.bid_depth_1c),
                    fmt(depth.ask_depth_1c),
                    fmt(liquidity.imbalance),
                    BOOK_SOURCE.into(),
                )
            } else {
                (
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                )
            };

        Some(MarketFlowSnapshot {
            asset_id: asset_id.into(),
            market_id: flow.market_id.clone(),
            observed_at_ms,
            window_ms: self.config.window_ms,
            source_rows: flow.trades.len() + usize::from(flow.book.is_some()),
            trade_count: flow.trades.len(),
            large_trade_count,
            buy_notional: fmt(buy),
            sell_notional: fmt(sell),
            unknown_notional: fmt(unknown),
            first_trade_price: first.map(fmt).unwrap_or_default(),
            last_trade_price: last.map(fmt).unwrap_or_default(),
            price_change: fmt(price_change),
            spread,
            midpoint,
            bid_depth_1c,
            ask_depth_1c,
            imbalance,
            direction_source: DIRECTION_SOURCE.into(),
            book_source,
            language: FLOW_LANGUAGE.into(),
        })
    }
}
