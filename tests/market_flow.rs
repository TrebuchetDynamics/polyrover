#![cfg(feature = "public")]

use polyrover::{
    market_data::{MarketUpdate, TrackedEvent, TradeUpdate},
    market_flow::{FlowObservation, MarketFlowConfig, MarketFlowTracker},
    stream::MarketEvent,
    Decimal,
};

fn trade(asset: &str, side: &str, price: &str, size: &str, at: i64) -> FlowObservation {
    FlowObservation {
        asset_id: asset.into(),
        market_id: "market-1".into(),
        observed_at_ms: at,
        side: side.into(),
        price: price.into(),
        size: size.into(),
    }
}

#[test]
fn flow_window_prunes_old_trades_and_labels_direction_source() {
    let mut tracker = MarketFlowTracker::new(MarketFlowConfig {
        window_ms: 60_000,
        large_trade_notional: Decimal::new(10, 0),
        max_trades_per_asset: 3,
    })
    .unwrap();
    tracker
        .observe_trade(trade("token-1", "BUY", "0.5", "10", 1_000))
        .unwrap();
    tracker
        .observe_trade(trade("token-1", "SELL", "0.4", "30", 30_000))
        .unwrap();
    tracker
        .observe_trade(trade("token-1", "BUY", "0.6", "20", 70_000))
        .unwrap();

    let row = tracker.snapshot("token-1", 70_000).unwrap();
    assert_eq!(row.trade_count, 2);
    assert_eq!(row.buy_notional, "12");
    assert_eq!(row.sell_notional, "12");
    assert_eq!(row.large_trade_count, 2);
    assert_eq!(
        row.direction_source,
        "polymarket-market-wss.last_trade.side"
    );
    assert!(row.language.contains("descriptive"));
}

#[test]
fn flow_window_rejects_unbounded_configuration_and_invalid_decimals() {
    assert!(MarketFlowTracker::new(MarketFlowConfig {
        window_ms: 0,
        large_trade_notional: Decimal::ZERO,
        max_trades_per_asset: 0,
    })
    .is_err());
    let mut tracker = MarketFlowTracker::new(MarketFlowConfig::default()).unwrap();
    assert!(tracker
        .observe_trade(trade("token-1", "BUY", "bad", "1", 1))
        .is_err());
}

#[test]
fn tracked_last_trade_uses_explicit_wss_side() {
    let mut tracker = MarketFlowTracker::new(MarketFlowConfig::default()).unwrap();
    let tracked = TrackedEvent {
        event: MarketEvent::Ignored,
        update: MarketUpdate::LastTrade(TradeUpdate {
            market: "market-1".into(),
            asset_id: "token-1".into(),
            price: "0.5".into(),
            side: "SELL".into(),
            size: "20".into(),
            timestamp: "1700000000".into(),
            transaction_hash: "0xhash".into(),
        }),
        snapshots: vec![],
    };
    tracker
        .observe_tracked(&tracked, 1_700_000_000_000)
        .unwrap();
    let row = tracker.snapshot("token-1", 1_700_000_000_000).unwrap();
    assert_eq!(row.trade_count, 1);
    assert_eq!(row.sell_notional, "10");
    assert_eq!(
        row.direction_source,
        "polymarket-market-wss.last_trade.side"
    );
}
