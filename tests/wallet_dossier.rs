#![cfg(feature = "public")]

use chrono::{TimeZone, Utc};
use polyrover::{
    data_types::{Activity, ClosedPosition, PortfolioValue, Position, TotalMarketsTraded, Trade},
    wallet_dossier::{build_wallet_dossier, WalletDossierInput},
};

#[test]
fn dossier_separates_realized_unrealized_and_time_windows() {
    let as_of = Utc.timestamp_opt(1_800_000_000, 0).unwrap();
    let input = WalletDossierInput {
        wallet: "0xwallet".into(),
        as_of,
        limit: 100,
        positions: vec![Position {
            size: 10.0,
            current_price: 0.6,
            realized_pnl: 2.0,
            unrealized_pnl: 1.5,
            ..Default::default()
        }],
        closed_positions: vec![ClosedPosition {
            position: Position {
                realized_pnl: 4.0,
                ..Default::default()
            },
            timestamp: (as_of.timestamp() - 5 * 86_400).to_string(),
        }],
        trades: vec![
            Trade {
                market: "m1".into(),
                price: 0.5,
                size: 10.0,
                created_at: (as_of.timestamp() - 2 * 86_400).to_string(),
                ..Default::default()
            },
            Trade {
                market: "m2".into(),
                price: 0.4,
                size: 20.0,
                created_at: (as_of.timestamp() - 20 * 86_400).to_string(),
                ..Default::default()
            },
            Trade {
                market: "m3".into(),
                price: 0.3,
                size: 30.0,
                created_at: (as_of.timestamp() - 60 * 86_400).to_string(),
                ..Default::default()
            },
        ],
        activity: vec![Activity {
            activity_type: "TRADE".into(),
            ..Default::default()
        }],
        portfolio_value: PortfolioValue {
            value: 6.0,
            ..Default::default()
        },
        markets_traded: TotalMarketsTraded {
            markets_traded: 3,
            ..Default::default()
        },
    };

    let dossier = build_wallet_dossier(input).unwrap();
    assert_eq!(dossier.open_realized_pnl, "2");
    assert_eq!(dossier.closed_realized_pnl, "4");
    assert_eq!(dossier.unrealized_pnl, "1.5");
    assert_eq!(dossier.gross_exposure, "6");
    assert_eq!(dossier.trade_notional_7d, "5");
    assert_eq!(dossier.trade_notional_30d, "13");
    assert_eq!(dossier.trade_notional_90d, "22");
    assert_eq!(dossier.unique_markets, 3);
    assert_eq!(dossier.activity_types["TRADE"], 1);
    assert!(dossier.language.contains("not a prediction"));
}

#[test]
fn dossier_rejects_blank_wallet_and_invalid_limit() {
    let mut input = WalletDossierInput::default();
    assert!(build_wallet_dossier(input.clone()).is_err());
    input.wallet = "0xwallet".into();
    input.limit = 501;
    assert!(build_wallet_dossier(input).is_err());
}
