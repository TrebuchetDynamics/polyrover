//! Fill simulation of a hypothetical order against a CLOB book snapshot.

use std::str::FromStr;

use rust_decimal::{Decimal, RoundingStrategy};
use serde::{Deserialize, Serialize};

use crate::{types::ClobOrderBook, Error, Result};

#[derive(Clone, Copy, Debug, Serialize, PartialEq)]
pub struct FeeScheduleRow {
    pub category: &'static str,
    pub taker_fee_rate: f64,
    pub maker_fee_rate: f64,
    pub maker_rebate_rate: Option<f64>,
}

/// Current category coefficients from <https://docs.polymarket.com/trading/fees>.
pub const FEE_SCHEDULE: &[FeeScheduleRow] = &[
    FeeScheduleRow {
        category: "crypto",
        taker_fee_rate: 0.07,
        maker_fee_rate: 0.0,
        maker_rebate_rate: Some(0.20),
    },
    FeeScheduleRow {
        category: "sports",
        taker_fee_rate: 0.05,
        maker_fee_rate: 0.0,
        maker_rebate_rate: Some(0.15),
    },
    FeeScheduleRow {
        category: "finance",
        taker_fee_rate: 0.04,
        maker_fee_rate: 0.0,
        maker_rebate_rate: Some(0.25),
    },
    FeeScheduleRow {
        category: "politics",
        taker_fee_rate: 0.04,
        maker_fee_rate: 0.0,
        maker_rebate_rate: Some(0.25),
    },
    FeeScheduleRow {
        category: "economics",
        taker_fee_rate: 0.05,
        maker_fee_rate: 0.0,
        maker_rebate_rate: Some(0.25),
    },
    FeeScheduleRow {
        category: "culture",
        taker_fee_rate: 0.05,
        maker_fee_rate: 0.0,
        maker_rebate_rate: Some(0.25),
    },
    FeeScheduleRow {
        category: "weather",
        taker_fee_rate: 0.05,
        maker_fee_rate: 0.0,
        maker_rebate_rate: Some(0.25),
    },
    FeeScheduleRow {
        category: "other",
        taker_fee_rate: 0.05,
        maker_fee_rate: 0.0,
        maker_rebate_rate: Some(0.25),
    },
    FeeScheduleRow {
        category: "mentions",
        taker_fee_rate: 0.04,
        maker_fee_rate: 0.0,
        maker_rebate_rate: Some(0.25),
    },
    FeeScheduleRow {
        category: "tech",
        taker_fee_rate: 0.04,
        maker_fee_rate: 0.0,
        maker_rebate_rate: Some(0.25),
    },
    FeeScheduleRow {
        category: "geopolitics",
        taker_fee_rate: 0.0,
        maker_fee_rate: 0.0,
        maker_rebate_rate: None,
    },
];

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Request {
    pub token_id: String,
    pub side: String,
    pub amount: String,
    pub limit_price: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct FillLevel {
    pub price: String,
    pub available_size: String,
    pub filled_size: String,
    pub notional: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct ResultRow {
    pub token_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub market: String,
    pub side: String,
    pub input_amount: String,
    pub input_amount_type: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub limit_price: String,
    pub complete: bool,
    pub filled_size: String,
    pub notional: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub fee_category: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub taker_fee_rate: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub estimated_taker_fee: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub average_price: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub expected_fill_price: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub best_price: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub worst_price: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub slippage: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub slippage_bps: String,
    pub unfilled_amount: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub book_hash: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub book_timestamp: String,
    pub levels: Vec<FillLevel>,
}

pub fn simulate_book(book: &ClobOrderBook, req: Request) -> Result<ResultRow> {
    let side = normalize_side(&req.side)?;
    let amount = parse_positive("--amount", &req.amount)?;
    let limit = if req.limit_price.trim().is_empty() {
        None
    } else {
        Some(parse_positive("--limit-price", &req.limit_price)?)
    };
    let mut levels = opposing_levels(book, side);
    levels.sort_by(|left, right| {
        if side == "buy" {
            left.0.cmp(&right.0)
        } else {
            right.0.cmp(&left.0)
        }
    });

    let best_price = levels.first().map(|level| fmt(level.0)).unwrap_or_default();
    let mut remaining = amount;
    let mut filled_size = Decimal::ZERO;
    let mut notional = Decimal::ZERO;
    let mut fills = Vec::new();
    let mut worst_price = String::new();

    for (price, size) in levels.iter().copied() {
        if limit.is_some_and(|bound| {
            (side == "buy" && price > bound) || (side == "sell" && price < bound)
        }) {
            break;
        }
        let (fill_size, fill_notional) = if side == "buy" {
            let level_notional = size * price;
            if remaining >= level_notional {
                (size, level_notional)
            } else {
                (remaining / price, remaining)
            }
        } else {
            let fill_size = remaining.min(size);
            (fill_size, fill_size * price)
        };
        if fill_size <= Decimal::ZERO {
            continue;
        }
        filled_size += fill_size;
        notional += fill_notional;
        fills.push(FillLevel {
            price: fmt(price),
            available_size: fmt(size),
            filled_size: fmt(fill_size),
            notional: fmt(fill_notional),
        });
        worst_price = fmt(price);
        remaining -= if side == "buy" {
            fill_notional
        } else {
            fill_size
        };
        if remaining <= Decimal::ZERO {
            remaining = Decimal::ZERO;
            break;
        }
    }

    let mut out = ResultRow {
        token_id: if book.asset_id.is_empty() {
            req.token_id
        } else {
            book.asset_id.clone()
        },
        market: book.market.clone(),
        side: side.into(),
        input_amount: fmt(amount),
        input_amount_type: if side == "buy" { "usdc" } else { "shares" }.into(),
        limit_price: limit.map(fmt).unwrap_or_default(),
        complete: remaining == Decimal::ZERO,
        filled_size: fmt(filled_size),
        notional: fmt(notional),
        best_price,
        worst_price,
        unfilled_amount: fmt(remaining),
        book_hash: book.hash.clone(),
        book_timestamp: book.timestamp.clone(),
        levels: fills,
        ..Default::default()
    };
    if filled_size > Decimal::ZERO {
        let average = notional / filled_size;
        out.average_price = fmt(average);
        out.expected_fill_price = out.average_price.clone();
        if let Ok(best) = Decimal::from_str(&out.best_price) {
            let slippage = if side == "buy" {
                average - best
            } else {
                best - average
            };
            out.slippage = fmt(slippage);
            if best > Decimal::ZERO {
                out.slippage_bps = fmt(slippage / best * Decimal::from(10_000));
            }
        }
    }
    Ok(out)
}

/// Adds the documented taker-fee estimate to an existing simulated fill.
pub fn apply_taker_fee(result: &mut ResultRow, category: &str) -> Result<()> {
    let normalized = category.trim().to_ascii_lowercase();
    let canonical = match normalized.as_str() {
        "general" => "other",
        "world events" | "world-events" | "world_events" => "geopolitics",
        value => value,
    };
    let row = FEE_SCHEDULE
        .iter()
        .find(|row| row.category == canonical)
        .ok_or_else(|| Error::Invalid(format!("unsupported fee category {category:?}")))?;
    let rate = Decimal::from_str(&row.taker_fee_rate.to_string())
        .map_err(|_| Error::Invalid("fee schedule contains an invalid rate".into()))?;
    let fee = result
        .levels
        .iter()
        .try_fold(Decimal::ZERO, |total, level| {
            let price = Decimal::from_str(&level.price)
                .map_err(|_| Error::Invalid("simulated fill contains an invalid price".into()))?;
            if !(Decimal::ZERO..=Decimal::ONE).contains(&price) {
                return Err(Error::Invalid(
                    "simulated fill price must be between 0 and 1".into(),
                ));
            }
            let shares = Decimal::from_str(&level.filled_size)
                .map_err(|_| Error::Invalid("simulated fill contains an invalid size".into()))?;
            Ok::<_, Error>(total + shares * rate * price * (Decimal::ONE - price))
        })?;
    result.fee_category = row.category.into();
    result.taker_fee_rate = fmt(rate);
    result.estimated_taker_fee = fmt_fee(fee);
    Ok(())
}

fn opposing_levels(book: &ClobOrderBook, side: &str) -> Vec<(Decimal, Decimal)> {
    let rows = if side == "sell" {
        &book.bids
    } else {
        &book.asks
    };
    rows.iter()
        .filter_map(|level| {
            let price = Decimal::from_str(level.price.trim()).ok()?;
            let size = Decimal::from_str(level.size.trim()).ok()?;
            (price > Decimal::ZERO && size > Decimal::ZERO).then_some((price, size))
        })
        .collect()
}

fn normalize_side(side: &str) -> Result<&'static str> {
    match side.trim().to_ascii_lowercase().as_str() {
        "" | "buy" => Ok("buy"),
        "sell" => Ok("sell"),
        _ => Err(Error::Invalid("--side must be buy or sell".into())),
    }
}

fn parse_positive(name: &str, value: &str) -> Result<Decimal> {
    if value.contains('/') {
        return Err(Error::Invalid(format!("{name} must be a decimal")));
    }
    let number = Decimal::from_str(value.trim())
        .map_err(|_| Error::Invalid(format!("{name} must be a positive decimal")))?;
    (number > Decimal::ZERO)
        .then_some(number)
        .ok_or_else(|| Error::Invalid(format!("{name} must be a positive decimal")))
}

fn fmt_fee(value: Decimal) -> String {
    format_decimal(value, 5)
}

fn fmt(value: Decimal) -> String {
    format_decimal(value, 6)
}

fn format_decimal(value: Decimal, places: u32) -> String {
    value
        .round_dp_with_strategy(places, RoundingStrategy::MidpointNearestEven)
        .normalize()
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ClobOrderBookLevel;

    #[test]
    fn seven_tenths_fill_completes_without_binary_residue() {
        let book = ClobOrderBook {
            asset_id: "tok".into(),
            asks: (0..7)
                .map(|_| ClobOrderBookLevel {
                    price: "1".into(),
                    size: "0.1".into(),
                })
                .collect(),
            ..Default::default()
        };

        let result = simulate_book(
            &book,
            Request {
                token_id: "tok".into(),
                side: "buy".into(),
                amount: "0.7".into(),
                ..Default::default()
            },
        )
        .unwrap();

        assert!(result.complete);
        assert_eq!(result.filled_size, "0.7");
        assert_eq!(result.notional, "0.7");
        assert_eq!(result.unfilled_amount, "0");
    }

    #[test]
    fn buy_walks_asks_low_to_high() {
        let book = ClobOrderBook {
            asset_id: "tok".into(),
            asks: vec![
                ClobOrderBookLevel {
                    price: "0.6".into(),
                    size: "10".into(),
                },
                ClobOrderBookLevel {
                    price: "0.5".into(),
                    size: "4".into(),
                },
            ],
            ..Default::default()
        };
        let got = simulate_book(
            &book,
            Request {
                token_id: "tok".into(),
                side: "buy".into(),
                amount: "5".into(),
                limit_price: "".into(),
            },
        )
        .unwrap();
        assert!(got.complete);
        assert_eq!(got.filled_size, "9");
        assert_eq!(got.average_price, "0.555556");
        assert_eq!(got.best_price, "0.5");
    }

    #[test]
    fn applies_documented_taker_fee_to_simulated_fills() {
        let book = ClobOrderBook {
            asks: vec![ClobOrderBookLevel {
                price: "0.5".into(),
                size: "100".into(),
            }],
            ..Default::default()
        };
        let mut got = simulate_book(
            &book,
            Request {
                side: "buy".into(),
                amount: "50".into(),
                ..Default::default()
            },
        )
        .unwrap();

        apply_taker_fee(&mut got, "crypto").unwrap();

        assert_eq!(got.fee_category, "crypto");
        assert_eq!(got.taker_fee_rate, "0.07");
        assert_eq!(got.estimated_taker_fee, "1.75");

        apply_taker_fee(&mut got, "world events").unwrap();
        assert_eq!(got.fee_category, "geopolitics");
        assert_eq!(got.estimated_taker_fee, "0");
    }

    #[test]
    fn taker_fee_rejects_prices_outside_probability_range() {
        let mut result = ResultRow {
            levels: vec![FillLevel {
                price: "1.1".into(),
                filled_size: "1".into(),
                ..Default::default()
            }],
            ..Default::default()
        };

        assert!(apply_taker_fee(&mut result, "crypto").is_err());
    }

    #[test]
    fn sell_respects_limit_price() {
        let book = ClobOrderBook {
            bids: vec![
                ClobOrderBookLevel {
                    price: "0.4".into(),
                    size: "10".into(),
                },
                ClobOrderBookLevel {
                    price: "0.3".into(),
                    size: "10".into(),
                },
            ],
            ..Default::default()
        };
        let got = simulate_book(
            &book,
            Request {
                side: "sell".into(),
                amount: "12".into(),
                limit_price: "0.35".into(),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(!got.complete);
        assert_eq!(got.filled_size, "10");
        assert_eq!(got.unfilled_amount, "2");
    }
}
