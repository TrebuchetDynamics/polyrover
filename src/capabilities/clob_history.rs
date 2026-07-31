//! Typed read-only responses for L2-authenticated CLOB history queries.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::jsonx::{int_or_zero, string_or_number};

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct CursorPage<T> {
    #[serde(default)]
    pub limit: u32,
    #[serde(default)]
    pub next_cursor: String,
    #[serde(default)]
    pub count: u32,
    #[serde(default)]
    pub data: Vec<T>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct UserEarning {
    #[serde(default)]
    pub date: String,
    #[serde(default)]
    pub condition_id: String,
    #[serde(default)]
    pub asset_address: String,
    #[serde(default)]
    pub maker_address: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub earnings: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub asset_rate: String,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct TotalUserEarning {
    #[serde(default)]
    pub date: String,
    #[serde(default)]
    pub asset_address: String,
    #[serde(default)]
    pub maker_address: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub earnings: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub asset_rate: String,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct AssetEarning {
    #[serde(default)]
    pub asset_address: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub earnings: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub asset_rate: String,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct UserRewardsMarket {
    #[serde(default)]
    pub condition_id: String,
    #[serde(default)]
    pub market_id: String,
    #[serde(default)]
    pub event_id: String,
    #[serde(default)]
    pub question: String,
    #[serde(default)]
    pub market_slug: String,
    #[serde(default)]
    pub event_slug: String,
    #[serde(default)]
    pub image: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub rewards_max_spread: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub rewards_min_size: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub volume_24hr: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub spread: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub market_competitiveness: String,
    #[serde(default)]
    pub tokens: Vec<Value>,
    #[serde(default)]
    pub rewards_config: Vec<Value>,
    #[serde(default)]
    pub maker_address: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub earning_percentage: String,
    #[serde(default)]
    pub earnings: Vec<AssetEarning>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct RewardsMarketPage {
    #[serde(default)]
    pub limit: u32,
    #[serde(default)]
    pub count: u32,
    #[serde(default)]
    pub total_count: u32,
    #[serde(default)]
    pub next_cursor: String,
    #[serde(default)]
    pub data: Vec<UserRewardsMarket>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct MakerOrderRecord {
    #[serde(default)]
    pub order_id: String,
    #[serde(default)]
    pub owner: String,
    #[serde(default)]
    pub maker_address: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub matched_amount: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub price: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub fee_rate_bps: String,
    #[serde(default)]
    pub asset_id: String,
    #[serde(default)]
    pub outcome: String,
    #[serde(default)]
    pub side: String,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct ClobTradeRecord {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub taker_order_id: String,
    #[serde(default)]
    pub market: String,
    #[serde(default)]
    pub asset_id: String,
    #[serde(default)]
    pub side: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub size: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub fee_rate_bps: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub price: String,
    #[serde(default)]
    pub status: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub match_time: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub match_time_nano: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub last_update: String,
    #[serde(default)]
    pub outcome: String,
    #[serde(default, deserialize_with = "int_or_zero")]
    pub bucket_index: i64,
    #[serde(default)]
    pub owner: String,
    #[serde(default)]
    pub maker_address: String,
    #[serde(default)]
    pub transaction_hash: String,
    #[serde(default)]
    pub err_msg: Option<String>,
    #[serde(default)]
    pub maker_orders: Vec<MakerOrderRecord>,
    #[serde(default)]
    pub trader_side: String,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct OrderRecord {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub owner: String,
    #[serde(default)]
    pub market: String,
    #[serde(default, alias = "assetId")]
    pub asset_id: String,
    #[serde(default)]
    pub side: String,
    #[serde(default, alias = "originalSize", deserialize_with = "string_or_number")]
    pub original_size: String,
    #[serde(default, alias = "sizeMatched", deserialize_with = "string_or_number")]
    pub size_matched: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub price: String,
    #[serde(default)]
    pub outcome: String,
    #[serde(default, rename = "type")]
    pub record_type: String,
    #[serde(default, alias = "orderType", deserialize_with = "string_or_number")]
    pub order_type: String,
    #[serde(default, alias = "signatureType", deserialize_with = "int_or_zero")]
    pub signature_type: i64,
    #[serde(default, alias = "createdAt", deserialize_with = "string_or_number")]
    pub created_at: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub expiration: String,
    #[serde(default, alias = "makerAddress")]
    pub maker_address: String,
    #[serde(default, alias = "associateTrades")]
    pub associate_trades: Vec<String>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}
