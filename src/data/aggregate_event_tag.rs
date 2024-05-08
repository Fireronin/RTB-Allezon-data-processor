use serde::{Deserialize, Serialize};

use crate::data::common::UserAction;

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct AggregateTagEventCompressedData {
	pub origin_id: u16,
	pub brand_id: u16,
	pub category_id: u16,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct AggregateTagEvent {
	pub origin_id: u16,
	pub brand_id: u16,
	pub category_id: u16,
	pub timestamp: i64,
	pub price: i32,
	pub action: UserAction,
}
