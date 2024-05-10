use anyhow::Result;
use serde::{Deserialize, Serialize};
use crate::api::ApiUserTag;

use crate::data::common::UserAction;
use crate::data::{Compress, Partial, time};
use crate::database::Compressor;

pub const AGGREGATE_BUCKET: i64 = 60000;

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct AggregateTagEvent {
	pub origin_id: u16,
	pub brand_id: u16,
	pub category_id: u16,
	pub timestamp: i64,
	pub price: i32,
	pub action: UserAction,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct AggregateTagEventCompressedData {
	pub origin_id: u16,
	pub brand_id: u16,
	pub category_id: u16,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct PartialAggregateTagEventCompressedData {
	pub origin_id: Partial<String, u16>,
	pub brand_id: Partial<String, u16>,
	pub category_id: Partial<String, u16>,
}

impl From<ApiUserTag> for PartialAggregateTagEventCompressedData {
	fn from(value: ApiUserTag) -> Self {
		Self {
			origin_id: Partial::Same(value.origin),
			brand_id: Partial::Same(value.product_info.brand_id),
			category_id: Partial::Same(value.product_info.category_id),
		}
	}
}

impl Compress for AggregateTagEvent {
	type From = ApiUserTag;
	type CompressedData = AggregateTagEventCompressedData;
	type PartialCompressedData = PartialAggregateTagEventCompressedData;
	
	async fn compress<T: Compressor<Self>>(value: &Self::From, compressor: &T) -> Result<Self> {
		let compressed_tag = compressor.compress(value).await;
		Ok(Self {
			origin_id: compressed_tag.origin_id,
			brand_id: compressed_tag.brand_id,
			category_id: compressed_tag.category_id,
			timestamp: time::parse_timestamp(value.time.as_str())?,
			price: value.product_info.price,
			action: UserAction::try_from(value.action.as_str())?,
		})
	}
}