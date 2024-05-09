use serde::{Deserialize, Serialize};

use crate::data::{AggregateTagEvent, Compress, Cookie, ProductInfo, UserAction, UserProfile};
use crate::data::time::TimeRange;
use crate::database::Compressor;
use crate::endpoints::GetAggregateApiRequest;

pub const MAX_TAGS: usize = 200;

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
pub struct ApiUserTag {
	pub product_info: ProductInfo,
	pub time: String,
	pub cookie: String,
	pub country: String,
	pub device: String,
	pub action: String,
	pub origin: String,
}

pub struct GetUserProfileRequest {
	pub cookie: Cookie,
	pub time_range: TimeRange,
	pub limit: usize,
}

pub struct GetUserProfileResponse {
	pub user_profile: UserProfile,
}

pub struct AddAggregateRequest {
	pub tag: AggregateTagEvent,
	pub timestamp: i64,
}

pub struct GetAggregateRequest {
	pub time_range: TimeRange,
	pub action: UserAction,
	pub origin: Option<u16>,
	pub brand_id: Option<u16>,
	pub category_id: Option<u16>,
}

pub struct AggregateBucket {
	pub sum: u64,
	pub count: u64,
}

pub struct GetAggregateResponse {
	pub aggregates: Vec<AggregateBucket>,
}

pub struct GetAggregateRequestCompressedData {
	pub origin: Option<u16>,
	pub brand_id: Option<u16>,
	pub category_id: Option<u16>,
}

impl Compress for GetAggregateRequest {
	type From = GetAggregateApiRequest;
	type CompressedData = GetAggregateRequestCompressedData;
	
	async fn compress<T: Compressor<Self>>(value: &Self::From, compressor: &T) -> anyhow::Result<Self> {
		let compressed = compressor.compress(value).await;
		Ok(Self {
			time_range: TimeRange::new(value.time_range.as_str()).unwrap(),
			action: UserAction::try_from(value.action.as_str()).unwrap(),
			origin: compressed.origin,
			brand_id: compressed.brand_id,
			category_id: compressed.category_id,
		})
	}
}