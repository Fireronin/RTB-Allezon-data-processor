use serde::{Deserialize, Serialize};

use crate::data::{Cookie, ProductInfo, UserAction, UserTagEvent};
use crate::data::time::TimeRange;

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

pub struct AddUserProfileRequest {
	pub cookie: Cookie,
	pub action: UserAction,
	pub tag: UserTagEvent,
}

pub struct GetUserProfileRequest {
	pub cookie: Cookie,
	pub time_range: TimeRange,
	pub limit: usize,
}

pub struct GetAggregateRequest {
	
}

pub struct GetAggregateResponse {
	pub count: Option<Vec<u64>>,
	pub sum: Option<Vec<u64>>,
}
