use serde::{Deserialize, Serialize};

use crate::data::{Cookie, ProductInfo, UserAction, UserTagEvent};

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

struct GetUserProfileRequest {
	cookie: Cookie,
	time_range: String,
	limit: i32,
}

pub struct GetUserProfileResponse {
	pub view_events: Vec<UserTagEvent>,
	pub buy_events: Vec<UserTagEvent>,
}

pub struct GetAggregateRequest {
	
}

pub struct GetAggregateResponse {
	pub count: Option<Vec<u64>>,
	pub sum: Option<Vec<u64>>,
}
