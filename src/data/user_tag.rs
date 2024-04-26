use std::cmp::Ordering;
use serde::{Deserialize, Serialize};
use crate::data::external_user_tag::UserTagExternal;

pub const MAX_TAGS: usize = 200;

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
pub struct UserTag {
	pub product_info: ProductInfo,
	pub time: i64,
	pub time_string: String,
	pub cookie: String,
	pub country: String,
	pub device: String,
	pub action: String,
	pub origin: String,
}

impl Ord for UserTag {
	fn cmp(&self, other: &Self) -> Ordering {
		self.time.cmp(&other.time)
	}
}

impl PartialOrd for UserTag {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		self.time.partial_cmp(&other.time)
	}
}

impl Into<UserTagExternal> for UserTag {
	fn into(self) -> UserTagExternal {
		UserTagExternal {
			product_info: self.product_info,
			time: self.time_string,
			cookie: self.cookie,
			country: self.country,
			device: self.device,
			action: self.action,
			origin: self.origin,
		}
	}
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
pub struct ProductInfo {
	pub product_id: u64,
	pub brand_id: String,
	pub category_id: String,
	pub price: i32,
}
