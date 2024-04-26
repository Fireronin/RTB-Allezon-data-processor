use std::cmp::Ordering;
use serde::{Deserialize, Serialize};
use crate::utils::parse_timestamp;

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
pub struct UserTagExternal {
	pub product_info: ProductInfo,
	pub time: String,
	pub cookie: String,
	pub country: String,
	pub device: String,
	pub action: String,
	pub origin: String,
}

impl Into<UserTag> for UserTagExternal {
	fn into(self) -> UserTag {
		UserTag {
			product_info: self.product_info,
			time: parse_timestamp(self.time.as_str()),
			time_string: self.time,
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
	product_id: u64,
	brand_id: String,
	category_id: String,
	price: i32,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub enum UserAction {
	VIEW,
	BUY,
}

impl TryFrom<&String> for UserAction {
	type Error = &'static str;
	
	fn try_from(value: &String) -> Result<Self, Self::Error> {
		match value.as_str() {
			"VIEW" => Ok(UserAction::VIEW),
			"BUY" => Ok(UserAction::BUY),
			_ => Err("Invalid action")
		}
	}
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct CompressedTag {
	timestamp: i64,
	origin_id: u16,
	brand_id: u16,
	category_id: u16,
	price: u32,
	action: UserAction
}
