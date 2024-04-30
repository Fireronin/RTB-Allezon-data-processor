use serde::{Deserialize, Serialize};
use crate::data::{ProductInfo, time, UserTag};

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
			time: time::parse_timestamp(self.time.as_str()).unwrap(),
			time_string: self.time,
			cookie: self.cookie,
			country: self.country,
			device: self.device,
			action: self.action,
			origin: self.origin,
		}
	}
}