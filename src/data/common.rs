use serde::{Deserialize, Serialize};
use strum_macros::{EnumString, IntoStaticStr};

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
pub struct ProductInfo {
	pub product_id: u64,
	pub brand_id: String,
	pub category_id: String,
	pub price: i32,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq, Hash)]
pub struct Cookie(pub String);

#[repr(u8)]
#[derive(Deserialize, Serialize, Clone, Copy, Debug, PartialEq, Eq, EnumString, IntoStaticStr)]
pub enum Device {
	PC,
	MOBILE,
	TV,
}

#[repr(u8)]
#[derive(Deserialize, Serialize, Clone, Copy, Debug, PartialEq, EnumString, IntoStaticStr)]
pub enum UserAction {
	VIEW,
	BUY,
}

impl Into<String> for Cookie {
	fn into(self) -> String {
		self.0
	}
}
