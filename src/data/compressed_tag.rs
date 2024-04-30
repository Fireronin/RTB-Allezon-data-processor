use serde::{Deserialize, Serialize};

use crate::data::common::UserAction;
use crate::data::UserTag;

#[derive(Default, Deserialize, Serialize, Clone, Debug)]
pub struct Compression {
	pub origin_id: Option<u16>,
	pub brand_id: Option<u16>,
	pub category_id: Option<u16>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct CompressedTag {
	pub timestamp: i64,
	pub price: i32,
	pub action: UserAction,
	pub origin_id: u16,
	pub brand_id: u16,
	pub category_id: u16,
}

impl CompressedTag {
	pub fn new(compression: Compression, tag: UserTag) {
		CompressedTag {
			timestamp: tag.time,
			price: tag.product_info.price,
			action: UserAction::try_from(&tag.action).unwrap(),
			origin_id: compression.origin_id.unwrap(),
			brand_id: compression.brand_id.unwrap(),
			category_id: compression.category_id.unwrap(),
		};
	}
}
