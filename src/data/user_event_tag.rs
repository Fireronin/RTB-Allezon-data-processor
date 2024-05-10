use std::cmp::Ordering;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::api::ApiUserTag;
use crate::data::{Compress, Cookie, Decompress, Device, Partial, ProductInfo, time, UserAction};
use crate::database::{Compressor, Decompressor};

#[derive(Deserialize, Serialize, Clone, Copy, Debug, PartialEq, Eq)]
pub struct UserTagEvent {
	pub product_id: u64,
	pub brand_id: u16,
	pub category_id: u16,
	pub country: u8,
	pub origin: u16,
	pub time: i64,
	pub price: i32,
	pub device: Device,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct UserTagEventCompressedData {
	pub product_id: u64,
	pub brand_id: u16,
	pub category_id: u16,
	pub country: u8,
	pub origin: u16,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct PartialUserTagEventCompressedData {
	pub product_id: Partial<String, u64>,
	pub brand_id: Partial<String, u16>,
	pub category_id: Partial<String, u16>,
	pub country: Partial<String, u8>,
	pub origin: Partial<String, u16>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct UserTagEventDecompressedData {
	pub product_id: String,
	pub brand_id: String,
	pub category_id: String,
	pub country: String,
	pub origin: String,
}

impl From<ApiUserTag> for PartialUserTagEventCompressedData {
	fn from(value: ApiUserTag) -> Self {
		Self {
			product_id: Partial::Same(value.product_info.product_id),
			brand_id: Partial::Same(value.product_info.brand_id),
			category_id: Partial::Same(value.product_info.category_id),
			country: Partial::Same(value.country),
			origin: Partial::Same(value.origin),
		}
	}
}

impl Compress for UserTagEvent {
	type From = ApiUserTag;
	type CompressedData = UserTagEventCompressedData;
	type PartialCompressedData = PartialUserTagEventCompressedData;
	
	async fn compress<T: Compressor<UserTagEvent>>(value: &ApiUserTag, compressor: &T) -> Result<UserTagEvent> {
		let compressed_data = compressor.compress(value).await;
		Ok(UserTagEvent {
			product_id: compressed_data.product_id,
			brand_id: compressed_data.brand_id,
			category_id: compressed_data.category_id,
			country: compressed_data.country,
			origin: compressed_data.origin,
			time: time::parse_timestamp(value.time.as_str())?,
			price: value.product_info.price,
			device: Device::try_from(value.device.as_str())?,
		})
	}
}

impl Decompress for UserTagEvent {
	type Type = ApiUserTag;
	type DecompressedData = UserTagEventDecompressedData;
	type AdditionalData = (Cookie, UserAction);
	
	async fn decompress<T: Decompressor<Self>>(&self, decompressor: &T, additional_data: Self::AdditionalData) -> Self::Type {
		let decompressed_data = decompressor.decompress(self).await;
		ApiUserTag {
			product_info: ProductInfo {
				product_id: decompressed_data.product_id,
				brand_id: decompressed_data.brand_id,
				category_id: decompressed_data.category_id,
				price: self.price,
			},
			time: time::timestamp_to_str(self.time),
			cookie: additional_data.0.0,
			country: decompressed_data.country,
			device: Into::<&'static str>::into(self.device).to_owned(),
			action: Into::<&'static str>::into(additional_data.1).to_owned(),
			origin: decompressed_data.origin,
		}
	}
}

impl Ord for UserTagEvent {
	fn cmp(&self, other: &Self) -> Ordering {
		self.time.cmp(&other.time)
	}
}

impl PartialOrd for UserTagEvent {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		self.time.partial_cmp(&other.time)
	}
}