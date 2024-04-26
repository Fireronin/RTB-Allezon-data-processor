use std::sync::{Arc, atomic};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use crate::data::common::UserAction;
use crate::data::UserTag;

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct CompressedTag {
	timestamp: i64,
	origin_id: u16,
	brand_id: u16,
	category_id: u16,
	price: u32,
	action: UserAction,
}

#[derive(Clone, Debug)]
pub struct CompressionMappings {
	pub origin_id_map: DashMap<String, u16>,
	pub brand_id_map: DashMap<String, u16>,
	pub category_id_map: DashMap<String, u16>,
	pub origin_id_count: Arc<atomic::AtomicUsize>,
	pub brand_id_count: Arc<atomic::AtomicUsize>,
	pub category_id_count: Arc<atomic::AtomicUsize>,
}

pub fn compress_tag(tag: &UserTag, compression_map: &CompressionMappings) -> CompressedTag {
	let origin_id = tag.origin.clone();
	let brand_id = tag.product_info.brand_id.clone();
	let category_id = tag.product_info.category_id.clone();
	let price = tag.product_info.price;
	
	let origin_id = match compression_map.origin_id_map.get(&origin_id) {
		Some(id) => *id,
		None => {
			let id = compression_map.origin_id_count.fetch_add(1, atomic::Ordering::SeqCst) as u16;
			compression_map.origin_id_map.insert(origin_id, id);
			id
		}
	};
	
	let brand_id = match compression_map.brand_id_map.get(&brand_id) {
		Some(id) => *id,
		None => {
			let id = compression_map.brand_id_count.fetch_add(1, atomic::Ordering::SeqCst) as u16;
			compression_map.brand_id_map.insert(brand_id, id);
			id
		}
	};
	
	let category_id = match compression_map.category_id_map.get(&category_id) {
		Some(id) => *id,
		None => {
			let id = compression_map.category_id_count.fetch_add(1, atomic::Ordering::SeqCst) as u16;
			compression_map.category_id_map.insert(category_id, id);
			id
		}
	};
	
	CompressedTag {
		timestamp: chrono::DateTime::parse_from_rfc3339(&tag.time_string).unwrap().timestamp_millis(),
		origin_id,
		brand_id,
		category_id,
		price: price as u32,
		action: UserAction::try_from(&tag.action).unwrap(),
	}
}