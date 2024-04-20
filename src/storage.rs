use std::{cmp::min, sync::atomic};
use std::sync::Arc;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

pub const MAX_TAGS: usize = 200;

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct UserTag {
	pub product_info: ProductInfo,
	pub time: String,
	pub cookie: String,
	pub country: String,
	pub device: String,
	pub action: String,
	pub origin: String,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct ProductInfo {
	product_id: u64,
	brand_id: String,
	category_id: String,
	price: i32,
}

#[derive(Debug)]
pub struct UserTags {
	current_id: usize,
	timestamps: Vec<i64>,
	tags: Vec<UserTag>,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub enum UserTagType {
	VIEW,
	BUY,
}

fn get_user_tag_type(action: &str) -> UserTagType {
	match action {
		"VIEW" => UserTagType::VIEW,
		"BUY" => UserTagType::BUY,
		_ => panic!("Invalid action"),
	}
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct CompressedTag {
	timestamp: i64,
	origin_id: u16,
	brand_id: u16,
	category_id: u16,
	price: u32,
	action: UserTagType
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
		timestamp: chrono::DateTime::parse_from_rfc3339(&tag.time).unwrap().timestamp_millis(),
		origin_id,
		brand_id,
		category_id,
		price: price as u32,
		action: get_user_tag_type(&tag.action),
	}
}

pub struct MinuteData {
	pub product_id: Vec<u16>,
	pub brand_id: Vec<u16>,
	pub category_id: Vec<u16>,
	pub price: Vec<u32>,
	pub action: Vec<UserTagType>
}


pub async fn add_user_tag(user_tags: &mut UserTags, tag: UserTag, timestamp: i64) {
	if user_tags.current_id < MAX_TAGS {
		user_tags.tags.push(tag);
		user_tags.timestamps.push(timestamp);
		user_tags.current_id += 1;
	} else {
		let index = user_tags.current_id;
		user_tags.tags[index] = tag;
		user_tags.timestamps[index] = timestamp;
		user_tags.current_id += 1;
	}
	if user_tags.current_id >= MAX_TAGS {
		user_tags.current_id = 0;
	}
}

pub async fn get_user_tags(user_tags: &UserTags, start_timestamp: i64, end_timestamp: i64, limit: Option<usize>) -> Vec<UserTag> {
	let mut tags = Vec::new();
	let mut count = 0;
	// iterate in reverse order from current_id to 0 in loop 1 and from MAX_TAGS to current_id+1 in loop 2
	// create combined iterator current_id to 0 and MAX_TAGS to current_id+1
	let iter = (0..user_tags.current_id).rev().chain((user_tags.current_id..min(MAX_TAGS, user_tags.tags.len())).rev());
	for i in iter {
		if count >= limit.unwrap_or(MAX_TAGS) {
			break;
		}
		if user_tags.timestamps[i] >= start_timestamp && user_tags.timestamps[i] < end_timestamp {
			tags.push(user_tags.tags[i].clone());
			count += 1;
		}
	}
	tags
}

impl Default for UserTags {
	fn default() -> Self {
		UserTags {
			current_id: 0,
			tags: Vec::new(),
			timestamps: Vec::new(),
		}
	}
}


pub async fn data_saver(minute_data_ptr: Arc<DashMap<i64, MinuteData>>, rx: &mut mpsc::Receiver<CompressedTag>) {
	const LIMIT: usize = 32;
	loop {
		let mut buffer = Vec::new();
		let _ = rx.recv_many(&mut buffer, LIMIT).await;
		for tag in buffer.iter() {
			minute_data_ptr.entry(tag.timestamp / 60000).or_insert_with(|| MinuteData {
				product_id: Vec::new(),
				brand_id: Vec::new(),
				category_id: Vec::new(),
				price: Vec::new(),
				action: Vec::new(),
			});
			minute_data_ptr.get_mut(&(tag.timestamp / 60000)).unwrap().product_id.push(tag.origin_id);
			minute_data_ptr.get_mut(&(tag.timestamp / 60000)).unwrap().brand_id.push(tag.brand_id);
			minute_data_ptr.get_mut(&(tag.timestamp / 60000)).unwrap().category_id.push(tag.category_id);
			minute_data_ptr.get_mut(&(tag.timestamp / 60000)).unwrap().price.push(tag.price);
			minute_data_ptr.get_mut(&(tag.timestamp / 60000)).unwrap().action.push(tag.action.clone());
			//println!("Received tag: {:?}", tag);
		}
	}
}