use std::collections::HashMap;
use dashmap::DashMap;
use tokio::sync::RwLock;
use crate::api::*;
use crate::data::*;
use crate::database::{Compressor, Database, Decompressor};

#[derive(Default)]
struct Mapper {
	mapper: RwLock<(HashMap<String, usize>, Vec<String>)>,
}

impl Mapper {
	pub async fn get_or_insert(&self, key: &String) -> usize {
		let mut write_lock = self.mapper.write().await;
		let length = write_lock.1.len();
		let id = *write_lock.0.entry(key.clone()).or_insert(length);
		if id == length {
			write_lock.1.push(key.clone());
		}
		id
	}
	
	pub async fn get(&self, id: usize) -> Option<String> {
		self.mapper.read().await.1.get(id).map(Clone::clone)
	}
	
	pub async fn get_or(&self, id: usize, default: String) -> String {
		self.get(id).await.unwrap_or(default)
	}
}

#[derive(Default)]
struct AggregateHolder {
	start_timestamp: Option<i64>,
	events: Vec<Vec<AggregateTagEvent>>,
}

pub struct LocalDB {
	user_profiles: DashMap<Cookie, UserProfile>,
	aggregates: RwLock<AggregateHolder>,
	
	product_id_map: Mapper,
	origin_id_map: Mapper,
	brand_id_map: Mapper,
	country_id_map: Mapper,
	category_id_map: Mapper,
}

impl Database for LocalDB {
	async fn new() -> Self {
		Self {
			user_profiles: Default::default(),
			aggregates: Default::default(),
			product_id_map: Default::default(),
			origin_id_map: Default::default(),
			brand_id_map: Default::default(),
			country_id_map: Default::default(),
			category_id_map: Default::default(),
		}
	}
	
	async fn add_user_event(&self, cookie: &Cookie, tag: UserTagEvent, action: UserAction) {
		let mut user_profile = self.user_profiles.entry(cookie.clone()).or_insert(UserProfile::default());
		let tags = match action {
			UserAction::VIEW => &mut user_profile.view_events,
			UserAction::BUY => &mut user_profile.buy_events,
		};
		tags.push(tag);
		if tags.len() > MAX_TAGS {
			tags.remove(0);
		}
	}
	
	async fn get_user_profile(&self, cookie: &Cookie) -> UserProfile {
		self.user_profiles.get(cookie)
			.map(|x| {
				let user_profile_ref = x.value();
				user_profile_ref.clone()
			})
			.unwrap_or(UserProfile::default())
	}
	
	async fn add_aggregate_event(&self, timestamp: i64, tag: AggregateTagEvent) {
		let mut writer = self.aggregates.write().await;
		if let None = writer.start_timestamp {
			writer.start_timestamp = Some(timestamp);
		}
		let bucket = (timestamp - writer.start_timestamp.unwrap()) as usize;
		writer.events.get_mut(bucket).unwrap().push(tag);
	}
	
	async fn get_aggregate(&self, request: &GetAggregateRequest) -> GetAggregateResponse {
		todo!()
	}
}

impl Compressor<UserTagEvent> for LocalDB {
	async fn compress(&self, value: &ApiUserTag) -> UserTagEventCompressedData {
		UserTagEventCompressedData {
			product_id: self.product_id_map.get_or_insert(&value.product_info.brand_id).await as u64,
			brand_id: self.brand_id_map.get_or_insert(&value.product_info.brand_id).await as u16,
			category_id: self.category_id_map.get_or_insert(&value.product_info.category_id).await as u16,
			country: self.country_id_map.get_or_insert(&value.country).await as u8,
			origin: self.origin_id_map.get_or_insert(&value.origin).await as u16,
		}
	}
}

impl Decompressor<UserTagEvent> for LocalDB {
	async fn decompress(&self, value: &UserTagEvent) -> UserTagEventDecompressedData {
		UserTagEventDecompressedData {
			product_id: self.product_id_map.get(value.product_id as usize).await.unwrap(),
			brand_id: self.brand_id_map.get(value.brand_id as usize).await.unwrap(),
			category_id: self.category_id_map.get(value.category_id as usize).await.unwrap(),
			country: self.country_id_map.get(value.country as usize).await.unwrap(),
			origin: self.origin_id_map.get(value.origin as usize).await.unwrap(),
		}
	}
}

impl Compressor<AggregateTagEvent> for LocalDB {
	async fn compress(&self, value: &ApiUserTag) -> AggregateTagEventCompressedData {
		AggregateTagEventCompressedData {
			origin_id: self.origin_id_map.get_or_insert(&value.origin).await as u16,
			brand_id: self.origin_id_map.get_or_insert(&value.product_info.brand_id).await as u16,
			category_id: self.origin_id_map.get_or_insert(&value.product_info.category_id).await as u16,
		}
	}
}
