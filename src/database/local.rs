use std::collections::HashMap;
use dashmap::DashMap;
use rayon::prelude::*;
use tokio::sync::RwLock;
use crate::api::*;
use crate::data::*;
use crate::database::{CompressingDB, Compressor, Database, Decompressor, PartialCompressor, Synced};

#[derive(Default)]
pub struct Mapper {
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
	
	pub async fn try_get(&self, key: &String) -> Partial<String, usize> {
		match self.mapper.read().await.0.get(key).map(Clone::clone) {
			Some(v) => Partial::Changed(v),
			None => Partial::Same(key.clone()),
		}
	}
	
	pub async fn get(&self, id: usize) -> Option<String> {
		self.mapper.read().await.1.get(id).map(Clone::clone)
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

impl LocalDB {
	pub fn new() -> Self {
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
}

impl Database for LocalDB {
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
		let reader = self.aggregates.read().await;
		let start = reader.start_timestamp.unwrap();
		let start_bucket = (request.time_range.start - start) as usize;
		let end_bucket = (request.time_range.end - start) as usize;
		
		fn eq_or_true(option: &Option<u16>, value: u16) -> bool {
			option.map(|x| x == value).unwrap_or(true)
		}
		
		let buckets = reader.events.as_slice()[start_bucket..end_bucket]
			.into_par_iter()
			.map(|minute_data|{
				minute_data.iter().fold(AggregateBucket::default(), |mut result, tag| {
					let correct_origin = eq_or_true(&request.origin, tag.origin_id);
					let correct_brand_id = eq_or_true(&request.brand_id, tag.brand_id);
					let correct_category_id = eq_or_true(&request.category_id, tag.category_id);
					let correct_action = tag.action == request.action;
					if correct_origin && correct_brand_id && correct_category_id && correct_action {
						result.count += 1;
						result.sum += tag.price as u64;
					}
					result
				})
			}).collect();
		
		GetAggregateResponse {
			aggregates: buckets,
		}
	}
}

impl Compressor<UserTagEvent> for LocalDB {
	async fn compress_with_partial(&self, partial: PartialUserTagEventCompressedData) -> UserTagEventCompressedData {
		UserTagEventCompressedData {
			product_id: match partial.product_id {
				Partial::Same(x) => self.product_id_map.get_or_insert(&x).await as u64,
				Partial::Changed(x) => x,
			},
			brand_id: match partial.brand_id {
				Partial::Same(x) => self.brand_id_map.get_or_insert(&x).await as u16,
				Partial::Changed(x) => x,
			},
			category_id: match partial.category_id {
				Partial::Same(x) => self.category_id_map.get_or_insert(&x).await as u16,
				Partial::Changed(x) => x,
			},
			country: match partial.country {
				Partial::Same(x) => self.country_id_map.get_or_insert(&x).await as u8,
				Partial::Changed(x) => x,
			},
			origin: match partial.origin {
				Partial::Same(x) => self.origin_id_map.get_or_insert(&x).await as u16,
				Partial::Changed(x) => x,
			},
		}
	}
}

impl PartialCompressor<UserTagEvent> for LocalDB {
	async fn partial_compress_with_partial(&self, partial: PartialUserTagEventCompressedData) -> PartialUserTagEventCompressedData {
		PartialUserTagEventCompressedData {
			product_id: match partial.product_id {
				Partial::Same(x) => self.product_id_map.try_get(&x).await.map_changed(|x| x as u64),
				Partial::Changed(x) => Partial::Changed(x),
			},
			brand_id: match partial.brand_id {
				Partial::Same(x) => self.brand_id_map.try_get(&x).await.map_changed(|x| x as u16),
				Partial::Changed(x) => Partial::Changed(x),
			},
			category_id: match partial.category_id {
				Partial::Same(x) => self.category_id_map.try_get(&x).await.map_changed(|x| x as u16),
				Partial::Changed(x) => Partial::Changed(x),
			},
			country: match partial.country {
				Partial::Same(x) => self.country_id_map.try_get(&x).await.map_changed(|x| x as u8),
				Partial::Changed(x) => Partial::Changed(x),
			},
			origin: match partial.origin {
				Partial::Same(x) => self.origin_id_map.try_get(&x).await.map_changed(|x| x as u16),
				Partial::Changed(x) => Partial::Changed(x),
			},
		}
	}
	
	async fn update_compression(&self, partial: &PartialUserTagEventCompressedData, compressed: &UserTagEventCompressedData) {
		todo!()
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
	async fn compress_with_partial(&self, partial: PartialAggregateTagEventCompressedData) -> AggregateTagEventCompressedData {
		AggregateTagEventCompressedData {
			brand_id: match partial.brand_id {
				Partial::Same(x) => self.brand_id_map.get_or_insert(&x).await as u16,
				Partial::Changed(x) => x,
			},
			category_id: match partial.category_id {
				Partial::Same(x) => self.category_id_map.get_or_insert(&x).await as u16,
				Partial::Changed(x) => x,
			},
			origin_id: match partial.origin_id {
				Partial::Same(x) => self.origin_id_map.get_or_insert(&x).await as u16,
				Partial::Changed(x) => x,
			},
		}
	}
}

impl PartialCompressor<AggregateTagEvent> for LocalDB {
	async fn partial_compress_with_partial(&self, partial: PartialAggregateTagEventCompressedData) -> PartialAggregateTagEventCompressedData {
		todo!()
	}
	
	async fn update_compression(&self, partial: &PartialAggregateTagEventCompressedData, compressed: &AggregateTagEventCompressedData) {
		todo!()
	}
}

impl Compressor<GetAggregateRequest> for LocalDB {
	async fn compress_with_partial(&self, partial: PartialGetAggregateRequestCompressedData) -> GetAggregateRequestCompressedData {
		let origin = match partial.origin {
			Partial::Same(x) => match x {
				Some(v) => Some(self.origin_id_map.get_or_insert(&v).await as u16),
				None => None,
			},
			Partial::Changed(x) => x,
		};
		let brand_id = match partial.brand_id {
			Partial::Same(x) => match x {
				Some(v) => Some(self.brand_id_map.get_or_insert(&v).await as u16),
				None => None,
			},
			Partial::Changed(x) => x,
		};
		let category_id = match partial.category_id {
			Partial::Same(x) => match x {
				Some(v) => Some(self.category_id_map.get_or_insert(&v).await as u16),
				None => None,
			},
			Partial::Changed(x) => x,
		};
		
		GetAggregateRequestCompressedData {
			origin,
			brand_id,
			category_id,
		}
	}
}

impl PartialCompressor<GetAggregateRequest> for LocalDB {
	async fn partial_compress_with_partial(&self, partial: PartialGetAggregateRequestCompressedData) -> PartialGetAggregateRequestCompressedData {
		todo!()
	}
	
	async fn update_compression(&self, partial: &PartialGetAggregateRequestCompressedData, compressed: &GetAggregateRequestCompressedData) {
		todo!()
	}
}

impl Synced for LocalDB {}
impl CompressingDB for LocalDB {}