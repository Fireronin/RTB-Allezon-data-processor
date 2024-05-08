use dashmap::DashMap;
use crate::data::{AggregateTagEvent, AggregateCompressedRequest, UserAction, UserTag};
use crate::data::time::TimeRange;
use crate::database::Database;

pub struct LocalDB {
	origin_id_map: DashMap<String, u16>,
	brand_id_map: DashMap<String, u16>,
	category_id_map: DashMap<String, u16>,
}

impl Database for LocalDB {
	async fn new() -> Self {
		todo!()
	}
	
	async fn add_user_event(&self, tag: &UserTag, action: UserAction) {
		todo!()
	}
	
	async fn get_user_profile(&self, cookie: &String) -> (Vec<UserTag>, Vec<UserTag>) {
		todo!()
	}
	
	async fn add_aggregate_event(&self, tag: &UserTag) {
		todo!()
	}
	
	async fn get_aggregate(&self, time_range: &TimeRange) -> Vec<Vec<AggregateTagEvent>> {
		todo!()
	}
	
	async fn compress(&self, origin: Option<&String>, brand: Option<&String>, category: Option<&String>) -> AggregateCompressedRequest {
		let mut out = AggregateCompressedRequest::default();
		
		let get = |store: &DashMap<String, u16>, option: Option<&String>| -> Option<u16> {
			if let Some(str) = option {
				let x =  store.get(str);
				x.map(|v| *v.value())
			} else {
				None
			}
		};
		
		out.origin_id = get(&self.origin_id_map, origin);
		out.brand_id = get(&self.brand_id_map, brand);
		out.category_id = get(&self.category_id_map, category);
		
		out
	}
}