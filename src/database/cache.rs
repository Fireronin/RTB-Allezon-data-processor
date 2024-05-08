use crate::api::*;
use crate::data::*;
use crate::data::time::TimeRange;
use crate::database::{Compressor, Database, Decompressor};

pub struct CachedDB<T: Database> {
	remote_db: T,
}

impl<T: Database> Database for CachedDB<T> {
	async fn new() -> Self {
		todo!()
	}
	
	async fn add_user_event(&self, request: AddUserProfileRequest) {
		todo!()
	}
	
	async fn get_user_profile(&self, cookie: &Cookie) -> UserProfile {
		todo!()
	}
	
	async fn add_aggregate_event(&self, tag: &AggregateTagEvent) {
		todo!()
	}
	
	async fn get_aggregate(&self, time_range: &TimeRange) -> GetAggregateResponse {
		todo!()
	}
}

impl<T: Database> Compressor<UserTagEvent> for CachedDB<T> {
	async fn compress(&self, value: &ApiUserTag) -> UserTagEventCompressedData {
		todo!()
	}
}

impl<T: Database> Decompressor<UserTagEvent> for CachedDB<T> {
	async fn decompress(&self, value: &UserTagEvent) -> UserTagEventDecompressedData {
		todo!()
	}
}
