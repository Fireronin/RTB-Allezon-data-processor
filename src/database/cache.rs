use crate::api::*;
use crate::data::*;
use crate::database::{Compressor, Database, Decompressor};
use crate::endpoints::GetAggregateApiRequest;

pub struct CachedDB<T: Database> {
	remote_db: T,
}

impl<T: Database> Database for CachedDB<T> {
	async fn new() -> Self {
		todo!()
	}
	
	async fn add_user_event(&self, cookie: &Cookie, tag: UserTagEvent, action: UserAction) {
		todo!()
	}
	
	async fn get_user_profile(&self, cookie: &Cookie) -> UserProfile {
		todo!()
	}
	
	async fn add_aggregate_event(&self, timestamp: i64, tag: AggregateTagEvent) {
		todo!()
	}
	
	async fn get_aggregate(&self, request: &GetAggregateRequest) -> GetAggregateResponse {
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

impl<T: Database> Compressor<AggregateTagEvent> for CachedDB<T> {
	async fn compress(&self, value: &ApiUserTag) -> AggregateTagEventCompressedData {
		todo!()
	}
}

impl<T: Database> Compressor<GetAggregateRequest> for CachedDB<T> {
	async fn compress(&self, value: &GetAggregateApiRequest) -> GetAggregateRequestCompressedData {
		todo!()
	}
}