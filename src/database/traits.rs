use std::future::Future;
use crate::data::{AggregateTagEvent, Compress, Cookie, Decompress, UserAction, UserProfile, UserTagEvent};
use crate::api::*;

pub trait Database {
	fn add_user_event(&self, cookie: &Cookie, tag: UserTagEvent, action: UserAction) -> impl Future<Output = ()> + Send;
	/// Get last MAX_TAGS buy tags and view tags for a given cookie
	fn get_user_profile(&self, cookie: &Cookie) -> impl Future<Output = UserProfile> + Send;
	fn add_aggregate_event(&self, timestamp: i64, tag: AggregateTagEvent) -> impl Future<Output = ()> + Send;
	fn get_aggregate(&self, request: &GetAggregateRequest) -> impl Future<Output = GetAggregateResponse> + Send;
}

pub trait Compressor<T: Compress> where T::From: Clone {
	async fn compress(&self, value: &T::From) -> T::CompressedData {
		self.compress_with_partial(T::PartialCompressedData::from(value.clone())).await
	}
	
	async fn compress_with_partial(&self, partial: T::PartialCompressedData) -> T::CompressedData;
}

pub trait PartialCompressor<T: Compress> {
	async fn partial_compress(&self, value: &T::From) -> T::PartialCompressedData;
}

pub trait Decompressor<T: Decompress> {
	async fn decompress(&self, value: &T) -> T::DecompressedData;
}
