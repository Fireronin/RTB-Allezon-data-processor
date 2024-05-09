use crate::data::{AggregateTagEvent, Compress, Cookie, Decompress, UserAction, UserProfile, UserTagEvent};
use crate::api::*;

pub trait Database {
	async fn add_user_event(&self, cookie: &Cookie, tag: UserTagEvent, action: UserAction);
	/// Get last MAX_TAGS buy tags and view tags for a given cookie
	async fn get_user_profile(&self, cookie: &Cookie) -> UserProfile;
	async fn add_aggregate_event(&self, timestamp: i64, tag: AggregateTagEvent);
	async fn get_aggregate(&self, request: &GetAggregateRequest) -> GetAggregateResponse;
}

/*
T - type being compressed
 */
pub trait Compressor<T: Compress> {
	async fn compress(&self, value: &T::From) -> T::CompressedData;
}

pub trait Decompressor<T: Decompress> {
	async fn decompress(&self, value: &T) -> T::DecompressedData;
}
