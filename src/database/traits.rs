use crate::data::*;
use crate::api::*;
use crate::data::time::TimeRange;

pub trait Database {
	async fn new() -> Self;
	async fn add_user_event(&self, request: AddUserProfileRequest);
	/// Get last MAX_TAGS buy tags and view tags for a given cookie
	async fn get_user_profile(&self, cookie: &Cookie) -> GetUserProfileResponse;
	async fn add_aggregate_event(&self, tag: &AggregateTagEvent);
	async fn get_aggregate(&self, time_range: &TimeRange) -> GetAggregateResponse;
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
