use std::sync::Arc;

use crate::api::*;
use crate::data::*;
use crate::database::{Compressor, Database, Decompressor, PartialCompressor, PartialDecompressor};

pub trait Synced: Send + Sync + 'static {}
pub trait CompressingDB: Synced {}
pub trait SyncedDB: Synced + Database {}

pub struct CachedDB<L: CompressingDB, T: SyncedDB> {
	local_db: L,
	remote_db: Arc<T>,
}

impl<L: CompressingDB, T: SyncedDB> CachedDB<L, T> {
	pub fn new(local_db: L, remote_db: T) -> Self {
		Self {
			local_db,
			remote_db: Arc::new(remote_db),
		}
	}
}

impl<L: CompressingDB, T: SyncedDB> Database for CachedDB<L, T> {
	async fn add_user_event(&self, cookie: &Cookie, tag: UserTagEvent, action: UserAction) {
		let remote = self.remote_db.clone();
		let cookie = cookie.clone();
		tokio::spawn(async move {
			remote.add_user_event(&cookie.clone(), tag, action).await;
		});
	}
	
	async fn get_user_profile(&self, cookie: &Cookie) -> UserProfile {
		self.remote_db.get_user_profile(cookie).await
	}
	
	async fn add_aggregate_event(&self, timestamp: i64, tag: AggregateTagEvent) {
		let remote = self.remote_db.clone();
		tokio::spawn(async move {
			remote.add_aggregate_event(timestamp, tag).await;
		});
	}
	
	async fn get_aggregate(&self, request: &GetAggregateRequest) -> GetAggregateResponse {
		self.remote_db.get_aggregate(request).await
	}
}

impl<L: CompressingDB + PartialCompressor<UserTagEvent>, T: SyncedDB + Compressor<UserTagEvent>> Compressor<UserTagEvent> for CachedDB<L, T> {
	async fn compress_with_partial(&self, partial: PartialUserTagEventCompressedData) -> UserTagEventCompressedData {
		let compressed_locally = self.local_db.partial_compress_with_partial(partial).await;
		let compressed = self.remote_db.compress_with_partial(compressed_locally.clone()).await;
		self.local_db.update_compression(&compressed_locally, &compressed).await;
		compressed
	}
}

impl<L: CompressingDB + PartialDecompressor<UserTagEvent>, T: SyncedDB + Decompressor<UserTagEvent>> Decompressor<UserTagEvent> for CachedDB<L, T> {
	async fn decompress(&self, value: &UserTagEvent) -> UserTagEventDecompressedData {
		let partial = PartialUserTagEventCompressedData::from(value.clone());
		let decompressed_locally = self.local_db.partial_decompress_with_partial(partial).await;
		let decompressed = self.remote_db.decompress_with_partial(decompressed_locally.clone()).await;
		self.local_db.update_compression(&decompressed_locally, &value).await;
		decompressed
	}
	
	// Does not update local compression database
	async fn decompress_with_partial(&self, partial: PartialUserTagEventCompressedData) -> UserTagEventDecompressedData {
		// let partial = PartialUserTagEventCompressedData::from(value.clone());
		let decompressed_locally = self.local_db.partial_decompress_with_partial(partial).await;
		let decompressed = self.remote_db.decompress_with_partial(decompressed_locally.clone()).await;
		// self.local_db.update_compression(&decompressed_locally, &value).await;
		decompressed
	}
}

impl<L: CompressingDB + PartialCompressor<AggregateTagEvent>, T: SyncedDB + Compressor<AggregateTagEvent>> Compressor<AggregateTagEvent> for CachedDB<L, T> {
	async fn compress_with_partial(&self, partial: PartialAggregateTagEventCompressedData) -> AggregateTagEventCompressedData {
		let compressed_locally = self.local_db.partial_compress_with_partial(partial).await;
		let compressed = self.remote_db.compress_with_partial(compressed_locally.clone()).await;
		self.local_db.update_compression(&compressed_locally, &compressed).await;
		compressed
	}
}

impl<L: CompressingDB + PartialCompressor<GetAggregateRequest>, T: SyncedDB + Compressor<GetAggregateRequest>> Compressor<GetAggregateRequest> for CachedDB<L, T> {
	async fn compress_with_partial(&self, partial: PartialGetAggregateRequestCompressedData) -> GetAggregateRequestCompressedData {
		let compressed_locally = self.local_db.partial_compress_with_partial(partial).await;
		let compressed = self.remote_db.compress_with_partial(compressed_locally.clone()).await;
		self.local_db.update_compression(&compressed_locally, &compressed).await;
		compressed
	}
}