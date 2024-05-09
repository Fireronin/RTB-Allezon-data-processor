use std::sync::Arc;

use crate::api::*;
use crate::data::*;
use crate::database::{Compressor, Database, Decompressor, PartialCompressor};

pub trait Synced: Send + Sync + 'static {}
pub trait CompressingDB: Synced + PartialCompressor<UserTagEvent> {}
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

impl<L: CompressingDB, T: SyncedDB> Compressor<UserTagEvent> for CachedDB<L, T> {
	async fn compress_with_partial(&self, partial: PartialUserTagEventCompressedData) -> UserTagEventCompressedData {
		todo!()
	}
}

impl<L: CompressingDB, T: SyncedDB> Decompressor<UserTagEvent> for CachedDB<L, T> {
	async fn decompress(&self, value: &UserTagEvent) -> UserTagEventDecompressedData {
		todo!()
	}
}

impl<L: CompressingDB, T: SyncedDB> Compressor<AggregateTagEvent> for CachedDB<L, T> {
	async fn compress_with_partial(&self, partial: PartialAggregateTagEventCompressedData) -> AggregateTagEventCompressedData {
		todo!()
	}
}

impl<L: CompressingDB, T: SyncedDB> Compressor<GetAggregateRequest> for CachedDB<L, T> {
	async fn compress_with_partial(&self, partial: PartialGetAggregateRequestCompressedData) -> GetAggregateRequestCompressedData {
		todo!()
	}
}