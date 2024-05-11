use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::database::{Compressor, Decompressor};

#[derive(Deserialize, Serialize, Clone, Debug)]
pub enum Partial<T, V> {
	Same(T),
	Changed(V),
}

impl<T, V> Partial<T, V> {
	pub fn map_same<U, F>(self, f: F) -> Partial<U, V>
		where
			F: FnOnce(T) -> U,
	{
		match self {
			Partial::Same(v) => Partial::Same(f(v)),
			Partial::Changed(v) => Partial::Changed(v),
		}
	}
	
	pub fn map_changed<U, F>(self, f: F) -> Partial<T, U>
		where
			F: FnOnce(V) -> U,
	{
		match self {
			Partial::Same(v) => Partial::Same(v),
			Partial::Changed(v) => Partial::Changed(f(v)),
		}
	}
	
	pub fn is_same(&self) -> bool {
		match &self {
			Partial::Same(_) => true,
			Partial::Changed(_) => false,
		}
	}
	
	pub fn is_changed(&self) -> bool {
		match &self {
			Partial::Same(_) => false,
			Partial::Changed(_) => true,
		}
	}
	
	pub fn change<F>(self, f: F) -> V
		where
			F: FnOnce(T) -> V
	{
		match self {
			Partial::Same(v) => f(v),
			Partial::Changed(v) => v
		}
	}
}

pub trait Compress where Self: Sized, Self::PartialCompressedData: From<Self::From>, Self::From: Clone {
	type From;
	type CompressedData;
	type PartialCompressedData;
	
	async fn compress<T: Compressor<Self>>(value: &Self::From, compressor: &T) -> Result<Self>;
}

pub trait Decompress where Self: Sized, Self::PartialDecompressedData: From<Self>, Self: Clone {
	type Type;
	type DecompressedData;
	type PartialDecompressedData;
	type AdditionalData;
	
	async fn decompress<T: Decompressor<Self>>(&self, decompressor: &T, additional_data: Self::AdditionalData) -> Self::Type;
}
