use anyhow::Result;
use crate::database::{Compressor, Decompressor};

pub struct CompressionError(String);

impl From<chrono::ParseError> for CompressionError {
	fn from(value: chrono::ParseError) -> Self {
		Self(value.to_string())
	}
}

pub trait Compress where Self: Sized {
	type From;
	type CompressedData;
	
	async fn compress<T: Compressor<Self>>(value: &Self::From, compressor: &T) -> Result<Self>;
}

pub trait Decompress where Self: Sized {
	type Type;
	type DecompressedData;
	type AdditionalData;
	
	async fn decompress<T: Decompressor<Self>>(&self, decompressor: &T, additional_data: Self::AdditionalData) -> Self::Type;
}
