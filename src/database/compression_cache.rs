use dashmap::DashMap;
use crate::data::Compression;

#[derive(Default)]
pub struct CompressionMappings {
	pub origin_id_map: DashMap<String, u16>,
	pub brand_id_map: DashMap<String, u16>,
	pub category_id_map: DashMap<String, u16>,
}

impl CompressionMappings {
	pub fn try_compress(&self, origin: Option<&String>, brand: Option<&String>, category: Option<&String>) -> Compression {
		let mut out = Compression::default();
		
		let get_from_local = |store: &DashMap<String, u16>, option: Option<&String>| -> Option<u16> {
			if let Some(str) = option {
				let x =  store.get(str);
				x.map(|v| *v.value())
			} else {
				None
			}
		};
		
		out.origin_id = get_from_local(&self.origin_id_map, origin);
		out.brand_id = get_from_local(&self.brand_id_map, brand);
		out.category_id = get_from_local(&self.category_id_map, category);
		
		out
	}
}
