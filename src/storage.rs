use std::sync::Arc;

use tokio::sync::mpsc;

use crate::data::*;
use crate::database::Database;

pub struct MinuteData {
	pub product_id: Vec<u16>,
	pub brand_id: Vec<u16>,
	pub category_id: Vec<u16>,
	pub price: Vec<u32>,
	pub action: Vec<UserAction>
}

pub async fn data_saver(_minute_data_ptr: Arc<Database>, _rx: &mut mpsc::Receiver<CompressedTag>) {
	// const LIMIT: usize = 32;
	// loop {
	// 	let mut buffer = Vec::new();
	// 	let _ = rx.recv_many(&mut buffer, LIMIT).await;
	// 	for tag in buffer.iter() {
	// 		minute_data_ptr.entry(tag.timestamp / 60000).or_insert_with(|| MinuteData {
	// 			product_id: Vec::new(),w
	// 			brand_id: Vec::new(),
	// 			category_id: Vec::new(),
	// 			price: Vec::new(),
	// 			action: Vec::new(),
	// 		});
	// 		minute_data_ptr.get_mut(&(tag.timestamp / 60000)).unwrap().product_id.push(tag.origin_id);
	// 		minute_data_ptr.get_mut(&(tag.timestamp / 60000)).unwrap().brand_id.push(tag.brand_id);
	// 		minute_data_ptr.get_mut(&(tag.timestamp / 60000)).unwrap().category_id.push(tag.category_id);
	// 		minute_data_ptr.get_mut(&(tag.timestamp / 60000)).unwrap().price.push(tag.price);
	// 		minute_data_ptr.get_mut(&(tag.timestamp / 60000)).unwrap().action.push(tag.action.clone());
	// 	}
	// }
}