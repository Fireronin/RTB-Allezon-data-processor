use std::env;

use aerospike::{as_key, as_val, Client, ClientPolicy, Expiration, Key, MapReturnType, Record, Value, WritePolicy};
use aerospike::operations::{lists, MapOrder, maps, Operation};
use aerospike::operations::cdt_context::ctx_map_key_create;
use aerospike::operations::lists::{ListOrderType, ListPolicy, ListReturnType, ListWriteFlags};
use aerospike::Value::{Int, List};
use dashmap::DashMap;

use crate::data::*;
use crate::data::time::TimeRange;

/*
namespace aero {
	set tags {
		record {
			key: cookie
			bins:
				view_tags: unordered_list
				buy_tags: unordered_list
		}
	}
	set minute_tags {
		record {
			key: none
			bins:
				tags: map minute->unordered_list
		}
	}
	set mappings {
		record {
			key: none
			bins:
				origin_id: list str
				brand_id: list str
				category_id: list str
		}
	}
}
*/

#[derive(Default)]
struct CompressionMappings {
	pub origin_id_map: DashMap<String, u16>,
	pub brand_id_map: DashMap<String, u16>,
	pub category_id_map: DashMap<String, u16>,
}

pub struct Database {
	client: Client,
	write_policy: WritePolicy,
	insert_unique_list_policy: ListPolicy,
	list_policy: ListPolicy,
	
	local_mappings_cache: CompressionMappings,
}

impl Database {
	const NAMESPACE: &'static str = "test";
	// tags
	const TAG_SET: &'static str = "tags";
	const VIEW_BIN: &'static str = "view";
	const BUY_BIN: &'static str = "buy";
	// minute tags
	const MINUTE_SET: &'static str = "minute_tags";
	const TAG_BIN: &'static str = "tags";
	// mappings
	const MAPPINGS_SET: &'static str = "mappings";
	const ORIGIN_ID_BIN: &'static str = "origin_id";
	const BRAND_ID_BIN: &'static str = "brand_id";
	const CATEGORY_ID_BIN: &'static str = "category_id";
	
	const EMPTY_KEY: &'static str = "empty";
	
	pub fn new() -> Self {
		let client_policy = ClientPolicy::default();
		let hosts = env::var("AEROSPIKE_HOSTS")
			.unwrap_or(String::from("127.0.0.1:3000"));
		Self {
			client: Client::new(&client_policy, &hosts)
				.expect("Failed to connect to cluster"),
			write_policy: WritePolicy {
				base_policy: Default::default(),
				record_exists_action: Default::default(),
				generation_policy: Default::default(),
				commit_level: Default::default(),
				generation: 0,
				expiration: Expiration::NamespaceDefault,
				send_key: false,
				respond_per_each_op: true,
				durable_delete: false,
				filter_expression: None,
			},
			insert_unique_list_policy: ListPolicy {
				attributes: ListOrderType::Unordered,
				flags: ListWriteFlags::AddUnique,
			},
			list_policy: ListPolicy::new(ListOrderType::Unordered, ListWriteFlags::Default),
			local_mappings_cache: CompressionMappings::default(),
		}
	}
	
	fn operate(&self, key: &Key, ops: &[Operation]) -> Record {
		match self.client.operate(&self.write_policy, key, ops) {
			Ok(record) => record,
			Err(err) => panic!("Operation failed {:?}:\n{}", key, err),
		}
	}
	
	pub fn add_user_tag(&self, tag: &UserTag, action: UserAction) {
		let key = as_key!(Self::NAMESPACE, Self::TAG_SET, &tag.cookie);
		let value = as_val!(serde_json::to_string(&tag).unwrap());
		
		let action = match action {
			UserAction::VIEW => Self::VIEW_BIN,
			UserAction::BUY => Self::BUY_BIN,
		};
		let add_operation = lists::append(&self.list_policy, &action, &value);
		
		let result = self.operate(&key, &vec![add_operation]);
		if let Int(count) = result.bins.get(action).unwrap() {
			if *count as usize > MAX_TAGS {
				let remove_first_operation = lists::remove_by_index(&action, 0, ListReturnType::None);
				self.operate(&key, &vec![remove_first_operation]);
			}
		}
	}
	
	pub fn get_tags(&self, cookie: &String) -> (Vec<UserTag>, Vec<UserTag>) {
		let key = as_key!(Self::NAMESPACE, Self::TAG_SET, cookie);
		let get_view_operation = lists::get_by_index_range(&Self::VIEW_BIN, 0, ListReturnType::Values);
		let get_buy_operation = lists::get_by_index_range(&Self::BUY_BIN, 0, ListReturnType::Values);
		
		let result = self.operate(&key, &vec![get_view_operation, get_buy_operation]);
		
		let value_to_user_tag = |v| -> Option<UserTag> {
			if let Value::String(str) = v {
				serde_json::from_str(str.as_str()).ok()
			} else {
				None
			}
		};
		let value_to_user_tag_list = |v: &Value| -> Option<Vec<UserTag>> {
			if let List(list) = v.clone() {
				Some(list.into_iter()
					.flat_map(value_to_user_tag)
					.collect())
			} else {
				None
			}
		};
		
		let view_list = result.bins.get(Self::VIEW_BIN)
			.and_then(value_to_user_tag_list)
			.unwrap_or(vec![]);
		let buy_list = result.bins.get(Self::BUY_BIN)
			.and_then(value_to_user_tag_list)
			.unwrap_or(vec![]);

		(view_list, buy_list)
	}
	
	pub fn add_minute(&self, tag: UserTag) {
		let key = as_key!(Self::NAMESPACE, Self::MINUTE_SET, Self::EMPTY_KEY);
		let map_key = as_val!(tag.time / 60000);
		let compressed_tag = CompressedTag::new(
			self.compress(
				Some(&tag.origin),
				Some(&tag.product_info.brand_id),
				Some(&tag.product_info.category_id)), tag);
		let value = as_val!(serde_json::to_string(&compressed_tag).unwrap());
		
		let context = [ctx_map_key_create(map_key, MapOrder::KeyOrdered)];
		let add_operation = lists::append(&self.list_policy, Self::TAG_BIN, &value);
		let add_operation = add_operation.set_context(&context);
		
		let _ = self.operate(&key, &vec![add_operation]);
	}
	
	pub fn get_minute_aggregate(&self, time_range: &TimeRange) -> Vec<Vec<CompressedTag>> {
		let key = as_key!(Self::NAMESPACE, Self::MINUTE_SET, Self::EMPTY_KEY);
		let start_key_range = as_val!(time_range.start / 60000);
		let end_key_range = as_val!(time_range.end / 60000);
		
		let get_tags = maps::get_by_key_range(
			&Self::TAG_BIN,
			&start_key_range,
			&end_key_range,
			MapReturnType::Value);
		
		let result = self.operate(&key, &vec![get_tags]);
		
		let tag_list = result.bins.get(Self::TAG_BIN);
		
		println!("Tag list {:?}", tag_list);
		
		vec![]
	}
	
	pub fn compress(&self, origin: Option<&String>, brand: Option<&String>, category: Option<&String>) -> Compression {
		let mut out = Compression::default();
		
		let get_from_local = |store: &DashMap<String, u16>, option: Option<&String>| -> Option<u16> {
			if let Some(str) = option {
				let x =  store.get(str);
				x.map(|v| *v.value())
			} else {
				None
			}
		};

		out.origin_id = get_from_local(&self.local_mappings_cache.origin_id_map, origin);
		out.brand_id = get_from_local(&self.local_mappings_cache.brand_id_map, brand);
		out.category_id = get_from_local(&self.local_mappings_cache.category_id_map, category);
		
		let key = as_key!(Self::NAMESPACE, Self::MAPPINGS_SET, Self::EMPTY_KEY);
		let mut operations = vec![];

		let mut keys = vec![];
		let mut add_or_get_from_remote = |option: &Option<&String>, bin: &'static str| {
			if let &Some(v) = option {
				keys.push((as_val!(v.clone()), bin));
			}
		};
		
		if out.origin_id.is_none() {
			add_or_get_from_remote(&origin, Self::ORIGIN_ID_BIN);
		}
		if out.brand_id.is_none() {
			add_or_get_from_remote(&brand, Self::BRAND_ID_BIN);
		}
		if out.category_id.is_none() {
			add_or_get_from_remote(&category, Self::CATEGORY_ID_BIN);
		}
		
		for (key, bin) in keys.iter() {
			operations.push(lists::append(&self.insert_unique_list_policy, bin, key));
			operations.push(lists::get_by_value(bin, key, ListReturnType::Index))
		}
		
		if !operations.is_empty() {
			let result = self.operate(&key, &operations);
			let retrieve_value_from_result = |bin| -> u16 {
				if let List(results_for_bin) = result.bins.get(bin).expect(format!("No bin named {} found", bin).as_str()) {
					let get_result_value = results_for_bin.get(1).expect(format!("Aerospike fucked up and didn't return index of key for bin {}", bin).as_str());
					if let List(get_result_list) = get_result_value {
						let result_value = get_result_list.get(0).expect(format!("Aerospike fucked up and didn't return index of key for bin {}", bin).as_str());
						if let Int(out) = result_value {
							return *out as u16;
						}
						unreachable!("Aerospike got a mindfuck and returned sth else than an int of return value of a get_by_value (expecting an int index)")
					}
					unreachable!("Aerospike got a mindfuck and returned sth else than a list of return value of a get_by_value (expecting list of indexes)")
				}
				unreachable!("Aerospike got a mindfuck and returned sth else than a list of return values for a list of operations")
			};
			
			if let Some(origin) = origin {
				if out.origin_id.is_none() {
					let v = retrieve_value_from_result(Self::ORIGIN_ID_BIN);
					out.origin_id = Some(v);
					self.local_mappings_cache.origin_id_map.insert(origin.clone(), v);
				}
			}
			if let Some(brand) = brand {
				if out.brand_id.is_none() {
					let v = retrieve_value_from_result(Self::BRAND_ID_BIN);
					out.brand_id = Some(v);
					self.local_mappings_cache.brand_id_map.insert(brand.clone(), v);
				}
			}
			if let Some(category) = category {
				if out.category_id.is_none() {
					let v = retrieve_value_from_result(Self::CATEGORY_ID_BIN);
					out.category_id = Some(v);
					self.local_mappings_cache.category_id_map.insert(category.clone(), v);
				}
			}
		}
		
		out
	}
}
