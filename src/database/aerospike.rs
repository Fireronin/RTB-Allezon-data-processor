use std::env;

use aerospike::{as_key, as_val, Client, ClientPolicy, Expiration, Key, Record, Value, WritePolicy};
use aerospike::operations::{lists, Operation};
use aerospike::operations::lists::{ListOrderType, ListPolicy, ListReturnType, ListWriteFlags};
use aerospike::Value::{Int, List};

use crate::api::*;
use crate::data::*;
use crate::data::time::TimeRange;
use crate::database::{Compressor, Database};

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
				product_id: list str
				country: list str
		}
	}
}
*/

pub struct AerospikeDB {
	client: Client,
	write_policy: WritePolicy,
	insert_unique_list_policy: ListPolicy,
	list_policy: ListPolicy,
}

impl AerospikeDB {
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
	const PRODUCT_ID_BIN: &'static str = "product_id";
	const COUNTRY_BIN: &'static str = "country";
	
	const EMPTY_KEY: &'static str = "empty";
	
	fn operate(&self, key: &Key, ops: &[Operation]) -> Record {
		match self.client.operate(&self.write_policy, key, ops) {
			Ok(record) => record,
			Err(err) => panic!("Operation failed {:?}:\n{}", key, err),
		}
	}
	
	fn add_or_get_mapping(&self, value: &String, bin: &'static str, operations: &mut Vec<(&str, Value)>) {
		operations.push((bin, as_val!(value)));
	}
	
	fn retrieve_value_from_mapping_result(bin: &str, result: &Record) -> i64 {
		if let List(results_for_bin) = result.bins.get(bin).expect(format!("No bin named {} found", bin).as_str()) {
			let get_result_value = results_for_bin.get(1).expect(format!("Aerospike fucked up and didn't return index of key for bin {}", bin).as_str());
			if let List(get_result_list) = get_result_value {
				let result_value = get_result_list.get(0).expect(format!("Aerospike fucked up and didn't return index of key for bin {}", bin).as_str());
				if let Int(out) = result_value {
					return *out;
				}
				unreachable!("Aerospike got a mindfuck and returned sth else than an int of return value of a get_by_value (expecting an int index)")
			}
			unreachable!("Aerospike got a mindfuck and returned sth else than a list of return value of a get_by_value (expecting list of indexes)")
		}
		unreachable!("Aerospike got a mindfuck and returned sth else than a list of return values for a list of operations")
	}
}

impl Database for AerospikeDB {
	async fn new() -> Self {
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
		}
	}
	
	async fn add_user_event(&self, request: AddUserProfileRequest) {
		let key = as_key!(Self::NAMESPACE, Self::TAG_SET, &request.cookie.0);
		let value = as_val!(serde_json::to_string(&request.tag).unwrap());
		
		let action = match request.action {
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
	
	async fn get_user_profile(&self, cookie: &Cookie) -> GetUserProfileResponse {
		let key = as_key!(Self::NAMESPACE, Self::TAG_SET, &cookie.0);
		let get_view_operation = lists::get_by_index_range(&Self::VIEW_BIN, 0, ListReturnType::Values);
		let get_buy_operation = lists::get_by_index_range(&Self::BUY_BIN, 0, ListReturnType::Values);
		
		let result = self.operate(&key, &vec![get_view_operation, get_buy_operation]);
		
		let value_to_user_tag = |v| -> Option<UserTagEvent> {
			if let Value::String(str) = v {
				serde_json::from_str(str.as_str()).ok()
			} else {
				None
			}
		};
		let value_to_user_tag_list = |v: &Value| -> Option<Vec<UserTagEvent>> {
			if let List(list) = v.clone() {
				Some(list.into_iter()
					.flat_map(value_to_user_tag)
					.collect())
			} else {
				None
			}
		};
		
		let view_events = result.bins.get(Self::VIEW_BIN)
			.and_then(value_to_user_tag_list)
			.unwrap_or(vec![]);
		let buy_events = result.bins.get(Self::BUY_BIN)
			.and_then(value_to_user_tag_list)
			.unwrap_or(vec![]);
		
		GetUserProfileResponse {
			view_events,
			buy_events,
		}
	}
	
	async fn add_aggregate_event(&self, tag: &AggregateTagEvent) {
		// let key = as_key!(Self::NAMESPACE, Self::MINUTE_SET, Self::EMPTY_KEY);
		// let map_key = as_val!(tag.timestamp / 60000);
		// let compressed_tag = AggregateTagEvent::new(
		// 	self.compress(
		// 		Some(&tag.origin),
		// 		Some(&tag.product_info.brand_id),
		// 		Some(&tag.product_info.category_id)), tag);
		// let value = as_val!(serde_json::to_string(&compressed_tag).unwrap());
		//
		// let context = [ctx_map_key_create(map_key, MapOrder::KeyOrdered)];
		// let add_operation = lists::append(&self.list_policy, Self::TAG_BIN, &value);
		// let add_operation = add_operation.set_context(&context);
		//
		// let _ = self.operate(&key, &vec![add_operation]);
		todo!()
	}
	
	async fn get_aggregate(&self, time_range: &TimeRange) -> GetAggregateResponse {
		// let key = as_key!(Self::NAMESPACE, Self::MINUTE_SET, Self::EMPTY_KEY);
		// let start_key_range = as_val!(time_range.start / 60000);
		// let end_key_range = as_val!(time_range.end / 60000);
		//
		// let get_tags = maps::get_by_key_range(
		// 	&Self::TAG_BIN,
		// 	&start_key_range,
		// 	&end_key_range,
		// 	MapReturnType::Value);
		//
		// let result = self.operate(&key, &vec![get_tags]);
		//
		// let tag_list = result.bins.get(Self::TAG_BIN);
		//
		// println!("Tag list {:?}", tag_list);
		//
		// tag_list
		todo!()
	}
}

impl Compressor<UserTagEvent> for AerospikeDB {
	async fn compress(&self, value: &ApiUserTag) -> UserTagEventCompressedData {
		let key = as_key!(Self::NAMESPACE, Self::MAPPINGS_SET, Self::EMPTY_KEY);

		let mut operation_stuff = vec![];
		self.add_or_get_mapping(&value.product_info.product_id, Self::PRODUCT_ID_BIN, &mut operation_stuff);
		self.add_or_get_mapping(&value.product_info.brand_id, Self::BRAND_ID_BIN, &mut operation_stuff);
		self.add_or_get_mapping(&value.product_info.category_id, Self::CATEGORY_ID_BIN, &mut operation_stuff);
		self.add_or_get_mapping(&value.country, Self::COUNTRY_BIN, &mut operation_stuff);
		self.add_or_get_mapping(&value.origin, Self::ORIGIN_ID_BIN, &mut operation_stuff);
		
		let mut operations = vec![];
		for (bin, value) in operation_stuff.iter() {
			operations.push(lists::append(&self.insert_unique_list_policy, bin, value));
			operations.push(lists::get_by_value(bin, value, ListReturnType::Index))
		}
		
		let result = self.operate(&key, &operations);
		
		UserTagEventCompressedData {
			product_id: AerospikeDB::retrieve_value_from_mapping_result(Self::PRODUCT_ID_BIN, &result) as u64,
			brand_id: AerospikeDB::retrieve_value_from_mapping_result(Self::BRAND_ID_BIN, &result) as u16,
			category_id: AerospikeDB::retrieve_value_from_mapping_result(Self::CATEGORY_ID_BIN, &result) as u16,
			country: AerospikeDB::retrieve_value_from_mapping_result(Self::COUNTRY_BIN, &result) as u8,
			origin: AerospikeDB::retrieve_value_from_mapping_result(Self::ORIGIN_ID_BIN, &result) as u16,
		}
	}
}
