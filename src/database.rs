use std::env;
use aerospike::{as_key, as_val, Client, ClientPolicy, Key, Record, Value, WritePolicy};
use aerospike::operations::{lists, Operation};
use aerospike::operations::lists::{ListOrderType, ListPolicy, ListReturnType, ListWriteFlags};
use crate::types::*;

/*
namespace aero {
	set tags {
		record {
			key -> cookie
			bins -> view_tags: unordered_list, buy_tags: unordered_list
		}
	}
	set minute_tags {
	
	}
	set mappings {
	
	}
}
 */

pub struct Database {
	client: Client,
	write_policy: WritePolicy,
	
	list_policy: ListPolicy,
}

impl Database {
	const NAMESPACE: &'static str = "test";
	const TAG_SET: &'static str = "tags";
	const MINUTE_SET: &'static str = "minute_tags";
	const MAPPINGS_SET: &'static str = "mappings";
	
	const VIEW_BIN: &'static str = "view";
	const BUY_BIN: &'static str = "buy";
	
	pub fn new() -> Self {
		let cpolicy = ClientPolicy::default();
		let hosts = env::var("AEROSPIKE_HOSTS")
			.unwrap_or(String::from("127.0.0.1:3000"));
		Self {
			client: Client::new(&cpolicy, &hosts)
				.expect("Failed to connect to cluster"),
			write_policy: Default::default(),
			list_policy: ListPolicy::new(ListOrderType::Unordered, ListWriteFlags::Default),
		}
	}
	
	fn operate(&self, key: &Key, ops: &[Operation]) -> Record {
		match self.client.operate(&self.write_policy, key, ops) {
			Ok(record) => record,
			Err(err) => panic!("{}", err),
		}
	}
	
	pub fn add_user_tag(&self, tag: UserTag, action: UserAction) {
		let key = as_key!(Self::NAMESPACE, Self::TAG_SET, &tag.cookie);
		let value = as_val!(serde_json::to_string(&tag).unwrap());
		
		let action = match action {
			UserAction::VIEW => Self::VIEW_BIN,
			UserAction::BUY => Self::BUY_BIN,
		};
		let add_operation = lists::append(&self.list_policy, &action, &value);
		
		let result = self.operate(&key, &vec![add_operation]);
		if let Value::Int(count) = result.bins.get(action).unwrap() {
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
			if let Value::List(list) = v.clone() {
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
}
