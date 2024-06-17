#[cfg(test)]
mod tests {
	use crate::data::time;

	// use std::env;
	// use std::time::Instant;
	// use aerospike::{as_bin, as_key, as_val, Bins, Client, ClientPolicy, MapPolicy, MapReturnType, ReadPolicy, WritePolicy};
	// use aerospike::operations;
	// use aerospike::operations::maps;
	//
	#[test]
	fn foo() {
		let start = 1646095518261;
		let end = 1646097440049;
		let range = time::TimeRange {
			start,
			end,
		};
		println!("{} {}", time::timestamp_to_str(start), time::timestamp_to_str(end));

		let test = "2022-03-01T00:45:18.261Z";
		let test_parsed = time::parse_timestamp(test).unwrap();
		println!("{} -> {}", test, test_parsed);
		println!("{}", range.within(test_parsed + 1));
		println!("{}", range.within(test_parsed));
		println!("{}", range.within(test_parsed - 1));

		println!("Test {}", time::timestamp_to_str(test_parsed + 1));
		println!("Test {}", time::timestamp_to_str(test_parsed));
		println!("Test {}", time::timestamp_to_str(test_parsed - 1));
	}

	// #[test]
	// fn test_aerospike() {
	// 	let cpolicy = ClientPolicy::default();
	// 	let hosts = env::var("AEROSPIKE_HOSTS")
	// 		.unwrap_or(String::from("127.0.0.1:3000"));
	// 	let client = Client::new(&cpolicy, &hosts)
	// 		.expect("Failed to connect to cluster");
	// 	let now = Instant::now();
	// 	let rpolicy = ReadPolicy::default();
	// 	let wpolicy = WritePolicy::default();
	// 	let key = as_key!("test", "test", "test");
	//
	// 	let bins = [
	// 		as_bin!("int", 999),
	// 		as_bin!("str", "Hello, World!"),
	// 	];
	// 	client.put(&wpolicy, &key, &bins).unwrap();
	// 	let rec = client.get(&rpolicy, &key, Bins::All);
	// 	println!("Record: {}", rec.unwrap());
	//
	// 	client.touch(&wpolicy, &key).unwrap();
	// 	let rec = client.get(&rpolicy, &key, Bins::All);
	// 	println!("Record: {}", rec.unwrap());
	//
	// 	let rec = client.get(&rpolicy, &key, Bins::None);
	// 	println!("Record Header: {}", rec.unwrap());
	//
	// 	let exists = client.exists(&wpolicy, &key).unwrap();
	// 	println!("exists: {}", exists);
	//
	// 	let bin = as_bin!("int", "123");
	// 	let ops = &vec![operations::put(&bin), operations::get()];
	// 	let op_rec = client.operate(&wpolicy, &key, ops);
	// 	println!("operate: {}", op_rec.unwrap());
	//
	// 	let existed = client.delete(&wpolicy, &key).unwrap();
	// 	println!("existed (should be true): {}", existed);
	//
	// 	let existed = client.delete(&wpolicy, &key).unwrap();
	// 	println!("existed (should be false): {}", existed);
	//
	// 	let mpolicy = MapPolicy::default();
	// 	let bin_name = "bin";
	// 	let (k, v) = (as_val!("c"), as_val!(3));
	// 	let op = maps::put(&mpolicy, bin_name, &k, &v);
	// 	let _rec = client.operate(&wpolicy, &key, &[op]).unwrap();
	//
	// 	let key_c: aerospike::Value = as_val!("c");
	// 	let val = maps::get_by_key(bin_name, &key_c, MapReturnType::Value);
	// 	let rec = client.operate(&wpolicy, &key, &[val]).unwrap();
	// 	println!("operate: {}", rec);
	//
	//
	// 	println!("total time: {:?}", now.elapsed());
	// }
}