use std::sync::Arc;
use actix_web::{App, HttpServer, web};
use futures::future::join_all;
use tokio::sync::mpsc;

use endpoints::add_user_tags::add_user_tags;
use endpoints::aggregates::aggregates;
use endpoints::user_profiles::user_profiles;
use storage::{CompressedTag, data_saver};

use crate::database::Database;

mod storage;
mod endpoints;
mod database;
mod types;
mod utils;

pub struct AppState {
	pub database: Arc<Database>,
	pub tag_sender: mpsc::Sender<CompressedTag>,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
	let (tx, mut rx) = mpsc::channel::<CompressedTag>(128);
	
	let database = Arc::new(Database::new());
	let database_clone = database.clone();
	
	// split control flow into two tasks
	// 1. to receive compressed tags and store them in memory
	// 2. to serve the HTTP requests
	let server = tokio::spawn(async move {
		HttpServer::new(move || {
			App::new()
				.app_data(web::Data::new(AppState {
					database: database.clone(),
					tag_sender: tx.clone(),
				}))
				.service(add_user_tags)
				.service(user_profiles)
				.service(aggregates)
		}).bind(("10.112.103.101", 8083))
			.expect("Creation of server failed")
			.run()
			.await
			.expect("Execution of server failed")
	});
	let data_saver = tokio::spawn(async move {
		data_saver(database_clone, &mut rx).await;
	});
	
	join_all([server, data_saver]).await;
	
	Ok(())
}

mod tests {
	use std::env;
	use std::time::Instant;
	use aerospike::{as_bin, as_key, as_val, Bins, Client, ClientPolicy, MapPolicy, MapReturnType, ReadPolicy, WritePolicy};
	use aerospike::operations;
	use aerospike::operations::maps;
	
	#[test]
	fn test_aerospike() {
		let cpolicy = ClientPolicy::default();
		let hosts = env::var("AEROSPIKE_HOSTS")
			.unwrap_or(String::from("127.0.0.1:3000"));
		let client = Client::new(&cpolicy, &hosts)
			.expect("Failed to connect to cluster");
		let now = Instant::now();
		let rpolicy = ReadPolicy::default();
		let wpolicy = WritePolicy::default();
		let key = as_key!("test", "test", "test");
		
		let bins = [
			as_bin!("int", 999),
			as_bin!("str", "Hello, World!"),
		];
		client.put(&wpolicy, &key, &bins).unwrap();
		let rec = client.get(&rpolicy, &key, Bins::All);
		println!("Record: {}", rec.unwrap());
		
		client.touch(&wpolicy, &key).unwrap();
		let rec = client.get(&rpolicy, &key, Bins::All);
		println!("Record: {}", rec.unwrap());
		
		let rec = client.get(&rpolicy, &key, Bins::None);
		println!("Record Header: {}", rec.unwrap());
		
		let exists = client.exists(&wpolicy, &key).unwrap();
		println!("exists: {}", exists);
		
		let bin = as_bin!("int", "123");
		let ops = &vec![operations::put(&bin), operations::get()];
		let op_rec = client.operate(&wpolicy, &key, ops);
		println!("operate: {}", op_rec.unwrap());
		
		let existed = client.delete(&wpolicy, &key).unwrap();
		println!("existed (should be true): {}", existed);
		
		let existed = client.delete(&wpolicy, &key).unwrap();
		println!("existed (should be false): {}", existed);
		
		let mpolicy = MapPolicy::default();
		let bin_name = "bin";
		let (k, v) = (as_val!("c"), as_val!(3));
		let op = maps::put(&mpolicy, bin_name, &k, &v);
		let rec = client.operate(&wpolicy, &key, &[op]).unwrap();
		
		let key_c: aerospike::Value = as_val!("c");
		let val = maps::get_by_key(bin_name, &key_c, MapReturnType::Value);
		let rec = client.operate(&wpolicy, &key, &[val]).unwrap();
		println!("operate: {}", rec);
		
		
		println!("total time: {:?}", now.elapsed());
	}
}