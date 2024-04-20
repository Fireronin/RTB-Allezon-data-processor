use std::{sync::atomic};

use actix_web::{web, App, HttpServer};
use dashmap::DashMap;
use tokio::sync::mpsc;
use std::sync::Arc;


mod storage;
pub mod endpoints;

use endpoints::add_user_tags::add_user_tags;
use endpoints::user_profiles::user_profiles;
use endpoints::aggregates::aggregates;
use storage::{CompressionMappings,UserTags,CompressedTag,MinuteData,data_saver};

 
pub struct AppState {
    pub user_tags_views: DashMap<String,UserTags>,
    pub user_tags_buys: DashMap<String,UserTags>,
    pub compression_mappings: CompressionMappings,
    pub tag_sender: mpsc::Sender<CompressedTag>,
    pub minute_data_ptr: Arc<DashMap<i64,MinuteData>>,
}



#[actix_web::main]
async fn main() -> std::io::Result<()> {

    let (tx, mut rx) = mpsc::channel::<CompressedTag>(128);

    let compression_mappings = CompressionMappings{
        origin_id_map: DashMap::new(),
        brand_id_map: DashMap::new(),
        category_id_map: DashMap::new(),
        origin_id_count: Arc::new(atomic::AtomicUsize::new(0)),
        brand_id_count: Arc::new(atomic::AtomicUsize::new(0)),
        category_id_count: Arc::new(atomic::AtomicUsize::new(0)),
    };

    let minute_data = DashMap::new();
    let minute_data_ptr = Arc::new(minute_data);

    let shared_app_state = web::Data::new(AppState {
        user_tags_views: DashMap::new(),
        user_tags_buys: DashMap::new(),
        compression_mappings: compression_mappings,
        minute_data_ptr: minute_data_ptr.clone(),
        tag_sender: tx,
    });

    let server =  HttpServer::new(move || {
        App::new()
            .app_data(shared_app_state.clone())
            .service(add_user_tags)
            .service(user_profiles)
            .service(aggregates)
    })
    .bind(("10.111.255.123", 8082))?;

    // split control flow into two tasks
    // 1. to receive compressed tags and store them in memory
    // 2. to serve the HTTP requests

    // task 1
    tokio::select! {
        _ = server.run() => {},
        _ = async {
            data_saver(minute_data_ptr, &mut rx).await;
        } => {},
    }

    Ok(())

}



#[test]
fn time_parsing() {
let time_range = "2022-03-01T00:00:01.000_2022-03-01T00:00:01.619";
let time_range: Vec<&str> = time_range.split("_").collect();
// add Z to the end of the time string to make it RFC3339 compliant
let time_range: Vec<String> = time_range.iter().map(|x| x.to_string() + "Z").collect();
let start_timestamp = chrono::DateTime::parse_from_rfc3339(time_range[0].as_str()).unwrap().timestamp_millis();
let end_timestamp = chrono::DateTime::parse_from_rfc3339(time_range[1].as_str()).unwrap().timestamp_millis();
println!("start_timestamp: {}", start_timestamp);
println!("end_timestamp: {}", end_timestamp);
assert_eq!(start_timestamp, 1646092801);
assert_eq!(end_timestamp, 1646092801);
}

#[macro_use]
extern crate aerospike;
use std::env;
use std::time::Instant;

use aerospike::{Bins, Client, ClientPolicy, ReadPolicy, WritePolicy, MapPolicy, MapReturnType};
use aerospike::operations::{maps, MapOrder};
use aerospike::operations;

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