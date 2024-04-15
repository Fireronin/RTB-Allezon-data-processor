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


