use std::{cmp::min, ops::Range, option, sync::atomic};

use actix_web::{post, web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use serde::{Deserialize, Serialize};
use dashmap::DashMap;
use tokio::sync::mpsc;
use std::sync::Arc;
use rayon::prelude::*;
use serde_querystring::DuplicateQS;
use serde_querystring_actix::QueryString;
#[derive(Deserialize, Serialize,Clone,Debug)]
struct UserTagRequest {
    product_info: ProductInfo,
    time: String,
    cookie: String,
    country: String,
    device: String,
    action: String,
    origin: String,   
}

#[derive(Deserialize, Serialize,Clone,Debug)]
struct ProductInfo {
    product_id: u64,
    brand_id: String,
    category_id: String,
    price: i32,
}

#[derive(Debug,Clone)]
struct UserTags {
    current_id: usize,
    timestamps: Vec<i64>,
    tags: Vec<UserTagRequest>,
}

#[derive(Deserialize, Serialize,Clone,Debug,PartialEq)]
enum UserTagType {
    VIEW,
    BUY,
}

fn get_user_tag_type (action: &str) -> UserTagType {
    match action {
        "VIEW" => UserTagType::VIEW,
        "BUY" => UserTagType::BUY,
        _ => panic!("Invalid action"),
    }
}

#[derive(Deserialize, Serialize,Clone,Debug)]
struct CompressedTag {
    timestamp: i64,
    origin_id: u16,
    brand_id: u16,
    category_id: u16,
    price: u32,
    action: UserTagType
}


#[derive(Clone,Debug)]
struct CompressionMappings {
    origin_id_map: DashMap<String,u16>,
    brand_id_map: DashMap<String,u16>,
    category_id_map: DashMap<String,u16>,
    origin_id_count: Arc<atomic::AtomicUsize>,
    brand_id_count: Arc<atomic::AtomicUsize>,
    category_id_count: Arc<atomic::AtomicUsize>,
}

fn compress_tag (tag: &UserTagRequest, compression_map: &CompressionMappings) -> CompressedTag {
    let origin_id = tag.origin.clone();
    let brand_id = tag.product_info.brand_id.clone();
    let category_id = tag.product_info.category_id.clone();
    let price = tag.product_info.price;

    let origin_id = match compression_map.origin_id_map.get(&origin_id) {
        Some(id) => *id,
        None => {
            let id = compression_map.origin_id_count.fetch_add(1, atomic::Ordering::SeqCst) as u16;
            compression_map.origin_id_map.insert(origin_id, id);
            id
        }
    };

    let brand_id = match compression_map.brand_id_map.get(&brand_id) {
        Some(id) => *id,
        None => {
            let id = compression_map.brand_id_count.fetch_add(1, atomic::Ordering::SeqCst) as u16;
            compression_map.brand_id_map.insert(brand_id, id);
            id
        }
    };

    let category_id = match compression_map.category_id_map.get(&category_id) {
        Some(id) => *id,
        None => {
            let id = compression_map.category_id_count.fetch_add(1, atomic::Ordering::SeqCst) as u16;
            compression_map.category_id_map.insert(category_id, id);
            id
        }
    };

    CompressedTag {
        timestamp: chrono::DateTime::parse_from_rfc3339(&tag.time).unwrap().timestamp_millis(),
        origin_id,
        brand_id,
        category_id,
        price: price as u32,
        action: get_user_tag_type(&tag.action),
    }
}

struct MinuteData {
    product_id: Vec<u16>,
    brand_id: Vec<u16>,
    category_id: Vec<u16>,
    price: Vec<u32>,
    action: Vec<UserTagType>
}


const MAX_TAGS: usize = 200;

fn add_user_tag (user_tags: &mut UserTags, tag: UserTagRequest, timestamp: i64) {
    if user_tags.current_id < MAX_TAGS {
        user_tags.tags.push(tag);
        user_tags.timestamps.push(timestamp);
        user_tags.current_id += 1;
    } else {
        let index = user_tags.current_id;
        user_tags.tags[index] = tag;
        user_tags.timestamps[index] = timestamp;
        user_tags.current_id += 1;
    }
    if user_tags.current_id >= MAX_TAGS {
        user_tags.current_id = 0;
    }
    // println!("current_id: {}", user_tags.current_id);
    // println!("tags: {:?}", user_tags.tags);

}

fn get_user_tags (user_tags: &UserTags, start_timestamp: i64, end_timestamp: i64, limit: option::Option<usize>) -> Vec<UserTagRequest> {
    let mut tags = Vec::new();
    let mut count = 0;
    // iterate in reverse order from current_id to 0 in loop 1 and from MAX_TAGS to current_id+1 in loop 2
    // create combined iterator current_id to 0 and MAX_TAGS to current_id+1
    let iter = (0..user_tags.current_id).rev().chain((user_tags.current_id..(min(MAX_TAGS,user_tags.tags.len()))).rev());
    for i in iter {
        if count >= limit.unwrap_or(MAX_TAGS) {
            break;
        }
        if user_tags.timestamps[i] >= start_timestamp && user_tags.timestamps[i] <= end_timestamp {
            tags.push(user_tags.tags[i].clone());
            count += 1;
        }
    }
    tags
}

impl Default for UserTags {
    fn default() -> Self {
        UserTags {
            current_id: 0,
            tags: Vec::new(),
            timestamps: Vec::new(),
        }
    }
}

#[post("/user_tags")]
async fn add_user_tags(data: web::Data<AppState>, req_body: String) -> impl Responder {
    //println!("~~~~~~~~~~~~~~ User Tags ~~~~~~~~~~~~~~");
    // print error during deserialization
    let user_tag_result = serde_json::from_str(&req_body);
    if user_tag_result.is_err() {
        print!("Invalid JSON: {}", req_body);
        return HttpResponse::BadRequest().body("Invalid JSON");
    }
    let user_tag: UserTagRequest = user_tag_result.unwrap();
        
    let action = &user_tag.action;
    let cookie = &user_tag.cookie;
    let timestamp = chrono::DateTime::parse_from_rfc3339(&user_tag.time).unwrap().timestamp_millis();

    fn add_to_map (user_tags_map: &DashMap<String,UserTags>, cookie: &str, user_tag: &UserTagRequest, timestamp: i64) {
        let mut user_tags_map = user_tags_map.entry(cookie.to_string()).or_insert_with(UserTags::default);
        add_user_tag(&mut user_tags_map, user_tag.clone(), timestamp);
    }

    if action == "VIEW" {
        add_to_map(&data.user_tags_views, cookie, &user_tag, timestamp);
    } else if action == "BUY" {
        add_to_map(&data.user_tags_buys, cookie, &user_tag, timestamp);
    }else {
        return HttpResponse::BadRequest().body("Invalid action");
    }

    match data.tag_sender.send(compress_tag(&user_tag, &data.compression_mappings)).await {
        Ok(_) => {},
        Err(_) => {
            println!("Error sending tag to channel");
        }
    } 

    HttpResponse::Ok().into()
    
}



#[derive(Deserialize, Serialize)]
struct UserProfileRequest {
    time_range: String,
    limit: Option<i32>
}

#[derive(Deserialize, Serialize)]
struct UserProfileResponse {
    cookie: String,
    views: Vec<UserTagRequest>,
    buys: Vec<UserTagRequest>,
}

#[post("/user_profiles/{cookie}")]
async fn user_profiles(data: web::Data<AppState>, _req_body: String, cookie: web::Path<String>,info: web::Query<UserProfileRequest>) -> impl Responder {
    // println!("~~~~~~~~~~~~~~ User Profiles ~~~~~~~~~~~~~~");
    // println!("req_body: {}", req_body);
    // println!("cookie: {}", cookie);
    // println!("time_range: {}", info.time_range);
    // println!("limit: {}", info.limit.unwrap_or(-1));

    let cookie = cookie.into_inner();

    let limit = match info.limit {
        Some(limit) => limit as usize,
        None => MAX_TAGS,
    };


    // split the time_range into start and end and parse it 2022-03-01T00:00:01.000_2022-03-01T00:00:01.619
    let time_range: Vec<&str> = info.time_range.split("_").collect();
    // add Z to the end of the time string to make it RFC3339 compliant
    let time_range: Vec<String> = time_range.iter().map(|x| x.to_string() + "Z").collect();
    let start_timestamp = chrono::DateTime::parse_from_rfc3339(time_range[0].as_str()).unwrap().timestamp_millis();
    let end_timestamp = chrono::DateTime::parse_from_rfc3339(time_range[1].as_str()).unwrap().timestamp_millis();


    // get the user tags
    let user_tags_views = data.user_tags_views.get(&cookie);
    let user_tags_buys = data.user_tags_buys.get(&cookie);

    let tags_views = match user_tags_views {
        Some(user_tags) => get_user_tags(&user_tags, start_timestamp, end_timestamp, Some(limit)),
        None => Vec::new(),
    };

    let tags_buys = match user_tags_buys {
        Some(user_tags) => get_user_tags(&user_tags, start_timestamp, end_timestamp, Some(limit)),
        None => Vec::new(),
    };

    let response = UserProfileResponse {
        cookie: cookie.clone(),
        views: tags_views,
        buys: tags_buys,
    };


    HttpResponse::Ok().json(response)
}




// #[derive(PartialEq)]
// enum AggregateType {
//     Count,
//     Sum,
// }

struct SpoolingResult {
    count: u64,
    sum: u64,
}

fn spool<const ORIGIN : bool, const BRAND_ID: bool, const CATEGORY_ID: bool> 
    (data: &Arc<DashMap<i64,MinuteData>>,
        minutes_to_process : Range<i64>,
        action: &UserTagType, 
        origin: u16, 
        brand_id: u16, 
        category_id: u16) -> Vec<SpoolingResult> {


    let par_itter = rayon::iter::IntoParallelIterator::into_par_iter(minutes_to_process);
    
    let result = par_itter.map(|minute| {
        let minute_data = data.get(&minute);
        // result 
        if minute_data.is_none() {
            return SpoolingResult {
                count: 0,
                sum: 0,
            };
        }
        let minute_data = minute_data.unwrap();
        let mut count: u64 = 0;
        let mut sum: u64 = 0;
        for i in 0..minute_data.product_id.len() {
            let correct_origin = !ORIGIN || (ORIGIN && minute_data.product_id[i] == origin);
            let correct_brand_id = !BRAND_ID || (BRAND_ID && minute_data.brand_id[i] == brand_id);
            let correct_category_id = !CATEGORY_ID || (CATEGORY_ID && minute_data.category_id[i] == category_id);
            let correct_action = minute_data.action[i] == *action;
            if correct_origin && correct_brand_id && correct_category_id && correct_action {
                count += 1;
                sum += minute_data.price[i] as u64;
            }
            
        }
        SpoolingResult {
            count,
            sum,
        }
        
    }).collect();
    

    result
}


#[derive(Deserialize, Serialize)]
struct AggregateResponse {
    columns: Vec<String>,
    rows: Vec<Vec<String>>,
}

#[derive(Deserialize, Serialize,Debug,Clone)]
enum AggregatesRequestEnum {
    Single(String),
    Multiple(Vec<String>),
}

#[derive(Deserialize, Serialize,Clone,Debug)]
struct AggregateRequest {
    time_range: String,
    action: String,
    origin: Option<String>,
    brand_id: Option<String>,
    category_id: Option<String>,
}

#[post("/aggregates")]
async fn aggregates(data: web::Data<AppState>, req_body: String,info: web::Query<AggregateRequest>,req: HttpRequest) -> impl Responder {
    println!("~~~~~~~~~~~~~~ Aggregates ~~~~~~~~~~~~~~");
    println!("time_range: {}", info.time_range);
    println!("action: {}", info.action);
    
    println!("origin: {:?}", info.origin);
    println!("brand_id: {:?}", info.brand_id);
    println!("category_id: {:?}", info.category_id);
    println!("req_body: {}", req_body);
    let parsed = DuplicateQS::parse(req.query_string().as_bytes());
    let values = parsed.values("aggregates".as_bytes()); // Will give you a vector of b"bar" and b"baz"
    if values.is_none() {
        println!("Invalid aggregates");
        return HttpResponse::BadRequest().body("Invalid aggregates");
    }

    let aggregates_vec = values.unwrap().iter().map(|x| std::str::from_utf8(&x.clone().unwrap()).unwrap().to_string()).collect::<Vec<String>>();
    println!("aggregates: {:?}", aggregates_vec);
    let time_range: Vec<&str> = info.time_range.split("_").collect();
    // add Z to the end of the time string to make it RFC3339 compliant
    let time_range: Vec<String> = time_range.iter().map(|x| x.to_string() + "Z").collect();
    let start_timestamp = chrono::DateTime::parse_from_rfc3339(time_range[0].as_str()).unwrap().timestamp_millis();
    let end_timestamp = chrono::DateTime::parse_from_rfc3339(time_range[1].as_str()).unwrap().timestamp_millis();

    // divide the time range into minutes
    let start_minute = start_timestamp/60000;
    let end_minute = end_timestamp/60000;

    // let  = match info.aggregates.clone() {
    //     AggregatesRequestEnum::Single(aggregate) => vec![aggregate],
    //     AggregatesRequestEnum::Multiple(aggregates) => aggregates,
    // };

    let minutes_to_process = start_minute..end_minute;

    let action = &info.action;
    let origin = match &info.origin {
        Some(origin) => data.compression_mappings.origin_id_map.get(origin).unwrap().clone(),
        None => 0,
    };
    let brand_id = match &info.brand_id {
        Some(brand_id) => data.compression_mappings.brand_id_map.get(brand_id).unwrap().clone(),
        None => 0,
    };
    let category_id = match &info.category_id {
        Some(category_id) => data.compression_mappings.category_id_map.get(category_id).unwrap().clone(),
        None => 0,
    };
    
    let process_origin = info.origin.is_some();
    let process_brand_id = info.brand_id.is_some();
    let process_category_id = info.category_id.is_some();



    let aggregate_requests =aggregates_vec.iter().map(|x| match x.as_str() {
        "COUNT" => false,
        "SUM_PRICE" => true,
        _ => panic!("Invalid aggregate type"),
        }).collect::<Vec<bool>>();
        
    

    let action = match action.as_str() {
        "VIEW" => UserTagType::VIEW,
        "BUY" => UserTagType::BUY,
        _ => panic!("Invalid action"),
    };

    // let result = spool::<{process_origin,process_brand_id,process_category_id,aggregate_type}>(
    //     &data.minute_data_ptr, minutes_to_process, &action, origin, brand_id, category_id);

    // based on 4 boolean values, we can have 16 different combinations
    
    let result = if process_origin {
        if process_brand_id {
            if process_category_id {
                spool::<true,true,true>(&data.minute_data_ptr, minutes_to_process, &action, origin, brand_id, category_id)
            } else {
                spool::<true,true,false>(&data.minute_data_ptr, minutes_to_process, &action, origin, brand_id, category_id)
            }
        } else {
            if process_category_id {
                spool::<true,false,true>(&data.minute_data_ptr, minutes_to_process, &action, origin, brand_id, category_id)
            } else {
                spool::<true,false,false>(&data.minute_data_ptr, minutes_to_process, &action, origin, brand_id, category_id)
            }
        }
    } else {
        if process_brand_id {
            if process_category_id {
                spool::<false,true,true>(&data.minute_data_ptr, minutes_to_process, &action, origin, brand_id, category_id)
            } else {
                spool::<false,true,false>(&data.minute_data_ptr, minutes_to_process, &action, origin, brand_id, category_id)
            }
        } else {
            if process_category_id {
                spool::<false,false,true>(&data.minute_data_ptr, minutes_to_process, &action, origin, brand_id, category_id)
            } else {
                spool::<false,false,false>(&data.minute_data_ptr, minutes_to_process, &action, origin, brand_id, category_id)
            }
        }
    };
    
    let mut columns = Vec::new();
    let mut rows = Vec::new();

    columns.push("1m_bucket".to_string());
    columns.push("action".to_string());


    if process_origin {
        columns.push("origin".to_string());
    }
    if process_brand_id {
        columns.push("brand_id".to_string());
    }
    if process_category_id {
        columns.push("category_id".to_string());
    }
    for aggregate_type in aggregate_requests.iter() {
        if *aggregate_type {
            columns.push("sum_price".to_string());
        } else {
            columns.push("count".to_string());
        }
    }

    for (i, _) in (start_minute..end_minute).enumerate() {
        let mut row = Vec::new();
        // 2022-03-22T12:15:00
        let minute_datetime = chrono::DateTime::from_timestamp_millis(start_timestamp+(i as i64)*60*1000 ).unwrap();
        let formated = minute_datetime.format("%Y-%m-%dT%H:%M:%S").to_string();
        row.push(formated);
        row.push(info.action.clone());
        if process_origin{
            row.push(info.origin.clone().unwrap());
        }
        if process_brand_id{
            row.push(info.brand_id.clone().unwrap());
        }
        if process_category_id{
            row.push(info.category_id.clone().unwrap());
        }
        for aggregate_type in aggregate_requests.iter() {
            if *aggregate_type {
                row.push(result[i].sum.to_string());
            } else {
                row.push(result[i].count.to_string());
            }
        }
        rows.push(row);

    }

    let response = AggregateResponse {
        columns,
        rows,
    };

    let json_response = serde_json::to_string(&response).unwrap();

    println!("json_response: {}", json_response);

    HttpResponse::Ok().json(response)
}



struct AppState {
    user_tags_views: DashMap<String,UserTags>,
    user_tags_buys: DashMap<String,UserTags>,
    compression_mappings: CompressionMappings,
    tag_sender: mpsc::Sender<CompressedTag>,
    minute_data_ptr: Arc<DashMap<i64,MinuteData>>,
}

async fn data_saver(minute_data_ptr: Arc<DashMap<i64,MinuteData>>, rx: &mut mpsc::Receiver<CompressedTag>) {
    
    const LIMIT: usize = 32;
    loop {
        let mut buffer = Vec::new();
        let _ = rx.recv_many(&mut buffer, LIMIT).await;
        for tag in buffer.iter() {
            minute_data_ptr.entry(tag.timestamp/60000).or_insert_with(|| MinuteData {
                product_id: Vec::new(),
                brand_id: Vec::new(),
                category_id: Vec::new(),
                price: Vec::new(),
                action: Vec::new(),
            });
            minute_data_ptr.get_mut(&(tag.timestamp/60000)).unwrap().product_id.push(tag.origin_id);
            minute_data_ptr.get_mut(&(tag.timestamp/60000)).unwrap().brand_id.push(tag.brand_id);
            minute_data_ptr.get_mut(&(tag.timestamp/60000)).unwrap().category_id.push(tag.category_id);
            minute_data_ptr.get_mut(&(tag.timestamp/60000)).unwrap().price.push(tag.price);
            minute_data_ptr.get_mut(&(tag.timestamp/60000)).unwrap().action.push(tag.action.clone());
            //println!("Received tag: {:?}", tag);
        }
    }
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


