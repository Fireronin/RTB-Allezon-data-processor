use actix_web::{post, web, HttpResponse, Responder};
use serde::{Deserialize, Serialize};

use crate::AppState;

use crate::storage::{UserTag,MAX_TAGS,get_user_tags};

#[derive(Deserialize, Serialize)]
struct UserProfileRequest {
    time_range: String,
    limit: Option<i32>
}

#[derive(Deserialize, Serialize)]
struct UserProfileResponse {
    cookie: String,
    views: Vec<UserTag>,
    buys: Vec<UserTag>,
}

#[post("/user_profiles/{cookie}")]
pub async fn user_profiles(data: web::Data<AppState>, _req_body: String, cookie: web::Path<String>,info: web::Query<UserProfileRequest>) -> impl Responder {
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

