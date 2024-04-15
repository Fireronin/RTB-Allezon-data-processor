use actix_web::{post, web,  HttpResponse, Responder};
use dashmap::DashMap;

use crate::storage::{UserTag,UserTags,add_user_tag,compress_tag};

use crate::AppState;

#[post("/user_tags")]
pub async fn add_user_tags(data: web::Data<AppState>, req_body: String) -> impl Responder {
    //println!("~~~~~~~~~~~~~~ User Tags ~~~~~~~~~~~~~~");
    // print error during deserialization
    let user_tag_result = serde_json::from_str(&req_body);
    if user_tag_result.is_err() {
        print!("Invalid JSON: {}", req_body);
        return HttpResponse::BadRequest().body("Invalid JSON");
    }
    let user_tag: UserTag = user_tag_result.unwrap();
        
    let action = &user_tag.action;
    let cookie = &user_tag.cookie;
    let timestamp = chrono::DateTime::parse_from_rfc3339(&user_tag.time).unwrap().timestamp_millis();

    fn add_to_map (user_tags_map: &DashMap<String,UserTags>, cookie: &str, user_tag: &UserTag, timestamp: i64) {
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