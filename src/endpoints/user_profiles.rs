use std::fmt::{Display, Formatter};
use actix_web::{HttpResponse, post, Responder, web};
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::types::{MAX_TAGS, UserTag, UserTagExternal};
use crate::utils::{parse_time_range, TimeRange};

#[derive(Deserialize, Serialize)]
struct UserProfileRequest {
	time_range: String,
	limit: Option<i32>
}

#[derive(Deserialize, Serialize, PartialEq, Eq)]
struct UserProfileResponse {
	cookie: String,
	views: Vec<UserTagExternal>,
	buys: Vec<UserTagExternal>,
}

impl Display for UserProfileResponse {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		write!(f, "Response {}:\n\tviews: {:?}\n\tbuys {:?}\n", &self.cookie, &self.views, &self.buys)
	}
}

pub fn filter_tags(mut user_tags: Vec<UserTag>, time_range: &TimeRange, limit: usize) -> Vec<UserTag> {
	user_tags.sort();
	let user_tags: Vec<UserTag> = user_tags.into_iter()
		.filter(|tag| -> bool {
			time_range.within(tag.time)
		})
		.collect();
	user_tags.into_iter().rev().take(limit).collect()
}

#[post("/user_profiles/{cookie}")]
pub async fn user_profiles(data: web::Data<AppState>, req_body: String, cookie: web::Path<String>, info: web::Query<UserProfileRequest>) -> impl Responder {
	let cookie = cookie.into_inner();
	
	let limit = match info.limit {
		Some(limit) => limit as usize,
		None => MAX_TAGS,
	};
	
	let time_range = parse_time_range(info.time_range.as_str());
	
	// get the user tags
	let (tags_views, tags_buys) = data.database.get_tags(&cookie);
	
	let response = UserProfileResponse {
		cookie,
		views: filter_tags(tags_views, &time_range, limit).into_iter().map(|x| x.into()).collect(),
		buys: filter_tags(tags_buys, &time_range, limit).into_iter().map(|x| x.into()).collect(),
	};
	
	let expected_response = serde_json::from_str(req_body.as_str()).unwrap();
	if response != expected_response {
		println!("#####################################");
		println!("User profile:\n{response}");
		println!("Expected user profile:\n{expected_response}");
	}

	HttpResponse::Ok().json(response)
}

