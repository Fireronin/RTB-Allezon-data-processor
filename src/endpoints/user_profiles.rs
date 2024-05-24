use actix_web::{HttpResponse, post, Responder, web, Result};
use actix_web::http::StatusCode;
use serde::{Deserialize, Serialize};

use crate::api::*;
use crate::AppState;
use crate::data::*;
use crate::data::time::*;
use crate::database::{Database, Decompressor};
use crate::endpoints::utils::IntoHttpError;

#[derive(Deserialize, Serialize)]
struct UserProfileApiRequest {
	time_range: String,
	limit: Option<i32>,
}

#[derive(Deserialize, Serialize, PartialEq, Eq)]
struct UserProfileApiResponse {
	cookie: String,
	views: Vec<ApiUserTag>,
	buys: Vec<ApiUserTag>,
}

#[derive(Deserialize, Serialize, PartialEq, Eq)]
struct UserProfileApiResponseWorking {
	cookie: String,
	views: Vec<ApiUserTagWorking>,
	buys: Vec<ApiUserTagWorking>,
}


async fn filter_tags<T: Decompressor<UserTagEvent>>(decompressor: &T, mut user_tags: Vec<UserTagEvent>, request: &GetUserProfileRequest, action: UserAction) -> Vec<ApiUserTag> {
	user_tags.sort();
	let filtered_tags: Vec<UserTagEvent> = user_tags.into_iter()
		.filter(|tag| -> bool {
			request.time_range.within(tag.time)
		})
		.collect();
	let mut tags = vec![];
	for tag in filtered_tags {
		tags.push(tag.decompress(decompressor, (request.cookie.clone(), action)).await);
	}
	tags.into_iter().rev().take(request.limit).collect()
}

fn filter_dumb_tags(mut user_tags: Vec<ApiUserTagWorking>, request: &GetUserProfileRequest) -> Vec<ApiUserTagWorking> {
	user_tags.sort_by(|a, b| {
		parse_timestamp(&a.time).unwrap().cmp(&parse_timestamp(&b.time).unwrap())
	});
	let filtered_tags: Vec<ApiUserTagWorking> = user_tags.into_iter()
		.filter(|tag| -> bool {
			request.time_range.within(parse_timestamp(&tag.time).unwrap())
		})
		.collect();
	filtered_tags.into_iter().rev().take(request.limit).collect()
}

#[post("/user_profiles/{cookie}")]
pub async fn user_profiles(data: web::Data<AppState>, _req_body: String, cookie: web::Path<String>, info: web::Query<UserProfileApiRequest>) -> Result<impl Responder> {
	let request = GetUserProfileRequest {
		cookie: Cookie(cookie.into_inner()),
		time_range: TimeRange::new(info.time_range.as_str()).map_error(StatusCode::BAD_REQUEST)?,
		limit: match info.limit {
			Some(limit) => limit as usize,
			None => MAX_TAGS,
		},
	};

	// get current time 
	let current_time = chrono::Utc::now().timestamp_millis();

	let user_profile = data.database.get_user_profile_uncompresed(&request.cookie).await;

	// compute latency 
	let latency = chrono::Utc::now().timestamp_millis() - current_time;
	//println!("Latency: {} ms", latency);

	let response = UserProfileApiResponseWorking {
		cookie: request.cookie.0.clone(),
		views: filter_dumb_tags(user_profile.view_events, &request),
		buys: filter_dumb_tags(user_profile.buy_events, &request),
	};

	// println!("user_profiles 0");
	// // print json and return
	// println!("Output {}", serde_json::to_string(&response).unwrap());
	// // print request
	// println!("Request {}", _req_body);

	//get the user tags
	// let user_profile = data.database.get_user_profile(&request.cookie).await;
	// let response = 	UserProfileApiResponse {
	// 	cookie: request.cookie.0.clone(),
	// 	views: filter_tags(data.database.as_ref(), user_profile.view_events, &request, UserAction::VIEW).await,
	// 	buys: filter_tags(data.database.as_ref(), user_profile.buy_events, &request, UserAction::BUY).await,
	// };

	Ok(HttpResponse::Ok().json(response))
}

