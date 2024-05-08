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
	
	// get the user tags
	let user_profile = data.database.get_user_profile(&request.cookie).await;
	let response = 	UserProfileApiResponse {
		cookie: request.cookie.0.clone(),
		views: filter_tags(data.database.as_ref(), user_profile.view_events, &request, UserAction::VIEW).await,
		buys: filter_tags(data.database.as_ref(), user_profile.buy_events, &request, UserAction::BUY).await,
	};

	Ok(HttpResponse::Ok().json(response))
}

