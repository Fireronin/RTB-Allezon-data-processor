use std::fmt::{Display, Formatter};
use actix_web::{HttpResponse, post, Responder, web};
use serde::{Deserialize, Serialize};

use crate::api::{ApiUserTag, GetUserProfileResponse};
use crate::AppState;
use crate::data::*;
use crate::data::time::*;
use crate::database::Database;

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

impl Display for UserProfileApiResponse {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		write!(f, "Response {}:\n\tviews: {:?}\n\tbuys {:?}\n", &self.cookie, &self.views, &self.buys)
	}
}

fn filter_tags<T: Database>(db: &T, mut user_tags: Vec<UserTagEvent>, request: UserProfileRequest) -> Vec<ApiUserTag> {
	user_tags.sort();
	let user_tags: Vec<ApiUserTag> = user_tags.into_iter()
		.filter(|tag| -> bool {
			request.time_range.within(tag.time)
		})
		.map(|tag| {
			let decompressed = db.decompress_event_tag(&tag.compressed_data).await;
			ApiUserTag {
				product_info: ProductInfo {
					product_id: decompressed.product_id,
					brand_id: decompressed.brand_id,
					category_id: decompressed.category_id,
					price: tag.price,
				},
				time: timestamp_to_str(tag.time),
				cookie: request.cookie.0,
				country: decompressed.country,
				device: tag.device,
				action: "".to_string(),
				origin: "".to_string(),
			}
		})
		.collect();
	user_tags.into_iter().rev().take(limit).collect()
}

#[post("/user_profiles/{cookie}")]
pub async fn user_profiles(data: web::Data<AppState>, _req_body: String, cookie: web::Path<String>, info: web::Query<UserProfileApiRequest>) -> impl Responder {
	let cookie = Cookie(cookie.into_inner());
	
	let limit = match info.limit {
		Some(limit) => limit as usize,
		None => MAX_TAGS,
	};
	
	let time_range = TimeRange::new(info.time_range.as_str()).unwrap();
	
	// get the user tags
	let GetUserProfileResponse {
		view_events, buy_events: buy_tags
	} = data.database.get_user_profile(&cookie).await;
	
	let response = UserProfileApiResponse {
		cookie: cookie.0.clone(),
		views: filter_tags(view_events, &time_range, limit).into_iter().map(|x| x.into()).collect(),
		buys: filter_tags(buy_tags, &time_range, limit).into_iter().map(|x| x.into()).collect(),
	};
	
	HttpResponse::Ok().json(response)
}

