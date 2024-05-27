use actix_web::{HttpResponse, post, web};
use actix_web::http::StatusCode;

use crate::api::ApiUserTag;
use crate::AppState;
use crate::data::{AGGREGATE_BUCKET, AggregateTagEvent, Compress, Cookie, UserAction, UserTagEvent};
use crate::database::Database;

#[post("/user_tags")]
pub async fn add_user_tags(data: web::Data<AppState>, req_body: String) -> HttpResponse {
	let user_tag: ApiUserTag = serde_json::from_str(&req_body).unwrap();
	
	let tag = UserTagEvent::compress(&user_tag, data.database.as_ref()).await.unwrap();
	let aggregate_tag = AggregateTagEvent::compress(&user_tag, data.database.as_ref()).await.unwrap();

	let cookie = Cookie(user_tag.cookie);
	let action = UserAction::try_from(user_tag.action.as_ref()).unwrap();
	
	data.database.add_user_event(&cookie, tag, action).await;
	data.database.add_aggregate_event(tag.time / AGGREGATE_BUCKET, aggregate_tag).await;
	
	HttpResponse::Ok().status(StatusCode::NO_CONTENT).finish()
}