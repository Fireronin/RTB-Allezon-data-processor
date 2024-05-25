use actix_web::{post, web, HttpResponse, Responder, Result};
use actix_web::http::StatusCode;
use crate::api::ApiUserTag;

use crate::AppState;
use crate::data::{AggregateTagEvent, Compress, Cookie, ProductInfo, UserAction, UserTagEvent, AGGREGATE_BUCKET};
use crate::database::Database;
use crate::endpoints::utils::IntoHttpError;

#[post("/user_tags")]
pub async fn add_user_tags(data: web::Data<AppState>, req_body: String) -> Result<impl Responder> {
	let user_tag: ApiUserTag = match serde_json::from_str(&req_body) {
		Ok(x) => x,
		Err(err) => {
			println!("added user 0.1");
			println!("{:?}", err);
			return Ok(HttpResponse::Ok().status(StatusCode::BAD_REQUEST).finish());
		}
	};
	
	//let tag = UserTagEvent::compress(&user_tag, data.database.as_ref()).await.map_error(StatusCode::BAD_REQUEST)?;
	//let aggregate_tag = AggregateTagEvent::compress(&user_tag, data.database.as_ref()).await.map_error(StatusCode::BAD_REQUEST)?;
	let cookie = Cookie(user_tag.cookie.clone());
	let action = UserAction::try_from(user_tag.action.as_ref()).map_error(StatusCode::BAD_REQUEST)?;
	data.database.add_user_event_uncompresed(&cookie, user_tag, action).await;
	//data.database.add_aggregate_event(tag.time / AGGREGATE_BUCKET, aggregate_tag).await;
	Ok(HttpResponse::Ok().status(StatusCode::NO_CONTENT).finish())
}