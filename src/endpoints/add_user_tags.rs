use actix_web::{post, web, HttpResponse, Responder, Result};
use actix_web::http::StatusCode;
use crate::api::ApiUserTag;
use crate::api::ApiUserTagWorking;

use crate::AppState;
use crate::data::{AggregateTagEvent, Compress, Cookie, ProductInfo, UserAction, UserTagEvent, AGGREGATE_BUCKET};
use crate::database::Database;
use crate::endpoints::utils::IntoHttpError;



#[post("/user_tags")]
pub async fn add_user_tags(data: web::Data<AppState>, req_body: String) -> Result<impl Responder> {
	let user_tag_working: std::prelude::v1::Result<ApiUserTagWorking, serde_json::Error> = serde_json::from_str(&req_body);
	if user_tag_working.is_err() {
		println!("added user 0.1");
		println!("{:?}", user_tag_working.err().unwrap());
		return Ok(HttpResponse::Ok().status(StatusCode::BAD_REQUEST).finish());
	}
	let user_tag_working = user_tag_working.unwrap();
	// convert to normal user tag
	// let user_tag = ApiUserTag { 
	// 	product_info: ProductInfo {
	// 		product_id: String::from(user_tag_working.product_info.product_id.to_string()),
	// 		brand_id: user_tag_working.product_info.brand_id.parse().unwrap(),
	// 		category_id: user_tag_working.product_info.category_id.parse().unwrap(),
	// 		price: user_tag_working.product_info.price,
	// 	},
	// 	time: user_tag_working.time,
	// 	cookie: user_tag_working.cookie,
	// 	country: user_tag_working.country,
	// 	device: user_tag_working.device,
	// 	action: user_tag_working.action,
	// 	origin: user_tag_working.origin
	// };
	
	//let tag = UserTagEvent::compress(&user_tag, data.database.as_ref()).await.map_error(StatusCode::BAD_REQUEST)?;
	//let aggregate_tag = AggregateTagEvent::compress(&user_tag, data.database.as_ref()).await.map_error(StatusCode::BAD_REQUEST)?;
	let cookie = Cookie(user_tag_working.cookie.clone());
	let action = UserAction::try_from(user_tag_working.action.as_ref()).map_error(StatusCode::BAD_REQUEST)?;
	data.database.add_user_event_uncompresed(&cookie, user_tag_working, action).await;
	//data.database.add_aggregate_event(tag.time / AGGREGATE_BUCKET, aggregate_tag).await;
	Ok(HttpResponse::Ok().status(StatusCode::NO_CONTENT).finish())
}