use actix_web::{post, web, HttpResponse, Responder};
use actix_web::http::StatusCode;

use crate::AppState;
use crate::types::{UserAction, UserTag, UserTagExternal};

#[post("/user_tags")]
pub async fn add_user_tags(data: web::Data<AppState>, req_body: String) -> impl Responder {
	let user_tag: UserTag = match serde_json::from_str::<UserTagExternal>(&req_body) {
		Ok(user_tag) => user_tag.into(),
		Err(err) => return HttpResponse::BadRequest().body(err.to_string()),
	};

	match UserAction::try_from(&user_tag.action) {
		Ok(user_action) => data.database.add_user_tag(user_tag, user_action),
		Err(_) => return HttpResponse::BadRequest().body("Invalid action"),
	}
	
	// match data.tag_sender.send(compress_tag(&user_tag, &data.compression_mappings)).await {
	// 	Ok(_) => {},
	// 	Err(_) => {
	// 		println!("Error sending tag to channel");
	// 	}
	// }
	
	HttpResponse::Ok().status(StatusCode::NO_CONTENT).finish()
}