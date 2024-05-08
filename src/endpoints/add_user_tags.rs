use actix_web::{post, web, HttpResponse, Responder, Result};
use actix_web::http::StatusCode;
use crate::api::{AddUserProfileRequest, ApiUserTag};

use crate::AppState;
use crate::data::{Compress, Cookie, UserAction, UserTagEvent};
use crate::database::Database;
use crate::endpoints::utils::IntoHttpError;

#[post("/user_tags")]
pub async fn add_user_tags(data: web::Data<AppState>, req_body: String) -> Result<impl Responder> {
	let user_tag: ApiUserTag = serde_json::from_str(&req_body)?;
	
	let tag = UserTagEvent::compress(&user_tag, data.database.as_ref()).await.map_error(StatusCode::BAD_REQUEST)?;
	let request = AddUserProfileRequest {
		cookie: Cookie(user_tag.cookie),
		action: UserAction::try_from(user_tag.action.as_ref()).map_error(StatusCode::BAD_REQUEST)?,
		tag,
	};
	
	data.database.add_user_event(request).await;
	
	// data.tag_sender.send(tag).await?;
	
	Ok(HttpResponse::Ok().status(StatusCode::NO_CONTENT).finish())
}