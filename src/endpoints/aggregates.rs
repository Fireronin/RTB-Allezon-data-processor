use std::str::from_utf8;

use actix_web::{HttpRequest, HttpResponse, post, Responder, Result, web};
use actix_web::http::StatusCode;
use serde::{Deserialize, Serialize};
use serde_querystring::DuplicateQS;
use strum_macros::{EnumString, IntoStaticStr};

use crate::api::*;
use crate::AppState;
use crate::data::*;
use crate::database::Database;
use crate::endpoints::utils::IntoHttpError;

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct GetAggregateApiRequest {
	pub time_range: String,
	pub action: String,
	pub origin: Option<String>,
	pub brand_id: Option<String>,
	pub category_id: Option<String>,
}

#[derive(Deserialize, Serialize)]
struct GetAggregateApiResponse {
	columns: Vec<String>,
	rows: Vec<Vec<String>>,
}


#[post("/aggregates")]
pub async fn aggregates(
	data: web::Data<AppState>,
	_req_body: String,
	request: web::Query<GetAggregateApiRequest>,
	aggregates_query_string: HttpRequest) -> Result<impl Responder> {
	
	//     let aggregate_requests =aggregates_vec.iter().map(|x| match x.as_str() {
	// 		"COUNT" => false,
	// 		"SUM_PRICE" => true,
	// 		_ => panic!("Invalid aggregate type"),
	// 		}).collect::<Vec<bool>>();

	// let request_types: Vec<AggregateRequestType> =
	// 	DuplicateQS::parse(aggregates_query_string.query_string().as_bytes())
	// 		.values(b"aggregates")
	// 		.ok_or("`aggregates` not in request")
	// 		.map_error(StatusCode::BAD_REQUEST)?
	// 		.iter()
	// 		.map(|x| {
	// 			let bytes: &[u8] = x.as_ref().unwrap();
	// 			from_utf8(bytes).unwrap().try_into().unwrap()
	// 		})
	// 		.collect();
	let parsed = DuplicateQS::parse(aggregates_query_string.query_string().as_bytes());
    let values = parsed.values("aggregates".as_bytes()); // Will give you a vector of b"bar" and b"baz"
    if values.is_none() {
        println!("Invalid aggregates");
        return Ok(HttpResponse::BadRequest().body("Invalid aggregates"));
    }
	let aggregates_vec = values.unwrap().iter().map(|x| std::str::from_utf8(&x.clone().unwrap()).unwrap().to_string()).collect::<Vec<String>>();
	let request_types =aggregates_vec.iter().map(|x| match x.as_str() {
				"COUNT" => AggregateRequestType::Count,
				"SUM_PRICE" => AggregateRequestType::Sum,
				_ => panic!("Invalid aggregate type"),
				}).collect::<Vec<AggregateRequestType>>();
	
	
			let time_range: Vec<&str> = request.time_range.split("_").collect();
			// add Z to the end of the time string to make it RFC3339 compliant
			let time_range: Vec<String> = time_range.iter().map(|x| x.to_string() + "Z").collect();
			let start_timestamp = chrono::DateTime::parse_from_rfc3339(time_range[0].as_str()).unwrap().timestamp_millis();
			let end_timestamp = chrono::DateTime::parse_from_rfc3339(time_range[1].as_str()).unwrap().timestamp_millis();
		
			// divide the time range into minutes
			let start_minute = start_timestamp/60000;
			let end_minute = end_timestamp/60000;
		
			let minutes_to_process = start_minute..end_minute;
		
	//		async fn get_aggregate_uncompresed(&self, request: &GetAggregateApiRequest,querry_types : Vec<AggregateRequestType>) -> GetAggregateResponse{
	let mut columns = vec![String::from("1m_bucket"), String::from("action")];
	
	let get_aggregate_response = data.database.get_aggregate_uncompresed(&request, request_types.clone()).await;
		let rows = get_aggregate_response.aggregates.iter()
		.enumerate()
		.map(|(i, value)| {
			let mut row = Vec::new();
			//let minute_datetime = chrono::DateTime::from_timestamp_millis(get_aggregate_request.time_range.start + (i as i64) * 60 * 1000).unwrap();
			let minute_datetime = chrono::DateTime::<chrono::Utc>::from_utc(
				chrono::NaiveDateTime::from_timestamp(
					start_timestamp + (i as i64) * 60 * 1000, 
					0
				), 
				chrono::Utc
			);
			row.push(minute_datetime.format("%Y-%m-%dT%H:%M:%S").to_string());
			row.push(request.action.clone());
			if let Some(value) = &request.origin {
				row.push(value.clone());
			}
			if let Some(value) = &request.brand_id {
				row.push(value.clone());
			}
			if let Some(value) = &request.category_id {
				row.push(value.clone());
			}
			for aggregate_type in request_types.iter() {
				row.push(match aggregate_type {
					AggregateRequestType::Count => value.count,
					AggregateRequestType::Sum => value.sum,
				}.to_string());
			}
			row
		})
		.collect();
	
	let response = GetAggregateApiResponse {
		columns,
		rows,
	};
	
	Ok(HttpResponse::Ok().json(response))

	//return Ok(HttpResponse::Ok().json("Hello"));
	// let get_aggregate_request = GetAggregateRequest::compress(&request, data.database.as_ref())
	// 	.await
	// 	.map_error(StatusCode::BAD_REQUEST)?;
	
	// let response = data.database.get_aggregate(&get_aggregate_request).await;
	
	
	//
	
	// if get_aggregate_request.origin.is_some() {
	// 	columns.push(String::from("origin"));
	// }
	// if get_aggregate_request.brand_id.is_some() {
	// 	columns.push(String::from("brand_id"));
	// }
	// if get_aggregate_request.category_id.is_some() {
	// 	columns.push(String::from("category_id"));
	// }
	// for aggregate_type in request_types.iter() {
	// 	columns.push(Into::<&'static str>::into(aggregate_type).to_string());
	// }


}
