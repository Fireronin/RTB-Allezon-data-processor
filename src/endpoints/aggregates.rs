use std::str::from_utf8;

use actix_web::{HttpRequest, HttpResponse, post, Responder, Result, web};
use serde::{Deserialize, Serialize};
use serde_querystring::DuplicateQS;

use crate::api::*;
use crate::AppState;
use crate::data::*;
use crate::database::Database;

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
	
	let x = DuplicateQS::parse(aggregates_query_string.query_string().as_bytes())
		.values(b"aggregates")
		.ok_or("`aggregates` not in request")
		.map(|x| {
			x.iter()
				.map(|x| {
					let bytes: &[u8] = x.as_ref().unwrap();
					from_utf8(bytes).unwrap().try_into().unwrap()
				})
				.collect()
		});
	let request_types: Vec<AggregateRequestType> = match x {
		Ok(x) => x,
		Err(e) => return Ok(HttpResponse::BadRequest().body(e)),
	};
	
	let time_range = match time::TimeRange::new(request.time_range.as_str()) {
		Ok(x) => x,
		Err(e) => return Ok(HttpResponse::BadRequest().body(e.to_string())),
	};
	
	let mut columns = vec![String::from("1m_bucket"), String::from("action")];
	// push the origin, brand_id, and category_id if they are present
	if request.origin.is_some() {
		columns.push(String::from("origin"));
	}
	if request.brand_id.is_some() {
		columns.push(String::from("brand_id"));
	}
	if request.category_id.is_some() {
		columns.push(String::from("category_id"));
	}
	// push the aggregate types
	for aggregate_type in request_types.iter() {
		if let AggregateRequestType::Count = aggregate_type {
			columns.push(String::from("count"));
		} else {
			columns.push(String::from("sum_price"));
		}
	}

	let get_aggregate_response = data.database.get_aggregate_uncompresed(&request, request_types.clone()).await;
		let rows = get_aggregate_response.aggregates.iter()
		.enumerate()
		.map(|(i, value)| {
			let mut row = Vec::new();
			let minute_datetime = chrono::DateTime::from_timestamp_millis(time_range.start + (i as i64) * 60 * 1000).unwrap();
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
}
