use std::str::from_utf8;
use actix_web::{HttpRequest, HttpResponse, post, Responder, web};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use serde_querystring::DuplicateQS;

use crate::AppState;
use crate::data::*;
use crate::data::time::TimeRange;
use crate::database::Database;
use crate::endpoints::aggregates::AggregateRequestType::{Count, Sum};

#[derive(Deserialize, Serialize, Clone, Debug)]
struct AggregateRequest {
	time_range: String,
	action: String,
	origin: Option<String>,
	brand_id: Option<String>,
	category_id: Option<String>,
}

#[derive(Deserialize, Serialize)]
struct AggregateResponse {
	columns: Vec<String>,
	rows: Vec<Vec<String>>,
}

enum AggregateRequestType {
	Count,
	Sum,
}

impl AggregateRequestType {
	fn name(&self) -> &'static str {
		match &self {
			Count => "count",
			Sum => "sum_price",
		}
	}
}

impl TryFrom<&str> for AggregateRequestType {
	type Error = &'static str;
	
	fn try_from(value: &str) -> Result<Self, Self::Error> {
		match value {
			"COUNT" => Ok(Count),
			"SUM_PRICE" => Ok(Sum),
			_ => Err("Invalid aggregate type"),
		}
	}
}

/*
Uses minute_data_ptr
 */
#[post("/aggregates")]
pub async fn aggregates(
	data: web::Data<AppState>,
	_req_body: String,
	request: web::Query<AggregateRequest>,
	aggregates_query_string: HttpRequest) -> impl Responder {
	
	let request_types: Vec<AggregateRequestType> =
		match DuplicateQS::parse(aggregates_query_string.query_string().as_bytes()).values(b"aggregates") {
			Some(x) => x,
			None => return HttpResponse::BadRequest().body("Invalid aggregates"),
		}.iter()
			.map(|x| {
				let bytes: &[u8] = x.as_ref().unwrap();
				AggregateRequestType::try_from(from_utf8(bytes).unwrap()).unwrap()
			})
			.collect();
	
	let time_range = TimeRange::new(request.time_range.as_str()).unwrap();
	
	let tags = data.database.get_aggregate(&time_range).await;
	let action = UserAction::try_from(&request.action).unwrap();
	let compression = data.database.compress(request.category_id.as_ref(), request.origin.as_ref(), request.brand_id.as_ref()).await;
	
	let result = spool(tags, action, compression);
	
	let mut columns = vec![String::from("1m_bucket"), String::from("action")];
	
	if request.origin.is_some() {
		columns.push(String::from("origin"));
	}
	if request.brand_id.is_some() {
		columns.push(String::from("brand_id"));
	}
	if request.category_id.is_some() {
		columns.push(String::from("category_id"));
	}
	for aggregate_type in request_types.iter() {
		columns.push(aggregate_type.name().to_string());
	}
	
	let rows = result.iter()
		.enumerate()
		.map(|(i, value)| {
			let mut row = Vec::new();
			let minute_datetime = chrono::DateTime::from_timestamp_millis(time_range.start + (i as i64) * 60 * 1000).unwrap();
			row.push(minute_datetime.format("%Y-%m-%dT%H:%M:%S").to_string());
			row.push(request.action.clone());
			if request.origin.is_some() {
				row.push(request.origin.clone().unwrap());
			}
			if request.brand_id.is_some() {
				row.push(request.brand_id.clone().unwrap());
			}
			if request.category_id.is_some() {
				row.push(request.category_id.clone().unwrap());
			}
			for aggregate_type in request_types.iter() {
				row.push(match aggregate_type {
					Count => value.count,
					Sum => value.sum,
				}.to_string());
			}
			row
		})
		.collect();
	
	let response = AggregateResponse {
		columns,
		rows,
	};
	
	HttpResponse::Ok().json(response)
}

#[derive(Default)]
struct SpoolingResult {
	count: u64,
	sum: u64,
}

fn eq_or_true(option: &Option<u16>, value: u16) -> bool {
	option.map(|x| x == value).unwrap_or(true)
}

fn spool(data: Vec<Vec<AggregateTagEvent>>,
         action: UserAction,
         compression: AggregateCompressedRequest) -> Vec<SpoolingResult> {
	data.into_par_iter().map(|minute_data|{
		minute_data.iter().fold(SpoolingResult::default(), |mut result, tag| {
			let correct_origin = eq_or_true(&compression.origin_id, tag.origin_id);
			let correct_brand_id = eq_or_true(&compression.brand_id, tag.brand_id);
			let correct_category_id = eq_or_true(&compression.category_id, tag.category_id);
			let correct_action = tag.action == action;
			if correct_origin && correct_brand_id && correct_category_id && correct_action {
				result.count += 1;
				result.sum += tag.price as u64;
			}
			result
		})
	}).collect()
}
