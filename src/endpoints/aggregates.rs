use std::{ops::Range};

use actix_web::{post, web, HttpRequest, HttpResponse, Responder};
use serde::{Deserialize, Serialize};
use dashmap::DashMap;
use std::sync::Arc;
use rayon::prelude::*;
use serde_querystring::DuplicateQS;

use crate::AppState;

use crate::storage::{MinuteData, UserTagType};

struct SpoolingResult {
	count: u64,
	sum: u64,
}

fn spool<const ORIGIN: bool, const BRAND_ID: bool, const CATEGORY_ID: bool>(data: &Arc<DashMap<i64, MinuteData>>,
                                                                            minutes_to_process: Range<i64>,
                                                                            action: &UserTagType,
                                                                            origin: u16,
                                                                            brand_id: u16,
                                                                            category_id: u16) -> Vec<SpoolingResult> {
	let par_itter = rayon::iter::IntoParallelIterator::into_par_iter(minutes_to_process);
	
	let result = par_itter.map(|minute| {
		let minute_data = data.get(&minute);
		// result
		if minute_data.is_none() {
			return SpoolingResult {
				count: 0,
				sum: 0,
			};
		}
		let minute_data = minute_data.unwrap();
		let mut count: u64 = 0;
		let mut sum: u64 = 0;
		for i in 0..minute_data.product_id.len() {
			let correct_origin = !ORIGIN || (ORIGIN && minute_data.product_id[i] == origin);
			let correct_brand_id = !BRAND_ID || (BRAND_ID && minute_data.brand_id[i] == brand_id);
			let correct_category_id = !CATEGORY_ID || (CATEGORY_ID && minute_data.category_id[i] == category_id);
			let correct_action = minute_data.action[i] == *action;
			if correct_origin && correct_brand_id && correct_category_id && correct_action {
				count += 1;
				sum += minute_data.price[i] as u64;
			}
		}
		SpoolingResult {
			count,
			sum,
		}
	}).collect();
	
	
	result
}


#[derive(Deserialize, Serialize)]
struct AggregateResponse {
	columns: Vec<String>,
	rows: Vec<Vec<String>>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
enum AggregatesRequestEnum {
	Single(String),
	Multiple(Vec<String>),
}

#[derive(Deserialize, Serialize, Clone, Debug)]
struct AggregateRequest {
	time_range: String,
	action: String,
	origin: Option<String>,
	brand_id: Option<String>,
	category_id: Option<String>,
}

#[post("/aggregates")]
pub async fn aggregates(data: web::Data<AppState>, _req_body: String, info: web::Query<AggregateRequest>, req: HttpRequest) -> impl Responder {
	// println!("~~~~~~~~~~~~~~ Aggregates ~~~~~~~~~~~~~~");
	// println!("time_range: {}", info.time_range);
	// println!("action: {}", info.action);
	//
	// println!("origin: {:?}", info.origin);
	// println!("brand_id: {:?}", info.brand_id);
	// println!("category_id: {:?}", info.category_id);
	// println!("req_body: {}", req_body);
	let parsed = DuplicateQS::parse(req.query_string().as_bytes());
	let values = parsed.values("aggregates".as_bytes()); // Will give you a vector of b"bar" and b"baz"
	if values.is_none() {
		// println!("Invalid aggregates");
		return HttpResponse::BadRequest().body("Invalid aggregates");
	}
	
	let aggregates_vec = values.unwrap().iter().map(|x| std::str::from_utf8(&x.clone().unwrap()).unwrap().to_string()).collect::<Vec<String>>();
	// println!("aggregates: {:?}", aggregates_vec);
	let time_range: Vec<&str> = info.time_range.split("_").collect();
	// add Z to the end of the time string to make it RFC3339 compliant
	let time_range: Vec<String> = time_range.iter().map(|x| x.to_string() + "Z").collect();
	let start_timestamp = chrono::DateTime::parse_from_rfc3339(time_range[0].as_str()).unwrap().timestamp_millis();
	let end_timestamp = chrono::DateTime::parse_from_rfc3339(time_range[1].as_str()).unwrap().timestamp_millis();
	
	// divide the time range into minutes
	let start_minute = start_timestamp / 60000;
	let end_minute = end_timestamp / 60000;
	
	// let  = match info.aggregates.clone() {
	//     AggregatesRequestEnum::Single(aggregate) => vec![aggregate],
	//     AggregatesRequestEnum::Multiple(aggregates) => aggregates,
	// };
	
	let minutes_to_process = start_minute..end_minute;
	
	let action = &info.action;
	let origin = match &info.origin {
		Some(origin) => data.compression_mappings.origin_id_map.get(origin).unwrap().clone(),
		None => 0,
	};
	let brand_id = match &info.brand_id {
		Some(brand_id) => data.compression_mappings.brand_id_map.get(brand_id).unwrap().clone(),
		None => 0,
	};
	let category_id = match &info.category_id {
		Some(category_id) => data.compression_mappings.category_id_map.get(category_id).unwrap().clone(),
		None => 0,
	};
	
	let process_origin = info.origin.is_some();
	let process_brand_id = info.brand_id.is_some();
	let process_category_id = info.category_id.is_some();
	
	
	let aggregate_requests = aggregates_vec.iter().map(|x| match x.as_str() {
		"COUNT" => false,
		"SUM_PRICE" => true,
		_ => panic!("Invalid aggregate type"),
	}).collect::<Vec<bool>>();
	
	
	let action = match action.as_str() {
		"VIEW" => UserTagType::VIEW,
		"BUY" => UserTagType::BUY,
		_ => panic!("Invalid action"),
	};
	
	// let result = spool::<{process_origin,process_brand_id,process_category_id,aggregate_type}>(
	//     &data.minute_data_ptr, minutes_to_process, &action, origin, brand_id, category_id);
	
	// based on 4 boolean values, we can have 16 different combinations
	
	let result = if process_origin {
		if process_brand_id {
			if process_category_id {
				spool::<true, true, true>(&data.minute_data_ptr, minutes_to_process, &action, origin, brand_id, category_id)
			} else {
				spool::<true, true, false>(&data.minute_data_ptr, minutes_to_process, &action, origin, brand_id, category_id)
			}
		} else {
			if process_category_id {
				spool::<true, false, true>(&data.minute_data_ptr, minutes_to_process, &action, origin, brand_id, category_id)
			} else {
				spool::<true, false, false>(&data.minute_data_ptr, minutes_to_process, &action, origin, brand_id, category_id)
			}
		}
	} else {
		if process_brand_id {
			if process_category_id {
				spool::<false, true, true>(&data.minute_data_ptr, minutes_to_process, &action, origin, brand_id, category_id)
			} else {
				spool::<false, true, false>(&data.minute_data_ptr, minutes_to_process, &action, origin, brand_id, category_id)
			}
		} else {
			if process_category_id {
				spool::<false, false, true>(&data.minute_data_ptr, minutes_to_process, &action, origin, brand_id, category_id)
			} else {
				spool::<false, false, false>(&data.minute_data_ptr, minutes_to_process, &action, origin, brand_id, category_id)
			}
		}
	};
	
	let mut columns = Vec::new();
	let mut rows = Vec::new();
	
	columns.push("1m_bucket".to_string());
	columns.push("action".to_string());
	
	
	if process_origin {
		columns.push("origin".to_string());
	}
	if process_brand_id {
		columns.push("brand_id".to_string());
	}
	if process_category_id {
		columns.push("category_id".to_string());
	}
	for aggregate_type in aggregate_requests.iter() {
		if *aggregate_type {
			columns.push("sum_price".to_string());
		} else {
			columns.push("count".to_string());
		}
	}
	
	for (i, _) in (start_minute..end_minute).enumerate() {
		let mut row = Vec::new();
		// 2022-03-22T12:15:00
		let minute_datetime = chrono::DateTime::from_timestamp_millis(start_timestamp + (i as i64) * 60 * 1000).unwrap();
		let formated = minute_datetime.format("%Y-%m-%dT%H:%M:%S").to_string();
		row.push(formated);
		row.push(info.action.clone());
		if process_origin {
			row.push(info.origin.clone().unwrap());
		}
		if process_brand_id {
			row.push(info.brand_id.clone().unwrap());
		}
		if process_category_id {
			row.push(info.category_id.clone().unwrap());
		}
		for aggregate_type in aggregate_requests.iter() {
			if *aggregate_type {
				row.push(result[i].sum.to_string());
			} else {
				row.push(result[i].count.to_string());
			}
		}
		rows.push(row);
	}
	
	let response = AggregateResponse {
		columns,
		rows,
	};
	
	// let json_response = serde_json::to_string(&response).unwrap();
	
	// println!("json_response: {}", json_response);
	
	HttpResponse::Ok().json(response)
}
