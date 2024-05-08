use std::sync::Arc;
use actix_web::{App, HttpServer, web};
use futures::future::select;
use tokio::sync::mpsc;

use endpoints::*;
use crate::data::AggregateTagEvent;

use crate::database::{AerospikeDB, CachedDB, Database};

mod endpoints;
mod database;
mod data;
mod tests;
pub mod api;

pub struct AppState {
	pub database: Arc<CachedDB<AerospikeDB>>,
	pub tag_sender: mpsc::Sender<AggregateTagEvent>,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
	let (tx, mut rx) = mpsc::channel::<AggregateTagEvent>(128);
	
	let database = Arc::new(CachedDB::new().await);
	let database_clone = database.clone();
	
	// split control flow into two tasks
	// 1. to receive compressed tags and store them in memory
	// 2. to serve the HTTP requests
	let server = tokio::spawn(async move {
		HttpServer::new(move || {
			App::new()
				.app_data(web::Data::new(AppState {
					database: database.clone(),
					tag_sender: tx.clone(),
				}))
				.service(add_user_tags)
				.service(user_profiles)
				// .service(aggregates)
		}).bind(("10.112.103.101", 8083))
			.expect("Creation of server failed")
			.run()
			.await
			.expect("Execution of server failed")
	});
	let data_saver = tokio::spawn(async move {
		const LIMIT: usize = 32;
		loop {
			let mut buffer = Vec::new();
			let _ = rx.recv_many(&mut buffer, LIMIT).await;
			for tag in buffer {
				// database_clone.add_minute(tag).await;
			}
		}
	});
	
	select(server, data_saver).await;
	
	Ok(())
}
