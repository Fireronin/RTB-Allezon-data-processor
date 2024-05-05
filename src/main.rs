use std::sync::Arc;
use actix_web::{App, HttpServer, web};
use futures::future::select;
use tokio::sync::mpsc;

use endpoints::add_user_tags::add_user_tags;
use endpoints::aggregates::aggregates;
use endpoints::user_profiles::user_profiles;
use crate::data::UserTag;

use crate::database::Database;

mod endpoints;
mod database;
mod data;
mod tests;

pub struct AppState {
	pub database: Arc<Database>,
	pub tag_sender: mpsc::Sender<UserTag>,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
	let (tx, mut rx) = mpsc::channel::<UserTag>(128);
	
	let database = Arc::new(Database::new().await.unwrap());
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
				.service(aggregates)
		}).bind(("10.0.0.30", 8083))
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
				database_clone.add_minute(tag).await;
			}
		}
	});
	
	select(server, data_saver).await;
	
	Ok(())
}
