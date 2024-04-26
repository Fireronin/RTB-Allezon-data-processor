use std::sync::Arc;
use actix_web::{App, HttpServer, web};
use futures::future::join_all;
use tokio::sync::mpsc;

use endpoints::add_user_tags::add_user_tags;
use endpoints::aggregates::aggregates;
use endpoints::user_profiles::user_profiles;
use storage::data_saver;
use crate::data::CompressedTag;

use crate::database::Database;

mod storage;
mod endpoints;
mod database;
mod data;
mod tests;

pub struct AppState {
	pub database: Arc<Database>,
	pub tag_sender: mpsc::Sender<CompressedTag>,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
	let (tx, mut rx) = mpsc::channel::<CompressedTag>(128);
	
	let database = Arc::new(Database::new());
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
		}).bind(("10.112.103.101", 8083))
			.expect("Creation of server failed")
			.run()
			.await
			.expect("Execution of server failed")
	});
	let data_saver = tokio::spawn(async move {
		data_saver(database_clone, &mut rx).await;
	});
	
	join_all([server, data_saver]).await;
	
	Ok(())
}
