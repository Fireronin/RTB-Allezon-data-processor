mod endpoints;
mod database;
mod data;
mod tests;
pub mod api;
mod compression;

use std::sync::Arc;
use actix_web::{App, HttpServer, web};
use crate::database::{CachedDB, LocalDB};
use endpoints::*;

pub struct AppState {
	pub database: Arc<LocalDB>,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
	env_logger::init();

	let database = Arc::new(LocalDB::new());

	log::info!("Listening for requests on 8082");

	HttpServer::new(move || {
		App::new()
			.app_data(web::Data::new(AppState { 
				database: database.clone()
			}))
			.service(add_user_tags)
			.service(user_profiles)
			.service(aggregates)
	}).bind(("10.112.103.101", 8082))
		.expect("Creation of server failed")
		.run()
		.await
}
