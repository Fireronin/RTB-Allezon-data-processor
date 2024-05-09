use std::sync::Arc;

use actix_web::{App, HttpServer, web};

use endpoints::*;

use crate::database::{AerospikeDB, CachedDB, Database};

mod endpoints;
mod database;
mod data;
mod tests;
pub mod api;

pub struct AppState {
	pub database: Arc<CachedDB<AerospikeDB>>,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
	let database = Arc::new(CachedDB::new().await);
	
	HttpServer::new(move || {
		App::new()
			.app_data(web::Data::new(AppState { 
				database: database.clone()
			}))
			.service(add_user_tags)
			.service(user_profiles)
			.service(aggregates)
	}).bind(("10.112.103.101", 8083))
		.expect("Creation of server failed")
		.run()
		.await
}
