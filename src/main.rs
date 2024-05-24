use std::{future::IntoFuture, sync::Arc};

use actix_web::{App, HttpServer, web};

use endpoints::*;
use futures::TryFutureExt;

use crate::database::{PostgresDB};

mod endpoints;
mod database;
mod data;
mod tests;
pub mod api;
mod compression;

pub struct AppState {
	pub database: Arc<PostgresDB>,
	// pub database: Arc<LocalDB>,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
	// let database = Arc::new(LocalDB::new());
	let database = Arc::new(PostgresDB::new().await.expect("Failed to create database"));
	
	HttpServer::new(move || {
		App::new()
			.app_data(web::Data::new(AppState { 
				database: database.clone()
			}))
			.service(add_user_tags)
			.service(user_profiles)
			.service(aggregates)
	}).bind(("10.111.255.123", 8082))
		.expect("Creation of server failed")
		.run()
		.await
}
