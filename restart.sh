#sudo docker-compose down
#sudo docker-compose up -d surrealdb
RUST_LOG=debug,actix_web::middleware::Logger=debug cargo run
