#![allow(unused)]
use core::hash;
use std::sync::{Arc, Mutex};

use crate::api::*;
use crate::data::*;
use crate::database::Database;
use crate::GetAggregateApiRequest;
use futures::Future;
use rayon::vec;
use serde::Deserialize;
use tokio::sync::mpsc::Receiver;

// use super::Synced;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::prelude::*;
use crate::data::time::*;

pub struct PostgresDB {
    pool: PgPool,
	poolAggregate: PgPool,
	tx: tokio::sync::mpsc::Sender<(Cookie, ApiUserTag, UserAction)>,
}

// #[derive(Debug, Deserialize)]
// struct UserTagRecord {
// 	#[allow(dead_code)]
// 	id: Thing,
// 	tags: Vec<UserTagEvent>
// }
const MAX_SHARD: u64 = 500;
fn hash_string(s: &str) -> i32 {
	
	use std::collections::hash_map::DefaultHasher;
	use std::hash::{Hash, Hasher};

	let mut hasher = DefaultHasher::new();
	s.hash(&mut hasher);
	let out = hasher.finish();
	(out % MAX_SHARD) as i32

}

impl PostgresDB {
    pub async fn new() -> Result<Self, anyhow::Error> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect("postgres://postgres:root@10.112.123.104:5432")
            .await?;
		let poolAggregate = PgPoolOptions::new()
			.max_connections(5)
			.connect("postgres://postgres:root@10.112.123.103:5432")
			.await?;
        // clear tables
        sqlx::query("DROP TABLE IF EXISTS view_tags")
            .execute(&pool)
            .await?;
        sqlx::query("DROP TABLE IF EXISTS buy_tags")
            .execute(&pool)
            .await?;

		// find and drop all tables that start with aggregate_
		let tables = sqlx::query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'aggregate_%'")
			.fetch_all(&poolAggregate)
			.await?;
		for table in tables {
			sqlx::query(&format!("DROP TABLE IF EXISTS {}", table.get::<String, &str>("table_name")))
				.execute(&poolAggregate)
				.await?;
		}
		println!("dropped tables Aggregate");

		// drop all tables that start with view_tags_
		let tables = sqlx::query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'view_tags_%'")
			.fetch_all(&pool)
			.await?;
		for table in tables {
			sqlx::query(&format!("DROP TABLE IF EXISTS {}", table.get::<String, &str>("table_name")))
				.execute(&pool)
				.await?;
		}
		println!("dropped tables View Tags");

		// drop all tables that start with buy_tags_
		let tables = sqlx::query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'buy_tags_%'")
			.fetch_all(&pool)
			.await?;
		for table in tables {
			sqlx::query(&format!("DROP TABLE IF EXISTS {}", table.get::<String, &str>("table_name")))
				.execute(&pool)
				.await?;
		}
		println!("dropped tables Buy Tags");



        sqlx::query(
            "
			CREATE TABLE IF NOT EXISTS view_tags (
				key TEXT,
				shard INT,  
				timestamp BIGINT, 
				value BYTEA,
				PRIMARY KEY (key,shard, timestamp)
			) PARTITION BY RANGE (shard)
		",
        )
        .execute(&pool)
        .await?;

		// create subtables for view_tags based on the shard , max shard is MAX_SHARD
		let mut tx = pool.begin().await?;

		for i in 0..MAX_SHARD {
			sqlx::query(&format!("CREATE TABLE IF NOT EXISTS view_tags_{} PARTITION OF view_tags FOR VALUES FROM ({}) TO ({})", i, i, i+1))
				.execute(&mut *tx).await;
		}
		
		tx.commit().await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS view_tags_key_idx ON view_tags(key)",
        )
        .execute(&pool)
        .await?;

		println!("created tables View Tags");

        sqlx::query(
            "
			CREATE TABLE IF NOT EXISTS buy_tags (
				key TEXT,
				shard INT,  
				timestamp BIGINT, 
				value BYTEA,
				PRIMARY KEY (key,shard, timestamp)
			) PARTITION BY RANGE (shard)
		",
        )
        .execute(&pool)
        .await?;

		// create subtables for buy_tags based on the shard , max shard is MAX_SHARD
		for i in 0..MAX_SHARD {
			sqlx::query(&format!("CREATE TABLE IF NOT EXISTS buy_tags_{} PARTITION OF buy_tags FOR VALUES FROM ({}) TO ({})", i, i, i+1)).execute(&pool).await?;
		}

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS buy_tags_key_idx ON buy_tags(key)",)
        .execute(&pool)
        .await?;


        println!("created tables");
		let (tx, rx) = tokio::sync::mpsc::channel(20000);
		// create a thread that will take from rx and insert into db add_user_events_batch
		let pool_clone = pool.clone();
		let pool_agregate_clone = poolAggregate.clone();
		let tx_clone = tx.clone();
		
		tokio::spawn(async move {
			let db = PostgresDB { pool: pool_clone, poolAggregate:pool_agregate_clone, tx : tx_clone };
			db.add_user_events_batch(rx).await;
		});
		

        Ok(Self { pool,poolAggregate, tx})
    }

    async fn compress(
        &self,
        origin: Option<&String>,
        brand: Option<&String>,
        category: Option<&String>,
    ) -> GetAggregateResponse {
        todo!()
    }


	// wee nned to do this but to the database
	// pub async fn data_saver(minute_data_ptr: Arc<DashMap<i64,MinuteData>>, rx: &mut mpsc::Receiver<CompressedTag>) {
    
	// 	const LIMIT: usize = 32;
	// 	loop {
	// 		let mut buffer = Vec::new();
	// 		let _ = rx.recv_many(&mut buffer, LIMIT).await;
	// 		for tag in buffer.iter() {
	// 			minute_data_ptr.entry(tag.timestamp/60000).or_insert_with(|| MinuteData {
	// 				product_id: Vec::new(),
	// 				brand_id: Vec::new(),
	// 				category_id: Vec::new(),
	// 				price: Vec::new(),
	// 				action: Vec::new(),
	// 			});
	// 			minute_data_ptr.get_mut(&(tag.timestamp/60000)).unwrap().product_id.push(tag.origin_id);
	// 			minute_data_ptr.get_mut(&(tag.timestamp/60000)).unwrap().brand_id.push(tag.brand_id);
	// 			minute_data_ptr.get_mut(&(tag.timestamp/60000)).unwrap().category_id.push(tag.category_id);
	// 			minute_data_ptr.get_mut(&(tag.timestamp/60000)).unwrap().price.push(tag.price);
	// 			minute_data_ptr.get_mut(&(tag.timestamp/60000)).unwrap().action.push(tag.action.clone());
	// 			//println!("Received tag: {:?}", tag);
	// 		}
	// 	}
	// }

	// 
	async fn add_for_aggregate(&self, data : Vec<(Cookie, ApiUserTag, UserAction)> ) {
		// one for each minute
		let mut tabes_to_create = vec![];

		// for each data point add it's timestamp rounded to the minute to the tabes_to_create
		for (cookie, tag, action) in &data {
			let time = parse_timestamp(tag.time.as_str()).unwrap();
			tabes_to_create.push(time/60000);
		}
		// dedup the tables
		tabes_to_create.sort();
		tabes_to_create.dedup();
		// for each table create a table if it does not exist
		for table in tabes_to_create {
			sqlx::query(&format!(
				"
				CREATE TABLE IF NOT EXISTS aggregate_{} (
					origin_id TEXT, 
					brand_id TEXT, 
					category_id TEXT, 
					price BIGINT, 
					action TEXT
				)
				",
				table
			))
			.execute(&self.poolAggregate)
			.await
			.unwrap();
		}
		// for each data point insert it into the table with the timestamp rounded to the minute
		// do this in one transaction
		let mut tx = self.poolAggregate.begin().await.unwrap();
		for (cookie, tag, action) in data {
			let time = parse_timestamp(tag.time.as_str()).unwrap();
			let table = time/60000;
			let str: &str = cookie.0.as_str();
			let price: i64 = tag.product_info.price as i64;
			sqlx::query(&format!(
				"
				INSERT INTO aggregate_{} (origin_id, brand_id, category_id, price, action) VALUES ($1, $2, $3, $4, $5)
				",
				table
			))
			.bind(tag.origin)
			.bind(tag.product_info.brand_id)
			.bind(tag.product_info.category_id)
			.bind(price)
			.bind(tag.action)
			.execute(&mut *tx)
			.await
			.unwrap();
		}
		tx.commit().await.unwrap();
		


	}


	// write a function that takes mpsc reciver and does
	async fn add_user_events_batch(
		&self,
		mut receiver: Receiver<(Cookie, ApiUserTag, UserAction)>,
	) {
		let mut buffer = Arc::new(Mutex::new(Vec::new()));
		while let Some((cookie, tag, action)) = receiver.recv().await {
			buffer.lock().unwrap().push((cookie, tag, action));
			if buffer.lock().unwrap().len() >= 100 {
				let pool = self.pool.clone();
				let buffer_clone : Vec<(Cookie, ApiUserTag, UserAction)> = buffer.lock().unwrap().clone();
				self.add_for_aggregate(buffer_clone.clone()).await;
				tokio::spawn(async move {
					let mut tx = pool.begin().await.unwrap();
					for (cookie, tag, action) in buffer_clone {
						let table = match action {
							UserAction::VIEW => "view_tags",
							UserAction::BUY => "buy_tags",
						};
						let serialized = bincode::serialize(&tag).unwrap();
						let str: &str = cookie.0.as_str();
						let time = parse_timestamp(tag.time.as_str()).unwrap();
						sqlx::query(&format!(
							"INSERT INTO {} (key,shard, timestamp, value) VALUES ($1,$2, $3, $4)",
							table
						))
						.bind(str)
						.bind(hash_string(str))
						.bind(time)
						.bind(serialized)
						.execute(&mut *tx)
						.await
						.unwrap();
					}
					tx.commit().await.unwrap();
				});
				buffer = Arc::new(Mutex::new(Vec::new()));
			}
		}
	}

}

impl Database for PostgresDB {
    async fn add_user_event(&self, cookie: &Cookie, tag: UserTagEvent, action: UserAction) {
        // let table = match action {
        //     UserAction::VIEW => "view_tags",
        //     UserAction::BUY => "buy_tags",
        // };

        // print!("table: {}", table);
        // // insert into table based on action, serialize UserTagEvent to bytes
        // let serialized = bincode::serialize(&tag).unwrap();
        // let str: &str = cookie.0.as_str();

        // sqlx::query(&format!(
        //     "INSERT INTO {} (key, timestamp, value) VALUES ($1, $2, $3)",
        //     table
        // ))
        // .bind(str)
        // .bind(tag.time)
        // .bind(serialized)
        // .execute(&self.pool)
        // .await
        // .unwrap();
    }

    async fn add_user_event_uncompresed(
        &self,
        cookie: &Cookie,
        tag: ApiUserTag,
        action: UserAction,
    ) {
		let result = self.tx.send((cookie.clone(), tag.clone(), action)).await;
		if result.is_err() {
			println!("Error sending to mpsc");
		}

        // let table = match action {
        //     UserAction::VIEW => "view_tags",
        //     UserAction::BUY => "buy_tags",
        // };
        // // insert into table based on action, serialize UserTagEvent to bytes
        // let serialized = bincode::serialize(&tag).unwrap();
        // let str: &str = cookie.0.as_str();
        // // parse tag.time to i64
        // let time = parse_timestamp(tag.time.as_str()).unwrap();

        // let pool = self.pool.clone();
        // let str_clone = str.to_string();
        // let serialized_clone = serialized.clone();

        // tokio::spawn(async move {
        //     sqlx::query(&format!(
        //         "INSERT INTO {} (key, timestamp, value) VALUES ($1, $2, $3)",
        //         table
        //     ))
        //     .bind(str_clone)
        //     .bind(time)
        //     .bind(serialized_clone)
        //     .execute(&pool)
        //     .await
        //     .unwrap();
        // });
    }

    async fn get_user_profile(&self, cookie: &Cookie) -> UserProfile {
        todo!()
        // let view_tags: Result<Option<UserTagRecord>, surrealdb::Error> = self.db.select(("view_tags", cookie)).await;
        // let buy_tags: Result<Option<UserTagRecord>, surrealdb::Error> = self.db.select(("buy_tags", cookie)).await;

        // let mapper = |record: Option<UserTagRecord>| -> Vec<UserTagEvent> {
        // 	record.map(|r| {
        // 		r.tags
        // 	}).unwrap_or(vec![])
        // };

        // let view_tags = mapper(view_tags.unwrap());
        // let buy_tags = mapper(buy_tags.unwrap());

        // (view_tags, buy_tags)
    }

    async fn get_user_profile_uncompresed(&self, cookie: &Cookie) -> UserProfileUncompresed {
        // get from both tables based on cookie and deserialize
        let mut view_tags: Vec<ApiUserTag> =
            sqlx::query(&format!("SELECT value FROM view_tags_{} WHERE shard = $1 AND key = $2 ",hash_string(cookie.0.as_str())))
                .bind(hash_string(cookie.0.as_str()))
				.bind(cookie.0.as_str())
                .fetch_all(&self.pool)
                .await
                .unwrap()
                .iter()
                .map(|row| {
                    let bytes: Vec<u8> = row.get(0);
                    bincode::deserialize(&bytes).unwrap()
                })
                .collect();
        let mut buy_tags: Vec<ApiUserTag> =
            sqlx::query(&format!("SELECT value FROM buy_tags_{} WHERE shard = $1 AND key = $2",hash_string(cookie.0.as_str())))
                .bind(hash_string(cookie.0.as_str()))
				.bind(cookie.0.as_str())
                .fetch_all(&self.pool)
                .await
                .unwrap()
                .iter()
                .map(|row| {
                    let bytes: Vec<u8> = row.get(0);
                    bincode::deserialize(&bytes).unwrap()
                })
                .collect();

        // itterate over both and parse timestamp, then return latest MAX_TAGS and send request to drop the rest
        view_tags.sort_by(|a, b| {
            parse_timestamp(&a.time)
                .unwrap()
                .cmp(&parse_timestamp(&b.time).unwrap())
        });

        buy_tags.sort_by(|a, b| {
            parse_timestamp(&a.time)
                .unwrap()
                .cmp(&parse_timestamp(&b.time).unwrap())
        });

        // get the latest MAX_TAGS
        let view_tags_taken: Vec<ApiUserTag> =
            view_tags.clone().into_iter().rev().take(MAX_TAGS).collect();
        let buy_tags_taken: Vec<ApiUserTag> =
            buy_tags.clone().into_iter().rev().take(MAX_TAGS).collect();

        // send request to drop the rest
        let view_tags_to_drop = view_tags
            .into_iter()
            .rev()
            .skip(MAX_TAGS)
            .collect::<Vec<ApiUserTag>>();
        let buy_tags_to_drop = buy_tags
            .into_iter()
            .rev()
            .skip(MAX_TAGS)
            .collect::<Vec<ApiUserTag>>();

        // drop the rest
        //if (view_tags_to_drop.len() > 0 || buy_tags_to_drop.len() > 0) {

        let pool = self.pool.clone();
        let cookie_clone = cookie.clone();

        tokio::spawn(async move {
            let mut tx = pool.begin().await.unwrap();

            for tag in view_tags_to_drop {
                sqlx::query("DELETE FROM view_tags WHERE shard = $1 AND key = $2 AND timestamp = $3")
                    .bind(hash_string(cookie_clone.0.as_str()))
					.bind(cookie_clone.0.as_str())
                    .bind(parse_timestamp(&tag.time).unwrap())
                    .execute(&mut *tx)
                    .await
                    .unwrap();
            }

            for tag in buy_tags_to_drop {
                sqlx::query("DELETE FROM buy_tags WHERE shard = $1 AND key = $2 AND timestamp = $3")
					.bind(hash_string(cookie_clone.0.as_str()))    
					.bind(cookie_clone.0.as_str())
                    .bind(parse_timestamp(&tag.time).unwrap())
                    .execute(&mut *tx)
                    .await
                    .unwrap();
            }

            tx.commit().await.unwrap();
        });
        //}
        // return both
        return UserProfileUncompresed {
            view_events: view_tags_taken,
            buy_events: buy_tags_taken,
        };
    }

    async fn add_aggregate_event(&self, timestamp: i64, tag: AggregateTagEvent) {
        todo!()
    }

    async fn get_aggregate(&self, request: &GetAggregateRequest) -> GetAggregateResponse {
        todo!()
    }

	async fn get_aggregate_uncompresed(&self, request: &GetAggregateApiRequest,querry_types : Vec<AggregateRequestType>) -> GetAggregateResponse{
		let time_range: Vec<&str> = request.time_range.split("_").collect();
		// add Z to the end of the time string to make it RFC3339 compliant
		let time_range: Vec<String> = time_range.iter().map(|x| x.to_string() + "Z").collect();
		let start_timestamp = chrono::DateTime::parse_from_rfc3339(time_range[0].as_str()).unwrap().timestamp_millis();
		let end_timestamp = chrono::DateTime::parse_from_rfc3339(time_range[1].as_str()).unwrap().timestamp_millis();
	
		// divide the time range into minutes
		let start_minute = start_timestamp/60000;
		let end_minute = end_timestamp/60000;
	
		let minutes_to_process = start_minute..end_minute;

		let mut aggregates: Arc<Mutex<Vec<AggregateBucket>>> = Arc::new(Mutex::new(vec![]));
		// alocaate the aggregates
		for _ in minutes_to_process.clone() {
			aggregates.lock().unwrap().push(AggregateBucket { sum: 0, count: 0 });
		}

		let pool = self.poolAggregate.clone();
		let mut handles = vec![];
		// for each minute get the data from the table
		for (index, minute) in minutes_to_process.enumerate() {
			let pool_clone = pool.clone();
			let request_clone = request.clone();
			let querry_types_clone = querry_types.clone();
			let aggregates_clone = Arc::clone(&aggregates); // Clone the Arc
			handles.push(tokio::spawn(async move {
			let table = format!("aggregate_{}", minute);
			let mut query = format!("SELECT COUNT(*),CAST(SUM(price) AS BIGINT) FROM {}", table);
			let mut binds = vec![];
			
			query.push_str(format!(" WHERE action = ${}", binds.len() + 1).as_str());
			binds.push(request_clone.action.clone());
			
			if request_clone.origin.is_some() {
				query.push_str(format!(" AND origin_id = ${}", binds.len() + 1).as_str());
				binds.push(request_clone.origin.clone().unwrap());
			}
			if request_clone.brand_id.is_some() {
				query.push_str(format!(" AND brand_id = ${}", binds.len() + 1).as_str());
				binds.push(request_clone.brand_id.clone().unwrap());
			}
			if request_clone.category_id.is_some() {
				query.push_str(format!(" AND category_id = ${}", binds.len() + 1).as_str());
				binds.push(request_clone.category_id.clone().unwrap());
			}
			
			let rows = {
				let mut rows = sqlx::query(&query);
				for bind in binds {
					rows = rows.bind(bind);
				}
				rows.fetch_one(&pool_clone).await.unwrap()
			};
			let sum  = rows.try_get::<i64, usize>(1);
			// if no data set sum to 0
			let sum = match sum {
				Ok(sum) => sum,
				Err(_) => 0,
			};
			let count = rows.get::<i64, usize>(0);

			aggregates_clone.lock().unwrap()[index].sum = sum as u64;
			aggregates_clone.lock().unwrap()[index].count = count as u64;
				
				
			}));
		}


		for handle in handles {
			handle.await.unwrap();
		}
		let aggregates = aggregates.lock().unwrap().clone();
		GetAggregateResponse { aggregates }
		
	}

}

// impl Compressor<UserTagEvent> for PostgresDB {
// 	async fn compress_with_partial(&self, partial: PartialUserTagEventCompressedData) -> UserTagEventCompressedData {
// 		todo!()
// 	}
// }

// impl Decompressor<UserTagEvent> for PostgresDB {
// 	async fn decompress_with_partial(&self, partial: PartialUserTagEventCompressedData) -> UserTagEventDecompressedData {
// 		todo!()
// 	}
// }

// impl Compressor<AggregateTagEvent> for PostgresDB {
// 	async fn compress_with_partial(&self, partial: PartialAggregateTagEventCompressedData) -> AggregateTagEventCompressedData {
// 		todo!()
// 	}
// }

// impl Compressor<GetAggregateRequest> for PostgresDB {
// 	async fn compress_with_partial(&self, partial: PartialGetAggregateRequestCompressedData) -> GetAggregateRequestCompressedData {
// 		todo!()
// 	}
// }

// impl Synced for PostgresDB {}
// impl SyncedDB for PostgresDB {}
