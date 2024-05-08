use serde::Deserialize;
use surrealdb::engine::remote::ws::{Client, Ws};
use surrealdb::opt::auth::Root;
use surrealdb::sql::Thing;
use surrealdb::Surreal;
use crate::data::{AggregateTagEvent, AggregateCompressedRequest, MAX_TAGS, UserAction, UserTag};
use crate::data::time::TimeRange;
use crate::database::Database;

pub struct SurrealDB {
	db: Surreal<Client>,
}

#[derive(Debug, Deserialize)]
struct UserTagRecord {
	#[allow(dead_code)]
	id: Thing,
	tags: Vec<UserTag>
}

impl Database for SurrealDB {
	async fn new() -> Result<Self, anyhow::Error> {
		let db = Surreal::new::<Ws>("127.0.0.1:8080").await?;
		
		// Signin as a namespace, database, or root user
		db.signin(Root {
			username: "root",
			password: "root",
		}).await?;
		
		db.query(format!("DEFINE NAMESPACE test;
USE NAMESPACE test;
DEFINE DATABASE test;
USE DATABASE test;

-- ------------------------------
-- TABLE: tags
-- ------------------------------

DEFINE FUNCTION async fn::push_and_keep_size($arr: option<array<object>>, $v: object) {{
    RETURN IF type::is::none($arr) THEN
        <array<object, {MAX_TAGS}>>[$v]
    ELSE IF array::len($arr) = {MAX_TAGS} THEN
        array::push(array::remove($arr, 0), $v)
    ELSE
        array::push($arr, $v)
    END;
}};

DEFINE TABLE view_tags SCHEMALESS;
DEFINE TABLE buy_tags SCHEMALESS;

DEFINE FIELD tags ON TABLE view_tags TYPE array<object, {MAX_TAGS}>;
DEFINE FIELD tags ON TABLE buy_tags TYPE array<object, {MAX_TAGS}>;
")).await?;
		
		// Select a specific namespace / database
		db.use_ns("test").use_db("test").await?;
		Ok(Self {
			db,
		})
	}
	
	async fn add_user_event(&self, tag: &UserTag, action: UserAction) {
		let table = match action {
			UserAction::VIEW => "view_tags",
			UserAction::BUY => "buy_tags",
		};
		
		let query_string = format!("UPDATE {}:{} SET tags = async fn::push_and_keep_size(tags, $value);", table, &tag.cookie);
		
		let result = self.db.query(query_string)
			.bind(("value", &tag))
			.await
			.unwrap();
	}
	
	async fn get_user_profile(&self, cookie: &String) -> (Vec<UserTag>, Vec<UserTag>) {
		let view_tags: Result<Option<UserTagRecord>, surrealdb::Error> = self.db.select(("view_tags", cookie)).await;
		let buy_tags: Result<Option<UserTagRecord>, surrealdb::Error> = self.db.select(("buy_tags", cookie)).await;
		
		let mapper = |record: Option<UserTagRecord>| -> Vec<UserTag> {
			record.map(|r| {
				r.tags
			}).unwrap_or(vec![])
		};
		
		let view_tags = mapper(view_tags.unwrap());
		let buy_tags = mapper(buy_tags.unwrap());
		
		(view_tags, buy_tags)
	}
	
	async fn add_aggregate_event(&self, _tag: UserTag) {
		todo!()
	}
	
	async fn get_aggregate(&self, _time_range: &TimeRange) -> Vec<Vec<AggregateTagEvent>> {
		todo!()
	}
	
	async fn compress(&self, origin: Option<&String>, brand: Option<&String>, category: Option<&String>) -> AggregateCompressedRequest {
		todo!()
	}
}
