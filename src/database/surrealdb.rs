use surrealdb::engine::remote::ws::{Client, Ws};
use surrealdb::opt::auth::Root;
use surrealdb::Surreal;
use crate::data::{CompressedTag, Compression, UserAction, UserTag};
use crate::data::time::TimeRange;

pub struct Database {
	db: Surreal<Client>,
}

impl Database {
	pub async fn new() -> Result<Self, anyhow::Error> {
		let db = Surreal::new::<Ws>("127.0.0.1:8080").await?;
		
		// Signin as a namespace, database, or root user
		db.signin(Root {
			username: "root",
			password: "root",
		}).await?;
		
		db.query("DEFINE NAMESPACE test;
USE NAMESPACE test;
DEFINE DATABASE test;
USE DATABASE test;

-- ------------------------------
-- TABLE: tags
-- ------------------------------

DEFINE FUNCTION fn::push_and_keep_size($arr: option<array<string>>, $v: string) {
    RETURN IF type::is::none($arr) THEN
        <array<string, 100>>[$v]
    ELSE IF array::len($arr) = 100 THEN
        array::push(array::remove($arr, 0), $v)
    ELSE
        array::push($arr, $v)
    END;
};

DEFINE TABLE view_tags SCHEMAFULL;
DEFINE TABLE buy_tags SCHEMAFULL;

DEFINE FIELD tags ON TABLE view_tags TYPE array<string, 100>;
DEFINE FIELD tags ON TABLE buy_tags TYPE array<string, 100>;
"
		).await?;
		
		// Select a specific namespace / database
		db.use_ns("test").use_db("test").await?;
		Ok(Self {
			db,
		})
	}
	
	pub async fn add_user_tag(&self, tag: &UserTag, action: UserAction) {
		let table = match action {
			UserAction::VIEW => "view_tags",
			UserAction::BUY => "buy_tags",
		};
		let query = format!(
			"UPDATE {table}:{} SET tags = fn::push_and_keep_size(tags, {});",
			&tag.cookie,
			serde_json::to_string(&tag).unwrap());
		let result = self.db.query(query).await.unwrap();
		println!("Add Tags: {:?}", result);
	}
	
	pub async fn get_tags(&self, cookie: &String) -> (Vec<UserTag>, Vec<UserTag>) {
		let view_tags = self.db.select(("view_tags", cookie)).await;
		let buy_tags = self.db.select(("buy_tags", cookie)).await;
		println!("Get Tags: {:?}", view_tags);
		println!("Buy Tags: {:?}", buy_tags);
		(view_tags.unwrap().unwrap(), buy_tags.unwrap().unwrap())
	}
	
	pub async fn add_minute(&self, tag: UserTag) {
		// unimplemented!()
	}
	
	pub async fn get_minute_aggregate(&self, time_range: &TimeRange) -> Vec<Vec<CompressedTag>> {
		// unimplemented!()
		vec![]
	}
	
	pub async fn compress(&self, origin: Option<&String>, brand: Option<&String>, category: Option<&String>) -> Compression {
		// unimplemented!()
		Compression {
			origin_id: None,
			brand_id: None,
			category_id: None,
		}
	}
}
