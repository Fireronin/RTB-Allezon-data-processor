use crate::data::UserTagEvent;
use crate::api::ApiUserTag;

#[derive(Default, Clone)]
pub struct UserProfile {
	pub view_events: Vec<UserTagEvent>,
	pub buy_events: Vec<UserTagEvent>,
}

#[derive(Default, Clone)]
pub struct UserProfileUncompresed {
	pub view_events: Vec<ApiUserTag>,
	pub buy_events: Vec<ApiUserTag>,
}
