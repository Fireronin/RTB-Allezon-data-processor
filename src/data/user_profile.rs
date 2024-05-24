use crate::{api::ApiUserTagWorking, data::UserTagEvent};

#[derive(Default, Clone)]
pub struct UserProfile {
	pub view_events: Vec<UserTagEvent>,
	pub buy_events: Vec<UserTagEvent>,
}

#[derive(Default, Clone)]
pub struct UserProfileUncompresed {
	pub view_events: Vec<ApiUserTagWorking>,
	pub buy_events: Vec<ApiUserTagWorking>,
}