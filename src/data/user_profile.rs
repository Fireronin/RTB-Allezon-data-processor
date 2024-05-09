use crate::data::UserTagEvent;

#[derive(Default, Clone)]
pub struct UserProfile {
	pub view_events: Vec<UserTagEvent>,
	pub buy_events: Vec<UserTagEvent>,
}