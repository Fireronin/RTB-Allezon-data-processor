use crate::data::UserTagEvent;

pub struct UserProfile {
	pub view_events: Vec<UserTagEvent>,
	pub buy_events: Vec<UserTagEvent>,
}