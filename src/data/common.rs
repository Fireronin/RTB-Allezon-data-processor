use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub enum UserAction {
	VIEW,
	BUY,
}

impl TryFrom<&String> for UserAction {
	type Error = &'static str;
	
	fn try_from(value: &String) -> Result<Self, Self::Error> {
		match value.as_str() {
			"VIEW" => Ok(UserAction::VIEW),
			"BUY" => Ok(UserAction::BUY),
			_ => Err("Invalid action")
		}
	}
}