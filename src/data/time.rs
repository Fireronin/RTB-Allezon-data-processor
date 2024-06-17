use chrono::SecondsFormat;
use anyhow::Result;

#[derive(Debug)]
pub struct TimeRange {
	pub start: i64,
	pub end: i64,
}

impl TimeRange {
	pub fn new(time_range: &str) -> Result<TimeRange> {
		// split the time_range into start and end and parse it 2022-03-01T00:00:01.000_2022-03-01T00:00:01.619
		let time_range :Vec<String> = time_range.split("_")
			.map(|x| x.to_string() + "Z")
			.collect();
		let start = parse_timestamp(time_range[0].as_str())?;
		let end = parse_timestamp(time_range[1].as_str())?;
		
		Ok(TimeRange {
			start,
			end,
		})
	}
	
	pub fn within(&self, timestamp: i64) -> bool {
		self.start <= timestamp && timestamp < self.end
	}
}

pub fn parse_timestamp(timestamp: &str) -> Result<i64> {
	Ok(chrono::DateTime::parse_from_rfc3339(timestamp)?.timestamp_millis())
}

pub fn timestamp_to_str(timestamp: i64) -> String {
	chrono::DateTime::from_timestamp_millis(timestamp).unwrap()
		.to_rfc3339_opts(SecondsFormat::Millis, true)
		.replace(".000Z", "Z") // TODO: this is required for debug only, tests pass without it, but strings with time are not equal
}
