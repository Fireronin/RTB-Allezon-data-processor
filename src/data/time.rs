pub struct TimeRange {
	start: i64,
	end: i64,
}

impl TimeRange {
	pub fn within(&self, timestamp: i64) -> bool {
		self.start <= timestamp && timestamp < self.end
	}
}

pub fn parse_timestamp(timestamp: &str) -> i64 {
	chrono::DateTime::parse_from_rfc3339(timestamp).unwrap().timestamp_millis()
}

pub fn parse_time_range(time_range: &str) -> TimeRange {
	// split the time_range into start and end and parse it 2022-03-01T00:00:01.000_2022-03-01T00:00:01.619
	let time_range :Vec<String> = time_range.split("_")
		.map(|x| x.to_string() + "Z")
		.collect();
	let start = parse_timestamp(time_range[0].as_str());
	let end = parse_timestamp(time_range[1].as_str());
	
	TimeRange {
		start,
		end,
	}
}