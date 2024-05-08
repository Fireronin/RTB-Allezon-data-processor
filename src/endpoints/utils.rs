use std::fmt::{Debug, Display};
use actix_web::error::InternalError;
use actix_web::http::StatusCode;
use actix_web::{Error, Result};

pub trait IntoHttpError<T, E: Display + Debug + 'static> {
	fn map_error(self, status_code: StatusCode) -> Result<T>;
}

impl<T, E: Display + Debug + 'static> IntoHttpError<T, E> for anyhow::Result<T, E> {
	fn map_error(self, status_code: StatusCode) -> Result<T> {
		match self {
			Ok(v) => Ok(v),
			Err(err) => Err(Error::from(InternalError::new(err, status_code))),
		}
	}
}