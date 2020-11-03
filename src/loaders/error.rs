use std::error::Error;
use arrow::error::ArrowError;


#[derive(Debug, Clone, PartialEq)]
pub enum DataFrameError {
    MemoryError(String),
    ParseError(String),
    ComputeError(String),
    DivideByZero,
    IoError(String),
    NoneError,
    ArrowError(String),
    SqlError(String),
}

impl From<ArrowError> for DataFrameError {
    fn from(error: ArrowError) -> Self {
        DataFrameError::ArrowError(error.to_string())
    }
}

impl From<::std::io::Error> for DataFrameError {
    fn from(error: ::std::io::Error) -> Self {
        DataFrameError::IoError(error.to_string())
    }
}

pub type Result<T> = ::std::result::Result<T, DataFrameError>;
