#![doc = include_str!("../README.md")]

mod client;
mod error;
mod types;

pub use client::{Client, ClientConfig};
pub use error::{AmbiguousWrite, ConnectError, Error, ServerError, WriteError};
pub use types::{RequestId, RequestIdError, Row, Status, TableConfig, WriteReceipt};
