mod kvs;
mod error;

pub use crate::kvs::{KvStore};
pub use error::{KvsError, Result};