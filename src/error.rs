use failure::Fail;
use std::io;

#[derive(Fail, Debug)]
pub enum KvsError {
    #[fail(display = "{}", _0)]
    Io(#[fail(cause)] io::Error),
    #[fail(display = "{}", _0)]
    SerdeDecode(#[cause] rmp_serde::decode::Error),
    #[fail(display = "{}", _0)]
    SerdeEncode(#[cause] rmp_serde::encode::Error),
    #[fail(display = "{}", _0)]
    Sled(#[cause] sled::Error),
    #[fail(display = "UTF-8 error: {}", _0)]
    Utf8(#[cause] std::string::FromUtf8Error),
    #[fail(display = "Key not found")]
    KeyNotFound, 
    #[fail(display = "Cannot find a command we involved in")]
    AppropriateCommandNotFound, 
}

impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> KvsError {
        KvsError::Io(err)
    }
}

impl From<rmp_serde::decode::Error> for KvsError {
    fn from(err: rmp_serde::decode::Error) -> KvsError {
        KvsError::SerdeDecode(err)
    }
}

impl From<rmp_serde::encode::Error> for KvsError {
    fn from(err: rmp_serde::encode::Error) -> KvsError {
        KvsError::SerdeEncode(err)
    }
}

impl From<sled::Error> for KvsError {
    fn from(err: sled::Error) -> KvsError {
        KvsError::Sled(err)
    }
}

impl From<std::string::FromUtf8Error> for KvsError {
    fn from(err: std::string::FromUtf8Error) -> KvsError {
        KvsError::Utf8(err)
    }
}

pub type Result<T> = std::result::Result<T, KvsError>;
