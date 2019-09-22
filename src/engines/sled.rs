use sled::{Db, Tree};
use crate::{KvsError, Result};
use super::KvsEngine;
use std::path::{Path, PathBuf};

pub struct SledStorage(Db);

impl SledStorage {
     /// Create new object of storage
    pub fn open(folder: impl Into<PathBuf>) -> Result<Self> {
        let db = Db::open(folder.into())?;
        Ok(SledStorage(db))
    }
}

impl KvsEngine for SledStorage {
    /// Get method tries to find value with `key`
    fn get(&mut self, k: String) -> Result<Option<String>> {
        let tree: &Tree = &self.0;
        Ok(tree
            .get(k)?
            .map(|i_vec| i_vec.as_ref().to_vec())
            .map(String::from_utf8)
            .transpose()?)
    }

    /// Set put new value in storage by key
    /// it rewrite value if that alredy exists
    fn set(&mut self, key: String, val: String) -> Result<()> {
        self.0.set(key, val.into_bytes())?;
        self.0.flush()?;
        Ok(())
    }

    /// Delete key value pair from storage
    fn remove(&mut self, key: String) -> Result<()> {
        if self.0.remove(key)?.is_none() {
            return Err(KvsError::KeyNotFound);
        };
        self.0.flush()?;
        Ok(())
    }
}