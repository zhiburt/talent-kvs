#![deny(missing_docs)]
use std::collections::HashMap;

/// KvStore represent simple key value storage
#[derive(Default)]
pub struct KvStore {
    store: HashMap<String, String>,
}

impl KvStore {
    /// Create new object of storage
    pub fn new() -> Self {
        KvStore {
            store: HashMap::new(),
        }
    }

    /// Get method tries to find value with `key`
    pub fn get(&self, key: String) -> Option<String> {
        self.store.get(&key).cloned()
    }

    /// Set put new value in storage by key
    /// it rewrite value if that alredy exists
    ///
    /// # Example
    ///
    /// ```
    /// use kvs::KvStore;
    /// let mut kv = KvStore::new();
    /// kv.set("some".to_owned(), "value".to_owned());
    /// kv.set("some".to_owned(), "new value".to_owned());
    /// 
    /// assert_eq!(kv.get("some".to_string()).unwrap(), "new value".to_string());
    /// ```
    pub fn set(&mut self, key: String, val: String) {
        self.store.insert(key, val);
    }

    /// Delete key value pair from storage
    pub fn remove(&mut self, key: String) {
        self.store.remove(&key);
    }
}
