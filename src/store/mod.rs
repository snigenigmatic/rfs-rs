use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub mod expire;
pub mod value;

mod hash;
mod keys;
mod list;
mod set;
mod zset;

use expire::Expiry;
use value::Value;

#[derive(Debug, Default)]
pub struct Database {
    data: HashMap<String, Value>,
    expiry: Expiry,
}

impl Database {
    pub fn new() -> Self {
        Self::default()
    }
}

pub type SharedStore = Arc<RwLock<Database>>;

pub fn new_shared() -> SharedStore {
    Arc::new(RwLock::new(Database::new()))
}
