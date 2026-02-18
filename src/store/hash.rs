use bytes::Bytes;

use super::Database;
use super::value::Value;

impl Database {
    pub fn hset(&mut self, key: String, fields: Vec<(Bytes, Bytes)>) -> usize {
        let hash = self
            .data
            .entry(key)
            .or_insert_with(|| Value::Hash(Default::default()));
        if let Value::Hash(hm) = hash {
            let mut added = 0;
            for (f, v) in fields {
                if hm.insert(f, v).is_none() {
                    added += 1;
                }
            }
            added
        } else {
            0
        }
    }

    pub fn hget(&self, key: &str, field: &Bytes) -> Option<Bytes> {
        if let Some(Value::Hash(hm)) = self.data.get(key) {
            hm.get(field).cloned()
        } else {
            None
        }
    }

    pub fn hgetall(&self, key: &str) -> Vec<(Bytes, Bytes)> {
        if let Some(Value::Hash(hm)) = self.data.get(key) {
            hm.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        } else {
            Vec::new()
        }
    }
}
