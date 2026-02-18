use bytes::Bytes;

use super::Database;
use super::value::Value;

impl Database {
    pub fn sadd(&mut self, key: String, members: Vec<Bytes>) -> usize {
        let set = self
            .data
            .entry(key)
            .or_insert_with(|| Value::Set(Default::default()));
        if let Value::Set(hs) = set {
            let mut added = 0;
            for m in members {
                if hs.insert(m) {
                    added += 1;
                }
            }
            added
        } else {
            0
        }
    }

    pub fn srem(&mut self, key: &str, members: Vec<Bytes>) -> usize {
        if let Some(Value::Set(hs)) = self.data.get_mut(key) {
            let mut removed = 0;
            for m in &members {
                if hs.remove(m) {
                    removed += 1;
                }
            }
            if hs.is_empty() {
                self.data.remove(key);
            }
            removed
        } else {
            0
        }
    }

    pub fn smembers(&self, key: &str) -> Vec<Bytes> {
        if let Some(Value::Set(hs)) = self.data.get(key) {
            hs.iter().cloned().collect()
        } else {
            Vec::new()
        }
    }
}
