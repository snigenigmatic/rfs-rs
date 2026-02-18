use std::time::{Duration, Instant};

use super::Database;
use super::value::Value;

impl Database {
    pub fn set(&mut self, key: String, value: Value) {
        self.expiry.remove(&key);
        self.data.insert(key, value);
    }

    pub fn set_with_expiry(&mut self, key: String, value: Value, ttl: Duration) {
        let deadline = Instant::now() + ttl;
        self.data.insert(key.clone(), value);
        self.expiry.set_deadline(key, deadline);
    }

    pub fn get(&mut self, key: &str) -> Option<Value> {
        if self.expiry.is_expired(key) {
            self.data.remove(key);
            self.expiry.remove(key);
            return None;
        }
        self.data.get(key).cloned()
    }

    pub fn exists(&mut self, keys: &[String]) -> usize {
        keys.iter()
            .filter(|k| {
                if self.expiry.is_expired(k) {
                    self.data.remove(k.as_str());
                    self.expiry.remove(k);
                    false
                } else {
                    self.data.contains_key(k.as_str())
                }
            })
            .count()
    }

    pub fn del(&mut self, keys: &[String]) -> usize {
        let mut removed = 0;
        for key in keys {
            if self.data.remove(key).is_some() {
                self.expiry.remove(key);
                removed += 1;
            }
        }
        removed
    }

    pub fn ttl_millis(&mut self, key: &str) -> i64 {
        if self.expiry.is_expired(key) {
            self.data.remove(key);
            self.expiry.remove(key);
            return -2; // key does not exist
        }
        if !self.data.contains_key(key) {
            return -2;
        }
        match self.expiry.get_deadline(key) {
            Some(deadline) => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                remaining.as_millis() as i64
            }
            None => -1, // no expiry
        }
    }

    /// Drain expired keys (called periodically).
    pub fn evict_expired(&mut self) -> usize {
        let expired = self.expiry.drain_expired();
        let count = expired.len();
        for key in expired {
            self.data.remove(&key);
        }
        count
    }

    pub fn is_type(&self, key: &str, expected: &str) -> bool {
        match self.data.get(key) {
            None => true, // key doesn't exist, any type is fine
            Some(Value::String(_)) => expected == "string",
            Some(Value::List(_)) => expected == "list",
            Some(Value::Set(_)) => expected == "set",
            Some(Value::Hash(_)) => expected == "hash",
            Some(Value::ZSet(_)) => expected == "zset",
        }
    }

    /// Snapshot current data for AOF rewrite.
    #[allow(dead_code)]
    pub fn snapshot_for_aof(&self) -> Vec<(String, Value)> {
        self.data
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}
