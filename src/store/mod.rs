use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use bytes::Bytes;

pub mod expire;
pub mod value;

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

    // ── key-level ──────────────────────────────────────────────

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

    // ── list commands ──────────────────────────────────────────

    pub fn lpush(&mut self, key: String, values: Vec<Bytes>) -> usize {
        self.expiry.remove(&key);
        let list = self
            .data
            .entry(key)
            .or_insert_with(|| Value::List(Default::default()));
        if let Value::List(deque) = list {
            for v in values {
                deque.push_front(v);
            }
            deque.len()
        } else {
            0 // type error handled by caller
        }
    }

    pub fn rpush(&mut self, key: String, values: Vec<Bytes>) -> usize {
        self.expiry.remove(&key);
        let list = self
            .data
            .entry(key)
            .or_insert_with(|| Value::List(Default::default()));
        if let Value::List(deque) = list {
            for v in values {
                deque.push_back(v);
            }
            deque.len()
        } else {
            0
        }
    }

    pub fn lpop(&mut self, key: &str) -> Option<Bytes> {
        if let Some(Value::List(deque)) = self.data.get_mut(key) {
            let val = deque.pop_front();
            if deque.is_empty() {
                self.data.remove(key);
            }
            val
        } else {
            None
        }
    }

    pub fn rpop(&mut self, key: &str) -> Option<Bytes> {
        if let Some(Value::List(deque)) = self.data.get_mut(key) {
            let val = deque.pop_back();
            if deque.is_empty() {
                self.data.remove(key);
            }
            val
        } else {
            None
        }
    }

    pub fn lrange(&mut self, key: &str, start: i64, stop: i64) -> Vec<Bytes> {
        if let Some(Value::List(deque)) = self.data.get(key) {
            let len = deque.len() as i64;
            let s = if start < 0 { (len + start).max(0) } else { start.min(len) } as usize;
            let e = if stop < 0 { (len + stop).max(0) } else { stop.min(len - 1) } as usize;
            if s > e {
                return Vec::new();
            }
            deque.iter().skip(s).take(e - s + 1).cloned().collect()
        } else {
            Vec::new()
        }
    }

    // ── set commands ───────────────────────────────────────────

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

    // ── hash commands ──────────────────────────────────────────

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

    // ── sorted set commands ─────────────────────────────────────
    pub fn zadd(&mut self, key: String, members: Vec<(Bytes, f64)>) -> usize {
        let zset = self
            .data
            .entry(key)
            .or_insert_with(|| Value::ZSet(Default::default()));
        if let Value::ZSet(vec) = zset {
            let mut added = 0;
            for (m, s) in members {
                if let Some(pos) = vec.iter().position(|(mb, _)| mb == &m) {
                    vec[pos] = (m, s); // update score
                } else {
                    vec.push((m, s));
                    added += 1;
                }
            }
            vec.sort_by(|a, b| {
                let ord = a.1.partial_cmp(&b.1).unwrap();
                if ord == std::cmp::Ordering::Equal {
                    a.0.as_ref().cmp(b.0.as_ref())
                } else {                    
                    ord
                }
            });
            added
        } else {
            0
        }
    }

    // ── type check helpers ─────────────────────────────────────

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

    // ── AOF snapshot ───────────────────────────────────────────

    #[allow(dead_code)]
    pub fn snapshot_for_aof(&self) -> Vec<(String, Value)> {
        self.data
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

pub type SharedStore = Arc<RwLock<Database>>;

pub fn new_shared() -> SharedStore {
    Arc::new(RwLock::new(Database::new()))
}

