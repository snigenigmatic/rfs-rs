use bytes::Bytes;

use crate::store::value::Value;

use super::Database;

impl Database {
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
            0
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
            let s = if start < 0 {
                (len + start).max(0)
            } else {
                start.min(len)
            } as usize;
            let e = if stop < 0 {
                (len + stop).max(0)
            } else {
                stop.min(len - 1)
            } as usize;
            if s > e {
                return Vec::new();
            }
            deque.iter().skip(s).take(e - s + 1).cloned().collect()
        } else {
            Vec::new()
        }
    }
}
