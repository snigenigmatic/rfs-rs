use bytes::Bytes;

use super::Database;
use super::value::Value;

impl Database {
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

    pub fn zscore(&self, key: &str, member: &Bytes) -> Option<f64> {
        if let Some(Value::ZSet(vec)) = self.data.get(key) {
            vec.iter()
                .find(|(m, _)| m == member)
                .map(|(_, score)| *score)
        } else {
            None
        }
    }

    pub fn zrank(&self, key: &str, member: &Bytes) -> Option<usize> {
        if let Some(Value::ZSet(vec)) = self.data.get(key) {
            vec.iter().position(|(m, _)| m == member)
        } else {
            None
        }
    }

    pub fn zcard(&self, key: &str) -> usize {
        if let Some(Value::ZSet(vec)) = self.data.get(key) {
            vec.len()
        } else {
            0
        }
    }

    pub fn zrem(&mut self, key: &str, members: Vec<Bytes>) -> usize {
        if let Some(Value::ZSet(vec)) = self.data.get_mut(key) {
            let mut removed = 0;
            for m in &members {
                if let Some(pos) = vec.iter().position(|(mb, _)| mb == m) {
                    vec.remove(pos);
                    removed += 1;
                }
            }
            if vec.is_empty() {
                self.data.remove(key);
            }
            removed
        } else {
            0
        }
    }

    pub fn zcount(&self, key: &str, min: f64, max: f64) -> usize {
        if let Some(Value::ZSet(vec)) = self.data.get(key) {
            vec.iter()
                .filter(|(_, score)| *score >= min && *score <= max)
                .count()
        } else {
            0
        }
    }

    pub fn zrange(
        &self,
        key: &str,
        start: i64,
        stop: i64,
        with_scores: bool,
    ) -> Vec<(Bytes, Option<f64>)> {
        if let Some(Value::ZSet(vec)) = self.data.get(key) {
            let len = vec.len() as i64;
            if len == 0 {
                return Vec::new();
            }
            let s = if start < 0 {
                (len + start).max(0)
            } else {
                start.min(len)
            } as usize;
            let e = if stop < 0 {
                (len + stop).max(-1) + 1
            } else {
                (stop + 1).min(len)
            } as usize;
            if s >= e {
                return Vec::new();
            }
            vec.iter()
                .skip(s)
                .take(e - s)
                .map(|(m, score)| {
                    if with_scores {
                        (m.clone(), Some(*score))
                    } else {
                        (m.clone(), None)
                    }
                })
                .collect()
        } else {
            Vec::new()
        }
    }

    pub fn zrevrange(
        &self,
        key: &str,
        start: i64,
        stop: i64,
        with_scores: bool,
    ) -> Vec<(Bytes, Option<f64>)> {
        if let Some(Value::ZSet(vec)) = self.data.get(key) {
            let len = vec.len() as i64;
            if len == 0 {
                return Vec::new();
            }
            let s = if start < 0 {
                (len + start).max(0)
            } else {
                start.min(len)
            } as usize;
            let e = if stop < 0 {
                (len + stop).max(-1) + 1
            } else {
                (stop + 1).min(len)
            } as usize;
            if s >= e {
                return Vec::new();
            }
            vec.iter()
                .rev()
                .skip(s)
                .take(e - s)
                .map(|(m, score)| {
                    if with_scores {
                        (m.clone(), Some(*score))
                    } else {
                        (m.clone(), None)
                    }
                })
                .collect()
        } else {
            Vec::new()
        }
    }
}
