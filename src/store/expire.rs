use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::time::Instant;

/// Tracks key expiration deadlines using a min-heap + map.
#[derive(Debug, Default)]
pub struct Expiry {
    /// Maps key → deadline
    deadlines: HashMap<String, Instant>,
    /// Min-heap ordered by soonest deadline
    heap: BinaryHeap<Reverse<(Instant, String)>>,
}

impl Expiry {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set a deadline for a key. Overwrites any previous deadline.
    pub fn set_deadline(&mut self, key: String, deadline: Instant) {
        self.deadlines.insert(key.clone(), deadline);
        self.heap.push(Reverse((deadline, key)));
    }

    /// Remove any deadline for a key.
    pub fn remove(&mut self, key: &str) {
        self.deadlines.remove(key);
        // Lazy removal: stale entries cleaned up in `drain_expired`.
    }

    /// Check if the key has expired (or has no expiry set).
    /// Returns true if the key IS expired, false otherwise.
    pub fn is_expired(&self, key: &str) -> bool {
        match self.deadlines.get(key) {
            Some(deadline) => Instant::now() >= *deadline,
            None => false, // no expiration = not expired
        }
    }

    /// Returns the deadline for a key, if one is set.
    pub fn get_deadline(&self, key: &str) -> Option<Instant> {
        self.deadlines.get(key).copied()
    }

    /// Drain all expired keys, returning them for removal from the store.
    pub fn drain_expired(&mut self) -> Vec<String> {
        let now = Instant::now();
        let mut expired = Vec::new();

        while let Some(Reverse((deadline, _key))) = self.heap.peek() {
            if *deadline > now {
                break;
            }
            let Reverse((deadline, key)) = self.heap.pop().unwrap();

            // Check if this entry is still current (not overwritten by a newer deadline).
            match self.deadlines.get(&key) {
                Some(&current_deadline) if current_deadline == deadline => {
                    self.deadlines.remove(&key);
                    expired.push(key);
                }
                _ => {
                    // Stale entry; skip.
                }
            }
        }

        expired
    }
}
