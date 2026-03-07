// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Deterministic in-process caches.
//!
//! This module provides a small, dependency-minimal 2Q cache.
//!
//! Design goals:
//! - bounded by bytes (and optionally by items)
//! - deterministic eviction order given the same access sequence
//! - no wall-clock TTL or background threads
//! - explicit keys (typically Hash32 of canonical artifact bytes)

use core::hash::Hash;
use std::collections::VecDeque;

use rustc_hash::FxHashMap;

/// Cache configuration (v1).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CacheCfgV1 {
    /// Total byte budget for the cache.
    pub max_bytes_total: u64,
    /// Optional total item cap. Use 0 for no item cap.
    pub max_items_total: u32,
    /// Percentage of the total byte budget reserved for A1 (0..=100).
    ///
    /// A1 is the probationary queue for items seen once recently.
    pub a1_ratio: u8,
}

impl CacheCfgV1 {
    /// Create a cache config with a total byte budget and defaults.
    ///
    /// Defaults:
    /// - max_items_total = 0 (no cap)
    /// - a1_ratio = 50
    pub fn new(max_bytes_total: u64) -> Self {
        Self {
            max_bytes_total,
            max_items_total: 0,
            a1_ratio: 50,
        }
    }

    fn a1_max_bytes(&self) -> u64 {
        let ratio = self.a1_ratio.min(100) as u128;
        let total = self.max_bytes_total as u128;
        ((total * ratio) / 100) as u64
    }

    fn am_max_bytes(&self) -> u64 {
        self.max_bytes_total.saturating_sub(self.a1_max_bytes())
    }
}

/// Cache statistics (v1).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CacheStatsV1 {
    /// Total number of lookups.
    pub lookups: u64,
    /// Hit count in A1 (probationary).
    pub hits_a1: u64,
    /// Hit count in Am (main).
    pub hits_am: u64,
    /// Total misses.
    pub misses: u64,
    /// Total inserts (successful cache stores).
    pub inserts: u64,
    /// Total evictions from A1.
    pub evicts_a1: u64,
    /// Total evictions from Am.
    pub evicts_am: u64,
    /// Total bytes evicted (cumulative).
    pub bytes_evicted_total: u64,
    /// Insert rejects because item cost exceeded total cache budget.
    pub rejects_oversize: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QueueKind {
    A1,
    Am,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct QKey {
    stamp: u64,
}

#[derive(Clone, Debug)]
struct Entry<V> {
    value: V,
    cost_bytes: u64,
    kind: QueueKind,
    stamp: u64,
}

/// A deterministic 2Q cache (A1 probationary + Am main).
///
/// The cache is bounded by bytes and optionally by number of items.
///
/// Determinism notes:
/// - The eviction decision depends only on the access sequence.
/// - No time-based behavior is used.
/// - Queue ordering is maintained by deterministic "stamps".
pub struct Cache2Q<K, V>
where
    K: Eq + Hash + Clone,
{
    cfg: CacheCfgV1,
    stats: CacheStatsV1,

    stamp_ctr: u64,

    bytes_a1: u64,
    bytes_am: u64,

    // Main storage.
    map: FxHashMap<K, Entry<V>>,
    // Each queue stores (key, stamp) pairs. Stale entries are skipped on eviction.
    a1q: VecDeque<(K, QKey)>,
    amq: VecDeque<(K, QKey)>,
}

impl<K, V> Cache2Q<K, V>
where
    K: Eq + Hash + Clone,
{
    /// Create a new cache with the given configuration.
    pub fn new(cfg: CacheCfgV1) -> Self {
        Self {
            cfg,
            stats: CacheStatsV1::default(),
            stamp_ctr: 0,
            bytes_a1: 0,
            bytes_am: 0,
            map: FxHashMap::default(),
            a1q: VecDeque::new(),
            amq: VecDeque::new(),
        }
    }

    /// Return a copy of current cache stats.
    pub fn stats(&self) -> CacheStatsV1 {
        self.stats
    }

    /// Total live bytes currently cached.
    pub fn bytes_live(&self) -> u64 {
        self.bytes_a1.saturating_add(self.bytes_am)
    }

    /// Total number of items currently cached.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// True if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Get a cached value by key, updating cache recency.
    pub fn get(&mut self, key: &K) -> Option<&V> {
        self.stats.lookups += 1;

        if !self.map.contains_key(key) {
            self.stats.misses += 1;
            return None;
        }

        // Update stamp and queue placement.
        //
        // Note: If Am has a zero byte budget (a1_ratio=100), we do not promote
        // A1 hits into Am. Promoting would immediately violate the Am cap and
        // force eviction on the same get.
        let (kind_after, stamp_now, was_a1) = {
            let e = self.map.get_mut(key).expect("present");
            self.stamp_ctr = self.stamp_ctr.wrapping_add(1);
            let s = self.stamp_ctr;
            let mut was_a1_local = false;
            if e.kind == QueueKind::A1 {
                was_a1_local = true;
                if self.cfg.am_max_bytes() > 0 {
                    e.kind = QueueKind::Am;
                    self.bytes_a1 = self.bytes_a1.saturating_sub(e.cost_bytes);
                    self.bytes_am = self.bytes_am.saturating_add(e.cost_bytes);
                }
            }
            e.stamp = s;
            (e.kind, s, was_a1_local)
        };

        if was_a1 {
            self.stats.hits_a1 += 1;
        } else {
            self.stats.hits_am += 1;
        }

        let qk = QKey { stamp: stamp_now };
        match kind_after {
            QueueKind::A1 => {
                self.a1q.push_front((key.clone(), qk));
            }
            QueueKind::Am => {
                self.amq.push_front((key.clone(), qk));
            }
        }

        self.enforce_caps();
        self.map.get(key).map(|e| &e.value)
    }

    /// Insert a key/value with a deterministic cost in bytes.
    ///
    /// Returns true if the item was stored in the cache.
    pub fn insert_cost(&mut self, key: K, value: V, cost_bytes: u64) -> bool {
        if self.cfg.max_bytes_total == 0 {
            return false;
        }
        if cost_bytes > self.cfg.max_bytes_total {
            self.stats.rejects_oversize += 1;
            return false;
        }

        self.stamp_ctr = self.stamp_ctr.wrapping_add(1);
        let s = self.stamp_ctr;
        let qk = QKey { stamp: s };

        if let Some(e) = self.map.get_mut(&key) {
            // Replace existing value and cost, keep placement kind.
            let old_cost = e.cost_bytes;
            e.value = value;
            e.cost_bytes = cost_bytes;
            e.stamp = s;

            match e.kind {
                QueueKind::A1 => {
                    self.bytes_a1 = self.bytes_a1.saturating_sub(old_cost);
                    self.bytes_a1 = self.bytes_a1.saturating_add(cost_bytes);
                    self.a1q.push_front((key.clone(), qk));
                }
                QueueKind::Am => {
                    self.bytes_am = self.bytes_am.saturating_sub(old_cost);
                    self.bytes_am = self.bytes_am.saturating_add(cost_bytes);
                    self.amq.push_front((key.clone(), qk));
                }
            }

            self.enforce_caps();
            return true;
        }

        // Miss insert goes into A1 when it fits the A1 byte cap; otherwise place into Am.
        let a1_cap = self.cfg.a1_max_bytes();
        let kind = if a1_cap > 0 && cost_bytes <= a1_cap {
            QueueKind::A1
        } else {
            QueueKind::Am
        };

        let e = Entry {
            value,
            cost_bytes,
            kind,
            stamp: s,
        };
        self.map.insert(key.clone(), e);
        match kind {
            QueueKind::A1 => {
                self.a1q.push_front((key, qk));
                self.bytes_a1 = self.bytes_a1.saturating_add(cost_bytes);
            }
            QueueKind::Am => {
                self.amq.push_front((key, qk));
                self.bytes_am = self.bytes_am.saturating_add(cost_bytes);
            }
        }
        self.stats.inserts += 1;

        self.enforce_caps();
        true
    }

    fn enforce_caps(&mut self) {
        // Enforce per-queue byte caps first.
        let a1_cap = self.cfg.a1_max_bytes();
        let am_cap = self.cfg.am_max_bytes();

        while self.bytes_a1 > a1_cap {
            if !self.evict_one(QueueKind::A1) {
                break;
            }
        }
        while self.bytes_am > am_cap {
            if !self.evict_one(QueueKind::Am) {
                break;
            }
        }

        // Enforce total bytes.
        while self.bytes_live() > self.cfg.max_bytes_total {
            if self.evict_one(QueueKind::A1) {
                continue;
            }
            if self.evict_one(QueueKind::Am) {
                continue;
            }
            break;
        }

        // Enforce item cap.
        if self.cfg.max_items_total > 0 {
            let cap = self.cfg.max_items_total as usize;
            while self.map.len() > cap {
                if self.evict_one(QueueKind::A1) {
                    continue;
                }
                if self.evict_one(QueueKind::Am) {
                    continue;
                }
                break;
            }
        }
    }

    fn evict_one(&mut self, kind: QueueKind) -> bool {
        let q = match kind {
            QueueKind::A1 => &mut self.a1q,
            QueueKind::Am => &mut self.amq,
        };

        while let Some((k, qk)) = q.pop_back() {
            let should_evict = match self.map.get(&k) {
                Some(e) => e.kind == kind && e.stamp == qk.stamp,
                None => false,
            };
            if !should_evict {
                continue;
            }

            let e = self.map.remove(&k).expect("present");
            match kind {
                QueueKind::A1 => {
                    self.bytes_a1 = self.bytes_a1.saturating_sub(e.cost_bytes);
                    self.stats.evicts_a1 += 1;
                }
                QueueKind::Am => {
                    self.bytes_am = self.bytes_am.saturating_sub(e.cost_bytes);
                    self.stats.evicts_am += 1;
                }
            }
            self.stats.bytes_evicted_total =
                self.stats.bytes_evicted_total.saturating_add(e.cost_bytes);
            return true;
        }

        false
    }

    #[cfg(test)]
    fn test_keys_in_kind(&self, kind: QueueKind) -> Vec<K>
    where
        K: Ord,
    {
        let mut out: Vec<K> = self
            .map
            .iter()
            .filter_map(|(k, e)| if e.kind == kind { Some(k.clone()) } else { None })
            .collect();
        out.sort();
        out
    }
}

#[cfg(test)]
mod tests {
    use super::{Cache2Q, CacheCfgV1, QueueKind};

    #[test]
    fn evicts_oldest_from_a1_on_overflow() {
        let mut cfg = CacheCfgV1::new(30);
        cfg.a1_ratio = 100;
        let mut c: Cache2Q<u32, u32> = Cache2Q::new(cfg);

        assert!(c.insert_cost(1, 10, 10));
        assert!(c.insert_cost(2, 20, 10));
        assert!(c.insert_cost(3, 30, 10));
        assert_eq!(c.len(), 3);

        // Overflow by one entry.
        assert!(c.insert_cost(4, 40, 10));
        assert_eq!(c.len(), 3);
        // Avoid get here because get updates recency and may promote A1 to Am.
        assert!(c.map.contains_key(&2));
        assert!(c.map.contains_key(&3));
        assert!(c.map.contains_key(&4));
        assert!(!c.map.contains_key(&1));
    }

    #[test]
    fn hit_in_a1_promotes_to_am_and_protects_from_a1_eviction() {
        // Use a config where A1 can hold two entries and Am has room.
        // Then validate that promoting an A1 entry to Am prevents it from being
        // evicted when A1 overflows.
        let mut cfg = CacheCfgV1::new(100);
        cfg.a1_ratio = 20; // A1 cap = 20 bytes
        let mut c: Cache2Q<u32, u32> = Cache2Q::new(cfg);

        assert!(c.insert_cost(1, 10, 10));
        assert!(c.insert_cost(2, 20, 10));

        // Promote key 1 into Am.
        assert_eq!(c.get(&1).copied(), Some(10));
        assert_eq!(c.test_keys_in_kind(QueueKind::Am), vec![1]);

        // Fill A1 with two entries (2 and 3).
        assert!(c.insert_cost(3, 30, 10));
        assert_eq!(c.test_keys_in_kind(QueueKind::A1), vec![2, 3]);

        // Overflow A1 by inserting a third probationary item; oldest A1 (2) is evicted.
        assert!(c.insert_cost(4, 40, 10));
        assert_eq!(c.test_keys_in_kind(QueueKind::A1), vec![3, 4]);

        // Key 1 remains in Am and is not affected by A1 eviction.
        assert!(c.map.contains_key(&1));
        assert!(!c.map.contains_key(&2));
        assert!(c.map.contains_key(&3));
        assert!(c.map.contains_key(&4));
    }

    #[test]
    fn enforces_a1_byte_cap() {
        let mut cfg = CacheCfgV1::new(100);
        cfg.a1_ratio = 20; // A1 cap = 20 bytes
        let mut c: Cache2Q<u32, u32> = Cache2Q::new(cfg);

        assert!(c.insert_cost(1, 10, 10));
        assert!(c.insert_cost(2, 20, 10));
        // This insert would make A1=30, so it must evict oldest (1).
        assert!(c.insert_cost(3, 30, 10));

        assert!(c.get(&1).is_none());
        assert!(c.get(&2).is_some());
        assert!(c.get(&3).is_some());
    }

    #[test]
    fn rejects_oversize_items() {
        let cfg = CacheCfgV1::new(15);
        let mut c: Cache2Q<u32, u32> = Cache2Q::new(cfg);
        assert!(!c.insert_cost(1, 10, 20));
        assert!(c.get(&1).is_none());
        assert_eq!(c.stats().rejects_oversize, 1);
    }

    #[test]
    fn a1_ratio_zero_places_new_items_in_am() {
        let mut cfg = CacheCfgV1::new(100);
        cfg.a1_ratio = 0;
        let mut c: Cache2Q<u32, u32> = Cache2Q::new(cfg);

        assert!(c.insert_cost(1, 10, 10));
        assert_eq!(c.test_keys_in_kind(QueueKind::A1), Vec::<u32>::new());
        assert_eq!(c.test_keys_in_kind(QueueKind::Am), vec![1]);
    }
}
