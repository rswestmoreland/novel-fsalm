// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Determinism helpers.
//!
//! This module provides stable sorting and tie-break utilities that should be used
//! across the codebase. Avoid relying on hash map iteration order for any behavior.
//!
//! Design goal: explicit ordering rules everywhere.

/// Stable sort by score (descending) and then by a deterministic tie-break (id ascending).
pub fn stable_sort_by_score_then_id<T, FScore, FId>(items: &mut [T], score: FScore, id: FId)
where
    FScore: Fn(&T) -> i64,
    FId: Fn(&T) -> u64,
{
    items.sort_by(|a, b| {
        let sa = score(a);
        let sb = score(b);
        match sb.cmp(&sa) {
            core::cmp::Ordering::Equal => id(a).cmp(&id(b)),
            other => other,
        }
    });
}

/// Deterministic clamp.
pub fn clamp_i64(v: i64, lo: i64, hi: i64) -> i64 {
    if v < lo {
        lo
    } else if v > hi {
        hi
    } else {
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_sort_score_then_id() {
        #[derive(Debug, Clone)]
        struct Item {
            id: u64,
            score: i64,
        }
        let mut v = vec![
            Item { id: 3, score: 10 },
            Item { id: 1, score: 10 },
            Item { id: 2, score: 11 },
        ];
        stable_sort_by_score_then_id(&mut v, |x| x.score, |x| x.id);
        assert_eq!(v[0].id, 2);
        assert_eq!(v[1].id, 1);
        assert_eq!(v[2].id, 3);
    }

    #[test]
    fn clamp() {
        assert_eq!(clamp_i64(5, 0, 10), 5);
        assert_eq!(clamp_i64(-1, 0, 10), 0);
        assert_eq!(clamp_i64(11, 0, 10), 10);
    }
}
