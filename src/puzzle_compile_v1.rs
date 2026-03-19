// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Compile a puzzle sketch into a solver spec (v1).
//!
//! This module bridges the conservative `PuzzleSketchV1` intermediate into the
//! deterministic solver input `PuzzleSpecV1` when the sketch is sufficiently
//! specified.
//!
//! The compiler is intentionally strict and bounded:
//! - numeric domains are required (range form) and capped
//! - constraints must be parseable using the v1 constraint line parser
//! - variable identifiers must be ASCII [A-Za-z0-9_] only

use crate::logic_solver_v1::{parse_constraints_from_text_v1, ConstraintLineV1, PuzzleSpecV1};
use crate::puzzle_sketch_v1::PuzzleSketchV1;

use std::collections::{BTreeMap, BTreeSet};

/// Compile errors (v1).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PuzzleCompileErrV1 {
    /// Constraints were present but could not be parsed.
    ConstraintParseFailed,
    /// The numeric domain range is invalid or exceeds caps.
    BadDomain,
    /// One or more variable identifiers are not supported.
    BadVarName,
    /// A constraint references a variable not present in the sketch.
    UnknownVarRef,
}

fn is_solver_ident(s: &str) -> bool {
    if s.is_empty() || s.len() > 32 {
        return false;
    }
    s.bytes()
        .all(|b| matches!(b, b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_'))
}

fn domain_from_range(lo: i32, hi: i32) -> Result<Vec<i64>, PuzzleCompileErrV1> {
    if lo > hi {
        return Err(PuzzleCompileErrV1::BadDomain);
    }
    let span = (hi as i64).saturating_sub(lo as i64).saturating_add(1);
    if span <= 0 || span > 64 {
        return Err(PuzzleCompileErrV1::BadDomain);
    }
    let mut out: Vec<i64> = Vec::with_capacity(span as usize);
    let mut v = lo as i64;
    while v <= hi as i64 {
        out.push(v);
        v += 1;
    }
    Ok(out)
}

fn build_casefold_map(vars: &[String]) -> Option<BTreeMap<String, String>> {
    // If there are collisions under ASCII lowercase, skip casefold mapping.
    let mut m: BTreeMap<String, String> = BTreeMap::new();
    for v in vars.iter() {
        let k = v.to_ascii_lowercase();
        if let Some(prev) = m.get(&k) {
            if prev != v {
                return None;
            }
        }
        m.insert(k, v.clone());
    }
    Some(m)
}

fn map_var_name(name: &str, cf_opt: Option<&BTreeMap<String, String>>) -> String {
    if let Some(m) = cf_opt {
        let k = name.to_ascii_lowercase();
        if let Some(v) = m.get(&k) {
            return v.clone();
        }
    }
    name.to_string()
}

fn apply_var_mapping(c: &mut ConstraintLineV1, cf_opt: Option<&BTreeMap<String, String>>) {
    match c {
        ConstraintLineV1::RelVarVal { var, .. } => {
            *var = map_var_name(var, cf_opt);
        }
        ConstraintLineV1::RelVarVar { a, b, .. } => {
            *a = map_var_name(a, cf_opt);
            *b = map_var_name(b, cf_opt);
        }
        ConstraintLineV1::AllDifferent { vars } => {
            for v in vars.iter_mut() {
                *v = map_var_name(v, cf_opt);
            }
        }
        ConstraintLineV1::IfThenVarVal { cond_var, var, .. } => {
            *cond_var = map_var_name(cond_var, cf_opt);
            *var = map_var_name(var, cf_opt);
        }
    }
}

fn validate_constraints_vars(vars_set: &BTreeSet<String>, cs: &[ConstraintLineV1]) -> Result<(), PuzzleCompileErrV1> {
    for c in cs.iter() {
        match c {
            ConstraintLineV1::RelVarVal { var, .. } => {
                if !vars_set.contains(var) {
                    return Err(PuzzleCompileErrV1::UnknownVarRef);
                }
            }
            ConstraintLineV1::RelVarVar { a, b, .. } => {
                if !vars_set.contains(a) || !vars_set.contains(b) {
                    return Err(PuzzleCompileErrV1::UnknownVarRef);
                }
            }
            ConstraintLineV1::AllDifferent { vars } => {
                for v in vars.iter() {
                    if !vars_set.contains(v) {
                        return Err(PuzzleCompileErrV1::UnknownVarRef);
                    }
                }
            }
            ConstraintLineV1::IfThenVarVal { cond_var, var, .. } => {
                if !vars_set.contains(cond_var) || !vars_set.contains(var) {
                    return Err(PuzzleCompileErrV1::UnknownVarRef);
                }
            }
        }
    }
    Ok(())
}

/// Attempt to compile a solver spec from a sketch and the current turn text.
///
/// Returns:
/// - Ok(Some(spec)) when compile-ready
/// - Ok(None) when not enough information is present yet
/// - Err(...) when the input looks ready but is malformed (for example, unparseable constraints)
pub fn try_compile_puzzle_spec_from_sketch_v1(
    sketch: &PuzzleSketchV1,
    text: &str,
    max_constraints: usize,
) -> Result<Option<PuzzleSpecV1>, PuzzleCompileErrV1> {
    if !sketch.is_logic_puzzle_likely {
        return Ok(None);
    }
    let (lo, hi) = match sketch.domain_range {
        Some(x) => x,
        None => return Ok(None),
    };
    if sketch.var_names.is_empty() {
        return Ok(None);
    }

    let mut vars: Vec<String> = Vec::new();
    for v in sketch.var_names.iter() {
        if is_solver_ident(v) {
            vars.push(v.clone());
        } else {
            return Err(PuzzleCompileErrV1::BadVarName);
        }
    }
    vars.sort();
    vars.dedup();
    if vars.is_empty() {
        return Ok(None);
    }
    if vars.len() > 32 {
        return Err(PuzzleCompileErrV1::BadVarName);
    }

    let domain = domain_from_range(lo, hi)?;

    let mut constraints = match parse_constraints_from_text_v1(text, max_constraints) {
        Ok(cs) => cs,
        Err(_) => return Err(PuzzleCompileErrV1::ConstraintParseFailed),
    };
    if constraints.is_empty() {
        return Ok(None);
    }

    let vars_set: BTreeSet<String> = vars.iter().cloned().collect();
    let cf_map_opt = build_casefold_map(&vars);
    let cf_ref = cf_map_opt.as_ref();
    for c in constraints.iter_mut() {
        apply_var_mapping(c, cf_ref);
    }

    validate_constraints_vars(&vars_set, &constraints)?;

    Ok(Some(PuzzleSpecV1 {
        vars,
        domain,
        expect_unique: false,
        constraints,
    }))
}

/// Attempt to compile a solver spec from a sketch and a pre-parsed constraint list.
///
/// This is used by the conversational pending-sketch path to avoid re-parsing
/// constraints multiple times and to ensure the same constraint parse is used
/// for both compile readiness and solver execution.
pub fn try_compile_puzzle_spec_from_sketch_and_constraints_v1(
    sketch: &PuzzleSketchV1,
    constraints: Vec<ConstraintLineV1>,
) -> Result<Option<PuzzleSpecV1>, PuzzleCompileErrV1> {
    if !sketch.is_logic_puzzle_likely {
        return Ok(None);
    }
    let (lo, hi) = match sketch.domain_range {
        Some(x) => x,
        None => return Ok(None),
    };
    if sketch.var_names.is_empty() {
        return Ok(None);
    }

    let mut vars: Vec<String> = Vec::new();
    for v in sketch.var_names.iter() {
        if is_solver_ident(v) {
            vars.push(v.clone());
        } else {
            return Err(PuzzleCompileErrV1::BadVarName);
        }
    }
    vars.sort();
    vars.dedup();
    if vars.is_empty() {
        return Ok(None);
    }
    if vars.len() > 32 {
        return Err(PuzzleCompileErrV1::BadVarName);
    }

    let domain = domain_from_range(lo, hi)?;

    if constraints.is_empty() {
        return Ok(None);
    }
    let mut constraints = constraints;

    let vars_set: BTreeSet<String> = vars.iter().cloned().collect();
    let cf_map_opt = build_casefold_map(&vars);
    let cf_ref = cf_map_opt.as_ref();
    for c in constraints.iter_mut() {
        apply_var_mapping(c, cf_ref);
    }

    validate_constraints_vars(&vars_set, &constraints)?;

    Ok(Some(PuzzleSpecV1 {
        vars,
        domain,
        expect_unique: false,
        constraints,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::puzzle_sketch_v1::{PuzzleShapeHintV1, PuzzleSketchV1};

    #[test]
    fn compile_requires_constraints_and_domain() {
        let sk = PuzzleSketchV1 {
            is_logic_puzzle_likely: true,
            var_names: vec!["A".to_string(), "B".to_string()],
            domain_range: Some((1, 2)),
            has_constraints: true,
            shape: PuzzleShapeHintV1::Ordering,
        };
        let spec = try_compile_puzzle_spec_from_sketch_v1(&sk, "A = 1\nB != A\n", 32).unwrap().unwrap();
        assert_eq!(spec.vars, vec!["A".to_string(), "B".to_string()]);
        assert_eq!(spec.domain, vec![1, 2]);
        assert_eq!(spec.constraints.len(), 2);
    }
}
