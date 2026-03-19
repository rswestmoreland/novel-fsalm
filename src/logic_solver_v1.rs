// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Deterministic finite-domain logic solver (v1).
//!
//! This module provides a small solver intended for structured logic puzzles.
//! It is not a general theorem prover.
//!
//! Input format (embedded in prompt text):
//!
//! [puzzle]
//! vars: A,B,C
//! domain: 1..3
//! expect_unique: true
//! constraints:
//!   all_different: A,B,C
//!   A != 1
//!   if A = 2 then B != 3
//! [/puzzle]
//!
//! The solver is deterministic:
//! - variables are sorted ascending before solving
//! - domain values are sorted ascending
//! - search uses fixed ordering and strict caps

use crate::proof_artifact::{
    ConstraintV1, ProofArtifactFlagsV1, ProofArtifactV1, ProofSolveStatsV1, PA_FLAG_EXPECT_UNIQUE,
    PA_FLAG_NO_SOLUTION, PA_FLAG_TRUNCATED, PA_FLAG_UNIQUE, PROOF_ARTIFACT_V1_VERSION,
};
use std::collections::BTreeMap;

/// Solver configuration (v1).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicSolveCfgV1 {
    /// Maximum explored nodes (assignment attempts).
    pub max_nodes: u64,
}

impl LogicSolveCfgV1 {
    /// Conservative defaults for v1.
    pub fn default_v1() -> Self {
        Self { max_nodes: 200_000 }
    }
}

/// Parsed puzzle spec.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PuzzleSpecV1 {
    /// Variable identifiers.
    pub vars: Vec<String>,
    /// Allowed values.
    pub domain: Vec<i64>,
    /// Whether the caller expects a unique solution.
    pub expect_unique: bool,
    /// Constraint lines.
    pub constraints: Vec<ConstraintLineV1>,
}

/// Parsed constraint line (v1).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConstraintLineV1 {
    /// var OP value.
    RelVarVal {
        /// Variable identifier.
        var: String,
        /// Relational operator.
        op: RelOpV1,
        /// Constant value.
        val: i64,
    },
    /// a OP b.
    RelVarVar {
        /// Left variable identifier.
        a: String,
        /// Relational operator.
        op: RelOpV1,
        /// Right variable identifier.
        b: String,
    },
    /// all values across the set are distinct.
    AllDifferent {
        /// Variable identifiers.
        vars: Vec<String>,
    },
    /// if cond_var == cond_val then var OP value (OP must be = or !=).
    IfThenVarVal {
        /// Condition variable identifier.
        cond_var: String,
        /// Condition constant value.
        cond_val: i64,
        /// Target variable identifier.
        var: String,
        /// Relational operator (must be = or !=).
        op: RelOpV1,
        /// Target constant value.
        val: i64,
    },
}

/// Parse a single constraint line (v1).
///
/// This parser is shared by both the structured [puzzle] block parser and
/// the free-text sketch compiler. It is intentionally conservative.
pub fn parse_constraint_line_v1(line: &str) -> Result<Option<ConstraintLineV1>, String> {
    parse_constraint_line(line)
}

fn line_might_be_constraint(line: &str) -> bool {
    let t = line.trim();
    if t.is_empty() {
        return false;
    }
    if t.starts_with('#') {
        return false;
    }
    let low = t.to_ascii_lowercase();
    if low.starts_with("all_different") {
        return true;
    }
    if low.starts_with("if ") {
        return true;
    }
    t.contains("!=")
        || t.contains("<=")
        || t.contains(">=")
        || t.contains('=')
        || t.contains('<')
        || t.contains('>')
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum InlineTokV1 {
    Ident(String),
    Num(i64),
    Op(RelOpV1),
}

fn lex_inline_simple_v1(line: &str) -> Result<Vec<InlineTokV1>, String> {
    let bytes = line.as_bytes();
    let mut i: usize = 0;
    let mut out: Vec<InlineTokV1> = Vec::new();

    while i < bytes.len() {
        let b = bytes[i];
        if b == b' ' || b == b'\t' || b == b'\r' || b == b'\n' || b == b',' || b == b';' {
            i += 1;
            continue;
        }

        // Two-byte operators.
        if i + 1 < bytes.len() {
            let two = &line[i..i + 2];
            if two == "!=" {
                out.push(InlineTokV1::Op(RelOpV1::Neq));
                i += 2;
                continue;
            }
            if two == "<=" {
                out.push(InlineTokV1::Op(RelOpV1::Le));
                i += 2;
                continue;
            }
            if two == ">=" {
                out.push(InlineTokV1::Op(RelOpV1::Ge));
                i += 2;
                continue;
            }
        }

        // One-byte operators.
        if b == b'=' {
            out.push(InlineTokV1::Op(RelOpV1::Eq));
            i += 1;
            continue;
        }
        if b == b'<' {
            out.push(InlineTokV1::Op(RelOpV1::Lt));
            i += 1;
            continue;
        }
        if b == b'>' {
            out.push(InlineTokV1::Op(RelOpV1::Gt));
            i += 1;
            continue;
        }

        // Number.
        if b == b'-' || (b'0' <= b && b <= b'9') {
            let start = i;
            i += 1;
            while i < bytes.len() && (b'0' <= bytes[i] && bytes[i] <= b'9') {
                i += 1;
            }
            let s = &line[start..i];
            let v = s.parse::<i64>().map_err(|_| "bad number".to_string())?;
            out.push(InlineTokV1::Num(v));
            continue;
        }

        // Identifier.
        if (b'a' <= b && b <= b'z') || (b'A' <= b && b <= b'Z') || b == b'_' {
            let start = i;
            i += 1;
            while i < bytes.len() {
                let c = bytes[i];
                if (b'a' <= c && c <= b'z')
                    || (b'A' <= c && c <= b'Z')
                    || (b'0' <= c && c <= b'9')
                    || c == b'_'
                {
                    i += 1;
                } else {
                    break;
                }
            }
            let ident = &line[start..i];
            let low = ident.to_ascii_lowercase();
            if low == "if" || low == "then" || low == "all_different" {
                return Err("inline keyword not supported".to_string());
            }
            out.push(InlineTokV1::Ident(ident.to_string()));
            continue;
        }

        // Unknown character: give up.
        return Err("bad inline character".to_string());
    }

    Ok(out)
}

fn parse_inline_simple_constraints_v1(
    line: &str,
    max_constraints: usize,
) -> Result<Vec<ConstraintLineV1>, String> {
    let toks = lex_inline_simple_v1(line)?;
    if toks.is_empty() {
        return Ok(Vec::new());
    }

    let mut out: Vec<ConstraintLineV1> = Vec::new();
    let mut i: usize = 0;

    while i < toks.len() {
        if out.len() >= max_constraints {
            break;
        }

        let var = match &toks[i] {
            InlineTokV1::Ident(s) => s.clone(),
            _ => return Err("bad inline constraint".to_string()),
        };
        i += 1;

        let op = match toks.get(i) {
            Some(InlineTokV1::Op(o)) => *o,
            _ => return Err("bad inline constraint".to_string()),
        };
        i += 1;

        match toks.get(i) {
            Some(InlineTokV1::Num(v)) => {
                out.push(ConstraintLineV1::RelVarVal { var, op, val: *v });
            }
            Some(InlineTokV1::Ident(s)) => {
                out.push(ConstraintLineV1::RelVarVar {
                    a: var,
                    op,
                    b: s.clone(),
                });
            }
            _ => return Err("bad inline constraint".to_string()),
        }
        i += 1;
    }

    Ok(out)
}

/// Parse constraint lines from free text (v1).
///
/// The scanner considers each non-empty line, strips a leading bullet marker
/// ('-' or '*') when present, and parses lines that look like constraints.
///
/// Returns a stable vector preserving input order.
pub fn parse_constraints_from_text_v1(
    text: &str,
    max_constraints: usize,
) -> Result<Vec<ConstraintLineV1>, String> {
    let mut out: Vec<ConstraintLineV1> = Vec::new();

    for raw in text.lines() {
        let mut line = raw.trim();
        if line.starts_with('-') || line.starts_with('*') {
            line = line[1..].trim();
        }
        if !line_might_be_constraint(line) {
            continue;
        }

        let max_rem = max_constraints.saturating_sub(out.len());
        if max_rem == 0 {
            break;
        }

        match parse_constraint_line(line) {
            Ok(Some(c)) => {
                let mut used_inline = false;
                if let ConstraintLineV1::RelVarVar { b, .. } = &c {
                    // If the RHS contains whitespace, this is likely an inline sequence like:
                    //   A = 1 B = 2 C = 3
                    if b.contains(' ') || b.contains('\t') {
                        if let Ok(mut cs) = parse_inline_simple_constraints_v1(line, max_rem) {
                            if cs.len() >= 2 {
                                out.append(&mut cs);
                                used_inline = true;
                            }
                        }
                    }
                }
                if !used_inline {
                    out.push(c);
                }
            }
            Ok(None) => {}
            Err(e) => {
                // Fallback: attempt to parse a simple inline sequence.
                if let Ok(mut cs) = parse_inline_simple_constraints_v1(line, max_rem) {
                    if !cs.is_empty() {
                        out.append(&mut cs);
                    } else {
                        return Err(e);
                    }
                } else {
                    return Err(e);
                }
            }
        }

        if out.len() >= max_constraints {
            out.truncate(max_constraints);
            break;
        }
    }

    Ok(out)
}

/// Extract simple equality constraints for known variables from free text (v1).
///
/// This is a conservative fallback intended for conversational flows where a user
/// replies with assignments like:
///   Alice = 1 Bob = 2 Carol = 3
///
/// It only emits constraints of the form: var = integer.
///
/// Matching is ASCII-only and respects identifier boundaries. The function is
/// deterministic and bounded by `max_constraints`.
pub fn extract_eq_constraints_for_vars_v1(
    text: &str,
    vars: &[String],
    max_constraints: usize,
) -> Vec<ConstraintLineV1> {
    if vars.is_empty() || max_constraints == 0 {
        return Vec::new();
    }

    let mut names: Vec<&str> = vars.iter().map(|s| s.as_str()).collect();
    names.sort();
    names.dedup();

    let bytes = text.as_bytes();
    let mut out: Vec<ConstraintLineV1> = Vec::new();

    for name in names {
        if out.len() >= max_constraints {
            break;
        }
        let nbytes = name.as_bytes();
        if nbytes.is_empty() {
            continue;
        }

        let mut i: usize = 0;
        while i + nbytes.len() <= bytes.len() {
            // Compare name bytes, ASCII case-insensitive.
            let mut ok = true;
            for j in 0..nbytes.len() {
                let a = bytes[i + j];
                let b = nbytes[j];
                if a.to_ascii_lowercase() != b.to_ascii_lowercase() {
                    ok = false;
                    break;
                }
            }
            if !ok {
                i += 1;
                continue;
            }

            // Boundary: preceding char must not be an identifier character.
            if i > 0 {
                let p = bytes[i - 1];
                if p.is_ascii_alphanumeric() || p == b'_' {
                    i += 1;
                    continue;
                }
            }

            // Boundary: following char must not be an identifier character.
            let end_ix = i + nbytes.len();
            if end_ix < bytes.len() {
                let f = bytes[end_ix];
                if f.is_ascii_alphanumeric() || f == b'_' {
                    i += 1;
                    continue;
                }
            }

            // Skip whitespace.
            let mut k = end_ix;
            while k < bytes.len() && bytes[k].is_ascii_whitespace() {
                k += 1;
            }

            // Require '=' operator.
            if k >= bytes.len() || bytes[k] != b'=' {
                i = end_ix;
                continue;
            }
            k += 1;

            // Skip whitespace.
            while k < bytes.len() && bytes[k].is_ascii_whitespace() {
                k += 1;
            }
            if k >= bytes.len() {
                break;
            }

            // Parse integer value.
            let mut sign: i64 = 1;
            if bytes[k] == b'-' {
                sign = -1;
                k += 1;
            }
            if k >= bytes.len() || !(b'0' <= bytes[k] && bytes[k] <= b'9') {
                i = end_ix;
                continue;
            }
            let start_num = k;
            while k < bytes.len() && (b'0' <= bytes[k] && bytes[k] <= b'9') {
                k += 1;
            }
            let ns = match std::str::from_utf8(&bytes[start_num..k]) {
                Ok(s) => s,
                Err(_) => {
                    i = end_ix;
                    continue;
                }
            };
            let v = match ns.parse::<i64>() {
                Ok(x) => x * sign,
                Err(_) => {
                    i = end_ix;
                    continue;
                }
            };

            out.push(ConstraintLineV1::RelVarVal {
                var: name.to_string(),
                op: RelOpV1::Eq,
                val: v,
            });
            if out.len() >= max_constraints {
                break;
            }

            i = k;
        }
    }

    out
}

/// Relational operator for constraint comparisons.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RelOpV1 {
    /// ==
    Eq,
    /// !=
    Neq,
    /// <
    Lt,
    /// <=
    Le,
    /// >
    Gt,
    /// >=
    Ge,
}

impl RelOpV1 {
    fn from_str(op: &str) -> Option<Self> {
        match op {
            "=" => Some(Self::Eq),
            "!=" => Some(Self::Neq),
            "<" => Some(Self::Lt),
            "<=" => Some(Self::Le),
            ">" => Some(Self::Gt),
            ">=" => Some(Self::Ge),
            _ => None,
        }
    }
}

/// Extract the first puzzle block from prompt text.
pub fn extract_puzzle_block(text: &str) -> Option<&str> {
    let start = text.find("[puzzle]")?;
    let rest = &text[start + "[puzzle]".len()..];
    let end_rel = rest.find("[/puzzle]")?;
    Some(rest[..end_rel].trim())
}

fn parse_ident_list(s: &str) -> Result<Vec<String>, String> {
    let mut out: Vec<String> = Vec::new();
    for part in s.split(',') {
        let t = part.trim();
        if t.is_empty() {
            continue;
        }
        if !t
            .bytes()
            .all(|b| matches!(b, b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_'))
        {
            return Err("invalid identifier".to_string());
        }
        out.push(t.to_string());
    }
    if out.is_empty() {
        return Err("empty identifier list".to_string());
    }
    Ok(out)
}

fn parse_domain(s: &str) -> Result<Vec<i64>, String> {
    let t = s.trim();
    if let Some(pos) = t.find("..") {
        let a = t[..pos]
            .trim()
            .parse::<i64>()
            .map_err(|_| "bad domain".to_string())?;
        let b = t[pos + 2..]
            .trim()
            .parse::<i64>()
            .map_err(|_| "bad domain".to_string())?;
        if b < a {
            return Err("bad domain range".to_string());
        }
        let mut out: Vec<i64> = Vec::new();
        let mut v = a;
        while v <= b {
            out.push(v);
            v += 1;
            if out.len() > 64 {
                return Err("domain too large".to_string());
            }
        }
        return Ok(out);
    }
    if t.starts_with('{') && t.ends_with('}') {
        let inner = &t[1..t.len() - 1];
        let mut out: Vec<i64> = Vec::new();
        for part in inner.split(',') {
            let v = part
                .trim()
                .parse::<i64>()
                .map_err(|_| "bad domain".to_string())?;
            out.push(v);
        }
        out.sort();
        out.dedup();
        if out.is_empty() {
            return Err("empty domain".to_string());
        }
        if out.len() > 64 {
            return Err("domain too large".to_string());
        }
        return Ok(out);
    }
    Err("bad domain".to_string())
}

fn split_rel(s: &str) -> Option<(&str, &str, &str)> {
    for op in ["!=", "<=", ">=", "=", "<", ">"].iter() {
        if let Some(pos) = s.find(op) {
            let a = s[..pos].trim();
            let b = s[pos + op.len()..].trim();
            if !a.is_empty() && !b.is_empty() {
                return Some((a, *op, b));
            }
        }
    }
    None
}

fn parse_constraint_line(line: &str) -> Result<Option<ConstraintLineV1>, String> {
    let t = line.trim();
    if t.is_empty() {
        return Ok(None);
    }
    if t.starts_with('#') {
        return Ok(None);
    }
    if t.to_ascii_lowercase().starts_with("all_different") {
        let rest = if let Some(pos) = t.find(':') {
            &t[pos + 1..]
        } else if t.contains('(') && t.ends_with(')') {
            let lp = t.find('(').unwrap();
            &t[lp + 1..t.len() - 1]
        } else {
            return Err("bad all_different".to_string());
        };
        let vars = parse_ident_list(rest)?;
        return Ok(Some(ConstraintLineV1::AllDifferent { vars }));
    }
    if t.to_ascii_lowercase().starts_with("if ") {
        let low = t.to_ascii_lowercase();
        let then_pos = low
            .find(" then ")
            .ok_or_else(|| "bad if-then".to_string())?;
        let cond = t[3..then_pos].trim();
        let cons = t[then_pos + 6..].trim();
        let (lhs, op, rhs) = split_rel(cond).ok_or_else(|| "bad if condition".to_string())?;
        if RelOpV1::from_str(op) != Some(RelOpV1::Eq) {
            return Err("if condition must be =".to_string());
        }
        let cond_var = lhs.trim().to_string();
        let cond_val = rhs
            .trim()
            .parse::<i64>()
            .map_err(|_| "bad if condition".to_string())?;
        let (lh2, op2, rh2) = split_rel(cons).ok_or_else(|| "bad then clause".to_string())?;
        let rop = RelOpV1::from_str(op2).ok_or_else(|| "bad then op".to_string())?;
        if rop != RelOpV1::Eq && rop != RelOpV1::Neq {
            return Err("then must be = or !=".to_string());
        }
        let var = lh2.trim().to_string();
        let val = rh2
            .trim()
            .parse::<i64>()
            .map_err(|_| "bad then value".to_string())?;
        return Ok(Some(ConstraintLineV1::IfThenVarVal {
            cond_var,
            cond_val,
            var,
            op: rop,
            val,
        }));
    }

    let (lhs, op, rhs) = split_rel(t).ok_or_else(|| "bad constraint".to_string())?;
    let rop = RelOpV1::from_str(op).ok_or_else(|| "bad op".to_string())?;
    if let Ok(v) = rhs.parse::<i64>() {
        return Ok(Some(ConstraintLineV1::RelVarVal {
            var: lhs.trim().to_string(),
            op: rop,
            val: v,
        }));
    }
    Ok(Some(ConstraintLineV1::RelVarVar {
        a: lhs.trim().to_string(),
        op: rop,
        b: rhs.trim().to_string(),
    }))
}

/// Parse a puzzle block into a PuzzleSpecV1.
pub fn parse_puzzle_block_v1(block: &str) -> Result<PuzzleSpecV1, String> {
    let mut vars: Option<Vec<String>> = None;
    let mut domain: Option<Vec<i64>> = None;
    let mut expect_unique: bool = false;
    let mut constraints: Vec<ConstraintLineV1> = Vec::new();
    let mut in_constraints = false;

    for raw in block.lines() {
        let line = raw.trim();
        if line.is_empty() {
            continue;
        }
        if in_constraints {
            if let Some(c) = parse_constraint_line(line)? {
                constraints.push(c);
            }
            continue;
        }
        if let Some(rest) = line.strip_prefix("vars:") {
            vars = Some(parse_ident_list(rest)?);
            continue;
        }
        if let Some(rest) = line.strip_prefix("domain:") {
            domain = Some(parse_domain(rest)?);
            continue;
        }
        if let Some(rest) = line.strip_prefix("expect_unique:") {
            let v = rest.trim().to_ascii_lowercase();
            expect_unique = v == "true" || v == "1" || v == "yes";
            continue;
        }
        if line.eq_ignore_ascii_case("constraints:") {
            in_constraints = true;
            continue;
        }
    }

    let vars = vars.ok_or_else(|| "missing vars".to_string())?;
    let domain = domain.ok_or_else(|| "missing domain".to_string())?;
    if constraints.len() > 256 {
        return Err("too many constraints".to_string());
    }
    Ok(PuzzleSpecV1 {
        vars,
        domain,
        expect_unique,
        constraints,
    })
}

fn build_var_index(vars: &[String]) -> (Vec<String>, BTreeMap<String, u16>) {
    let mut names: Vec<String> = vars.to_vec();
    names.sort();
    names.dedup();
    let mut map: BTreeMap<String, u16> = BTreeMap::new();
    for (i, n) in names.iter().enumerate() {
        map.insert(n.clone(), i as u16);
    }
    (names, map)
}

fn map_var(map: &BTreeMap<String, u16>, name: &str) -> Result<u16, String> {
    map.get(name)
        .copied()
        .ok_or_else(|| "unknown var".to_string())
}

fn compile_constraints(
    spec: &PuzzleSpecV1,
    map: &BTreeMap<String, u16>,
) -> Result<Vec<ConstraintV1>, String> {
    let mut out: Vec<ConstraintV1> = Vec::with_capacity(spec.constraints.len());
    for c in spec.constraints.iter() {
        match c {
            ConstraintLineV1::RelVarVal { var, op, val } => {
                let ix = map_var(map, var)?;
                match op {
                    RelOpV1::Eq => out.push(ConstraintV1::EqVarVal { var: ix, val: *val }),
                    RelOpV1::Neq => out.push(ConstraintV1::NeqVarVal { var: ix, val: *val }),
                    _ => return Err("var-val only supports = or !=".to_string()),
                }
            }
            ConstraintLineV1::RelVarVar { a, op, b } => {
                let ia = map_var(map, a)?;
                let ib = map_var(map, b)?;
                if ia == ib {
                    return Err("var-var uses same var".to_string());
                }
                match op {
                    RelOpV1::Eq => out.push(ConstraintV1::EqVarVar { a: ia, b: ib }),
                    RelOpV1::Neq => out.push(ConstraintV1::NeqVarVar { a: ia, b: ib }),
                    RelOpV1::Lt => out.push(ConstraintV1::Lt { a: ia, b: ib }),
                    RelOpV1::Le => out.push(ConstraintV1::Le { a: ia, b: ib }),
                    RelOpV1::Gt => out.push(ConstraintV1::Gt { a: ia, b: ib }),
                    RelOpV1::Ge => out.push(ConstraintV1::Ge { a: ia, b: ib }),
                }
            }
            ConstraintLineV1::AllDifferent { vars } => {
                let mut vix: Vec<u16> = Vec::with_capacity(vars.len());
                for n in vars.iter() {
                    vix.push(map_var(map, n)?);
                }
                vix.sort();
                vix.dedup();
                if vix.is_empty() {
                    return Err("all_different empty".to_string());
                }
                out.push(ConstraintV1::AllDifferent { vars: vix });
            }
            ConstraintLineV1::IfThenVarVal {
                cond_var,
                cond_val,
                var,
                op,
                val,
            } => {
                let ic = map_var(map, cond_var)?;
                let iv = map_var(map, var)?;
                match op {
                    RelOpV1::Eq => out.push(ConstraintV1::ImpEqVarVal {
                        cond_var: ic,
                        cond_val: *cond_val,
                        var: iv,
                        val: *val,
                    }),
                    RelOpV1::Neq => out.push(ConstraintV1::ImpNeqVarVal {
                        cond_var: ic,
                        cond_val: *cond_val,
                        var: iv,
                        val: *val,
                    }),
                    _ => return Err("if-then only supports = or !=".to_string()),
                }
            }
        }
    }
    Ok(out)
}

fn check_constraint(c: &ConstraintV1, asn: &[Option<i64>]) -> bool {
    match c {
        ConstraintV1::EqVarVal { var, val } => match asn[*var as usize] {
            Some(v) => v == *val,
            None => true,
        },
        ConstraintV1::NeqVarVal { var, val } => match asn[*var as usize] {
            Some(v) => v != *val,
            None => true,
        },
        ConstraintV1::EqVarVar { a, b } => match (asn[*a as usize], asn[*b as usize]) {
            (Some(x), Some(y)) => x == y,
            _ => true,
        },
        ConstraintV1::NeqVarVar { a, b } => match (asn[*a as usize], asn[*b as usize]) {
            (Some(x), Some(y)) => x != y,
            _ => true,
        },
        ConstraintV1::Lt { a, b } => match (asn[*a as usize], asn[*b as usize]) {
            (Some(x), Some(y)) => x < y,
            _ => true,
        },
        ConstraintV1::Le { a, b } => match (asn[*a as usize], asn[*b as usize]) {
            (Some(x), Some(y)) => x <= y,
            _ => true,
        },
        ConstraintV1::Gt { a, b } => match (asn[*a as usize], asn[*b as usize]) {
            (Some(x), Some(y)) => x > y,
            _ => true,
        },
        ConstraintV1::Ge { a, b } => match (asn[*a as usize], asn[*b as usize]) {
            (Some(x), Some(y)) => x >= y,
            _ => true,
        },
        ConstraintV1::AllDifferent { vars } => {
            for i in 0..vars.len() {
                let ai = asn[vars[i] as usize];
                if ai.is_none() {
                    continue;
                }
                for j in (i + 1)..vars.len() {
                    let aj = asn[vars[j] as usize];
                    if aj.is_none() {
                        continue;
                    }
                    if ai == aj {
                        return false;
                    }
                }
            }
            true
        }
        ConstraintV1::ImpEqVarVal {
            cond_var,
            cond_val,
            var,
            val,
        } => match asn[*cond_var as usize] {
            Some(v) if v == *cond_val => match asn[*var as usize] {
                Some(x) => x == *val,
                None => true,
            },
            _ => true,
        },
        ConstraintV1::ImpNeqVarVal {
            cond_var,
            cond_val,
            var,
            val,
        } => match asn[*cond_var as usize] {
            Some(v) if v == *cond_val => match asn[*var as usize] {
                Some(x) => x != *val,
                None => true,
            },
            _ => true,
        },
    }
}

fn check_all(constraints: &[ConstraintV1], asn: &[Option<i64>]) -> bool {
    for c in constraints.iter() {
        if !check_constraint(c, asn) {
            return false;
        }
    }
    true
}

fn pick_next_var(asn: &[Option<i64>]) -> Option<usize> {
    for (i, v) in asn.iter().enumerate() {
        if v.is_none() {
            return Some(i);
        }
    }
    None
}

/// Solve a parsed puzzle spec.
pub fn solve_puzzle_v1(
    spec: &PuzzleSpecV1,
    cfg: LogicSolveCfgV1,
) -> Result<ProofArtifactV1, String> {
    let (vars_sorted, map) = build_var_index(&spec.vars);
    let mut domain = spec.domain.clone();
    domain.sort();
    domain.dedup();
    if domain.is_empty() {
        return Err("empty domain".to_string());
    }

    let constraints = compile_constraints(spec, &map)?;
    let n = vars_sorted.len();
    if n == 0 || n > 32 {
        return Err("bad var count".to_string());
    }
    let mut asn: Vec<Option<i64>> = vec![None; n];
    let mut solutions: Vec<Vec<i64>> = Vec::new();
    let mut nodes: u64 = 0;
    let mut backtracks: u64 = 0;
    let mut truncated = false;

    fn dfs(
        asn: &mut [Option<i64>],
        domain: &[i64],
        constraints: &[ConstraintV1],
        cfg: LogicSolveCfgV1,
        nodes: &mut u64,
        backtracks: &mut u64,
        truncated: &mut bool,
        solutions: &mut Vec<Vec<i64>>,
    ) {
        if *truncated {
            return;
        }
        if solutions.len() >= 2 {
            return;
        }
        let next = match pick_next_var(asn) {
            Some(i) => i,
            None => {
                let mut row: Vec<i64> = Vec::with_capacity(asn.len());
                for v in asn.iter() {
                    row.push(v.unwrap());
                }
                solutions.push(row);
                return;
            }
        };

        for &val in domain.iter() {
            *nodes = nodes.saturating_add(1);
            if *nodes >= cfg.max_nodes {
                *truncated = true;
                return;
            }
            asn[next] = Some(val);
            if check_all(constraints, asn) {
                dfs(
                    asn,
                    domain,
                    constraints,
                    cfg,
                    nodes,
                    backtracks,
                    truncated,
                    solutions,
                );
                if *truncated || solutions.len() >= 2 {
                    asn[next] = None;
                    return;
                }
            }
            asn[next] = None;
            *backtracks = backtracks.saturating_add(1);
        }
    }

    dfs(
        &mut asn,
        &domain,
        &constraints,
        cfg,
        &mut nodes,
        &mut backtracks,
        &mut truncated,
        &mut solutions,
    );

    let mut flags: ProofArtifactFlagsV1 = 0;
    if spec.expect_unique {
        flags |= PA_FLAG_EXPECT_UNIQUE;
    }
    if truncated {
        flags |= PA_FLAG_TRUNCATED;
    } else if solutions.is_empty() {
        flags |= PA_FLAG_NO_SOLUTION;
    } else if solutions.len() == 1 {
        flags |= PA_FLAG_UNIQUE;
    }

    Ok(ProofArtifactV1 {
        version: PROOF_ARTIFACT_V1_VERSION,
        flags,
        vars: vars_sorted,
        domain,
        constraints,
        solutions,
        stats: ProofSolveStatsV1 { nodes, backtracks },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_and_solve_simple_unique() {
        let block = "vars: A,B\n\
domain: 1..2\n\
expect_unique: true\n\
constraints:\n\
  A = 1\n\
  B != A\n";
        let spec = parse_puzzle_block_v1(block).unwrap();
        let proof = solve_puzzle_v1(&spec, LogicSolveCfgV1 { max_nodes: 10_000 }).unwrap();
        assert!((proof.flags & PA_FLAG_EXPECT_UNIQUE) != 0);
        assert!((proof.flags & PA_FLAG_UNIQUE) != 0);
        assert_eq!(proof.solutions.len(), 1);
        assert_eq!(proof.vars, vec!["A".to_string(), "B".to_string()]);
        assert_eq!(proof.solutions[0], vec![1, 2]);
    }
}
