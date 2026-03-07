// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Proof artifact schema (v1).
//!
//! This artifact is intended to hold deterministic, replayable outputs from
//! verifiers and small solvers. It is referenced by EvidenceBundleV1 via
//! EvidenceItemDataV1::Proof(ProofRefV1).
//!
//! The v1 contract is deliberately small:
//! - Finite-domain variable assignment problems.
//! - Bounded search with a truncation flag.
//! - Canonical encoding with stable ordering.

use crate::codec::{ByteReader, ByteWriter, DecodeError, EncodeError};

/// ProofArtifactV1 schema version.
pub const PROOF_ARTIFACT_V1_VERSION: u32 = 1;

/// Maximum variables in v1.
pub const PROOF_V1_MAX_VARS: usize = 32;

/// Maximum domain size in v1.
pub const PROOF_V1_MAX_DOMAIN: usize = 64;

/// Maximum constraints in v1.
pub const PROOF_V1_MAX_CONSTRAINTS: usize = 256;

/// Maximum solutions recorded in v1.
pub const PROOF_V1_MAX_SOLUTIONS: usize = 2;

/// Proof artifact flags.
pub type ProofArtifactFlagsV1 = u32;

/// Caller asked for a unique solution.
pub const PA_FLAG_EXPECT_UNIQUE: ProofArtifactFlagsV1 = 1u32 << 0;

/// Exactly one solution was found (and not truncated).
pub const PA_FLAG_UNIQUE: ProofArtifactFlagsV1 = 1u32 << 1;

/// Search hit its bounded work cap.
pub const PA_FLAG_TRUNCATED: ProofArtifactFlagsV1 = 1u32 << 2;

/// No solution was found (and not truncated).
pub const PA_FLAG_NO_SOLUTION: ProofArtifactFlagsV1 = 1u32 << 3;

/// Mask of all known v1 flags.
pub const PA_FLAGS_V1_ALL: ProofArtifactFlagsV1 =
    PA_FLAG_EXPECT_UNIQUE | PA_FLAG_UNIQUE | PA_FLAG_TRUNCATED | PA_FLAG_NO_SOLUTION;

/// Constraint kinds (v1).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConstraintKindV1 {
    /// var == value.
    EqVarVal = 1,
    /// var != value.
    NeqVarVal = 2,
    /// a == b.
    EqVarVar = 3,
    /// a != b.
    NeqVarVar = 4,
    /// a < b.
    Lt = 5,
    /// a <= b.
    Le = 6,
    /// a > b.
    Gt = 7,
    /// a >= b.
    Ge = 8,
    /// all values across the set are pairwise distinct.
    AllDifferent = 9,
    /// if cond_var == cond_val then var == val.
    ImpEqVarVal = 10,
    /// if cond_var == cond_val then var != val.
    ImpNeqVarVal = 11,
}

impl ConstraintKindV1 {
    fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::EqVarVal),
            2 => Some(Self::NeqVarVal),
            3 => Some(Self::EqVarVar),
            4 => Some(Self::NeqVarVar),
            5 => Some(Self::Lt),
            6 => Some(Self::Le),
            7 => Some(Self::Gt),
            8 => Some(Self::Ge),
            9 => Some(Self::AllDifferent),
            10 => Some(Self::ImpEqVarVal),
            11 => Some(Self::ImpNeqVarVal),
            _ => None,
        }
    }
}

/// One constraint entry (v1).
///
/// Canonical rules:
/// - var indices must be in-range.
/// - AllDifferent list is ascending, unique, and non-empty.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConstraintV1 {
    /// var == val.
    EqVarVal {
        /// Variable index (0-based).
        var: u16,
        /// Constant value.
        val: i64,
    },
    /// var != val.
    NeqVarVal {
        /// Variable index (0-based).
        var: u16,
        /// Constant value.
        val: i64,
    },
    /// a == b.
    EqVarVar {
        /// Left variable index (0-based).
        a: u16,
        /// Right variable index (0-based).
        b: u16,
    },
    /// a != b.
    NeqVarVar {
        /// Left variable index (0-based).
        a: u16,
        /// Right variable index (0-based).
        b: u16,
    },
    /// a < b.
    Lt {
        /// Left variable index (0-based).
        a: u16,
        /// Right variable index (0-based).
        b: u16,
    },
    /// a <= b.
    Le {
        /// Left variable index (0-based).
        a: u16,
        /// Right variable index (0-based).
        b: u16,
    },
    /// a > b.
    Gt {
        /// Left variable index (0-based).
        a: u16,
        /// Right variable index (0-based).
        b: u16,
    },
    /// a >= b.
    Ge {
        /// Left variable index (0-based).
        a: u16,
        /// Right variable index (0-based).
        b: u16,
    },
    /// all values in the set are pairwise distinct.
    AllDifferent {
        /// Variable indices (ascending, unique).
        vars: Vec<u16>,
    },
    /// if cond_var == cond_val then var == val.
    ImpEqVarVal {
        /// Condition variable index (0-based).
        cond_var: u16,
        /// Condition constant value.
        cond_val: i64,
        /// Target variable index (0-based).
        var: u16,
        /// Target constant value.
        val: i64,
    },
    /// if cond_var == cond_val then var != val.
    ImpNeqVarVal {
        /// Condition variable index (0-based).
        cond_var: u16,
        /// Condition constant value.
        cond_val: i64,
        /// Target variable index (0-based).
        var: u16,
        /// Target constant value.
        val: i64,
    },
}

/// Solve stats (v1).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProofSolveStatsV1 {
    /// Explored node count (assignment attempts).
    pub nodes: u64,
    /// Backtrack count.
    pub backtracks: u64,
}

/// Proof artifact (v1).
///
/// Canonical rules:
/// - vars are sorted ascending and unique.
/// - domain is sorted ascending and unique.
/// - constraints are in builder-stable order (no canonical sort).
/// - each solution row has exactly vars.len() values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProofArtifactV1 {
    /// Schema version (must be PROOF_ARTIFACT_V1_VERSION).
    pub version: u32,
    /// Flags.
    pub flags: ProofArtifactFlagsV1,
    /// Variable names (sorted ascending, unique).
    pub vars: Vec<String>,
    /// Domain values (sorted ascending, unique).
    pub domain: Vec<i64>,
    /// Constraint list.
    pub constraints: Vec<ConstraintV1>,
    /// Solutions, each a row aligned with `vars`.
    pub solutions: Vec<Vec<i64>>,
    /// Solve stats.
    pub stats: ProofSolveStatsV1,
}

impl ProofArtifactV1 {
    /// Validate schema invariants.
    pub fn validate(&self) -> Result<(), DecodeError> {
        if self.version != PROOF_ARTIFACT_V1_VERSION {
            return Err(DecodeError::new("bad ProofArtifactV1 version"));
        }
        if (self.flags & !PA_FLAGS_V1_ALL) != 0 {
            return Err(DecodeError::new("bad ProofArtifactV1 flags"));
        }
        if self.vars.is_empty() || self.vars.len() > PROOF_V1_MAX_VARS {
            return Err(DecodeError::new("bad ProofArtifactV1 var count"));
        }
        if self.domain.is_empty() || self.domain.len() > PROOF_V1_MAX_DOMAIN {
            return Err(DecodeError::new("bad ProofArtifactV1 domain size"));
        }
        if self.constraints.len() > PROOF_V1_MAX_CONSTRAINTS {
            return Err(DecodeError::new("too many ProofArtifactV1 constraints"));
        }
        if self.solutions.len() > PROOF_V1_MAX_SOLUTIONS {
            return Err(DecodeError::new("too many ProofArtifactV1 solutions"));
        }

        // vars sorted + unique, ASCII identifier-ish.
        let mut prev: Option<&str> = None;
        for v in self.vars.iter() {
            if v.is_empty() || v.len() > 32 {
                return Err(DecodeError::new("bad ProofArtifactV1 var name"));
            }
            if !v
                .bytes()
                .all(|b| matches!(b, b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_'))
            {
                return Err(DecodeError::new("bad ProofArtifactV1 var name"));
            }
            if let Some(p) = prev {
                if v.as_str() <= p {
                    return Err(DecodeError::new("non-canonical ProofArtifactV1 vars"));
                }
            }
            prev = Some(v.as_str());
        }

        // domain sorted + unique.
        let mut prevd: Option<i64> = None;
        for &d in self.domain.iter() {
            if let Some(p) = prevd {
                if d <= p {
                    return Err(DecodeError::new("non-canonical ProofArtifactV1 domain"));
                }
            }
            prevd = Some(d);
        }

        let nvars = self.vars.len() as u16;
        for c in self.constraints.iter() {
            match c {
                ConstraintV1::EqVarVal { var, .. } | ConstraintV1::NeqVarVal { var, .. } => {
                    if *var >= nvars {
                        return Err(DecodeError::new("constraint var out of range"));
                    }
                }
                ConstraintV1::EqVarVar { a, b }
                | ConstraintV1::NeqVarVar { a, b }
                | ConstraintV1::Lt { a, b }
                | ConstraintV1::Le { a, b }
                | ConstraintV1::Gt { a, b }
                | ConstraintV1::Ge { a, b } => {
                    if *a >= nvars || *b >= nvars {
                        return Err(DecodeError::new("constraint var out of range"));
                    }
                    if *a == *b {
                        return Err(DecodeError::new("constraint uses same var"));
                    }
                }
                ConstraintV1::AllDifferent { vars } => {
                    if vars.is_empty() {
                        return Err(DecodeError::new("AllDifferent empty"));
                    }
                    let mut pv: Option<u16> = None;
                    for &ix in vars.iter() {
                        if ix >= nvars {
                            return Err(DecodeError::new("AllDifferent var out of range"));
                        }
                        if let Some(p) = pv {
                            if ix <= p {
                                return Err(DecodeError::new("AllDifferent non-canonical"));
                            }
                        }
                        pv = Some(ix);
                    }
                }
                ConstraintV1::ImpEqVarVal { cond_var, var, .. }
                | ConstraintV1::ImpNeqVarVal { cond_var, var, .. } => {
                    if *cond_var >= nvars || *var >= nvars {
                        return Err(DecodeError::new("implication var out of range"));
                    }
                }
            }
        }

        for sol in self.solutions.iter() {
            if sol.len() != self.vars.len() {
                return Err(DecodeError::new("bad ProofArtifactV1 solution width"));
            }
        }

        if (self.flags & PA_FLAG_UNIQUE) != 0 {
            if self.solutions.len() != 1 {
                return Err(DecodeError::new("UNIQUE flag but solution count != 1"));
            }
            if (self.flags & PA_FLAG_TRUNCATED) != 0 {
                return Err(DecodeError::new("UNIQUE flag with TRUNCATED"));
            }
        }
        if (self.flags & PA_FLAG_NO_SOLUTION) != 0 {
            if !self.solutions.is_empty() {
                return Err(DecodeError::new("NO_SOLUTION flag but solutions present"));
            }
            if (self.flags & PA_FLAG_TRUNCATED) != 0 {
                return Err(DecodeError::new("NO_SOLUTION flag with TRUNCATED"));
            }
        }
        Ok(())
    }

    /// Encode as canonical bytes.
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        self.validate()
            .map_err(|_| EncodeError::new("bad ProofArtifactV1"))?;

        let cap = 64
            + self.vars.len() * 16
            + self.domain.len() * 8
            + self.constraints.len() * 24
            + self.solutions.len() * self.vars.len() * 8;
        let mut w = ByteWriter::with_capacity(cap);
        w.write_u32(self.version);
        w.write_u32(self.flags);
        w.write_u16(self.vars.len() as u16);
        for v in self.vars.iter() {
            w.write_str(v)
                .map_err(|_| EncodeError::new("var write failed"))?;
        }
        w.write_u16(self.domain.len() as u16);
        for &d in self.domain.iter() {
            w.write_i64(d);
        }
        w.write_u16(self.constraints.len() as u16);
        for c in self.constraints.iter() {
            match c {
                ConstraintV1::EqVarVal { var, val } => {
                    w.write_u8(ConstraintKindV1::EqVarVal as u8);
                    w.write_u16(*var);
                    w.write_i64(*val);
                }
                ConstraintV1::NeqVarVal { var, val } => {
                    w.write_u8(ConstraintKindV1::NeqVarVal as u8);
                    w.write_u16(*var);
                    w.write_i64(*val);
                }
                ConstraintV1::EqVarVar { a, b } => {
                    w.write_u8(ConstraintKindV1::EqVarVar as u8);
                    w.write_u16(*a);
                    w.write_u16(*b);
                }
                ConstraintV1::NeqVarVar { a, b } => {
                    w.write_u8(ConstraintKindV1::NeqVarVar as u8);
                    w.write_u16(*a);
                    w.write_u16(*b);
                }
                ConstraintV1::Lt { a, b } => {
                    w.write_u8(ConstraintKindV1::Lt as u8);
                    w.write_u16(*a);
                    w.write_u16(*b);
                }
                ConstraintV1::Le { a, b } => {
                    w.write_u8(ConstraintKindV1::Le as u8);
                    w.write_u16(*a);
                    w.write_u16(*b);
                }
                ConstraintV1::Gt { a, b } => {
                    w.write_u8(ConstraintKindV1::Gt as u8);
                    w.write_u16(*a);
                    w.write_u16(*b);
                }
                ConstraintV1::Ge { a, b } => {
                    w.write_u8(ConstraintKindV1::Ge as u8);
                    w.write_u16(*a);
                    w.write_u16(*b);
                }
                ConstraintV1::AllDifferent { vars } => {
                    w.write_u8(ConstraintKindV1::AllDifferent as u8);
                    w.write_u16(vars.len() as u16);
                    for &ix in vars.iter() {
                        w.write_u16(ix);
                    }
                }
                ConstraintV1::ImpEqVarVal {
                    cond_var,
                    cond_val,
                    var,
                    val,
                } => {
                    w.write_u8(ConstraintKindV1::ImpEqVarVal as u8);
                    w.write_u16(*cond_var);
                    w.write_i64(*cond_val);
                    w.write_u16(*var);
                    w.write_i64(*val);
                }
                ConstraintV1::ImpNeqVarVal {
                    cond_var,
                    cond_val,
                    var,
                    val,
                } => {
                    w.write_u8(ConstraintKindV1::ImpNeqVarVal as u8);
                    w.write_u16(*cond_var);
                    w.write_i64(*cond_val);
                    w.write_u16(*var);
                    w.write_i64(*val);
                }
            }
        }
        w.write_u16(self.solutions.len() as u16);
        for sol in self.solutions.iter() {
            for &v in sol.iter() {
                w.write_i64(v);
            }
        }
        w.write_u64(self.stats.nodes);
        w.write_u64(self.stats.backtracks);
        Ok(w.into_bytes())
    }

    /// Decode from canonical bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut r = ByteReader::new(bytes);
        let version = r.read_u32()?;
        if version != PROOF_ARTIFACT_V1_VERSION {
            return Err(DecodeError::new("bad ProofArtifactV1 version"));
        }
        let flags = r.read_u32()?;
        if (flags & !PA_FLAGS_V1_ALL) != 0 {
            return Err(DecodeError::new("bad ProofArtifactV1 flags"));
        }
        let nv = r.read_u16()? as usize;
        if nv == 0 || nv > PROOF_V1_MAX_VARS {
            return Err(DecodeError::new("bad ProofArtifactV1 var count"));
        }
        let mut vars: Vec<String> = Vec::with_capacity(nv);
        for _ in 0..nv {
            vars.push(r.read_str_view()?.to_string());
        }
        let nd = r.read_u16()? as usize;
        if nd == 0 || nd > PROOF_V1_MAX_DOMAIN {
            return Err(DecodeError::new("bad ProofArtifactV1 domain size"));
        }
        let mut domain: Vec<i64> = Vec::with_capacity(nd);
        for _ in 0..nd {
            domain.push(r.read_i64()?);
        }
        let nc = r.read_u16()? as usize;
        if nc > PROOF_V1_MAX_CONSTRAINTS {
            return Err(DecodeError::new("too many ProofArtifactV1 constraints"));
        }
        let mut constraints: Vec<ConstraintV1> = Vec::with_capacity(nc);
        for _ in 0..nc {
            let tag = r.read_u8()?;
            let kind = ConstraintKindV1::from_tag(tag)
                .ok_or_else(|| DecodeError::new("bad constraint tag"))?;
            let c = match kind {
                ConstraintKindV1::EqVarVal => ConstraintV1::EqVarVal {
                    var: r.read_u16()?,
                    val: r.read_i64()?,
                },
                ConstraintKindV1::NeqVarVal => ConstraintV1::NeqVarVal {
                    var: r.read_u16()?,
                    val: r.read_i64()?,
                },
                ConstraintKindV1::EqVarVar => ConstraintV1::EqVarVar {
                    a: r.read_u16()?,
                    b: r.read_u16()?,
                },
                ConstraintKindV1::NeqVarVar => ConstraintV1::NeqVarVar {
                    a: r.read_u16()?,
                    b: r.read_u16()?,
                },
                ConstraintKindV1::Lt => ConstraintV1::Lt {
                    a: r.read_u16()?,
                    b: r.read_u16()?,
                },
                ConstraintKindV1::Le => ConstraintV1::Le {
                    a: r.read_u16()?,
                    b: r.read_u16()?,
                },
                ConstraintKindV1::Gt => ConstraintV1::Gt {
                    a: r.read_u16()?,
                    b: r.read_u16()?,
                },
                ConstraintKindV1::Ge => ConstraintV1::Ge {
                    a: r.read_u16()?,
                    b: r.read_u16()?,
                },
                ConstraintKindV1::AllDifferent => {
                    let n = r.read_u16()? as usize;
                    if n == 0 || n > PROOF_V1_MAX_VARS {
                        return Err(DecodeError::new("bad AllDifferent size"));
                    }
                    let mut vs: Vec<u16> = Vec::with_capacity(n);
                    for _ in 0..n {
                        vs.push(r.read_u16()?);
                    }
                    ConstraintV1::AllDifferent { vars: vs }
                }
                ConstraintKindV1::ImpEqVarVal => ConstraintV1::ImpEqVarVal {
                    cond_var: r.read_u16()?,
                    cond_val: r.read_i64()?,
                    var: r.read_u16()?,
                    val: r.read_i64()?,
                },
                ConstraintKindV1::ImpNeqVarVal => ConstraintV1::ImpNeqVarVal {
                    cond_var: r.read_u16()?,
                    cond_val: r.read_i64()?,
                    var: r.read_u16()?,
                    val: r.read_i64()?,
                },
            };
            constraints.push(c);
        }
        let ns = r.read_u16()? as usize;
        if ns > PROOF_V1_MAX_SOLUTIONS {
            return Err(DecodeError::new("too many ProofArtifactV1 solutions"));
        }
        let mut solutions: Vec<Vec<i64>> = Vec::with_capacity(ns);
        for _ in 0..ns {
            let mut row: Vec<i64> = Vec::with_capacity(nv);
            for _ in 0..nv {
                row.push(r.read_i64()?);
            }
            solutions.push(row);
        }
        let nodes = r.read_u64()?;
        let backtracks = r.read_u64()?;
        if r.remaining() != 0 {
            return Err(DecodeError::new("trailing bytes"));
        }

        let out = ProofArtifactV1 {
            version,
            flags,
            vars,
            domain,
            constraints,
            solutions,
            stats: ProofSolveStatsV1 { nodes, backtracks },
        };
        out.validate()?;
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proof_artifact_round_trip_minimal() {
        let a = ProofArtifactV1 {
            version: PROOF_ARTIFACT_V1_VERSION,
            flags: PA_FLAG_EXPECT_UNIQUE | PA_FLAG_UNIQUE,
            vars: vec!["A".to_string(), "B".to_string()],
            domain: vec![1, 2],
            constraints: vec![ConstraintV1::NeqVarVar { a: 0, b: 1 }],
            solutions: vec![vec![1, 2]],
            stats: ProofSolveStatsV1 {
                nodes: 3,
                backtracks: 1,
            },
        };
        let b = a.encode().unwrap();
        let a2 = ProofArtifactV1::decode(&b).unwrap();
        assert_eq!(a, a2);
    }
}
