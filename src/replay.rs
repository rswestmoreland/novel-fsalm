// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Replay log artifacts (baseline).
//!
//! A replay log is a canonical artifact that records, per step, the input and output
//! artifact hashes. This enables deterministic replay and regression testing.

use crate::codec::{ByteReader, ByteWriter, DecodeError, EncodeError};
use crate::hash::Hash32;

/// Replay log version.
pub const REPLAY_LOG_VERSION: u16 = 1;

// Decode safety caps (defensive against non-replay bytes).
const MAX_REPLAY_STEPS: usize = 1024;
const MAX_REPLAY_HASHES_PER_STEP: usize = 8192;
const MAX_REPLAY_STEP_NAME_BYTES: usize = 256;

/// A single replay step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayStep {
    /// Human-readable step name (UTF-8).
    pub name: String,
    /// Input artifact hashes. Canonical encoding sorts lexicographically.
    pub inputs: Vec<Hash32>,
    /// Output artifact hashes. Canonical encoding sorts lexicographically.
    pub outputs: Vec<Hash32>,
}

/// Replay log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayLog {
    /// Replay log version.
    pub version: u16,
    /// Ordered list of replay steps.
    pub steps: Vec<ReplayStep>,
}

impl ReplayLog {
    /// Create an empty replay log.
    pub fn new() -> Self {
        Self {
            version: REPLAY_LOG_VERSION,
            steps: Vec::new(),
        }
    }

    /// Encode to canonical bytes.
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        let mut w = ByteWriter::with_capacity(256);
        w.write_u16(self.version);
        w.write_u32(self.steps.len() as u32);

        for st in &self.steps {
            w.write_str(&st.name)?;

            let mut ins = st.inputs.clone();
            ins.sort();
            let mut outs = st.outputs.clone();
            outs.sort();

            w.write_u32(ins.len() as u32);
            for h in &ins {
                w.write_raw(h);
            }

            w.write_u32(outs.len() as u32);
            for h in &outs {
                w.write_raw(h);
            }
        }

        Ok(w.into_bytes())
    }

    /// Decode from canonical bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut r = ByteReader::new(bytes);
        let version = r.read_u16()?;
        if version != REPLAY_LOG_VERSION {
            return Err(DecodeError::new("unsupported replay log version"));
        }

        let steps_n_u32 = r.read_u32()?;
        if steps_n_u32 > (MAX_REPLAY_STEPS as u32) {
            return Err(DecodeError::new("steps too large"));
        }
        let steps_n = steps_n_u32 as usize;
        let mut steps = Vec::with_capacity(steps_n);

        for _ in 0..steps_n {
            // Read step name with a hard cap to prevent huge allocations on invalid bytes.
            let name_len_u32 = r.read_u32()?;
            if name_len_u32 > (MAX_REPLAY_STEP_NAME_BYTES as u32) {
                return Err(DecodeError::new("step name too large"));
            }
            let name_bytes = r.read_fixed(name_len_u32 as usize)?;
            let name = core::str::from_utf8(name_bytes)
                .map_err(|_| DecodeError::new("invalid UTF-8"))?
                .to_string();

            let ins_n_u32 = r.read_u32()?;
            if ins_n_u32 > (MAX_REPLAY_HASHES_PER_STEP as u32) {
                return Err(DecodeError::new("inputs too large"));
            }
            let ins_n = ins_n_u32 as usize;
            let mut inputs = Vec::with_capacity(ins_n);
            for _ in 0..ins_n {
                let b = r.read_fixed(32)?;
                let mut h = [0u8; 32];
                h.copy_from_slice(b);
                inputs.push(h);
            }

            let outs_n_u32 = r.read_u32()?;
            if outs_n_u32 > (MAX_REPLAY_HASHES_PER_STEP as u32) {
                return Err(DecodeError::new("outputs too large"));
            }
            let outs_n = outs_n_u32 as usize;
            let mut outputs = Vec::with_capacity(outs_n);
            for _ in 0..outs_n {
                let b = r.read_fixed(32)?;
                let mut h = [0u8; 32];
                h.copy_from_slice(b);
                outputs.push(h);
            }

            steps.push(ReplayStep {
                name,
                inputs,
                outputs,
            });
        }

        if r.remaining() != 0 {
            return Err(DecodeError::new("trailing bytes"));
        }

        Ok(Self { version, steps })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::blake3_hash;

    #[test]
    fn replay_log_round_trip() {
        let mut log = ReplayLog::new();
        let a = blake3_hash(b"a");
        let b = blake3_hash(b"b");
        let c = blake3_hash(b"c");

        log.steps.push(ReplayStep {
            name: "step1".to_string(),
            inputs: vec![b, a],
            outputs: vec![c],
        });

        let enc = log.encode().unwrap();
        let dec = ReplayLog::decode(&enc).unwrap();
        assert_eq!(dec.version, REPLAY_LOG_VERSION);
        assert_eq!(dec.steps.len(), 1);
        assert_eq!(dec.steps[0].name, "step1");

        let mut ins = dec.steps[0].inputs.clone();
        ins.sort();
        let mut exp = vec![a, b];
        exp.sort();
        assert_eq!(ins, exp);
        assert_eq!(dec.steps[0].outputs, vec![c]);
    }
}
