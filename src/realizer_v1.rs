// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Realizer v1.
//!
//! The realizer converts an [`crate::answer_plan::AnswerPlanV1`] plus the
//! supporting [`crate::evidence_bundle::EvidenceBundleV1`] into a
//! deterministic, evidence-first textual output.
//!
//! v1 is intentionally simple:
//! - Emit a compact plan section.
//! - Emit an evidence section with stable per-item identifiers.
//! - Optionally load FrameRowV1 fields for Frame evidence items.
//!
//! This is a debugging-grade output intended to prove end-to-end determinism
//! before adding richer synthesis.

use crate::answer_plan::{AnswerPlanItemKindV1, AnswerPlanV1, AnswerPlanValidateError};
use crate::artifact::ArtifactStore;
use crate::evidence_bundle::{EvidenceBundleV1, EvidenceItemDataV1};
use crate::forecast::ForecastV1;
use crate::frame::{derive_id64, Id64};
use crate::frame_store::get_frame_segment_v1;
use crate::hash::{blake3_hash, hex32, Hash32};
use crate::markov_hints::{MarkovChoiceKindV1, MarkovHintsV1};
use crate::planner_hints::{PlannerHintsV1, PH_FLAG_PREFER_CLARIFY};
use crate::proof_artifact::{PA_FLAG_NO_SOLUTION, PA_FLAG_TRUNCATED, PA_FLAG_UNIQUE};
use crate::proof_artifact_store::get_proof_artifact_v1;
use crate::realizer_directives::{
    RealizerDirectivesError, RealizerDirectivesV1, StyleV1, ToneV1, FORMAT_FLAG_BULLETS,
    FORMAT_FLAG_INCLUDE_ASSUMPTIONS, FORMAT_FLAG_INCLUDE_NEXT_STEPS, FORMAT_FLAG_INCLUDE_RISKS,
    FORMAT_FLAG_INCLUDE_SUMMARY, FORMAT_FLAG_NUMBERED,
};

/// Realizer configuration schema version (v1).
pub const REALIZER_CFG_V1_VERSION: u16 = 1;

/// Realizer config validation errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RealizerCfgError {
    /// The version field is not supported.
    BadVersion,
    /// max_evidence_items must be non-zero.
    MaxEvidenceItemsZero,
    /// max_terms_per_row must be non-zero.
    MaxTermsPerRowZero,
}

impl core::fmt::Display for RealizerCfgError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            RealizerCfgError::BadVersion => f.write_str("bad version"),
            RealizerCfgError::MaxEvidenceItemsZero => f.write_str("max evidence items is zero"),
            RealizerCfgError::MaxTermsPerRowZero => f.write_str("max terms per row is zero"),
        }
    }
}

impl std::error::Error for RealizerCfgError {}

/// Realizer configuration (v1).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RealizerCfgV1 {
    /// Schema version.
    pub version: u16,
    /// Maximum number of evidence items to render.
    pub max_evidence_items: u16,
    /// Maximum number of term ids to show per row.
    pub max_terms_per_row: u16,
    /// If true, attempt to load FrameRowV1 fields for Frame evidence.
    pub load_frame_rows: bool,
}

impl Default for RealizerCfgV1 {
    fn default() -> Self {
        RealizerCfgV1::new()
    }
}

impl RealizerCfgV1 {
    /// Create a conservative default config.
    pub fn new() -> RealizerCfgV1 {
        RealizerCfgV1 {
            version: REALIZER_CFG_V1_VERSION,
            max_evidence_items: 32,
            max_terms_per_row: 12,
            load_frame_rows: true,
        }
    }

    /// Validate canonical invariants.
    pub fn validate(&self) -> Result<(), RealizerCfgError> {
        if self.version != REALIZER_CFG_V1_VERSION {
            return Err(RealizerCfgError::BadVersion);
        }
        if self.max_evidence_items == 0 {
            return Err(RealizerCfgError::MaxEvidenceItemsZero);
        }
        if self.max_terms_per_row == 0 {
            return Err(RealizerCfgError::MaxTermsPerRowZero);
        }
        Ok(())
    }
}

/// Errors that can occur during realization.
#[derive(Debug)]
pub enum RealizerV1Error {
    /// Config validation failed.
    Config(RealizerCfgError),
    /// AnswerPlan validation failed.
    PlanInvalid(AnswerPlanValidateError),
    /// Realizer directives validation failed.
    DirectivesInvalid(RealizerDirectivesError),
    /// Plan and evidence bundle are not compatible.
    PlanEvidenceMismatch,
    /// EvidenceBundleV1 could not be encoded canonically.
    EvidenceEncode(String),
    /// A FrameSegment referenced by evidence could not be loaded.
    FrameLoad(String),
}

impl core::fmt::Display for RealizerV1Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            RealizerV1Error::Config(e) => write!(f, "cfg: {}", e),
            RealizerV1Error::PlanInvalid(e) => write!(f, "plan: {}", e),
            RealizerV1Error::DirectivesInvalid(e) => write!(f, "directives: {}", e),
            RealizerV1Error::PlanEvidenceMismatch => f.write_str("plan/evidence mismatch"),
            RealizerV1Error::EvidenceEncode(s) => write!(f, "evidence encode: {}", s),
            RealizerV1Error::FrameLoad(s) => write!(f, "frame load: {}", s),
        }
    }
}

impl std::error::Error for RealizerV1Error {}

fn evidence_bundle_id(bundle: &EvidenceBundleV1) -> Result<Hash32, RealizerV1Error> {
    let bytes = bundle
        .encode()
        .map_err(|e| RealizerV1Error::EvidenceEncode(e.to_string()))?;
    Ok(blake3_hash(&bytes))
}

/// Markov surface-choice events observed during realization (v1).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RealizerMarkovEventsV1 {
    /// If a preface line was emitted, this records the deterministic choice id
    /// of the selected preface template.
    pub opener_preface_choice: Option<Id64>,
}

impl RealizerMarkovEventsV1 {
    fn none() -> RealizerMarkovEventsV1 {
        RealizerMarkovEventsV1 {
            opener_preface_choice: None,
        }
    }
}

/// Realizer output plus optional Markov surface-choice events (v1).
#[derive(Debug)]
pub struct RealizerOutputV1 {
    /// Realized answer text.
    pub text: String,
    /// Markov surface-choice events observed during realization.
    pub markov: RealizerMarkovEventsV1,
}

/// Realize an answer plan into a deterministic text output, optionally guided by
/// RealizerDirectivesV1.
///
/// This is a formatting control plane only: the output may change in tone and
/// layout, but it does not introduce new claims beyond the selected evidence.
///
/// If `directives` is None, this is identical to `realize_answer_plan_v1`.
pub fn realize_answer_plan_v1_with_directives<S: ArtifactStore>(
    store: &S,
    evidence: &EvidenceBundleV1,
    plan: &AnswerPlanV1,
    cfg: &RealizerCfgV1,
    directives: Option<&RealizerDirectivesV1>,
) -> Result<String, RealizerV1Error> {
    Ok(realize_answer_plan_v1_with_directives_and_markov_events(
        store, evidence, plan, cfg, directives, None,
    )?
    .text)
}

/// Realize an answer plan into a deterministic text output, optionally guided by
/// RealizerDirectivesV1 and MarkovHintsV1.
///
/// MarkovHintsV1 is advisory only: it MAY select among pre-defined surface-form
/// templates (opener/transition/closer) but MUST NOT introduce new claims.
///
/// Invalid MarkovHintsV1 inputs are ignored (treated as None).
pub fn realize_answer_plan_v1_with_directives_and_markov<S: ArtifactStore>(
    store: &S,
    evidence: &EvidenceBundleV1,
    plan: &AnswerPlanV1,
    cfg: &RealizerCfgV1,
    directives: Option<&RealizerDirectivesV1>,
    markov_hints: Option<&MarkovHintsV1>,
) -> Result<String, RealizerV1Error> {
    Ok(realize_answer_plan_v1_with_directives_and_markov_events(
        store,
        evidence,
        plan,
        cfg,
        directives,
        markov_hints,
    )?
    .text)
}

/// Realize an answer plan and return the realized text plus any observed Markov
/// surface-choice events (v1).
pub fn realize_answer_plan_v1_with_directives_and_markov_events<S: ArtifactStore>(
    store: &S,
    evidence: &EvidenceBundleV1,
    plan: &AnswerPlanV1,
    cfg: &RealizerCfgV1,
    directives: Option<&RealizerDirectivesV1>,
    markov_hints: Option<&MarkovHintsV1>,
) -> Result<RealizerOutputV1, RealizerV1Error> {
    realize_answer_plan_v1_with_directives_inner_events(
        store,
        evidence,
        plan,
        cfg,
        directives,
        markov_hints,
    )
}

fn realize_answer_plan_v1_with_directives_inner_events<S: ArtifactStore>(
    store: &S,
    evidence: &EvidenceBundleV1,
    plan: &AnswerPlanV1,
    cfg: &RealizerCfgV1,
    directives: Option<&RealizerDirectivesV1>,
    markov_hints: Option<&MarkovHintsV1>,
) -> Result<RealizerOutputV1, RealizerV1Error> {
    cfg.validate().map_err(RealizerV1Error::Config)?;
    plan.validate().map_err(RealizerV1Error::PlanInvalid)?;

    let mut markov_events = RealizerMarkovEventsV1::none();

    if directives.is_none() {
        let base = realize_answer_plan_v1(store, evidence, plan, cfg)?;
        return Ok(RealizerOutputV1 {
            text: base,
            markov: markov_events,
        });
    }
    let d = directives.unwrap();
    d.validate().map_err(RealizerV1Error::DirectivesInvalid)?;

    // Keep the existing v1 output shape when explicitly requested.
    if d.style == StyleV1::Debug {
        let base = realize_answer_plan_v1(store, evidence, plan, cfg)?;
        let mut out = String::with_capacity(base.len() + 128);

        let mut lines = base.lines();
        // First line should be "Answer v1".
        if let Some(first) = lines.next() {
            out.push_str(first);
            out.push('\n');
        }
        out.push_str(&format!(
            "directives tone={:?} style={:?} flags=0x{:08x}\n",
            d.tone, d.style, d.format_flags
        ));
        for l in lines {
            out.push_str(l);
            out.push('\n');
        }
        return Ok(RealizerOutputV1 {
            text: out,
            markov: markov_events,
        });
    }

    // Non-debug styles: sectioned plan output.
    // Check plan/evidence compatibility (same rules as v1).
    if plan.query_id != evidence.query_id {
        return Err(RealizerV1Error::PlanEvidenceMismatch);
    }
    if plan.snapshot_id != evidence.snapshot_id {
        return Err(RealizerV1Error::PlanEvidenceMismatch);
    }
    if plan.evidence_item_count != evidence.items.len() as u32 {
        return Err(RealizerV1Error::PlanEvidenceMismatch);
    }
    let eb_id = evidence_bundle_id(evidence)?;
    if plan.evidence_bundle_id != eb_id {
        return Err(RealizerV1Error::PlanEvidenceMismatch);
    }

    let mut out = String::with_capacity(4096);

    out.push_str("Answer v1\n");
    out.push_str("query_id=");
    out.push_str(&hex32(&plan.query_id));
    out.push_str(" snapshot_id=");
    out.push_str(&hex32(&plan.snapshot_id));
    out.push_str("\n");
    out.push_str(&format!(
        "directives tone={:?} style={:?} flags=0x{:08x}\n",
        d.tone, d.style, d.format_flags
    ));

    if d.max_preface_sentences > 0 {
        let (cid, line) = preface_choice_for_tone(d.tone, markov_hints);
        out.push_str("\n");
        out.push_str(line);
        out.push_str("\n");
        markov_events.opener_preface_choice = Some(cid);
    }

    if should_show_proof_solution_line_v1(plan, Some(d)) {
        if let Some(line) = format_proof_solution_line_v1(store, evidence) {
            out.push_str("\n");
            out.push_str(&line);
            out.push_str("\n");
        }
    }

    out.push_str("\nPlan\n");

    let numbered = (d.format_flags & FORMAT_FLAG_NUMBERED) != 0;
    let bullets_flag = (d.format_flags & FORMAT_FLAG_BULLETS) != 0;
    let use_numbered = if numbered {
        true
    } else if bullets_flag {
        false
    } else {
        d.style == StyleV1::StepByStep
    };

    render_plan_sections_v1(&mut out, plan, d, use_numbered);

    out.push_str("\nEvidence\n");

    // Evidence section mirrors v1 so it stays auditable.
    let max_items = core::cmp::min(cfg.max_evidence_items as usize, evidence.items.len());
    for i in 0..max_items {
        let item = &evidence.items[i];
        out.push('[');
        out.push_str("E");
        out.push_str(&(i as u32).to_string());
        out.push_str("] ");
        out.push_str("score=");
        out.push_str(&item.score.to_string());

        match &item.data {
            EvidenceItemDataV1::Frame(fr) => {
                out.push(' ');
                out.push_str("frame=");
                out.push_str(&hex32(&fr.segment_id));
                out.push(' ');
                out.push_str("row=");
                out.push_str(&fr.row_ix.to_string());

                if cfg.load_frame_rows {
                    let seg_opt = get_frame_segment_v1(store, &fr.segment_id)
                        .map_err(|e| RealizerV1Error::FrameLoad(e.to_string()))?;
                    if let Some(seg) = seg_opt {
                        if let Some(row) = seg.get_row(fr.row_ix) {
                            out.push(' ');
                            out.push_str("doc_id=");
                            out.push_str(&row.doc_id.0 .0.to_string());
                            out.push(' ');
                            out.push_str("source_id=");
                            out.push_str(&row.source_id.0 .0.to_string());
                            out.push(' ');
                            out.push_str("confidence_q16=");
                            out.push_str(&row.confidence.0.to_string());

                            let term_cap = cfg.max_terms_per_row as usize;
                            if !row.terms.is_empty() {
                                out.push(' ');
                                out.push_str("terms=");
                                out.push('[');
                                let n = core::cmp::min(term_cap, row.terms.len());
                                for tix in 0..n {
                                    if tix != 0 {
                                        out.push(',');
                                    }
                                    out.push_str(&row.terms[tix].term.0 .0.to_string());
                                }
                                if row.terms.len() > n {
                                    out.push_str(",...");
                                }
                                out.push(']');
                            }
                        } else {
                            out.push(' ');
                            out.push_str("row_missing=1");
                        }
                    } else {
                        out.push(' ');
                        out.push_str("segment_missing=1");
                    }
                }
            }
            EvidenceItemDataV1::Lexicon(lx) => {
                out.push(' ');
                out.push_str("lexicon=");
                out.push_str(&hex32(&lx.segment_id));
                out.push(' ');
                out.push_str("row=");
                out.push_str(&lx.row_ix.to_string());
            }
            EvidenceItemDataV1::Proof(p) => {
                out.push(' ');
                out.push_str("proof=");
                out.push_str(&hex32(&p.proof_id));
            }
        }

        out.push_str("\n");
    }

    if evidence.items.len() > max_items {
        out.push_str("...\n");
    }

    Ok(RealizerOutputV1 {
        text: out,
        markov: markov_events,
    })
}

fn markov_choice_id_for_kind(h: &MarkovHintsV1, kind: MarkovChoiceKindV1) -> Option<Id64> {
    let mut best_score: i64 = 0;
    let mut best_id: Id64 = Id64(0);
    let mut any = false;

    for c in h.choices.iter() {
        if c.kind != kind {
            continue;
        }
        if !any {
            any = true;
            best_score = c.score;
            best_id = c.choice_id;
            continue;
        }
        if c.score > best_score {
            best_score = c.score;
            best_id = c.choice_id;
            continue;
        }
        if c.score == best_score && c.choice_id.0 < best_id.0 {
            best_id = c.choice_id;
        }
    }
    if any {
        Some(best_id)
    } else {
        None
    }
}

fn preface_choice_id_v1(t: ToneV1, variant: u8) -> Id64 {
    match t {
        ToneV1::Supportive => match variant {
            0 => derive_id64(b"markov_choice_v1", b"preface:supportive:0"),
            _ => derive_id64(b"markov_choice_v1", b"preface:supportive:1"),
        },
        ToneV1::Neutral => match variant {
            0 => derive_id64(b"markov_choice_v1", b"preface:neutral:0"),
            _ => derive_id64(b"markov_choice_v1", b"preface:neutral:1"),
        },
        ToneV1::Direct => match variant {
            0 => derive_id64(b"markov_choice_v1", b"preface:direct:0"),
            _ => derive_id64(b"markov_choice_v1", b"preface:direct:1"),
        },
        ToneV1::Cautious => match variant {
            0 => derive_id64(b"markov_choice_v1", b"preface:cautious:0"),
            _ => derive_id64(b"markov_choice_v1", b"preface:cautious:1"),
        },
    }
}

fn preface_choice_for_tone(
    t: ToneV1,
    markov_hints: Option<&MarkovHintsV1>,
) -> (Id64, &'static str) {
    let desired = match markov_hints {
        Some(h) => {
            if h.validate().is_ok() {
                markov_choice_id_for_kind(h, MarkovChoiceKindV1::Opener)
            } else {
                None
            }
        }
        None => None,
    };

    match t {
        ToneV1::Supportive => {
            const V0: &str = "I can help with that. Here is what the evidence supports:";
            const V1: &str = "Happy to help. Here is what the evidence supports:";
            let cid0 = preface_choice_id_v1(t, 0);
            let cid1 = preface_choice_id_v1(t, 1);
            if desired == Some(cid1) {
                (cid1, V1)
            } else {
                (cid0, V0)
            }
        }
        ToneV1::Neutral => {
            const V0: &str = "Here is what the evidence supports:";
            const V1: &str = "Here is the evidence:";
            let cid0 = preface_choice_id_v1(t, 0);
            let cid1 = preface_choice_id_v1(t, 1);
            if desired == Some(cid1) {
                (cid1, V1)
            } else {
                (cid0, V0)
            }
        }
        ToneV1::Direct => {
            const V0: &str = "Based on the evidence:";
            const V1: &str = "Evidence suggests:";
            let cid0 = preface_choice_id_v1(t, 0);
            let cid1 = preface_choice_id_v1(t, 1);
            if desired == Some(cid1) {
                (cid1, V1)
            } else {
                (cid0, V0)
            }
        }
        ToneV1::Cautious => {
            const V0: &str = "Based on the available evidence:";
            const V1: &str = "Based on the evidence we have:";
            let cid0 = preface_choice_id_v1(t, 0);
            let cid1 = preface_choice_id_v1(t, 1);
            if desired == Some(cid1) {
                (cid1, V1)
            } else {
                (cid0, V0)
            }
        }
    }
}

fn should_show_proof_solution_line_v1(
    plan: &AnswerPlanV1,
    d: Option<&RealizerDirectivesV1>,
) -> bool {
    if plan
        .items
        .iter()
        .any(|it| it.kind == AnswerPlanItemKindV1::Step)
    {
        return true;
    }
    let dd = match d {
        Some(x) => x,
        None => return false,
    };
    if (dd.format_flags & FORMAT_FLAG_INCLUDE_NEXT_STEPS) != 0 {
        return true;
    }
    matches!(
        dd.style,
        StyleV1::Checklist | StyleV1::StepByStep | StyleV1::Debug
    )
}

fn format_proof_solution_line_v1<S: ArtifactStore>(
    store: &S,
    evidence: &EvidenceBundleV1,
) -> Option<String> {
    let mut proof_id_opt: Option<Hash32> = None;
    for it in evidence.items.iter() {
        if let EvidenceItemDataV1::Proof(p) = &it.data {
            proof_id_opt = Some(p.proof_id);
            break;
        }
    }
    let proof_id = proof_id_opt?;
    let proof_opt = get_proof_artifact_v1(store, &proof_id).ok()?;
    let proof = proof_opt?;

    let mut status = "one";
    if (proof.flags & PA_FLAG_NO_SOLUTION) != 0 || proof.solutions.is_empty() {
        status = "no_solution";
    } else if (proof.flags & PA_FLAG_UNIQUE) != 0 {
        status = "unique";
    } else if proof.solutions.len() > 1 {
        status = "multiple";
    }

    let mut out = String::with_capacity(96);
    out.push_str("Proof solution: (");
    out.push_str(status);
    if (proof.flags & PA_FLAG_TRUNCATED) != 0 {
        out.push_str(",truncated");
    }
    out.push_str(")");

    if status != "no_solution" {
        let row0 = &proof.solutions[0];
        if row0.len() == proof.vars.len() {
            out.push(' ');
            for i in 0..proof.vars.len() {
                if i != 0 {
                    out.push(',');
                }
                out.push_str(&proof.vars[i]);
                out.push('=');
                out.push_str(&row0[i].to_string());
            }
        }
    }

    Some(out)
}

fn render_plan_sections_v1(
    out: &mut String,
    plan: &AnswerPlanV1,
    d: &RealizerDirectivesV1,
    numbered: bool,
) {
    let flags = d.format_flags;
    let include_summary = (flags & FORMAT_FLAG_INCLUDE_SUMMARY) != 0
        || d.style == StyleV1::Debug
        || d.style == StyleV1::Default;
    let include_steps = (flags & FORMAT_FLAG_INCLUDE_NEXT_STEPS) != 0
        || d.style == StyleV1::Checklist
        || d.style == StyleV1::StepByStep
        || d.style == StyleV1::Debug;
    let include_details = (flags & FORMAT_FLAG_INCLUDE_ASSUMPTIONS) != 0
        || d.style == StyleV1::Default
        || d.style == StyleV1::Debug;
    let include_caveats = (flags & FORMAT_FLAG_INCLUDE_RISKS) != 0
        || d.tone == ToneV1::Cautious
        || d.style == StyleV1::Debug;

    if include_summary {
        render_plan_group(
            out,
            plan,
            AnswerPlanItemKindV1::Summary,
            "Summary",
            numbered,
        );
    }
    if include_steps {
        render_plan_group(out, plan, AnswerPlanItemKindV1::Step, "Steps", numbered);
    }
    if include_details {
        render_plan_group(out, plan, AnswerPlanItemKindV1::Bullet, "Details", numbered);
    }
    if include_caveats {
        render_plan_group(out, plan, AnswerPlanItemKindV1::Caveat, "Caveats", numbered);
    }
}

fn render_plan_group(
    out: &mut String,
    plan: &AnswerPlanV1,
    kind: AnswerPlanItemKindV1,
    heading: &str,
    numbered: bool,
) {
    let mut any = false;
    for it in plan.items.iter() {
        if it.kind == kind {
            any = true;
            break;
        }
    }
    if !any {
        return;
    }

    out.push_str(heading);
    out.push_str("\n");

    let mut n = 0usize;
    for (i, it) in plan.items.iter().enumerate() {
        if it.kind != kind {
            continue;
        }
        n += 1;
        if numbered {
            out.push_str(&format!("{}. item={} strength={}", n, i, it.strength));
        } else {
            out.push_str(&format!("- item={} strength={}", i, it.strength));
        }
        if !it.evidence_item_ix.is_empty() {
            out.push_str(" refs=");
            for (j, ix) in it.evidence_item_ix.iter().enumerate() {
                if j != 0 {
                    out.push(',');
                }
                out.push_str("E");
                out.push_str(&ix.to_string());
            }
        }
        out.push_str("\n");
    }

    out.push_str("\n");
}

/// Realize an answer plan into a deterministic text output.
///
/// The returned string is UTF-8 and ASCII-only under current v1 formatting.
pub fn realize_answer_plan_v1<S: ArtifactStore>(
    store: &S,
    evidence: &EvidenceBundleV1,
    plan: &AnswerPlanV1,
    cfg: &RealizerCfgV1,
) -> Result<String, RealizerV1Error> {
    cfg.validate().map_err(RealizerV1Error::Config)?;
    plan.validate().map_err(RealizerV1Error::PlanInvalid)?;

    // Check plan/evidence compatibility.
    if plan.query_id != evidence.query_id {
        return Err(RealizerV1Error::PlanEvidenceMismatch);
    }
    if plan.snapshot_id != evidence.snapshot_id {
        return Err(RealizerV1Error::PlanEvidenceMismatch);
    }
    if plan.evidence_item_count != evidence.items.len() as u32 {
        return Err(RealizerV1Error::PlanEvidenceMismatch);
    }
    let eb_id = evidence_bundle_id(evidence)?;
    if plan.evidence_bundle_id != eb_id {
        return Err(RealizerV1Error::PlanEvidenceMismatch);
    }

    let mut out = String::with_capacity(4096);

    out.push_str("Answer v1\n");
    out.push_str("query_id=");
    out.push_str(&hex32(&plan.query_id));
    out.push_str(" snapshot_id=");
    out.push_str(&hex32(&plan.snapshot_id));
    out.push_str("\n\n");

    if should_show_proof_solution_line_v1(plan, None) {
        if let Some(line) = format_proof_solution_line_v1(store, evidence) {
            out.push_str(&line);
            out.push_str("\n\n");
        }
    }

    out.push_str("Plan\n");
    for (i, it) in plan.items.iter().enumerate() {
        out.push_str("-");
        out.push(' ');
        out.push_str("item=");
        out.push_str(&(i as u32).to_string());
        out.push_str(" kind=");
        match it.kind {
            AnswerPlanItemKindV1::Summary => out.push_str("summary"),
            AnswerPlanItemKindV1::Bullet => out.push_str("bullet"),
            AnswerPlanItemKindV1::Step => out.push_str("step"),
            AnswerPlanItemKindV1::Caveat => out.push_str("caveat"),
        }
        if !it.evidence_item_ix.is_empty() {
            out.push_str(" refs=");
            for (j, ix) in it.evidence_item_ix.iter().enumerate() {
                if j != 0 {
                    out.push(',');
                }
                out.push_str("E");
                out.push_str(&ix.to_string());
            }
        }
        out.push_str("\n");
    }

    out.push_str("\nEvidence\n");

    let max_items = core::cmp::min(cfg.max_evidence_items as usize, evidence.items.len());
    for i in 0..max_items {
        let item = &evidence.items[i];
        out.push('[');
        out.push_str("E");
        out.push_str(&(i as u32).to_string());
        out.push_str("] ");
        out.push_str("score=");
        out.push_str(&item.score.to_string());

        match &item.data {
            EvidenceItemDataV1::Frame(fr) => {
                out.push(' ');
                out.push_str("frame=");
                out.push_str(&hex32(&fr.segment_id));
                out.push(' ');
                out.push_str("row=");
                out.push_str(&fr.row_ix.to_string());

                if cfg.load_frame_rows {
                    let seg_opt = get_frame_segment_v1(store, &fr.segment_id)
                        .map_err(|e| RealizerV1Error::FrameLoad(e.to_string()))?;
                    if let Some(seg) = seg_opt {
                        if let Some(row) = seg.get_row(fr.row_ix) {
                            out.push(' ');
                            out.push_str("doc_id=");
                            out.push_str(&row.doc_id.0 .0.to_string());
                            out.push(' ');
                            out.push_str("source_id=");
                            out.push_str(&row.source_id.0 .0.to_string());
                            out.push(' ');
                            out.push_str("confidence_q16=");
                            out.push_str(&row.confidence.0.to_string());

                            let term_cap = cfg.max_terms_per_row as usize;
                            if !row.terms.is_empty() {
                                out.push(' ');
                                out.push_str("terms=");
                                out.push('[');
                                let n = core::cmp::min(term_cap, row.terms.len());
                                for tix in 0..n {
                                    if tix != 0 {
                                        out.push(',');
                                    }
                                    out.push_str(&row.terms[tix].term.0 .0.to_string());
                                }
                                if row.terms.len() > n {
                                    out.push_str(",...");
                                }
                                out.push(']');
                            }
                        } else {
                            out.push(' ');
                            out.push_str("row_missing=1");
                        }
                    } else {
                        out.push(' ');
                        out.push_str("segment_missing=1");
                    }
                }
            }
            EvidenceItemDataV1::Lexicon(lx) => {
                out.push(' ');
                out.push_str("lexicon=");
                out.push_str(&hex32(&lx.segment_id));
                out.push(' ');
                out.push_str("row=");
                out.push_str(&lx.row_ix.to_string());
            }
            EvidenceItemDataV1::Proof(p) => {
                out.push(' ');
                out.push_str("proof=");
                out.push_str(&hex32(&p.proof_id));
            }
        }

        out.push_str("\n");
    }

    if evidence.items.len() > max_items {
        out.push_str("...\n");
    }

    Ok(out)
}

/// Append a single clarifying question, if requested by PlannerHintsV1.
///
/// Policy (v1):
/// - Only append when `PH_FLAG_PREFER_CLARIFY` is set.
/// - Only append when `max_questions > 0`.
/// - Use the highest-ranked forecast question (index 0), if present.
/// - Respect a fixed byte cap (256) by truncating the question text if needed.
///
/// Returns true if a question was appended.
pub fn append_clarifying_question_v1(
    out: &mut String,
    hints: &PlannerHintsV1,
    fc: &ForecastV1,
    max_questions: u8,
) -> bool {
    const MAX_Q_BYTES: usize = 256;

    if max_questions == 0 {
        return false;
    }
    if (hints.flags & PH_FLAG_PREFER_CLARIFY) == 0 {
        return false;
    }
    let q = match fc.questions.first() {
        Some(q) => q,
        None => return false,
    };
    let qtxt = q.text.trim();
    if qtxt.is_empty() {
        return false;
    }

    let prefix = "

Clarifying question: ";
    let suffix = if qtxt.ends_with('?') { "" } else { "?" };

    let mut qclip = qtxt;
    if qclip.len() > MAX_Q_BYTES {
        let mut end = MAX_Q_BYTES;
        while end > 0 && !qclip.is_char_boundary(end) {
            end -= 1;
        }
        qclip = &qclip[..end];
    }

    out.push_str(prefix);
    out.push_str(qclip);
    out.push_str(suffix);
    true
}

#[cfg(test)]
mod directed_realizer_tests {
    use super::*;
    use crate::answer_plan::{AnswerPlanItemKindV1, AnswerPlanItemV1};
    use crate::artifact::{ArtifactResult, ArtifactStore};
    use crate::evidence_bundle::{
        EvidenceBundleV1, EvidenceItemDataV1, EvidenceItemV1, EvidenceLimitsV1, LexiconRowRefV1,
        ProofRefV1,
    };
    use crate::hash::Hash32;
    use std::path::PathBuf;

    struct NullStore;

    impl ArtifactStore for NullStore {
        fn put(&self, bytes: &[u8]) -> ArtifactResult<Hash32> {
            Ok(blake3_hash(bytes))
        }

        fn get(&self, _hash: &Hash32) -> ArtifactResult<Option<Vec<u8>>> {
            Ok(None)
        }

        fn path_for(&self, hash: &Hash32) -> PathBuf {
            PathBuf::from(hex32(hash))
        }
    }

    #[test]
    fn realize_with_directives_sections_and_header() {
        let store = NullStore;

        let qid: Hash32 = [1u8; 32];
        let sid: Hash32 = [2u8; 32];

        let limits = EvidenceLimitsV1 {
            segments_touched: 0,
            max_items: 8,
            max_bytes: 0,
        };
        let mut bundle = EvidenceBundleV1::new(qid, sid, limits, 1);
        bundle.items.push(EvidenceItemV1 {
            score: 100,
            data: EvidenceItemDataV1::Proof(ProofRefV1 {
                proof_id: [9u8; 32],
            }),
        });
        bundle.items.push(EvidenceItemV1 {
            score: 90,
            data: EvidenceItemDataV1::Lexicon(LexiconRowRefV1 {
                segment_id: [8u8; 32],
                row_ix: 7,
            }),
        });
        bundle.canonicalize_in_place().unwrap();

        let eb_id = evidence_bundle_id(&bundle).expect("bundle id");
        let mut plan = AnswerPlanV1::new(qid, sid, eb_id, bundle.items.len() as u32);
        plan.items.push(AnswerPlanItemV1 {
            kind: AnswerPlanItemKindV1::Summary,
            strength: 500,
            evidence_item_ix: vec![0],
        });
        plan.items.push(AnswerPlanItemV1 {
            kind: AnswerPlanItemKindV1::Step,
            strength: 400,
            evidence_item_ix: vec![1],
        });

        let mut rcfg = RealizerCfgV1::new();
        rcfg.max_evidence_items = 8;
        rcfg.load_frame_rows = false;
        rcfg.max_terms_per_row = 4;

        let d = RealizerDirectivesV1 {
            version: crate::realizer_directives::REALIZER_DIRECTIVES_V1_VERSION,
            tone: ToneV1::Supportive,
            style: StyleV1::Checklist,
            format_flags: FORMAT_FLAG_BULLETS
                | FORMAT_FLAG_INCLUDE_SUMMARY
                | FORMAT_FLAG_INCLUDE_NEXT_STEPS,
            max_preface_sentences: 1,
            max_softeners: 0,
            max_hedges: 0,
            max_questions: 0,
            rationale_codes: Vec::new(),
        };

        let text = realize_answer_plan_v1_with_directives(&store, &bundle, &plan, &rcfg, Some(&d))
            .expect("realize");
        assert!(text.contains("directives tone=Supportive style=Checklist"));
        assert!(text.contains("Plan"));
        assert!(text.contains("Summary"));
        assert!(text.contains("Steps"));
        assert!(text.contains("Evidence"));
    }

    #[test]
    fn append_clarifying_question_policy_appends() {
        let mut out = String::from("Hello");

        let hints = PlannerHintsV1 {
            version: crate::planner_hints::PLANNER_HINTS_V1_VERSION,
            query_id: [1u8; 32],
            flags: PH_FLAG_PREFER_CLARIFY,
            hints: Vec::new(),
            followups: Vec::new(),
        };

        let fc = ForecastV1 {
            version: crate::forecast::FORECAST_V1_VERSION,
            query_id: [2u8; 32],
            flags: 0,
            horizon_turns: 1,
            intents: Vec::new(),
            questions: vec![crate::forecast::ForecastQuestionV1::new(
                crate::frame::Id64(1),
                1,
                "What is your budget".to_string(),
                0,
            )],
        };

        let appended = append_clarifying_question_v1(&mut out, &hints, &fc, 1);
        assert!(appended);
        assert!(out.contains("Clarifying question:"));
        assert!(out.ends_with('?'));

        let mut out2 = String::from("Hello");
        let appended2 = append_clarifying_question_v1(&mut out2, &hints, &fc, 0);
        assert!(!appended2);
    }
}
