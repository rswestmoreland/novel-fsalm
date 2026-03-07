// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Runtime Markov hint derivation helpers.
//!
//! introduces online usage of MarkovModelV1 to suggest
//! surface-template choice ids. Guidance is advisory-only and MUST NOT
//! introduce new claims.

use crate::frame::{derive_id64, Id64};
use crate::hash::Hash32;
use crate::markov_hints::{
    MarkovChoiceKindV1, MarkovHintsFlagsV1, MarkovHintsV1, MH_FLAG_USED_PPM,
};
use crate::markov_model::{MarkovModelV1, MarkovTokenV1};
use crate::markov_train::derive_markov_hints_v1;
use crate::realizer_directives::ToneV1;

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

fn allowed_preface_choice_ids_v1(t: ToneV1) -> [Id64; 2] {
    [preface_choice_id_v1(t, 0), preface_choice_id_v1(t, 1)]
}

/// Derive MarkovHintsV1 for the Opener preface selection site.
///
/// This is a bounded filter over generic Markov hints:
/// - only kind=Opener
/// - only known preface template ids for the provided tone
///
/// The returned record remains deterministic and may be empty.
pub fn derive_markov_hints_opener_preface_v1(
    query_id: Hash32,
    base_flags: MarkovHintsFlagsV1,
    model_hash: Hash32,
    model: &MarkovModelV1,
    tone: ToneV1,
    context_tokens: &[MarkovTokenV1],
    max_choices: usize,
) -> MarkovHintsV1 {
    let mut h = derive_markov_hints_v1(
        query_id,
        base_flags,
        model_hash,
        model,
        context_tokens,
        max_choices,
    );

    let allowed = allowed_preface_choice_ids_v1(tone);
    h.choices.retain(|c| {
        if c.kind != MarkovChoiceKindV1::Opener {
            return false;
        }
        c.choice_id == allowed[0] || c.choice_id == allowed[1]
    });

    // If filtering removes all choices, clear the "used" flag so the record
    // is explicit that no final suggestions are present.
    if h.choices.is_empty() {
        h.flags &= !MH_FLAG_USED_PPM;
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::markov_hints::MarkovChoiceKindV1;
    use crate::markov_model::{MarkovNextV1, MarkovStateV1, MARKOV_MODEL_V1_VERSION};

    fn sample_model_with_preface_tokens() -> MarkovModelV1 {
        let s0 = MarkovStateV1 {
            escape_count: 0,
            context: Vec::new(),
            next: vec![
                MarkovNextV1 {
                    token: MarkovTokenV1::new(MarkovChoiceKindV1::Opener, Id64(900)),
                    count: 40,
                },
                MarkovNextV1 {
                    token: MarkovTokenV1::new(MarkovChoiceKindV1::Transition, Id64(901)),
                    count: 30,
                },
                MarkovNextV1 {
                    token: MarkovTokenV1::new(
                        MarkovChoiceKindV1::Opener,
                        preface_choice_id_v1(ToneV1::Supportive, 0),
                    ),
                    count: 20,
                },
                MarkovNextV1 {
                    token: MarkovTokenV1::new(
                        MarkovChoiceKindV1::Opener,
                        preface_choice_id_v1(ToneV1::Supportive, 1),
                    ),
                    count: 10,
                },
            ],
        };
        let model = MarkovModelV1 {
            version: MARKOV_MODEL_V1_VERSION,
            order_n_max: 3,
            max_next_per_state: 8,
            corpus_hash: [0u8; 32],
            total_transitions: 100,
            states: vec![s0],
        };
        assert!(model.validate().is_ok());
        model
    }

    #[test]
    fn runtime_hints_filters_to_allowed_preface_ids_for_tone() {
        let model = sample_model_with_preface_tokens();
        let query_id: Hash32 = [7u8; 32];
        let model_hash: Hash32 = [9u8; 32];
        let h = derive_markov_hints_opener_preface_v1(
            query_id,
            0,
            model_hash,
            &model,
            ToneV1::Supportive,
            &[],
            8,
        );
        assert!(h.validate().is_ok());
        assert_eq!(h.choices.len(), 2);
        assert_eq!(h.choices[0].kind, MarkovChoiceKindV1::Opener);
        assert_eq!(
            h.choices[0].choice_id,
            preface_choice_id_v1(ToneV1::Supportive, 0)
        );
        assert_eq!(
            h.choices[1].choice_id,
            preface_choice_id_v1(ToneV1::Supportive, 1)
        );
    }

    #[test]
    fn runtime_hints_can_be_empty_after_filtering() {
        let model = sample_model_with_preface_tokens();
        let query_id: Hash32 = [7u8; 32];
        let model_hash: Hash32 = [9u8; 32];
        let h = derive_markov_hints_opener_preface_v1(
            query_id,
            0,
            model_hash,
            &model,
            ToneV1::Neutral,
            &[],
            8,
        );
        assert!(h.validate().is_ok());
        assert!(h.choices.is_empty());
        assert_eq!(h.flags & MH_FLAG_USED_PPM, 0);
    }
}
