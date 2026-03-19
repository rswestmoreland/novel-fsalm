PragmaticsFrameV1 (nuance, tone, social cues)
=============================================

Purpose
-------
Transformer LLMs often learn "how to talk" (tone, tact, idioms, emphasis) implicitly from
massive parameterization and huge corpora.

Novel will not have that luxury. Instead, we treat pragmatics as a deterministic coprocessor:
a small rules-first signal extractor that produces compact "control signals" used by planning
and realization. This helps Novel:
- mirror or soften user tone safely
- detect urgency, hostility, or vulnerability cues
- interpret idioms and figurative language (with lexicon help)
- render answers with better tact and emphasis without copying style verbatim from sources

This module is not an inverted-index or search engine feature. It does not retrieve facts.
It produces constraints for how to respond.

Design constraints
------------------
- CPU-first, integer-only (no floats).
- Bitwise deterministic given identical input bytes + config.
- Bounded memory. O(n) scan over prompt bytes.
- No unsafe. Minimal crates.

Where it fits in the pipeline
-----------------------------
Pragmatics runs in parallel with term/metaphone extraction:

PromptPack -> tokenize/term_freqs -> (retrieval)
 -> metaphone -> (entity resolution / expansion)
 -> pragmatics -> style/tact constraints -> planner/realizer

Pragmatics outputs do NOT directly change the evidence set. They shape:
- answer tone and framing
- how many clarifying questions (if any)
- cautious vs direct phrasing
- formatting style (bullets vs prose) and emphasis

Schema
------
PragmaticsFrameV1 is a compact, portable structure. It can be stored as an artifact and
referenced by other stages.

All scores are u16 or i16 with fixed ranges. Higher is "more".

Core fields
-----------
- version: u16
- source_id: Id64
 - deterministic identifier for the input source (e.g., PromptPack request id)
- msg_ix: u32
 - message index within PromptPack this frame summarizes
- byte_len: u32
- ascii_only: u8 (0/1)
 - 1 if message bytes are all ASCII; 0 otherwise

Tone/temperature signals (integer scaled)
-----------------------------------------
All are u16 unless noted.

- temperature: u16 in [0..1000]
 - 0 calm, 1000 heated
- valence: i16 in [-1000..1000]
 - negative to positive affect
- arousal: u16 in [0..1000]
 - low-energy to high-energy (excitement, urgency)

Social/tact signals
-------------------
- politeness: u16 in [0..1000]
- formality: u16 in [0..1000]
- directness: u16 in [0..1000]
 - 0 very indirect/hedged, 1000 very direct
- empathy_need: u16 in [0..1000]
 - higher suggests user may expect supportive framing

Rhetoric/intent
---------------
- mode: u16 enum (RhetoricModeV1)
 - 0 Unknown
 - 1 Ask
 - 2 Command
 - 3 Vent
 - 4 Debate
 - 5 Brainstorm
 - 6 Story
 - 7 Negotiation
- flags: u32 bitset (IntentFlagsV1)
 - bit 0: has_question
 - bit 1: has_request
 - bit 2: has_constraints
 - bit 3: has_math
 - bit 4: has_code
 - bit 5: is_meta_prompt (talking about the system itself)
 - bit 6: is_follow_up (short question referencing earlier turn)
 - bit 7: safety_sensitive (self-harm/violence cues) (rules-only, conservative)
 - bit 8: is_problem_solve (troubleshooting, debugging, reverse engineering, retrospection)
 - bit 9: is_logic_puzzle (logic puzzle / constraint satisfaction intent)
 - bit 10: is_compare_request (explicit compare / versus intent)
 - bit 11: is_recommend_request (explicit best-choice / recommendation intent)
 - bit 12: is_summarize_request (explicit summary / recap intent)
 - bit 13: is_explain_request (explicit explain / walkthrough intent)
 - bit 14: has_compare_targets (explicit alternatives or named compare targets detected)
 - bit 15: has_focus_summary (asks for a summary-first or high-level response shape)
 - bit 16: has_focus_steps (asks for a step-by-step or detailed response shape)
 - bit 17: has_focus_example (asks for an example-led response shape)

Punctuation/emphasis summary
----------------------------
These are compact, not span-based (v1). They are intended as planning hints.

- exclamations: u16 (# of '!')
- questions: u16 (# of '?')
- ellipses: u16 (# of "..." occurrences)
- caps_words: u16 (# of all-caps ASCII words length >= 2)
- repeat_punct_runs: u16 (# of repeated punctuation runs like "!!!" or "??")
- quotes: u16 (# of '"' and ''' occurrences, ASCII only)
- emphasis_score: u16 in [0..1000]
 - derived from punctuation runs + caps + repetition

Lexical cue counts (optional, v1)
---------------------------------
These are counts of matched cue terms. Cues should be matched via tokenization (TermId),
not raw string scanning, so the same machinery works for forum/blog/social sources.

- hedge_count: u16
 - "maybe", "probably", "I think", "kind of"
- intensifier_count: u16
 - "very", "extremely", "super"
- profanity_count: u16
 - conservative list, ASCII only
- apology_count: u16
 - "sorry", "apologies"
- gratitude_count: u16
 - "thanks", "appreciate"
- insult_count: u16
 - "idiot", "stupid" (conservative)

Lexicon cue neighborhoods (optional)
-----------------------------------
When a lexicon snapshot is loaded, Novel can build small, bounded lemma-id neighborhoods
from seed lemma keys expanded via lexicon relations (synonym/related/hypernym/etc.).

Pragmatics can use neighborhood membership to infer higher-level intent signals
(planning, problem solving, logic puzzles) without relying on large hardcoded keyword
lists.

In v1, these higher-level intent signals map to intent flag bits (is_problem_solve,
is_logic_puzzle) when lexicon cues co-occur with request/question/constraints structure. This remains rules-first and deterministic:
- identical lexicon artifacts + identical cfg => identical neighborhoods
- strict caps and depth limits keep memory and CPU bounded

If no lexicon snapshot is available, pragmatics should fall back to the existing small
cue sets.

Computation (rules-first)
-------------------------
V1 is rules-only. Later we can add a learned style policy (GBDT) trained on post-ingested
data, but the schema remains stable.

Suggested deterministic algorithm outline:

1) Scan bytes once to compute:
 - punctuation counts, runs, whitespace, ascii_only
 - tokenize spans (existing tokenizer) and compute TermIds

2) Match TermIds against small builtin cue sets:
 - hedge/intensifier/apology/gratitude/profanity/insult
 - store counts

3) Derive scores (all integer arithmetic):
 - temperature increases with punctuation runs, profanity/insult, caps_words
 - politeness increases with gratitude/apology and decreases with profanity/insult
 - directness increases with imperative verbs in first token positions (simple rules)
 - mode inferred from patterns:
 - Ask if '?' present or begins with WH-word
 - Command if starts with imperative verb and no '?'
 - Vent if high temperature + negative valence + first-person complaint cues
 - Brainstorm if contains "ideas", "brainstorm", "what if", "could we"
 - clamp to fixed ranges

Interpretation in planning
--------------------------
The planner should convert PragmaticsFrameV1 into directives, for example:
- If temperature >= 700 and politeness <= 300:
 - de-escalate: neutral tone, avoid blame, reflect feelings briefly
- If empathy_need >= 600:
 - start with supportive framing before technical content
- If mode == Brainstorm:
 - prefer options + pros/cons, invite iteration
- If directness low:
 - ask one clarifying question before strong recommendations
- If has_compare_targets:
 - prefer criteria-focused compare clarifiers over generic "which options?"
- If has_focus_summary / has_focus_steps / has_focus_example:
 - bias clarifiers toward the requested response shape without changing evidence selection

Idioms and metaphors
--------------------
Idioms should be handled primarily by Lexicon ingestion. PragmaticsFrameV1 may include
a future "idiom_hits" summary, but v1 keeps this out of the core schema.

Future extensions (v2+)
-----------------------
- Span-level emphasis map (start,end,kind,weight) for precise realization.
- Sarcasm_hint score (rules + learned).
- Conversation-level aggregation (rolling pragmatics state across turns).
