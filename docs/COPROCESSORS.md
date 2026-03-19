Coprocessors and advisory modules
==================================

Purpose
-------
Novel's core architecture is deterministic and evidence-first:
- structured memory (frames + lexicon)
- deterministic retrieval
- constrained synthesis (claims + uncertainty)

Coprocessors are optional advisory modules that can improve planning, extraction,
and fluency without replacing the core. The current live surface is rules-first
and bounded. Any future learned modules must still preserve CPU-first,
integer-oriented determinism, bounded memory, and anti-plagiarism constraints.

Coprocessor classes
-------------------
1) Local language hints (Markov/PPM)
 - Integer counts and deterministic updates.
 - Used for token/phrase suggestions and normalization, not factual claims.

2) Graph relevance (fixed-point random walks)
 - Entity/verb relation graph; stable iteration order.
 - Produces candidate expansions for evidence planning (multi-hop hints).

3) Intent and slots (HMM/CRF-lite)
 - Deterministic Viterbi over a small state set.
 - Improves which evidence types to seek and how to form frames.

4) Exemplar patch memory (kNN)
 - Deterministic nearest neighbor search over compact hashes of prior traces.
 - Stabilizes behavior on repeated patterns and reduces re-derivation.

5) Compression-as-learning (MDL)
 - Grammar/phrase induction to improve offline extraction heuristics.

Optional tiny transformer modules
---------------------------------
Transformers are optional plug-ins, primarily for:
- additional feature encoding for gating/ranking
- surface text realization from claims

Hard constraints:
- CPU-only inference
- integer-only kernels
- deterministic scheduling and reductions
- fixed weights and stable model hash
- must be fully optional (core works without it)

Pragmatics coprocessor (tone, tact, social cues)
-----------------------------------------------
Goal: estimate "how to respond" constraints without relying on a huge neural model.

- Input: PromptPack message text + tokenizer output + metaphone output (optional).
- Output: PragmaticsFrameV1 (docs/PRAGMATICS_FRAME_V1.md)
 - temperature, politeness, formality, directness, empathy_need
 - intent/mode flags (ask/command/vent/debate/brainstorm)
 - punctuation/emphasis summary

Properties:
- rules-first v1 (deterministic), later optionally a learned policy (GBDT) trained on
 post-ingested data.
- never produces factual claims
- never triggers retrieval; only shapes planning/realization

Realizer directives (rendering control plane)
--------------------------------------------
PragmaticsFrameV1 is a signal capture layer. RealizerDirectivesV1 is the downstream
control plane used by the text Realizer.

- Input: PragmaticsFrameV1 (and possibly small planner outputs in later stages).
- Output: RealizerDirectivesV1 (docs/REALIZER_DIRECTIVES_V1.md)

Directives constrain style, tone, and formatting without changing evidence.

stage status:
- contract and codec are complete.
- derivation rules from PragmaticsFrameV1 are implemented.
- Realizer integration is implemented (realizer applies directives for tone/style/format).


Safety reflex (Asimov-inspired)
-------------------------------
Goal: deterministically decide whether an operation is allowed, allowed with
constraints, or refused before realization.

- Input: PromptPack + proposed operation + optional EvidenceBundleV1
- Output: SafetyDecisionV1 artifact (planned)
- Design spec: docs/SAFETY_REFLEX_V1.md

This reflex is rules-first in v1. It is separate from PragmaticsFrameV1.
