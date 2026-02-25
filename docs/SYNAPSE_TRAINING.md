Synapse training on post-ingested state
==================================================

Purpose
-------
Novel keeps durable knowledge in disk-first structured memory ("engrams"):
frames, lexicon, segment signatures, postings, and replay traces.

A learned layer ("synapses") can be trained on the post-ingested state to improve:
- routing and planning (caps, pass selection)
- evidence scoring and ranking
- claim confidence calibration

This stage focuses on GBDT-style training as the default learned module, because it
matches Novel's constraints: CPU-first, deterministic, minimal memory, and small code.

Scope
----------------
This stage adds:
- deterministic feature extraction from replay traces and retrieval artifacts
- a compact training export format (binary or CSV-like)
- a strict reference spec for targets/labels and metrics
- Rust-side inference-only hooks (optional later), but NO requirement to implement
 training inside Rust

This stage does not add:
- a transformer
- online learning
- anything that changes correctness-critical behavior without tests

(A) GBDT training on the post-ingested state
--------------------------------------------
GBDT is trained on tabular features computed from Novel's pipeline artifacts:

Feature sources:
- Query/QFV:
 - counts of base terms/meta codes/entities
 - intent kind
 - required anchor counts
 - expansion counts by channel (LEX/META/ENT/GRAPH)
- Retrieval planning:
 - caps selected (segments_touched, postings_reads, evidence_items)
 - pass count and pass2 objectives
- Segment gating stats (D):
 - segments scanned, rejected, kept
 - sketch overlap stats (min/max/avg)
 - bloom hit counts
- Candidate selection stats (C):
 - postings lists read
 - df/idf buckets (by anchor)
 - intersection depth and candidate counts
- EvidenceSet stats:
 - evidence items emitted
 - slot coverage stats (who/what/when/where present)
 - diversity stats (unique doc_id, lemma_id)
 - polarity distribution and time spread
 - source confidence summaries
- ClaimSet stats:
 - claims created and merged
 - contradiction flags/counts
 - confidence distribution (min/max/avg)

Label/target sources:
- correctness labels:
 - exact match for definitional tasks (Wiktionary)
 - structured fact checks for frame tasks (Wikipedia-derived)
 - verifier results for math/logic (BigInt/rational)
- user feedback labels (optional later):
 - accept/reject or rating bucket
- pipeline improvement labels:
 - did pass 2 add novel evidence that improved claim coverage?
 - did reranking improve final answer quality?

What the GBDT learns:
- Evidence scoring/ranking:
 - replaces or calibrates fixed MRS feature weights
 - predicts which evidence items contribute to correct claims
- Planner policy:
 - predicts when pass 2 is beneficial
 - suggests cap scaling within allowed bounds
- Confidence calibration:
 - maps evidence/claim features to better confidence thresholds

Benefits:
- small model size and very fast deterministic inference
- learns from corpus as represented (post-ingest), not raw text
- easy to update by re-exporting training rows from new ingests or new replays

(B) Training records: what "post-ingested training" looks like
--------------------------------------------------------------
Each run can emit a training record. Minimal record layout:

X (features):
- q: QFV summary features (counts/buckets, hashed ids optional)
- plan: planner choices and caps
- gate: gating statistics
- cand: candidate selection statistics
- ev: evidence statistics
- claim: claim statistics

Y (labels):
- outcome: correctness bucket (exact/partial/incorrect/unknown)
- pass2_gain: did pass 2 improve outcome or claim coverage? (0/1)
- ev_labels: per-evidence item usefulness labels (pairwise or graded relevance)

Note on text:
- Training exports should prefer ids and numeric summaries.
- Raw text fields should be avoided by default.

Export formats
--------------
- CSV-like (debug):
 - stable header order, integer columns
 - only for small experiments
- Binary (preferred):
 - versioned schema
 - compact varint encoding where applicable
 - deterministic ordering of repeated fields

Integration points (planned)
----------------------------
- replay: extend AnswerTrace to include summary stats and chosen evidence ids
- retrieval: export gating/candidate stats for each pass
- scoring: export per-evidence feature vectors for reranking labels

Evaluation harness (planned)
----------------------------
A deliverable is a minimal offline evaluation plan:
- dataset splits (train/valid/test)
- metrics:
 - ranking: NDCG@K, MAP@K (integer approximations ok)
 - routing: accuracy and confusion matrix
 - calibration: Brier-like fixed-point score or bucketed reliability
- guardrails:
 - deterministic replays
 - no silent regressions
