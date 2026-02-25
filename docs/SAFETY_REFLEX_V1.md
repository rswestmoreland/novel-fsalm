Safety Reflex V1 (Asimov-inspired)
=================================

Purpose
-------
Novel is evidence-first and deterministic. Safety is handled by a rules-first
reflex stage that runs after retrieval planning and before realization.

This stage is inspired by Asimov's Three Laws as a set of software principles:
- Prevent harm (highest priority).
- Obey the user when safe.
- Protect system integrity and availability when it does not conflict with the
 higher priorities.

This document defines the v1 contract. It is a design spec, not a legal policy.

Stage position
--------------
Suggested pipeline placement:
- PromptPack -> retrieval planning -> EvidenceBundle selection -> SafetyReflexV1
 -> realization (draft/rewrite) -> output

The safety decision should be stored as an artifact so runs are auditable and
reproducible.

Inputs
------
- PromptPack (user messages and system/operator constraints).
- Proposed operation (what the system is about to do):
 - query intent classification (rules-first)
 - planned response mode (answer, summarize, code, explain, etc.)
- EvidenceBundleV1 (optional): selected evidence items for grounding.

Outputs
-------
SafetyDecisionV1 (artifact, planned type)
- decision: Allow | AllowWithConstraints | Refuse
- reasons: stable list of reason codes (ASCII)
- constraints: stable list of constraints to apply to realization
- policy_version: "safety_reflex_v1"
- input_hashes: prompt_hash, evidence_hash (if present)

Determinism rules
-----------------
- Rule evaluation order is fixed and documented.
- Pattern matching uses explicit ASCII patterns and tokenization.
- When multiple rules trigger, the highest priority decision wins:
 Refuse > AllowWithConstraints > Allow.
- Reason codes are emitted in a stable sorted order.

Decision categories (v1)
------------------------
These categories are intended to be conservative. V1 is rules-first.

Refuse categories
-----------------
- PHYSICAL_HARM: instructions that materially enable violence or physical harm.
- SELF_HARM: encouragement or instructions for self-harm.
- ILLEGAL_ACCESS: instructions for unauthorized access, malware creation,
 credential theft, or evasion.

Allow-with-constraints categories
--------------------------------
- PRIVACY: requests that could enable doxxing, stalking, or targeting.
 Constraint: remove personal identifiers and refuse targeted details.
- HARASSMENT_HATE: hateful or targeted harassment content.
 Constraint: refuse hateful framing; offer de-escalation or neutral info.
- FRAUD: requests enabling fraud or deception.
 Constraint: refuse enabling steps; allow high-level prevention guidance.
- HIGH_STAKES: medical/legal/financial advice.
 Constraint: provide general info; recommend professional guidance;
 be explicit about uncertainty.

Allow category
--------------
- Everything else, including defensive security guidance, education, and
 evidence-grounded summarization.

Constraints (examples)
----------------------
Constraints are instructions to downstream realization components.
Examples:
- NO_INSTRUCTIONS: do not provide step-by-step instructions for wrongdoing.
- GENERALIZE: answer at a high level; avoid actionable details.
- NO_TARGETING: avoid identifying a person or directing harassment.
- SAFETY_TONE: enforce calm, non-inciting language.
- RECOMMEND_HELP: include crisis/help resources in self-harm contexts.

Notes
-----
- SafetyReflexV1 is separate from PragmaticsFrameV1. Pragmatics shapes style.
 SafetyReflex enforces allowed actions.
- SafetyReflexV1 is designed to be replaced or supplemented later by
 verifiers/guards, but the v1 rules must remain deterministic.
