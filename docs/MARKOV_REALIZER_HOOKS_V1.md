# Markov Realizer Hooks V1

Purpose

Markov guidance is advisory-only and MUST NOT introduce new claims. It may only
select among pre-defined surface-form templates that are already allowed by the
realizer.

This document describes the first "hook" point added in 

Selection sites

The Markov training contract defines three choice kinds:

- Opener
- Transition
- Closer

Currently, only the Opener site is wired.

Opener (preface line)

When RealizerDirectivesV1 is present and max_preface_sentences > 0, the realizer
may emit a single preface line before the main content.

This preface line has a small fixed set of tone-specific templates. MarkovHintsV1
can select among them by providing a MarkovChoiceV1 with:

- kind = Opener
- choice_id = template choice id

If MarkovHintsV1 is absent, invalid, or does not match any known template id,
the realizer falls back to the default (legacy) template for that tone.

Template ids

Template ids are deterministic Id64 values derived with:

- salt bytes: "markov_choice_v1"
- label bytes: "preface:<tone>:<variant>"

Where:

- <tone> is one of: supportive, neutral, direct, cautious
- <variant> is 0 (default) or 1 (alternate)

Example labels:

- preface:supportive:0
- preface:supportive:1

 (Option B): Trace emission

When a realizer selection site is wired, the answer pipeline should record the
actual surface-template choice id in MarkovTraceV1. This improves the training
signal without storing free-form text.

For the Opener preface line:

The pipeline should use the realizer-reported choice id (surface events) rather than re-parsing
the rendered output text.

- If a preface line is emitted, record MarkovTokenV1(kind=Opener, choice_id=
 preface:<tone>:<variant>) as the first token in the stream.
- If no preface line is emitted (for example max_preface_sentences == 0), do
 not emit a preface token.

Structural placeholder tokens (for example plan_item:*) may remain in the trace
until additional selection sites are wired.

Notes

- This hook is intentionally narrow: it is a surface-form selection point only.
- Future stages may add Transition and Closer hooks using the same scheme.
