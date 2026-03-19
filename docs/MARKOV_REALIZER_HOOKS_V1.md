# Markov Realizer Hooks V1

Purpose

Markov guidance is advisory-only and MUST NOT introduce new claims. It may only
select among pre-defined surface-form templates that are already allowed by the
realizer.

This document describes the currently wired realizer selection hooks.

Selection sites

The Markov training contract defines four choice kinds:

- Opener
- Transition
- Closer
- Other

Currently, the Opener site, one Transition site, one Closer site, and one Other site are wired.

Opener (preface line)

When RealizerDirectivesV1 is present and max_preface_sentences > 0, the realizer
may emit a single preface line before the main content.

This preface line has a small fixed set of tone-specific conversational templates. MarkovHintsV1
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

Trace emission


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

Transition (Default/Concise details heading)

When Default or Concise style emits the Bullet/details group heading, the
realizer may select between a small fixed set of approved transition labels.

This heading uses deterministic Id64 values derived with:

- salt bytes: "markov_choice_v1"
- label bytes: "transition:details_heading:<variant>"

Where <variant> is 0 (default) or 1 (alternate).

Example labels:

- transition:details_heading:0
- transition:details_heading:1

If MarkovHintsV1 is absent, invalid, or does not match an approved transition
id, the realizer falls back to variant 0.

Notes

- This hook is intentionally narrow: it is a surface-form selection point only.

Closer (Default/Concise caveat heading)

When Default or Concise style emits the Caveat group heading, the realizer may
select between a small fixed set of approved closer labels.

This heading uses deterministic Id64 values derived with:

- salt bytes: "markov_choice_v1"
- label bytes: "closer:caveat_heading:<variant>"

Where <variant> is 0 (default) or 1 (alternate).

Example labels:

- closer:caveat_heading:0
- closer:caveat_heading:1

If MarkovHintsV1 is absent, invalid, or does not match an approved closer
id, the realizer falls back to variant 0.

Notes

- This hook is intentionally narrow: it is a surface-form selection point only.
- Future stages may add additional Transition, Closer, and Other hooks using the same scheme.


Other (clarifier intro)

When the quality gate appends a clarifying question, it may select between a
small fixed set of approved clarifier-intro labels. The actual clarifying
question text remains forecast-driven; this hook changes only the intro line
that precedes the question label.

This intro uses deterministic Id64 values derived with:

- salt bytes: "markov_choice_v1"
- label bytes: "other:clarifier_intro:<variant>"

Where <variant> is 0 (default) or 1 (alternate).

Example labels:

- other:clarifier_intro:0
- other:clarifier_intro:1

If MarkovHintsV1 is absent, invalid, or does not match an approved Other id,
the quality gate falls back to variant 0.
