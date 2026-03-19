# MarkovTraceV1

stage:

MarkovTraceV1 is a replayable, deterministic per-turn record of the bounded
surface/structure choice stream observed while rendering an answer.

It is intended as the primary input to offline Markov training, and is
intentionally compact: it stores only stable identifiers for template choices,
not free-form text.

## Fields

- version: u32
 - must be 1
- query_id: Hash32
 - blake3 hash of the user query bytes (same convention as other answer-path
 artifacts)
- tokens: Vec<MarkovTokenV1>
 - ordered stream of (kind, choice_id)

MarkovTokenV1 is defined in `src/markov_model.rs` and is:

- kind: MarkovChoiceKindV1
 - 1=Opener, 2=Transition, 3=Closer, 4=Other
- choice_id: Id64
 - stable identifier for the chosen template/variant

## Hard caps

- MARKOV_TRACE_V1_MAX_TOKENS = 2048

## Canonical encoding

Byte layout (little-endian integers):

- u32 version
- [u8; 32] query_id
- u16 token_count
- repeated token_count times:
 - u8 kind
 - u64 choice_id

Decoding rejects:

- wrong version
- token_count above the hard cap
- trailing bytes

## Notes

- Trace tokens are not sorted. The order is the observed sequence.
- The trace does not prescribe how the realizer chooses templates. It only
 records the stable choice identifiers.


## Token sources and namespaces

Trace tokens are intentionally stable identifiers (Id64) derived from ASCII
labels.

In early stages, the answer pipeline may emit structural placeholder tokens
before all realizer surface-template sites are wired.

Label namespaces (v1)

- plan_item:* (structural placeholders)
 - Example: plan_item:summary
- append:* (post-render append events)
 - Example: append:clarify_question
- preface:* (realizer opener surface templates)
 - Example: preface:neutral:0
- transition:* (wired transition surface templates)
 - Example: transition:details_heading:0
- closer:* (wired closer surface templates)
 - Example: closer:caveat_heading:0
- other:* (wired Other surface templates)
 - Example: other:clarifier_intro:0

Starting in (Option B), when a realizer surface-template site is
wired, the trace MUST record the actual template choice id used at that site.
For the preface opener, this means:

- If a preface line is emitted, record MarkovTokenV1(kind=Opener, choice_id=
 preface:<tone>:<variant>) as the first token in the stream.

For the Default/Concise details heading transition, this means:

- If the details heading is emitted, record MarkovTokenV1(kind=Transition,
 choice_id=transition:details_heading:<variant>) before the first Bullet
 placeholder token for that rendered group.

For the Default/Concise caveat heading closer, this means:

- If the caveat heading is emitted, record MarkovTokenV1(kind=Closer,
 choice_id=closer:caveat_heading:<variant>) before the first Caveat
 placeholder token for that rendered group.

For the clarifier intro Other site, this means:

- If a clarifying question is appended, record MarkovTokenV1(kind=Other,
 choice_id=other:clarifier_intro:<variant>) before append:clarify_question.

Until all selection sites are wired, v1 traces may contain both structural
placeholder tokens and surface-template tokens. Training treats them as the
observed token stream; corpus_hash ensures different trace sets remain
distinguishable.


## Replay step

The answer CLI emits this artifact via the ReplayLog step name:
- markov-trace-v1

This step is intended to be replayable and deterministic. It binds the
realized answer text hash (and the same guidance inputs used by answer-v1) to
a compact token stream that can be used for offline Markov training.
