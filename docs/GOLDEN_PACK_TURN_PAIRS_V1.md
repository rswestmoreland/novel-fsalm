# Golden Pack Turn Pairs v1

Goal
----

The turn-pairs golden pack is a small deterministic workload intended to
regression-test conversational "two turn" behavior in the answering loop.

This pack is designed to cover:

- Realizer directives in non-debug style (tone=Supportive, style=Default)
- Markov opener preface template selection (variant 0 vs variant 1)
- Markov trace emission for surface-template choice ids

Command
-------

The v1 turn-pairs pack is executed via:

- `fsa_lm golden-pack-turn-pairs --root <dir> [--expect <hash32hex>] [--out-file <path>]`

It prints a single line beginning with:

- `golden_pack_turn_pairs_report_v1...`

Locking in CI
-------------

To lock the report hash:

- Pass `--expect <hash32hex>`, or
- Set the environment variable `FSA_LM_GOLDEN_PACK_TURN_PAIRS_V1_REPORT_HEX=<hash32hex>`

If the computed report hash differs, the command exits non-zero.

Workload parameters
-------------------

The current v1 "tiny" workload is:

- seed: 9
- docs: 16
- queries: 2
- min_doc_tokens: 20
- max_doc_tokens: 40
- vocab: 256
- query_tokens: 6
- tie_pair: 0

Turn behavior
-------------

The pack executes two turns:

- Turn 1: Supportive preface with default template (variant 0)
- Turn 2: Supportive preface with Markov-selected template (variant 1)

A tiny MarkovModelV1 is stored in the artifact store and used only for the
second turn. This keeps coverage focused and deterministic.

Artifacts
---------

The command stores:

- Two answer text artifacts (turns 1 and 2)
- Two MarkovTraceV1 artifacts (turns 1 and 2)
- One MarkovHintsV1 artifact (turn 2 only)
- One GoldenPackTurnPairsReportV1 artifact (the printed report hash)
