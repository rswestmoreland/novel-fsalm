# Golden Pack Conversation v1

Goal
----

The conversation golden pack is a deterministic workload intended to
regression-test the end-to-end conversational answering loop.

It bundles two existing packs:

- `golden-pack` (scale-demo end-to-end pack)
- `golden-pack-turn-pairs` (two-turn conversational pack)

The output is a single content-addressed report artifact that embeds both
sub-reports and validates that their hashes match the embedded bytes.

Command
-------

Run:

- `fsa_lm golden-pack-conversation --root <dir> [--expect <hash32hex>] [--out-file <path>]`

It prints one line beginning with:

- `golden_pack_conversation_report_v1...`

Locking in CI
-------------

To lock the report hash:

- Pass `--expect <hash32hex>`, or
- Set `FSA_LM_GOLDEN_PACK_CONVERSATION_V1_REPORT_HEX=<hash32hex>`

If the computed report hash differs, the command exits non-zero.

Artifacts
---------

The conversation pack stores:

- One GoldenPackConversationReportV1 artifact (the printed report hash)
- One GoldenPackReportV1 artifact (scale-demo pack)
- One GoldenPackTurnPairsReportV1 artifact (turn-pairs pack)

The two embedded sub-reports are canonical and validated during decode.
