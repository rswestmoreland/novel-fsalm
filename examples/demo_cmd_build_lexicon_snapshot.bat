@echo off
REM demo: build LexiconSnapshot from LexiconSegment hashes.
REM
REM This command expects LexiconSegmentV1 artifacts to already exist in the store.
REM (Wiktionary ingest) will produce those segments.
REM
REM Usage pattern:
REM - Put or ingest LexiconSegmentV1 bytes to obtain their artifact hashes.
REM - Call build-lexicon-snapshot with one or more --segment values.

set ROOT=./fsa_lm_store

REM Replace these with real LexiconSegment hashes.
set SEG1=0000000000000000000000000000000000000000000000000000000000000000
set SEG2=1111111111111111111111111111111111111111111111111111111111111111

fsa_lm build-lexicon-snapshot --root %ROOT% --segment %SEG1% --segment %SEG2%