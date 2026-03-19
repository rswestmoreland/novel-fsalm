Interfaces and supported surfaces
===================================

This document is an architectural reference for how Novel exposes the runtime.
For the current supported command surface, see `docs/CLI.md`.

Current intent
--------------
Expose the runtime in a way that is:
- deterministic and reproducible
- CPU-friendly and low overhead
- compatible with parallel and multi-machine execution

Principle
---------
The core runtime operates on canonical artifacts (PromptPack, JobReq, JobResp, ReplayLog)
identified by content hashes. Interfaces are thin wrappers that translate user input
into canonical artifacts and invoke the pipeline.

Interfaces
----------
1) CLI (primary for development and reproducibility)
 - Reads prompt text (stdin/args/file)
 - Builds a PromptPack artifact and stores it (content-addressed)
 - Runs the pipeline locally (initially stub stages)
 - Emits output text and a ReplayLog artifact
 - Provides a "replay" mode that re-runs using stored artifacts

2) TCP framed binary protocol (architectural distribution path)
 - Messages are length-delimited frames: u32(len) + canonical bytes
 - Payloads are canonical artifacts (PromptPack, JobEnvelope, JobResp, ReplayLog)
 - Enables a worker model across machines without redesigning the core

3) REST-style wrappers (non-core integration surface)
 - A convenience wrapper for integrations
 - Not part of the current supported runtime surface
 - Must map directly onto canonical artifacts to preserve determinism if added

Illustrative artifact-level CLI commands
----------------------------------------
- fsa_lm put <file> Store bytes as an artifact, print hash
- fsa_lm get <hash> Fetch artifact bytes
- fsa_lm prompt "<text>" Build PromptPack from a text prompt, store it
- fsa_lm run --prompt <hash|file> Run pipeline locally, store outputs and replay log
- fsa_lm replay --log <hash|file> Deterministic replay and hash verification

Notes
-----
- Determinism is defined on artifact bytes and hashes, not on timestamps or logs.
- Distributed execution relies on snapshot IDs and deterministic merge rules.

TCP Framed Protocol
-----------------------------
The current architecture also includes a minimal artifact exchange protocol:
- Put(bytes) -> returns Hash32
- Get(hash) -> returns bytes or not-found

These messages are sent as framed payloads (u32 LE length prefix).
The wire format can grow additional envelope types without changing the core
artifact model.
