Caches and Storage Tiers (v1)
============================

Scope
-----
This document defines the v1 contracts for in-process caches and the hot/warm/cold
storage tiers used by Novel (Novel FSA-LM).

The goal is bounded memory and improved throughput on a consumer laptop while
preserving strict determinism:
- identical artifacts + identical config => identical outputs
- caches may change performance, never the result

This document is about the runtime and artifact loading paths. It is not a model
training document.

Definitions
-----------
Cold storage:
- Immutable artifacts stored in the ArtifactStore (filesystem).

Warm storage:
- Read-through caches of decoded artifacts in process memory.
- Bounded and evicting.
- Intended to reduce repeated artifact reads and decode cost.

Hot storage:
- Per-operation scratch state.
- Lives only for the duration of a single call (query, build-evidence, etc).
- Freed at the end of the call.

Determinism contracts
---------------------
A cache MUST NOT change the logical output of the system. It MAY change cost.

A1. No cache-driven ordering
- Retrieval/scoring/tie-break rules must be pure functions of canonical inputs.
- Cache hits may reduce work but must not affect output ordering.

A2. No time-based behavior
- No wall-clock TTL, jitter, background refresh, or randomized sampling.

A3. Deterministic eviction
- Given the same access sequence, eviction decisions must be identical.

A4. Canonical keys only
- Cache keys are stable content identifiers (Hash32) derived from canonical bytes.

A5. Version discipline
- Cache entries are per-format-version. A v2 codec must not decode into a v1 key
 without changing the key derivation.

Storage tier policy (v1)
------------------------
Cold is the source of truth. Warm and hot are performance layers only.

Cold:
- ArtifactStore blobs (content-addressed by Hash32).

Warm:
- Decoded objects keyed by artifact hash.
- MVP targets:
 - FrameSegmentV1
 - IndexSegmentV1
 - IndexSnapshotV1 (small; may be pinned or given a separate small cap)

Hot:
- Query-local scratch:
 - tokenization products (term ids, term freqs, meta codes)
 - row sketch scratch buffers

Cache policy choice (v1)
------------------------
Warm caches use a deterministic eviction policy that handles "scan then reuse".

Recommendation: 2Q (two-queue) cache
- A1: probationary queue for items seen once recently
- Am: main queue for items seen at least twice (protected)

2Q operations:
- Miss: insert into A1 (front)
- Hit in A1: promote to Am (front)
- Hit in Am: refresh within Am (front)
- Evict: from A1 tail first; if empty, evict from Am tail

Deterministic tie-break:
- Eviction always removes the oldest element at the tail (FIFO within each queue).
- Promotions and refreshes always push to the front.

Alternative (allowed later): SLRU
- probationary + protected queues with similar rules.
- is preferred for the first implementation because it is simple and scan-friendly.

Capacity and accounting (v1)
----------------------------
Caches are bounded by bytes, optionally also by items.

Each entry carries a deterministic cost estimate:
- cost_bytes = encoded_len_bytes (MVP)
 Rationale:
 - encoded_len is stable and cheap to measure
 - decoded overhead is harder to estimate without intrusive instrumentation
 - this can be refined later once metrics exist

Cache configuration (v1)
---------------------------------
CacheCfgV1:
- max_bytes_total: u64
- max_items_total: u32 (optional, 0 means no item cap)
- a1_ratio: u8 (optional; default 50 means A1 gets 50% of byte budget)

 implements CacheCfgV1 and Cache2Q in src/cache.rs.

Metrics (minimal)
-----------------
Caches should track integer counters only:
- lookups
- hits
- misses
- inserts
- evicts
- bytes_live
- bytes_evicted_total

Do not add timing metrics in v1. Timing is inherently noisy and can lead to
incorrect conclusions and non-reproducible benches.

Integration guidance (MVP)
--------------------------
Read-through cache integration points:
- FrameStore: get_frame_segment_v1_cached(store, cache, hash)
- IndexSegment load: get_index_segment_v1_cached(store, cache, hash)
- IndexSnapshot load: get_index_snapshot_v1_cached(store, cache, hash)

Cache state must be passed explicitly by the caller. Avoid global singletons.

Non-goals for v1
----------------
- TTL or wall clock expiration
- background refresh threads
- prefetch heuristics
- mmap/zero-copy decode
- caching of query results (results caching can be explored later, but requires
 careful keying and explicit invalidation discipline)

See also
--------
- docs/DETERMINISM.md
- docs/ARTIFACTS.md
- docs/RETRIEVAL_PIPELINE.md

## CLI cache stats

The `fsa_lm` CLI can print cache statistics after running certain commands.
This is intended for debugging and cache tuning, not for production telemetry.

Commands supported:
- `query-index`
- `build-evidence`

Enable with the `--cache-stats` flag. Cache stats are printed to stderr as stable
key=value pairs on lines beginning with `cache_stats`.

### Tuning via environment variables

The cache system is configured at process startup via environment variables.
If an environment variable is present but cannot be parsed, it is ignored and
a default is used.

Global settings:
- `FSA_LM_CACHE_BYTES` (u64): default max bytes for a cache (default: 67108864).
- `FSA_LM_CACHE_A1_RATIO` (u8 0..100): percent of capacity reserved for A1 (default: 50).
- `FSA_LM_CACHE_MAX_ITEMS` (u32): max number of items per cache (default: 0 for unbounded).

Per-cache overrides (take precedence over globals):
- `FSA_LM_CACHE_BYTES_SNAPSHOT`, `FSA_LM_CACHE_MAX_ITEMS_SNAPSHOT`, `FSA_LM_CACHE_A1_RATIO_SNAPSHOT`
- `FSA_LM_CACHE_BYTES_INDEX`, `FSA_LM_CACHE_MAX_ITEMS_INDEX`, `FSA_LM_CACHE_A1_RATIO_INDEX`
- `FSA_LM_CACHE_BYTES_FRAME`, `FSA_LM_CACHE_MAX_ITEMS_FRAME`, `FSA_LM_CACHE_A1_RATIO_FRAME`

Practical tuning notes:
- If you see many `rejects_oversize`, increase the corresponding `*_BYTES_*` value
 or reduce segment sizes.
- If you see frequent evictions with very low hit rates, either increase bytes
 or reduce `A1_RATIO` to protect frequently reused items in Am.
- If artifacts are small and numerous, setting `MAX_ITEMS_*` can prevent a cache
 from holding too many low-value entries.
