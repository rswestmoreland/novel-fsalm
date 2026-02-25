SegmentSig v1
==============

Goal
----
SegmentSig is a small, deterministic summary used for query-time gating.
The purpose is to skip loading and decoding segments that cannot match a query.

The gating rule is conservative:
- If the signature says "no", the segment must not contain the query term(s).
- If the signature says "maybe", the segment may contain the term(s) and must be checked.

v1 content
----------
v1 contains:
- A Bloom filter over TermId values.
- An optional "sketch" payload reserved for future use (v1 typically uses empty).

Bloom filter details
--------------------
- Bitset: fixed-length byte array (bloom_bits).
- Bit numbering: bit 0 is the low bit of byte 0.
- Probes: k independent probe indices per inserted term.
- No false negatives: any inserted term must always test true.
- False positives: possible (tunable via bloom_bits length and k).

Hashing
-------
Probe indices are derived from the TermId u64 payload using a deterministic 64-bit
mixer (SplitMix64-like) and double hashing:

- h1 = mix64(term ^ C1)
- h2 = mix64(term ^ C2) | 1
- probe_i = h1 + i * h2

Indices are mapped into the bitset by:
- If m_bits is a power of two, use `probe_i & (m_bits - 1)`.
- Otherwise use `probe_i % m_bits`.

Canonical byte layout
---------------------
All integers are little-endian.

- MAGIC[8] = "FSALMSIG"
- version(u16) = 1
- reserved(u16) = 0
- index_seg_hash[32] (Hash32 of the index artifact bytes this signature describes; the artifact is typically IndexSegmentV1 or IndexPackV1)
- bloom_k(u8)
- pad[3] = 0
- bloom_len_bytes(u32)
- bloom_bits[bloom_len_bytes]
- sketch_len_bytes(u32)
- sketch[sketch_len_bytes]

Decode rules
------------
- Reject wrong MAGIC or unsupported version.
- Reject bloom_k == 0 (and bloom_k > 32).
- Reject bloom_len_bytes == 0.
- Reject trailing bytes.
- Defensive caps prevent pathological allocations on corrupt inputs.

Implementation status
---------------------
SegmentSigV1 is used as a query-time gating artifact.

Status by stage:
- DONE: SegmentSigV1 format + codec.
- DONE: SegmentSig store helpers plus IndexSigMapV1 sidecar mapping.
- DONE: build-index and compact-index emit SegmentSigV1 artifacts and an
 IndexSigMapV1 for each produced snapshot.
- DONE: query-index and build-evidence can consult SegmentSigV1 first and
 deterministically skip loading index artifacts that cannot match the query,
 when the caller provides an IndexSigMapV1 hash (CLI flag: --sig-map).
