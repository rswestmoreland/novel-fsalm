# Artifact Sync V1

Artifact Sync V1 provides deterministic, verified replication of content-addressed
artifacts over TCP.

See also:
- docs/OPERATOR_WORKFLOW.md (operator workflow: shard -> reduce -> replicate -> query/answer)
- docs/SHARDED_REDUCE_V1.md (reduce-index produces ReduceManifestV1)
- docs/SHARDED_INGEST_V1.md (sharded ingest prerequisites)
- docs/CLI.md (serve-sync, sync-reduce, sync-reduce-batch, run-phase6)

Scope

- Replicate artifacts between two artifact store roots.
- Manifest-driven fetch for outputs, primarily ReduceManifestV1.
- Deterministic ordering and verification (hash must match bytes).
- No extra crates, no unsafe.

Non-goals (for V1)

- Parallel transfer.
- Delta/rsync style patching.
- Authentication or encryption.
- Cross-store (multi-root) addressing.

Recommended workflow

After reduce-index, the primary root contains all merged index artifacts
and all referenced frame/index/sig artifacts copied into that root.

To replicate to another machine:

1) On the source machine:

 fsa_lm serve-sync --root <primary_root> --addr 0.0.0.0:9091

2) On the destination machine:

 fsa_lm sync-reduce --root <dest_root> --addr <src_ip:9091> --reduce-manifest <hash32hex>

For syncing multiple reduced outputs in one session:

 fsa_lm sync-reduce-batch --root <dest_root> --addr <src_ip:9091> --reduce-manifests <file>

sync-reduce prints a single stats line:

 needed_total=<n> already_present=<n> fetched=<n> bytes_fetched=<n>

needed_total counts unique hashes required by the ReduceManifestV1 closure.
already_present counts hashes already available locally (not fetched).
fetched counts hashes fetched from the remote.
bytes_fetched is the sum of bytes written for fetched artifacts.

sync-reduce-batch prints:

 needed_total=<n> already_present=<n> fetched=<n> bytes_fetched=<n> manifests=<n>
 manifest=<hash32hex> needed_total=<n>
...

The first line reports global stats for the union closure.
Each following line reports the per-manifest closure size (input order).


The destination root will contain:

- ReduceManifestV1
- merged IndexSnapshotV1 + IndexSigMapV1
- all referenced FrameSegmentV1, IndexSegmentV1, SegmentSigV1 artifacts

Example scripts

See the copy/paste-friendly scripts under examples/:

- examples/demo_cmd_sync_reduce.bat
- examples/demo_cmd_sync_reduce.sh

Determinism and verification

- The destination fetch list is derived deterministically from ReduceManifestV1
 by loading the merged snapshot and sig-map, collecting all referenced hashes,
 sorting by hex hash, and fetching missing artifacts in that order.
- Each transferred artifact is verified:
 - total length must match the announced length
 - BLAKE3 hash of received bytes must equal the requested hash
- If an artifact already exists locally, it is not rewritten.

TCP framing

All protocol messages are carried inside length-delimited frames:

- u32 little-endian payload length
- payload bytes

Max frame sizes are enforced for safety.

Protocol messages

All payloads begin with a u8 kind.

Client -> Server

- HELLO (kind=1)
 - u32 version (must be 1)
 - u32 max_chunk_bytes (client preference)
 - u32 max_artifact_bytes (client preference)

- GET (kind=2)
 - 32 bytes hash

Server -> Client

- HELLO_ACK (kind=3)
 - u32 version (1)
 - u32 max_chunk_bytes (server cap)
 - u32 max_artifact_bytes (server cap)

- GET_BEGIN (kind=4)
 - u8 found (0/1)
 - if found=1:
 - u32 total_len

- GET_CHUNK (kind=5)
 - u32 chunk_len
 - bytes

- GET_END (kind=6)

- ERR (kind=7)
 - u16 ascii_len
 - ascii bytes message

Sizing defaults

- Default request frame cap: 64 KiB
- Default chunk size: 1 MiB
- Default max artifact bytes: 512 MiB

If the server rejects a request or an artifact exceeds limits, it returns ERR.

Timeouts and failure modes

- Both serve-sync and sync-reduce support a read/write timeout:
 - serve-sync: --rw_timeout_ms <n>
 - sync-reduce: --rw_timeout_ms <n>
 - sync-reduce-batch: --rw_timeout_ms <n>
- Default is 30000 ms. Set to 0 to disable.
- If the connection drops mid-transfer, the client returns a deterministic
 disconnected error and does not publish a partial artifact.
- If a read or write operation times out, the client returns a deterministic
 timeout error.
