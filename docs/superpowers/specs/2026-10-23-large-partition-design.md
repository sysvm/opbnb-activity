# Large partitions with bounded writes and fine-grained recovery

Date: 2026-10-23

## Goal and approval boundary

The user approved the direction: reduce permanent small-file counts with streaming large partitions, bounded-memory validation and recovery points independent of final file size. This document specifies the storage/recovery changes for review before implementation. The current project is `/Users/zhaoyu/corp/bnbchain/opbnb/opbnb-activity`, based on `f252ad1`. No new commit, branch, production endpoint access or data migration is authorized.

## Design decisions

Support integer `blocks_per_partition` from1 to100000. Keep the existing default500 for compatibility; document100000 as an opt-in for new runs, not a measured universal optimum. A final partition still contains exactly four Zstandard Parquet tables and `_partition.json` under the existing plain `data/YYYY-MM-DD/START-END/` layout. Existing legacy/plain partitions remain readable without rewriting their contents.

Add runtime setting `blocks_per_checkpoint`, default500, integer1..5000. Exclude this new runtime setting from structural identity so old config/manifest hashes remain unchanged. Changing final `blocks_per_partition` still requires a new run ID; changing runtime checkpoint frequency may resume an existing run because stored checkpoint ranges are self-describing. Old manifests omit the new field and load its default.

Keep final file size separate from in-memory batching and durable recovery. The coordinator buffers at most one normalized checkpoint batch, not the whole100000-block partition. Flush at the configured block count or an estimated64MiB serialized Arrow-data threshold, whichever comes first, only at a complete-block boundary. Avoid repeated conversion of the entire accumulated buffer merely to estimate size; estimate/count per incoming block and write bounded Arrow batches. This is an estimated payload bound, not a hard Python RSS cap; a single large block and bounded concurrent fetches are additional memory. Existing prefetch stays at most `2 * concurrency` block results.

## Why durable temporary fragments

An open Parquet file is not reliably append-resumable after a crash because its footer may be absent. Do not checkpoint an unfinished ParquetWriter and pretend its row groups are safely recoverable.

Instead, persist complete checkpoint fragments only for the currently active final partition in a hidden private staging area. Each fragment contains closed Parquet tables plus checksummed range metadata. Publish a fragment by writing/fsyncing files and metadata, atomically renaming its temporary directory and fsyncing its parent. Recovery uses only fully published, checksum-verified, consecutive fragments starting at the partition boundary. Do not change the existing completed-partition SQLite task schema or count these private fragments as completed final partitions.

For100000-block final partitions with500-block checkpoints and no earlier byte-limit flush, up to200 temporary fragments are needed for the active partition. This is not a strict file-count bound when large payloads trigger earlier flushes. Private fragments are not retained for every historical partition and are excluded from all report/quality discovery. No background merge service is introduced.

When the final range is fully collected, stream each fragment's tables through bounded record batches into four final ParquetWriter files. Emit row groups of useful bounded sizes rather than one row group per block. Close/fsync, hash and publish the final directory atomically, then mark the existing SQLite partition task complete. Only after the final files have been checksum-verified and their checkpoint recorded may this partition's private fragments be removed. An untrusted/corrupt final directory is not grounds to delete its only valid recovery fragments.

This design adds temporary writes and one bounded streaming read/rewrite of the active partition. It is not a zero-copy Parquet concatenation or an I/O-free speedup. Unlike an after-the-entire-run merge, it needs no second full-dataset copy, never waits for three months to finish and releases staging space after each committed partition. Benchmark the additional local I/O before claiming a throughput gain; the primary guaranteed change is fewer final files and finer recovery, not faster disks.

Small partitions that fit a single checkpoint may publish directly with the existing atomic commit semantics, avoiding needless staging/rewriting. Preserve the public `write_block`, `commit_partition`, `completed`, `recover`, `read_table` and `connection` behavior used by tests and analysis. New bounded/private helpers can isolate fragment lifecycle and streaming concatenation from Store.

## Recovery and failure contract

1. Verify/recover an already published final partition first and skip its RPC work as before.
2. Otherwise inspect only that partition's private fragments. Require matching identity, kind/schema, bounds, checksums and contiguous ranges, with no overlap or duplicate ranges. Reuse the maximal valid prefix. An invalid suffix must be quarantined or excluded before resuming so it cannot silently join later fragments; do not label incomplete data as complete.
3. Start RPC collection at the verified prefix's exclusive end, not the final partition's original start. Preserve the original full-window manifest and final task ranges. Fresh checkpoint batches may use a changed runtime block frequency without reinterpreting existing fragments.
4. If fragments cover the entire partition but finalization was interrupted, rebuild/publish the final files entirely from disk without refetching blocks.
5. If final publication succeeded but SQLite checkpoint recording failed, existing final recovery verifies and indexes it before any staging cleanup.
6. On worker/normalization/storage error, cancel queued RPC work and join running workers as currently implemented. Discard only the unfinished in-memory batch/unfinished temporary output. Retain fully published recovery fragments and previously completed final partitions. Restart revalidates disk before reuse.
7. Default SIGTERM may terminate immediately. Durability comes from completed atomic fragments/final directories, not a promise to flush on shutdown. The unfinished checkpoint batch plus bounded prefetched work may require refetch, excluding corrupt cached data; byte-triggered earlier checkpoints can reduce the unfinished batch. Requests already in flight cannot be recalled.

One writer process per run remains mandatory. SQLite and Store mutation stay on the coordinator. There is no external database, daemon, network cache or remote object store.

## Bounded quality validation

Current `iter_partitions` converts all four files of a final partition to Python lists. Production validation must stop using that whole-partition path before lifting the cap.

Use typed, bounded Parquet batches for physical/schema inspection, and the existing memory-limited, spill-capable DuckDB connection for cross-table/range checks. Do not construct a100000-block partition's transaction/receipt/log dictionaries in Python. Keep `read_table` as explicitly small-fixture convenience, not a production validation path.

Preserve all existing quality checks and refusal-on-failure: expected final range coverage, checksums and metadata consistency, block numbering/order/parent hashes and time window, per-block transaction counts, unique transaction/receipt hashes, exact receipt joins and indices, log joins/hash/range/duplicate keys, and cross-partition duplicates. Do not assume a missing receipt has a zero fee or silently skip an invalid chunk. Retain result keys, count meanings and `analysis/report` quality gate. Reject private fragments as final evidence.

DuckDB512MiB is its own budget, not a total-process RSS guarantee. Validation can spill and still be disk/CPU expensive. No claim that whole-run checks become constant-time or require no scratch disk.

## Progress and documentation

Keep `committed_blocks` for final published partitions. `processed_blocks` includes verified final partitions, reused durable fragment blocks and newly normalized blocks; disclose reused-prefix counts separately so recovered disk data is not counted as fresh RPC throughput. `new_blocks` and its ETA rate count only this invocation's newly fetched/normalized work. Add a durable checkpoint progress count and finalization stage start/end so a100000-block directory is not the first visible sign of progress.

The existing ETA remains an observational scan estimate, excluding future compaction/final validation/analysis; label those phases separately. Report how many RPC blocks were avoided on restart. Do not expose credentials, RPC bodies or URLs in logs.

Update config errors to name the invalid partition/checkpoint setting and its legal range, rather than reusing only `Invalid bounded RPC settings`. Preserve safe diagnostic redaction and other runtime settings. Document setup, directory lifecycle, temporary-space overhead, interrupt/resume semantics and same-run immutable final partition size. Existing user datasets are never automatically migrated, merged or renamed.

## Acceptance and measurement

- Test-first implementation with the existing222-test suite preserved; no node needed for correctness tests.
- A new run accepts100000 blocks/partition, rejects noninteger/bool/out-of-range settings, and produces four final table files plus one metadata file per range, with no private staging left after successful completion.
- Serial and concurrent collection with checkpoint boundaries produce the same complete normalized tables and financial/activity metrics as the existing implementation on synthetic fixtures. Empty tables and multi-day partitions remain valid.
- Fault injection before/after fragment publication, during final writing/rename and before/after SQLite completion proves correct maximal-prefix recovery, unchanged earlier final bytes and no missing/duplicated rows. Check RPC request counts to prove durable blocks are not re-fetched.
- Missing/corrupt/overlapping fragments never produce successful quality. Corrupt final rewrites retain safe backups as before. Recovery validates final files before deleting private fragments.
- Tests prevent production quality/finalization from calling full-file `.read().to_pylist()` or fixture `read_table`, exercise streaming batches and retain all previous corruption/duplicate/identity rejection cases.
- A reproducible offline benchmark compares500-block versus100000-block final partitions on the same generated synthetic chain. Record final directories/files, staging peak disk, collection/finalization/validation time and process peak RSS in isolated runs. Verify exact counts and checksums/quality before reporting measurements. Simulate interruption after durable checkpoints and record avoided RPC calls. Avoid fragile timing/RSS-ratio assertions in unit tests.
- Actually exercise at least100000 synthetic blocks for the large-partition acceptance run. Measure representative fixture-derived transactions/logs as well as empty blocks; do not call a six-block fixture proof of production memory capacity. A workload without production-size calldata/receipts is still not a16GB mainnet-scale guarantee.
- Final full tests, compile, offline CLI smoke, independent storage/recovery/validation review and guarded deployment. No commits/push without another explicit user request.

## Out of scope

No historical data compaction command, automatic migration, arbitrary unbounded partition size, row-group bytewise concatenation, append to crash-truncated Parquet files, concurrent writers, RPC provider changes, project identification, new business metrics or changes to report monetary definitions.
