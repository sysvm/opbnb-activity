# High-QPS bounded collection design

Date: 2026-10-23

## Goal and approval

The user approved controlled concurrent block/receipt reads with a shared global QPS budget, bounded prefetch, ordered single-writer persistence and unchanged resume semantics. This document makes those boundaries explicit before implementation. No endpoint or production throughput has been measured. No new Git commit or branch is authorized by this feature request.

## Existing bottlenecks

`collect.py` fetches one full block and its receipts before starting another block. `RpcClient._execute` holds one lock across rate waiting, HTTP I/O and retries. Raising QPS alone cannot overlap network latency. The block-receipts fast path does not use transaction-receipt batching.

## Chosen architecture

Use a bounded thread pool with one shared synchronous HTTP client and global limiter. Avoid an asyncio rewrite or concurrent checkpoint writers. Workers fetch a block and its matching receipts; the coordinator consumes results in increasing block-number order, normalizes and commits through the existing Store. Workers must never access SQLite or mutate the Store buffer.

Keep HTTP in-flight concurrency at or below configured `concurrency`. Prefetch at most `2 * concurrency` blocks, counting running tasks and completed-but-unconsumed results together, not just active workers. A slow early block must not permit unbounded later submissions. The existing partition buffer remains additional bounded memory. This is a block-count bound, not a strict byte/RSS guarantee for exceptionally large blocks.

Process one partition at a time, skipping verified/recoverable partitions before fetching any of their blocks. This keeps later partition work from being stranded behind a failed partition and preserves ordered atomic writes. It may briefly idle workers at partition boundaries; do not introduce parallel partition persistence to eliminate this safe boundary.

## Configuration and compatibility

- Add integer `concurrency`, default 1 for backward-compatible behavior, bounded to 1..128. Reject booleans, fractional values and invalid limits. A 16GB machine should begin around32 workers, not assume128 is safe.
- Continue `qps` as an aggregate logical JSON-RPC-element rate, not HTTP requests/second or a per-worker allowance. Require finite positive values; no automatic increase to an unknown provider limit.
- `batch_size` remains1..100, primarily for the per-transaction receipt fallback. `blocks_per_partition`, schemas, table names and partition layout stay unchanged.
- Exclude `concurrency` from Config.identity so existing run manifests/checkpoints remain usable. Missing concurrency in an old manifest loads as1. Changing runtime QPS/concurrency must not invalidate existing completed data.
- Wire the current config into the collection client and scheduler when resuming; do not silently use a stale runtime concurrency from a manifest. Manifest structural identity and historical settings remain auditable.
- Reuse the same CLI commands and nohup workflow. One collector process per run remains required. No migration, database service, new deployment process or automatic Git commit.

## Shared RPC scheduling

Split ID allocation and rate scheduling from HTTP I/O; never hold the global ID/limiter lock during a network call or retry sleep. Allocate globally unique IDs under a short lock. HTTP connection-pool capacity matches concurrency; the client remains open until all workers have stopped.

Use one monotonic-clock pacing schedule for all attempts. Reserve capacity by the number of JSON-RPC elements in an outgoing request; retries reserve again only for pending failed elements. There is no accumulated idle-time credit or arbitrary full-QPS burst. A batch inherently sends its elements together; its budget must be charged before dispatch, not advertised as strictly burst-free. Concurrent starts must not each receive an independent QPS budget.

HTTP429 publishes a shared cooldown before either retries or new requests may dispatch. Honor bounded Retry-After and existing retry limits. Already in-flight requests cannot be recalled. Requests waiting for permits must recheck a subsequently extended cooldown. Retain bounded transport/5xx/transient retry handling, partial-batch retry behavior, ID validation, redirection/proxy restrictions and credential-safe diagnostics. No uncontrolled retry storm or automatic unlimited concurrency escalation.

Keep injectable clock/sleep and HTTP transport boundaries so deterministic limiter tests do not require a real endpoint. Runtime statistics must be thread-safe and must not store URLs, params, bodies or raw exceptions.

## Failure, fallback and shutdown

Receipt fallback remains: whole-block receipts when supported, otherwise batch/individual transaction receipts. Coordinate the unsupported capability across workers; in-flight workers may already be probing it, but subsequent work should not repeatedly request a known unsupported method. Missing, duplicate or mismatched receipts still prevent completion.

On fatal worker, normalization or storage error, stop submitting work, cancel queued futures and discard uncommitted results. Running HTTP attempts are bounded by timeout/retry policy; shut workers down before closing the shared client. Never mark an incomplete partition complete. On restart it is safely fetched again; previously completed partitions remain checksum-verified and skipped. Default SIGTERM may terminate immediately as before; atomic partition recovery, not graceful full-partition flushing, is the durability guarantee.

## Observability

Preserve existing processed/committed/new-block progress and ETA definitions. Add concurrency, scheduled/buffered blocks, current/peak HTTP in-flight, logical attempted elements, HTTP attempts, retries,429 counts and measured RPC throughput. Progress reports reflect coordinator state; no per-transaction logging or raw node data. There is no claim of a timer heartbeat while an ordered result is blocked. Completion logs separate collection from later full integrity validation/analysis.

## Acceptance tests

1. Delayed first block and out-of-order worker completion still produce identical ordered Parquet contents, quality counts and metrics to serial collection.
2. Controlled transport barriers demonstrate concurrent HTTP requests and unique IDs, while observed in-flight never exceeds concurrency.
3. Deterministic clock tests prove one shared pacing budget for threads, batches and retries, including waits interrupted by extended cooldown.
4. HTTP429 Retry-After blocks new dispatches as well as retries; oversized wait budget stops safely, and all credential-redaction tests still pass.
5. Slow-first-block tests prove submitted plus buffered tasks never exceed the configured prefetch bound.
6. Missing receipts and worker/storage failures leave the current partition uncompleted; reopening and resuming yields complete nonduplicated data, preserving earlier committed files.
7. Old manifests open without concurrency and keep their original identity. Runtime-only QPS/concurrency changes can resume the same run. Invalid settings fail before workers start.
8. Whole-block-receipt unsupported fallback works under concurrency without repeated requests on later blocks; batch ID matching and partial retries stay covered.
9. A reproducible offline benchmark simulates network latency, reports serial/concurrent elapsed time and actual request counts, and first verifies equal output. It is not a mainnet speed or capacity promise.
10. Full existing suite, new concurrency tests, CLI smoke and diff checks pass. No production endpoint, original data or private config is required or modified.

## Server rollout and tuning

After offline validation, update the existing installation, stop only the confirmed collector PID, wait for exit, and restart the same run with unchanged window/partition size. Start with qps500, concurrency32, batch_size20, blocks_per_partition500 as an explicit trial, not a provider capacity claim. Increase QPS/concurrency in measured steps only if actual throughput improves without rising429/timeouts/RSS. Report scan rate and ETA from observations; do not promise a fixed three-month completion time. Retain free disk for Parquet, backups and DuckDB spill.

## Scope exclusions

No changes to project labels, business classification, token valuation, reporting metrics, price feeds, all-chain traces, pruning, reorg rollback, multi-process writers or multi-endpoint routing. Full dataset validation remains necessary and may itself be expensive; faster downloading is not a guarantee of faster final analysis.
