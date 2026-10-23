# High-QPS Collection Implementation Plan

> **For agentic workers:** Use subagent-driven-development with TDD and task reviews. No commits or branches are authorized for this feature. Work in the staging copy only; the parent hash-guards deployment to the requested project.

**Goal:** Overlap RPC latency with bounded threads while retaining one shared rate/cooldown budget and ordered resumable persistence.

**Architecture:** A shared synchronous HTTP client supports concurrent requests under a brief ID lock, HTTP semaphore and monotonic limiter. Collection uses a bounded ordered block-future window inside one partition at a time, and only the coordinator normalizes/writes. Runtime concurrency is excluded from structural identity.

**Tech Stack:** Python3.11+ stdlib concurrent.futures/threading, existing httpx, PyArrow, SQLite, pytest. No new dependencies.

## Global Constraints

- The approved spec is docs/superpowers/specs/2026-10-23-high-qps-collection-design.md. Preserve schemas, identity for all preexisting configs, partition size/path, checksum/recovery and single-writer semantics. No migrations, endpoint access, credentials, private config or real data changes.
- `concurrency` defaults1, bounded integer1..128 (bool rejected), excluded from Config.identity; qps finite positive. Prefetch running plus completed-unconsumed blocks <=2*concurrency, one partition at a time.
- Shared QPS counts logical elements on every attempt, including partial retries; no idle credit or per-thread rate. Batch elements may arrive together, budget charged first. HTTP429 imposes a rechecked shared bounded cooldown. HTTP in-flight<=concurrency, safe shutdown before client close. No network I/O or sleeps under the ID lock.
- Keep default serial compatibility and stdout JSON, safe logs; no inline production-code comments or one-letter variables. Use apply_patch for edits. No unrelated cleanup.
- Runtime /private/tmp/opbnb-activity-venv/bin/python. Test prefix PYTHONPATH=src MPLCONFIGDIR=/private/tmp/opbnb-mpl XDG_CACHE_HOME=/private/tmp/opbnb-xdg-cache. Baseline168 tests. Work from staging, not canonical or protocol repos.

## Task1: Concurrent RPC client and shared scheduling

**Files:** src/opbnb_activity/rpc.py; optional new focused module src/opbnb_activity/rate_limit.py; tests/test_rpc_concurrency.py; existing RPC tests only if injection setup must adapt for an explicit deterministic clock.

**Interface:** RpcClient(..., concurrency=1, clock=time.monotonic) keeps existing call/batch/probe_batch/close and sleep/transport injection. `snapshot()` returns a safe dict with rpc_elements, http_attempts, retries, http_429, in_flight, peak_in_flight, rpc_elements_per_second. Stats are aggregate per-client since creation. Existing fakes need not implement snapshot. No endpoint/body params in snapshots.

- [ ] RED tests before production implementation: concurrent MockTransport handlers overlap using barriers (bounded join/deadline); globally unique IDs; capped HTTP in-flight; shared time pacing with an advancing clock; element rather than HTTP counting for mixed batches and partial retries; zero idle-credit; new requests paused by429/Retry-After; waiters recheck an extended cooldown; bad/oversized retry-after remains bounded; invalid concurrency/nonfinite QPS rejected. Assert relevant numeric results, not only absence of exception. Do not write flaky fixed speed-ratio unit tests.
- [ ] Implement independent brief ID lock, configured connection pool and request concurrency bound. Retain HTTP envelope/ID validation, transient-only partial retries, secret redaction, redirects/proxy settings and bounded Retry-After. Inject clock/sleep consistently without busy looping when older no-op sleepers are used by synthetic tests. Never sleep with the ID lock held.
- [ ] Implement shared limiter with explicit monotonic pacing and cooldown extension, plus thread-safe snapshot counters. `rpc_elements` counts elements of actual HTTP attempts (including attempts yielding errors), not unused reservations; reserve retry budget again and do not count successful items twice in partial retries. `retries` counts additional HTTP attempts, not final errors.429 count includes terminal429.
- [ ] Run focused RED/GREEN and existing RPC/progress/enrichment tests. Write task-1-report.md with commands/results, interfaces, any concerns. No source changes outside owned files. Task2 can proceed on the above contract without requiring this task's source complete.

## Task2: Bounded ordered collector, config/CLI, rollout and benchmark

**Files:** src/opbnb_activity/config.py, collect.py, cli.py, optional new collection helper module; tests/test_collection_concurrency.py; config.example.toml; README.md; new tools/benchmark_collection.py; docs/2026-10-23-high-qps.md. Do not edit Task1 files or storage/quality/report modules.

**Consumes:** RpcClient concurrency keyword and snapshot() above. `collect` reads store.config.concurrency; coordinator may call snapshot when callable, and omit safe RPC fields for legacy fixtures. Do not change collect public arguments or existing progress fields/substrings. Config/CLI must pass current runtime concurrency to RpcClient including enrich construction; probe remains bounded not a stress test.

- [ ] RED tests: Config default1/range/noninteger/bool rejection; nonfinite QPS rejected; identity unchanged against old-config hash computed with concurrency omitted; Store.open supports a manifest whose config lacks concurrency; resume with changed runtime qps/concurrency preserves completed partition bytes. CLI forwards live config and logs concurrency without breaking existing stdout/log contracts.
- [ ] RED collector tests: out-of-order work with delayed first block still yields correct ordered exact transaction/receipt/log counts and identical normalized serial/concurrent contents. In tests coordinator and worker thread IDs demonstrate only coordinator writes. Track submitted tasks through a blocked first result to prove <=2*concurrency window with no unbounded scheduling. Missing receipt/worker/storage errors leave current partition incomplete and earlier commits intact; reopen/resume succeeds without duplicated transactions. Queued futures cancelled and running workers finish before client close. Known unsupported receipt method disabled for subsequent blocks; in-flight races are bounded to workers, serial still probes only once.
- [ ] Implement Config and runtime wiring, bounded ThreadPoolExecutor fetch per block, main-thread ordered normalize/write, and capability coordination. Default1 can keep serial fast path to preserve fake-clock progress and old fallback behavior. Partition skipping stays before requests; keep original receipt count/hash checks. Stop submission/cancel pending on any exception; wait for active HTTP workers to exit using bounded retry/timeouts, propagate sanitized original error. Do not close shared HTTP client in workers.
- [ ] Extend progress logging with configured concurrency, scheduled/buffered work and RPC snapshot counters/rate; document metric scopes and non-heartbeat limitation. Keep new-block speed and ETA definitions unchanged, separate full integrity validation cost.
- [ ] Add a runnable offline benchmark using synthetic fixture-derived blocks and simulated transport latency, recording serial and concurrent elapsed/blocks_per_second/actual HTTP attempt counts/peak_in_flight, and verify outputs and quality before publishing speedup. Defaults must run in a few seconds, clean temp outputs via TemporaryDirectory, no production URL accepted. No speed ratio as a flaky pytest requirement. CLI example `PYTHONPATH=src python tools/benchmark_collection.py --blocks 120 --latency-ms 10 --concurrency 16 --qps 10000` (arguments may have bounded parser validation).
- [ ] Update config.example with concurrency=1 and README contradictory single-I/O descriptions. Add rollout/tuning doc (trial qps500/concurrency32/batch20/partition500, configurable largerQPS, stop confirmed PID then same-run resume, no guaranteed throughput). Connection pool and limiter vs batch must be explained. No need new services or config migrations.
- [ ] Run focused tests and compile; write task-2-report.md with RED/GREEN evidence and self-review. Parent runs full suite/benchmark after both tasks and obtains independent task/final reviews. No commits or canonical changes.

## Integration / Parent validation and delivery

- [ ] Generate per-task unified diffs against baseline canonical, review spec+quality independently. Fix concrete findings with focused tests and re-review; no user check-in needed.
- [ ] Run full pytest, compileall, offline benchmark and CLI demo/validate, inspect HTTP in-flight/rate counters and equal serial/concurrent outputs; record exact measured evidence in docs/2026-10-23-high-qps.md. Full baseline168 must remain green.
- [ ] Final concurrency-aware whole-feature review with spec, both reports and diff; resolve all important findings before deployment.
- [ ] Hash-guard copy selected changed/new source/tests/docs/tools only to canonical project. Never copy internal reports, caches, data, config.toml or .git. No commit/push requested. Repeat full canonical tests with PYTHONDONTWRITEBYTECODE and temp cache. Refresh portable source archive including tools.
- [ ] Final concise Chinese handoff with configs, restart/resume behavior, tests and explicitly synthetic benchmark speedup, no real-node performance promise.
