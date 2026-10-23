# Large-partition storage implementation plan

> **For agentic workers:** Use subagent-driven-development and test-driven-development. Each task has a fresh implementer and independent review. No commits, branches or canonical writes by workers. Parent delivers only reviewed files with SHA256 guards.

**Goal:** Allow100000-block final partitions without whole-partition Python buffering or whole-partition validation; persist fine-grained durable recovery and reduce final file counts.

**Architecture:** One coordinator publishes closed checksummed hidden fragments, streams them into the existing final four Parquet files, then records the existing SQLite final task and removes staging. Typed batch scans and spill-capable DuckDB replace Python whole-partition quality dictionaries. Collection resumes the verified durable prefix while preserving current ordered/bounded concurrent RPC behavior.

**Tech Stack:** Existing Python3.11+, PyArrow, DuckDB and SQLite; no additional dependencies or services. Approved spec: docs/superpowers/specs/2026-10-23-large-partition-design.md.

## Global Constraints

- Final partitions1..100000 integer, default500; new runtime blocks_per_checkpoint1..5000 integer, default500, excluded from structural identity. bool/fractional rejected. Old manifests/hashes/final schemas/SQLite task schema and both path layouts remain compatible. No old-data migration, merging, renaming or configured partition-size change for an existing run.
- Buffer only one checkpoint batch, flush on its block count or estimated64MiB Arrow payload at a whole-block boundary. Estimate per incoming block, not repeated whole-buffer conversions. This is not a strict RSS guarantee; single-block and2*concurrency prefetch overhead remain explicit.
- Only fully closed/fsynced/atomically published fragment files are durable. Verify identity/kind/bounds/schema/checksums and maximal consecutive prefix before reuse; quarantine bad/overlapping/gapped suffix so stale fragments cannot rejoin. A private fragment must never count as a completed final partition/report source.
- Finalize via bounded Parquet record batches; four final files plus _partition.json, final path/checksum/atomic replace semantics retained. Retain fragments until final content verified AND SQLite completion recorded; crash after final rename before SQLite recovers from final without RPC. Interrupted finalization can replay all durable fragments without RPC.
- Preserve bounded ordered concurrent reads, global QPS/cooldown and single writer. Abort drops only unfinished memory/temp output, keeps durable prefix. Default SIGTERM immediate as before, unfinished batch plus bounded prefetch may be refetched. No credentials/URLs in logs or review artifacts.
- Production quality/finalization must not use full-file `.read().to_pylist()`, Store.read_table or Store.iter_partitions. Typed Arrow batches4096 + existing DuckDB512MiB spill budget acceptable; no whole-partition Python dictionaries. Preserve prior quality checks/result keys, exact amounts and failure gate.
- Staging only: activity-large-partitions. Canonical project /Users/zhaoyu/corp/bnbchain/opbnb/opbnb-activity untouched by workers. No inline production comments or one-letter variable names. Use apply_patch. Runtime /private/tmp/opbnb-activity-venv/bin/python; PYTHONPATH=src MPLCONFIGDIR=/private/tmp/opbnb-mpl XDG_CACHE_HOME=/private/tmp/opbnb-xdg-cache. Baseline222 tests. No new dependencies.

## Shared storage interface contract

Task1 adds these Store methods for Task3 (names are binding):

```python
def resume_partition(self, block_number):
    # Contract only: exclusive end of verified durable prefix; prepares writer state.
    # Full durable coverage finalizes without RPC and returns final end.
    ...

def durable_end(self, block_number):
    # Cached verified/published exclusive prefix end, no hashing on each call.
    ...

def abort_partition(self):
    # Clear unfinished batch/writer state only; retain published fragments.
    ...
```

The ellipses above document interfaces, not production stubs. Existing public write_block/commit_partition/completed/recover/read_table/connection stay usable. Preserve buffer/buffer_start/buffer_next expectations in old tests; callers must not manually clear these instead of abort_partition after the upgrade. Caller verifies completed/recover first, then calls resume_partition(start), fetches only [resume,end), and calls durable_end(number) after successful writes. Finalization logs belong to Store at actual start/end.

## Task1: Durable fragment lifecycle, bounded storage and config

**Own files:** src/opbnb_activity/storage.py, config.py, new focused storage helper modules as needed, tests/test_streaming_storage.py. No quality/collector/CLI/docs changes.

- [ ] RED Config examples: `Config(run_id='large',blocks_per_partition=100000,blocks_per_checkpoint=500)` accepts,100001/bool/fractional reject named safe messages; checkpoint0/5001/bool/fractional reject; pre-feature identity formula matches exactly with new field removed; old manifest missing new field opens unchanged.
- [ ] RED storage examples: Config partition6/checkpoint2; write first2 fixture blocks, final dirs empty, completed false, durable_end(100)==102; reopen/resume_partition(100)==102; write remaining4, final dirs1,4Parquet+metadata, staging absent, full normalized contents equal direct writes. Changed runtime checkpoint frequency resumes existing durable ranges.
- [ ] RED failure cases: incomplete fragment pending is ignored; fsync/rename/checkpoint-complete faults cannot lose prior published prefixes; corruption/gaps/overlap quarantine suffix and return last trusted end; corrupted final cannot delete valid prefix; full prefix can re-finalize without write_block/RPC; final rename followed by SQLite failure is recoverable, cleanup only after verified completion. Inject fault before/after fragment and final publication boundaries, not only generic errors. Preserve legacy corrupt replacement hidden backup tests.
- [ ] Implement typed config checks with fixed safe diagnostics `blocks_per_partition must be an integer in [1,100000]` and `blocks_per_checkpoint must be an integer in [1,5000]`; Task3 whitelists those exact messages. Old identity unchanged after removing checkpoint field.
- [ ] Isolate private fragment helpers instead of growing Store into one huge lifecycle module. Private `.staging` under run root, per-final-range fragments (self-describing actual start/end). No SQLite task schema changes. Metadata kind/schema/identity/ranges/row counts and file hashes validated before reuse; hidden/quarantined paths never discovered as valid. Explicit symlink/path/bounds rejection for untrusted fragment metadata; don't construct paths from arbitrary metadata. In-memory writer has one checkpoint buffer with per-block Arrow-byte estimate; small single-batch final partitions may publish directly.
- [ ] Stream final concatenation via ParquetFile.iter_batches and ParquetWriter; no full final table materialization. Footer closed before publication, fsync parent, verified final hashes and task completion before cleanup. Exception cleanup preserves durable fragments and .replaced backup semantics. Add storage start/end finalization and durable fragment commit logs (safe ranges/counts only). Implement shared Store APIs and keep old API tests green.
- [ ] GREEN focused storage/core/layout tests, compile; record exact RED/GREEN commands/results, crash evidence and interfaces in task-1-report.md, self-review and freeze. Do not run global suites while other tasks are actively editing or benchmark100000 by yourself; parent integrates later.

## Task2: Bounded typed quality and relational consistency

**Own files:** src/opbnb_activity/quality.py, optional new quality SQL/helper modules, tests/test_streaming_quality.py. Do not edit storage/config or other tasks' files. Uses existing Store.partition_dirs/ranges/completed/recover/connection; does not require new writer methods to begin tests.

- [ ] RED: monkeypatch Store.iter_partitions/read_table and ParquetFile.read to raise on a valid old-format fixture; validate must still produce exact counts and oktrue. Iterator batch-size instrumentation proves bounded typed Arrow inspection. Keep physical row order/continuity checks, not sorted-only scans hiding corruption.
- [ ] RED corruption matrix after updating synthetic metadata/checksums: wrong schema, null required values, dishonest row counts, duplicated/missing/out-of-range blocks, wrong physical order and parent hash, transaction block/timestamp/count mismatch, missing/extra/duplicate/index/hash-mismatched receipts, log joins/block/hash/duplicate key mismatch and cross-partition duplicate hashes; result okfalse with existing safe diagnostic meanings. Preserve current test compatibility and incomplete final task gating. Private staging must not satisfy final data coverage.
- [ ] Implement bounded physical schema/footer and iteration checks per final path. Expected schema imported from storage, observed row counts checked against metadata files rows. No per-partition bigPython maps; use existing DuckDB connection with explicit file/physical row positions if needed, relational joins/anti-joins/counts/window checks, all memory-limited/spill-capable. Metadata's expected partition interval must agree with actual block coverage. Retain result keys identity/start/end/counts/errors, atomic quality.json and exceptions converted to safe types. Empty window is valid with zero counts.
- [ ] GREEN focused newquality/core/layout/activity/pipeline tests; compile. Write task-2-report.md exact RED/GREEN and self-review, avoid rerunning reported large suites unnecessarily. No claims on production RSS until parent benchmark. Freeze for independent review.

## Task3: Resume-aware collection, CLI, docs and isolated benchmark

**Own files:** src/opbnb_activity/collect.py, cli.py, tests/test_checkpoint_collection.py, config.example.toml, README.md, docs/2026-10-23-large-partitions.md, tools/benchmark_partitions.py, tests/test_partition_benchmark.py. Existing test adjustments only if new log behavior intentionally adds messages; coordinate ownership first. No rpc/storage/config/quality edits.

- [ ] RED collector: partition6/checkpoint2 fake client fails after third normalized block; old first2 remain durable; after Store.open current settings/restart, no block RPC below102, original exact tables/metrics recovered. Serial/concurrent variants, failures, prior final partitions unchanged, full-fragment-prefix finalization makes no block/receipt RPC. Recovered prefix advances processed/durable not new-block speed; committed only finals. Keep old default checkpoint500 progress exact substring tests green.
- [ ] Implement shared Store APIs with current config. After completed/recover skip, call resume_partition(start); log recovered prefix and adjust processed/recovered/durable counts without inflating new RPC throughput. Use `_ordered_blocks` on resume..end. On error call abort_partition; retain current worker cancellation/close guarantees. Progress includes blocks_per_checkpoint/runtime and durable/reused counts without per-block logging; finalization timing separately logged by Store.
- [ ] CLI whitelist exact two Config diagnostics from Task1; current runtime blocks_per_checkpoint printed on startup. Config.example retains final default500/checkpoint500 for backward compatibility but documents new-run example100000/500. README old cap5000/wholepartition in-memory statements updated; no migration or same-run partition-size changes suggested.
- [ ] New benchmark accepts bounded --blocks up to at least100000, --checkpoint-blocks default500 and --interrupt-after1250; defaults can be smaller for fast smoke. Generate blocks/receipts lazily from fixture templates with unique indices/hashes, valid timestamps/parents and deterministic logs. Include representative fixture-derived transactions/logs and empty-block modes, no all-chain Python list/map and no production endpoint input. Run final-size500 and100000 modes in isolated child processes so RSS isn't inherited from another workload. Count RPC methods without retaining one item per request. Source generator/validation can reuse existing module functions without importing full old benchmark dictionaries.
- [ ] Measure final directories/files and private staging peak disk, elapsed collection/finalization/validation, process peak RSS with Darwin/Linux unit correction. Validate exact counts and ordered per-table SHA256 fingerprints via bounded batches before reporting comparisons. No whole-dataset read_table. Simulate one interruption after1250normalized blocks in the large run, then resume and prove1000durable blocks' RPCs avoided (for checkpoint500 absent earlier byte flush), no duplicate rows. RSS/timing are measurements, not brittle unit-test ratios.
- [ ] GREEN collector/progress/pipeline newbenchmark tests and compile; small smoke of benchmark CLI, document commands and honest temporary I/O/RSS limits. Exact full100000-block execution and final numbers are parent's integration responsibility. Write task-3-report.md with RED/GREEN evidence/interfaces/concerns and freeze.

## Parent integration, review and delivery

- [ ] Check plan/spec coverage and no conflicting file ownership; dispatch independent implementations on the contracts above. Baseline canonical222 tests read-only. Keep progress ledger updated.
- [ ] Generate task-scoped diff packages against canonical. Independent task review spec+quality, use concrete TDD fix waves for important findings and re-review. No per-task commits.
- [ ] Integrate full suite, compile src/tests/tools, offline demo/validate; exercise both legacy and new-file datasets and exact metrics. Verify no API secret or fullchain structures in tests/tools.
- [ ] Actually execute at least100000 generated blocks for both500 and100000 final sizes in representative and empty modes with independent RSS. Run interrupted-resume acceptance, save output evidence, update user docs with observed figures and explicit synthetic limits. Inspect terminal file counts and no remaining active private staging.
- [ ] Final whole-feature concurrency/storage/recovery/quality review by a fresh capable reviewer. Resolve all important findings; no node throughput or16GB production-capacity promise.
- [ ] Hash-guard deploy only approved changed/new code/tests/tools/docs, excluding configs/data/.git/caches/internal reports. Repeat canonical full tests with no bytecode/temp caches, git diff --check, refresh source archive including tools. Do not commit or push until user asks.
