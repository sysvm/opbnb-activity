# Large Analysis Memory Repair Plan

**Goal:** Allow the 16GB server to validate/report the existing 212.8m-transaction run without recollecting or bypassing checks.

**Architecture:** Configurable DuckDB memory/threads/spill parent, runtime-only overrides on Store.open, partition-local joins plus exact global duplicate detection using external sorting and bounded cursor consumption. Existing raw files and identity unchanged; normal report remains offline.

**Tech stack:** Existing Python >=3.11, DuckDB/PyArrow and pytest; no new dependencies or services.

## Task 1: Runtime resource settings

- [ ] Add failing tests in tests/test_analysis_resources.py for validated analysis_memory_limit_mb (default8192), analysis_threads (default2), analysis_temp_directory (defaultNone); reject bool/non-int/invalid limits and empty/wrong-type path. Exclude new fields from identity, prove the legacy formula matches with these fields removed.
- [ ] Store.open accepts keyword analysis_settings and copies only the three runtime keys onto manifest config; dates/system/partition and saved manifest remain unchanged. CLI passes actual loaded config for report/analyze/validate/enrich. Test both the real Store and real CLI entry point.
- [ ] Configure every Store.connection with configured memory/threads and preserve_insertion_order=false; use unique owned subdirectory under chosen spill parent/default run .duckdb-tmp, without deleting user data. Close connection on setup exception.
- [ ] Run targeted red/green and existing config/storage/CLI tests. Update formula regression to explicitly exclude runtime-only fields.

## Task 2: Bounded relational quality checks

- [ ] Delegated isolated worker writes tests first in tests/test_quality_memory.py, then changes quality_sql.py / quality.py only.
- [ ] Bind the existing six checks to one physical partition at a time (same columns, same diagnostic messages); retain cross-partition duplicates for blocks/transactions/receipts using full exact hash order and bounded fetchmany; compare adjacent values across batches. Never sample or drop a check.
- [ ] Add partition/check/global-sort progress logging and safe failure_stage context when validation fails, without logging sensitive exception text or skipping the gate. All failures still ok=false.
- [ ] Verify corruption/duplicate/batch-boundary checks and prove queries do not build full-window hash joins/groups; no fetchall whole dataset.

## Task 3: Documentation and validation

- [ ] Config example + README + docs/2026-10-27-analysis-memory.md give 8192MB/2threads and optional spill directory, df/free preflight, original run-id recovery command. Explain process memory isn't a hard DuckDB budget, SQL spill needs disk and other workloads can OOM. Never promise production 200m-row run tested locally.
- [ ] Full existing suite + no-network CLI smoke. Moderate synthetic low-memory Parquet validation to exercise disk-spilling global sort; real run has no local raw files.
- [ ] Independent review of compatibility/coverage/resource lifecycle and fixes with tests, then transfer only selected files to original repo; no commit unless requested.

No changes to raw data/schema/checkpoint/partition sizing or RPC behavior. No disabling quality checks; no blanket retry with unbounded memory.

## Execution record

Tasks1–3 implemented and independently reviewed. Direct stdlib resource-settings assertion failed before implementation; quality worker's partition-binding contract failed before implementation. Full pytest became available after recreating an isolated temporary environment (previous environment had been partially purged). New safe TOML path-validation test also failed before its fix. DuckDB2 required fully draining scalar/sorted cursors before replacing dependent views; covered by real multi-partition tests. Two older formula tests now exclude new runtime-only fields; an actual manifest fixture with all new fields absent reopens with identical identity and bytes. Full suite375 passed in38.04seconds. CLI synthetic report26CSVs passed; 1m-row complete-hash sort with64MB and actualdiskspill passed. Independent review reports no P1/P2. Originalserver200m-rowdata isnot availablelocally; no productionperformanceclaim or commit. Copy only selected code/tests/configexample/docs backafter checking originals unchanged.
