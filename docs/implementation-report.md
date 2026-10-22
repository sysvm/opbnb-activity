# Offline opBNB activity tool — implementation report

Status: offline phases A–C implemented and verified with synthetic fixtures and mocked HTTP. No live endpoint was supplied, no real chain data was collected, and no mainnet activity or throughput conclusion is claimed. All implementation artifacts are under this task artifact directory; no protocol-repository changes, Git commits or branches were made.

## Deliverables

Tool root: `opbnb-activity/`.

- Installable Python package with `pyproject.toml`, console entry point `opbnb-activity`, and `python -m opbnb_activity`.
- Commands: `probe`, `collect`, `validate`, `analyze`, `report`, `demo`. Every command uses an explicit run ID; endpoint environment variable is required only by `probe`/`collect`.
- Full Chinese `README.md` with configuration, isolated server installation, `nohup`, resume, metric definitions and limits. `config.example.toml`; complete tested dependency snapshot `requirements-tested-py312.txt`.
- Modules: `__init__`, `__main__`, `cli`, `config`, `models`, `rpc`, `probe`, `block_range`, `collect`, `checkpoint`, `storage`, `decode`, `labels`, `metrics`, `quality`, `report`.
- Packaged SQL: `src/opbnb_activity/sql/{daily,contracts,projects,senders,retention}.sql`.
- Packaged offline fixture: `src/opbnb_activity/fixtures/synthetic.json`; 6 contiguous synthetic blocks, 11 transactions, 11 receipts and 2 logs across 6 dates.
- Tests: `tests/{conftest,test_bootstrap,test_core,test_rpc,test_pipeline}.py`.
- Executed demo output: `opbnb-activity/demo-output/synthetic-demo/`, with manifest, range Parquet, SQLite checkpoint, quality/analysis JSON, CSVs, Markdown report and two PNG charts.

## Implemented behavior

Configuration defaults to Asia/Shanghai `[2026-07-22,2026-10-22)`, stores UTC and an end-exclusive block range, and isolates source/window/system classification/shard size through a config identity. The demo explicitly overrides to its distinct synthetic `[2026-10-01,2026-10-22)` window. Manifest paths are relocatable; dataset paths are supplied at runtime. Labels/assets have separate content versions so offline relabeling does not silently claim the same report provenance.

RPC is synchronous with one HTTP request in flight, default 1 logical element per second and batch size 1. Explicit larger batches debit element budget before sending; they have an acknowledged batch burst rather than a false instantaneous 1-QPS claim. Batches match arbitrary response order, reject missing/duplicate/unexpected IDs, retain successes and retry transient failed elements only. Retry/timeout counts are bounded. Retry-After is respected up to the wait budget; longer requested waits stop without an early retry. HTTP, transport, malformed envelope and invalid-endpoint errors are redacted. Unsupported, pruned and transient states are separate. A bounded real two-element batch capability sample is independent of configured batch size.

Probe checks chain ID 204 before historical work, resolves timestamp lower bounds, and samples receipt/log/trace capabilities. It does not equate archive with trace availability or claim throughput. Future/incomplete end windows and insufficient historical start coverage are rejected.

Collection normalizes full blocks/transactions/receipts/logs and verifies transaction/receipt identity before storage. Unsupported block-receipt methods fall back to transaction receipts; missing, duplicate or mismatched receipts prevent partition completion. Output uses date plus block-range directories, default 500 blocks per partition, four typed Parquet files per range rather than per block. A whole range directory is finalized before SQLite completion. Checksums and identity are rechecked on resume; a final directory written before checkpoint can be recovered without recounting. Incomplete partitions are re-collected. The buffer holds one configured range.

Quality verifies declared range completeness, checksums, parent hashes within the collected range, transaction counts, transaction/receipt block and index joins, log joins, and cross-partition duplicates. Incomplete quality gates both analytics and final reports. A valid empty range is handled coherently.

System L1 attributes classification requires type 0x7e, exact configured depositor and configured protocol recipient together; ordinary user deposits remain non-system. The known depositor/recipient source evidence supplied by the controller is recorded in the README. Contract creation and selector/created-address data are preserved. Empty input is never treated as proof of EOA. CSV labels retain provenance/confidence and non-overlapping effective block ranges. Direct project attribution uses at most one project per transaction; successful senders deduplicate after grouping contracts into projects. Unknown coverage is explicit.

DuckDB scans typed Parquet for grouping, with 512MB memory limit and disk spill path. Quality works range-by-range. Amount reductions read 4096-row batches and sum Python integers, avoiding DECIMAL(38) overflow; scaled presentation uses an explicit Decimal context >=100 digits. Daily rows include exact native value, known full fees, partial L2 fees, unknown counts and coverage. Reports include daily/whole-window/weekly/monthly successful non-system senders, supplemental all non-system senders, direct-recipient/project/sender rankings, mature first-observed D1/D7 retention, identified-project Top1/5/10 contribution/HHI and full-data Top1% successful-sender transaction share. Unknown is a separate unassigned share, not one fictitious project in concentration calculations.

Native financial scope is successful non-system top-level value. Verified fungible token Transfer logs require supplied decimals and explicit verification; NFT-like/ambiguous/unverified logs are excluded with counts. Raw values remain decimal strings. Full fees are known only with observed L1 and L2 components; failures contribute fees where known; unknown is never zero. No price, TVL, DEX/bridge economic volume, bot, real-human or trace-attribution estimates are produced.

Reports contain CSV tables, Markdown metric definitions/provenance/coverage, `daily.png` and `projects.png`. Synthetic marking is present in both report and charts. English chart labels avoid CJK font requirements; documentation is Chinese.

## TDD evidence

Dependency installation initially blocked pytest. A dependency-free `unittest` red/green cycle was used first:

1. `/private/tmp/opbnb-activity-venv/bin/python tests/test_bootstrap.py` initially failed with the explicit assertion `Missing offline config implementation`.
2. After configuration implementation, the same command passed one test, verifying default UTC half-open boundaries and identity change.
3. Added timestamp/precision/system/decoder/label tests: observed 3 failures for missing modules; implemented and observed 4 passing tests. RPC taxonomy, checkpoint identity and probe wrong-chain behavior likewise each had observed missing-feature reds before green.
4. Once pytest/httpx became available, `pytest tests/test_core.py tests/test_rpc.py tests/test_pipeline.py -q` observed **6 missing-feature failures / 11 passes**, before storage/quality/collector/metrics/report/CLI completion.
5. Later focused red/green regression cycles observed real assertion/behavior failures for contract creation, null canonical `contractAddress`, actual two-element batch probe, re-analysis retaining fixture labels, concentration metrics, daily L2 coverage, long Retry-After, invalid endpoint redaction and malformed error envelopes.

Final command, from the tool root:

```bash
PYTHONPATH=src MPLCONFIGDIR=/private/tmp/opbnb-mpl XDG_CACHE_HOME=/private/tmp/opbnb-xdg-cache /private/tmp/opbnb-activity-venv/bin/python -m pytest -q
```

Result: **32 passed in 2.57s**. Tests require no node or network; HTTP uses `httpx.MockTransport` and sleep hooks. Tests include missing receipt blocking completion, atomic multi-block shards, crash-after-final-write recovery, corruption, manifest isolation, exact large financial sums, project deduplication, retention maturity, empty results, and CLI integration.

Import/compile command:

```bash
PYTHONPATH=src /private/tmp/opbnb-activity-venv/bin/python -m compileall -q src tests
```

Result: exit 0, no output.

## Executed demo and inspection

```bash
PYTHONPATH=src MPLCONFIGDIR=/private/tmp/opbnb-mpl XDG_CACHE_HOME=/private/tmp/opbnb-xdg-cache /private/tmp/opbnb-activity-venv/bin/python -m opbnb_activity demo --dataset ./demo-output --run-id synthetic-demo
PYTHONPATH=src /private/tmp/opbnb-activity-venv/bin/python -m opbnb_activity validate --dataset ./demo-output --run-id synthetic-demo
```

Both exit 0. Demo was re-run to exercise resume. Final validation: PASS, no errors; **6 blocks / 11 transactions / 11 receipts / 2 logs**, block range `[100,106)`. The controller independently validated an installed console demo from outside the source root without PYTHONPATH; it will perform final packaging review separately.

Inspected generated Markdown, daily CSV and quality JSON. Viewed `daily.png` visually: synthetic title, legend, Shanghai date labels and curves are legible with no clipping. Two PNGs are generated and tests assert nontrivial file size. The controller also visually inspected the daily chart.

Fixture exact expectations verified:

| Metric | Expected / verified |
|---|---:|
| Total / system / successful non-system / failed non-system tx | 11 / 1 / 8 / 2 |
| Unique successful non-system senders | 5 |
| Alpha direct transactions / deduplicated successful senders | 5 / 2 |
| Native top-level wei | `1000000000000000000000000000000000000000000081` |
| Native BNB scaled exactly | `1000000000000000000000000000.000000000000000081` |
| Known total fees / partial L2 fees (wei) | 336800 / 378000 |
| Unknown total-fee transactions | 2 |
| Verified token raw | `10000000000000000000000000000000000000001` |
| Verified token scaled | `10000000000000000000000.000000000000000001` |
| First-day native / known total fees (wei) | `10^45 + 15` / 84200 |
| First-day active / all non-system senders | 2 / 3 |
| First cohort D1 / D7 retained | 1 of 2 / 1 of 2 |
| Project Top1 / Top5 / Top10 / Unknown shares | 0.5 / 0.9 / 0.9 / 0.1 |
| Top 1% successful sender tx share | 0.5 |

## Runtime and operational handoff

Validated Python **3.12**, httpx **0.28.1**, PyArrow **26.0.0**, DuckDB **2.0.0**, pytest **9.1.1**, Matplotlib **3.11.2**. The dependency snapshot contains full tested versions and is explicitly Python 3.12-specific. General project metadata permits Python 3.11+, but that environment was not executed; its resolver may select older NumPy.

For a server, create a Python 3.12 virtual environment, install the tested dependency snapshot then the local package. Start with `demo`, then use a separately named one-day sample run with the supplied endpoint. Inspect `probe.json`, `quality.json` and actual provider load before initiating the default full window. `collect` can run via `nohup` as documented. Run one writer per run. Resume the same run with the same configuration; create a new run for a different date/window/source/system rule/shard size. Only request `report` after quality succeeds.

## Scope limitations and design rationale

- No live endpoint, real historical coverage, real provider canonicality, full three-month load, throughput, memory/disk benchmark or mainnet classification coverage has been verified. Conservative 1-QPS collection may take substantial time. These are the intended next empirical checks once a node is supplied.
- Collection is one process with serial HTTP requests and a bounded number of blocks per shard; exceptionally large blocks still increase memory, so shard size is configurable. No concurrent-writer coordination is supplied. `Store.read_table` deliberately materializes only for small tests; production paths use partition scanning and DuckDB.
- Automatic canonical-chain reorg rollback and an independent canonical hash source are not implemented. Parent-hash consistency is checked within the declared collected range; a new run/recollection is required for historical reorg/provider inconsistency. The tool does not certify a provider's chain independently.
- Header-range probe coverage is distinct from receipt/state availability. Block receipt methods, transaction receipts, logs and trace are sampled, not exhaustively verified ahead of collection. Missing data during collection blocks completion.
- Business/type classification is deliberately conservative; labels provide direct-recipient project/category/role attribution, historical EOA/contract evidence is not fetched, and internal/trace attribution is absent. These unknowns are in the report rather than fabricated.
- Retention uses first observed activity in the window, not all-history acquisition cohorts. Partial/maturing days are excluded from cohort rates. Period tables retain partial edge weeks/months.
- The packaged SQL/fixture directories live inside `src/opbnb_activity/` rather than repository-root `sql/` so wheel/non-editable installation can resolve them without source-tree paths. Interfaces are intentionally small: one normalized tables mapping, one range store and one analysis result dictionary.
- No deployment service, live dashboard, prices/TVL or unsupported economic metric was added.

Source frozen after the final 32-test green run. Independent reviewer/controller integration verification remains outside this implementation subtask.

## Offline review corrections — 2026-10-22

Resolved both Important findings from `offline-review-report.md`, with no protocol repository changes, Git commits/branches, node access or network requests.

- `src/opbnb_activity/models.py`: transaction `value` and receipt `logs` are now required. Missing/invalid values raise field-specific `ValueError` before `normalize_block` can return tables to storage; booleans, floats, nulls, containers, invalid quantity strings and negative values are rejected. Logs must be a list of objects. Explicit zero native value (hex string or integer) and empty log lists remain valid. Optional fee handling is unchanged.
- `src/opbnb_activity/cli.py`: `validate` dispatches immediately after opening the store, before label loading or analysis asset selection. `analyze/report` still reject missing or invalid configured labels. README documents this migration behavior and the continued external-label requirement for attributed analysis/reports.
- `tests/test_core.py`: regressions prove missing fields prevent even single-block partition completion, malformed field shapes are rejected, and legitimate zero/empty values are preserved.
- `tests/test_pipeline.py`: offline CLI regressions relocate an empty valid dataset, then delete or invalidate its configured external labels; validation succeeds while analyze/report fail explicitly.

All commands below ran in the standalone `opbnb-activity/` directory. The focused command ran first with only the tests added:

```bash
PYTHONPATH=src MPLCONFIGDIR=/private/tmp/opbnb-mpl XDG_CACHE_HOME=/private/tmp/opbnb-xdg-cache /private/tmp/opbnb-activity-venv/bin/python -m pytest -q tests/test_core.py tests/test_pipeline.py -k 'required_fields or invalid_value_shapes or invalid_logs_shapes or explicit_zero or external_labels'
```

RED result: **16 failed, 2 passed, 13 deselected in 0.54s** (exit 1). Both missing-field cases incorrectly completed instead of raising; malformed values were accepted or failed with incorrect/non-field-specific exception types; both relocated-dataset CLI cases failed on external labels. The explicit zero/empty preservation controls passed. These failures reproduced the reviewed defects before production changes.

After the minimal production fixes, the exact same focused command returned **18 passed, 13 deselected in 1.01s** (exit 0).

Final full-suite command:

```bash
PYTHONPATH=src MPLCONFIGDIR=/private/tmp/opbnb-mpl XDG_CACHE_HOME=/private/tmp/opbnb-xdg-cache /private/tmp/opbnb-activity-venv/bin/python -m pytest -q
```

Result: **50 passed in 3.38s** (exit 0), including existing demo/report end-to-end integration coverage.

```bash
PYTHONPATH=src MPLCONFIGDIR=/private/tmp/opbnb-mpl XDG_CACHE_HOME=/private/tmp/opbnb-xdg-cache /private/tmp/opbnb-activity-venv/bin/python -m compileall -q src tests
```

Result: exit 0, no output. No live-provider or full-window performance claims are added. Previously completed datasets whose original missing fields were already erased by the old normalizer cannot be repaired from stored tables alone; recollect such data if its source completeness is in doubt. Source is frozen again after these corrections; controller packaging and focused reviewer confirmation remain separate.
