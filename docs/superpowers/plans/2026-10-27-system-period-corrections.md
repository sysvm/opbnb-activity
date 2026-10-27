# System and Period Corrections Implementation Plan

> Execute inline with test-driven development. No subagents, branches or commits are requested.

**Goal:** Correct exact weekly/monthly address counts and exclude known protocol TxDAG from non-system activity without recollection.

**Architecture:** Keep raw schemas/identity/checkpoints intact. Normalize date keys in staged core queries. Share TxDAG constants and equivalent Python/SQL predicates across normalization and partition staging; all downstream metrics consume the corrected activity/log views.

**Tech Stack:** Python 3.11+, DuckDB, PyArrow, pytest.

## Global Constraints

- No HTML in this change; user deferred it.
- No raw data changes, network calls, new configuration or dependencies.
- Unknown/failed/non-matching TxDAG-like calls stay non-system.
- Run tests on a temporary source copy, then transfer only reviewed source/test/doc files.

## Tasks

- [x] Add `tests/test_core_stages.py` assertions that `period_start` is an ISO date and survives `activity_metrics.periods`; run that test and observe failure. Change both period key expressions in `core_metrics.period_tables` to `strftime(date_trunc(...), '%Y-%m-%d')`; rerun the tests.
- [x] Add `tests/test_system_transactions.py` with a terminal known-sender legacy zero-value/zero-price successful call and a complete dynamic-bytes calldata; assert normalization marks `system_tx_dag`. Parameterize each mismatching condition; observe positive failure before implementing `system_transactions.py` and calling its Python predicate from `models.normalize_block`.
- [x] Persist the positive fixture with legacy `is_system=false` and `classification=unknown`, record Parquet/manifest/checkpoint bytes, and analyze it. Assert total transactions unchanged; system count increases; protocol sender/recipient/method/event disappears from non-system outputs; user deposits/BNB totals remain; weekly/monthly counts survive. Observe failure, then apply the SQL predicate to a partition-local normalized transaction view in `analysis_stages.prepare_activity`.
- [x] Assert the two system technical types remain separate and metadata/technical notes identify the rule version. Update `activity_metrics.py`, `metrics.py`, and `report.py` accordingly. Run focused tests including normalization, workspace ownership and pipeline tests.
- [x] Extend the existing 250k-row/128MB typed-Parquet pipeline test to verify weekly/monthly exact sets. Run the full suite with `MPLCONFIGDIR` and `XDG_CACHE_HOME` under `/private/tmp`; inspect diff and transfer only this change. Document the unchanged report command and requirement to rerun on server raw Parquet.

## Verification Commands

`PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src /private/tmp/opbnb-memory-venv/bin/python -m pytest -q tests/test_core_stages.py tests/test_system_transactions.py`

`MPLCONFIGDIR=/private/tmp/opbnb-fixes-mpl XDG_CACHE_HOME=/private/tmp/opbnb-fixes-xdg PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src /private/tmp/opbnb-memory-venv/bin/python -m pytest -q`

## Results

Expected initial regression failures: ISO period key, missing normalization, legacy staging and missing TxDAG subtype (four failures). Full suite: 418 passed. Added explicit CSV export assertions to the subtype regression afterward: 34 focused tests passed. No production-window recount was attempted because local inputs are exported reports only.
