# Offline Attribution Implementation Plan

> **For agentic workers:** Use subagent-driven-development for independent registry work and review; execute tightly coupled metric/report work inline. No commits or branches without a new user request.

**Goal:** Explain daily transaction changes and head-address behavior using existing validated files, without recollection or implicit RPC.

**Architecture:** Add a bounded-output attribution module invoked after activity_metrics constructs metric_activity. Reuse activity's historical label joins, existing CSV escaping and Markdown rendering. Existing raw storage, identity and collector are unchanged.

**Tech Stack:** Python >=3.11, existing DuckDB/PyArrow, pytest; no new dependencies.

## Global Constraints

- Follow approved `docs/2026-10-24-attribution-review.md`, recommendation B phase one only.
- No database service, no trace calls in report, no bots/people claims, no float conversion of wei.
- Preserve original source files and local user exports; edit a temporary staging copy and transfer only changed code/tests/docs.
- Unknown remains explicit; current deployment association is not independently confirmed historical ownership.

## Task 1: Address evidence registry

Files: fixtures/address-registry.json; tests/test_enrichment.py.

- [ ] Add failing tests for BeatSwap and Cortis AI associations on chain204, provenance and synthetic isolation.
- [ ] Run `python -m pytest tests/test_enrichment.py -q`; expect new association assertions to fail.
- [ ] Add only the two researched addresses with source URL, date, source digest whose normalization is documented, project_published confidence, category, role, and temporal limitation. Retain existing entries and user overrides. Do not mark IPL as verified asset.
- [ ] Run registry/enrichment tests; review exact diff. Do not commit.

## Task 2: Offline attribution tables

Files: new attribution.py; tests/test_attribution.py; metrics.py integration.

Interface: `augment_attribution(connection, result)` mutates result with daily_projects, daily_recipients, daily_changes, sender_profiles, sender_daily, sender_overlap, failure_methods, failure_samples. Consumes activity/metric_activity views, result daily (all local days), and successful non-system result counters.

- [ ] Write real DuckDB tests before implementation; use a synthetic three-day view containing a zero day, creation, Unknown, label switch, overlapping senders, same timestamps, failures and `10**50+3` wei. Assert exact counters and money, stable Top20 + Other + creation, day changes and non-additive DAU disclaimers.
- [ ] Run `python -m pytest tests/test_attribution.py -q` and confirm expected missing-feature failures.
- [ ] Implement daily_projects with all window projects and zero days; each project/day counts successful senders independently. Implement stable global top20 recipients plus separate Other/creation groups, all zero days, and daily_changes by project and recipient. Use previous calendar day, nullable first-day delta and null growth when prior zero. Per-day dimension deltas must reconcile to daily total delta; ranks are per transition, and summaries cap display only (not underlying contributions).
- [ ] Implement top20 sender profiles and daily panel using full selected history, exact Python integer values for sums, SQL lag(timestamp) ordered by timestamp/block/index/hash for gaps, null gaps for <2tx. Exclude creation from distinct recipient overlap. All 190 pairs max, intersection/union/Jaccard, with null when union empty.
- [ ] Implement failure recipient/selector global top20, deterministic earliest up to3 real failed samples per selected group (<=60). Include hash/block/index/time/value and no fabricated revert reason.
- [ ] Integrate after activity augment creates metric_activity. Preserve existing helper-only tests which use reduced activity schemas by invoking new module only from metrics.analyze.
- [ ] Run new and existing metrics tests.

## Task 3: Report integration and boundaries

Files: report_tables.py; new attribution_report.py; report.py; enrichment.py; tests/test_report.py; tests/test_enrichment.py; README.md; docs/2026-10-24-offline-attribution.md.

- [ ] Write failing tests for exported schemas on empty windows, rendered daily change explanation, formula escaping, sample evidence and code observation fields.
- [ ] Add CSV schemas for eight tables above, all string identifiers included in TEXT_FIELDS. Preserve defaults on empty lists.
- [ ] Add a Markdown section using escaped existing formatter callbacks: largest absolute day change and top3 project contributors plus remaining net change, window Top sender behavior and failure diagnostic links. Explicitly distinguish additive tx contribution from overlapping DAU. Do not invent causation from correlation.
- [ ] Keep historical_kind unknown; add code_observation_scope derived from sample statuses/kinds without claiming full historical identity. Use this field in report explanatory text and CSV.
- [ ] Explain fee group coverage, D1 left-boundary bias and D7 maturity; include D1 excluding window first date alongside existing all-cohort D1.
- [ ] Document no recollection, same report command after reinstall, new table definitions/TopN limits and explicit future trace scope. Update outdated exact CSV count references.
- [ ] Run report/enrichment tests, full `python -m pytest -q`, whitespace check and a synthetic CLI report smoke test with no RPC environment.

## Task 4: Review and delivery

- [ ] Independent read-only review of staging vs original, addressing correctness/scalability/financial precision and approved scope.
- [ ] Fix substantive findings with regression tests and rerun suite.
- [ ] Transfer only reviewed modified files/new files to original repo after verifying originals unchanged since staging. Do not copy test artifacts, data, logs, or .git.
- [ ] Verify repository diff and tests; report exact command to run on server. No production seven-day recomputation claim because local raw files absent.

## Execution record (2026-10-24)

Tasks 1–3 implemented and tested in a temporary source copy; no branch/commit or production data changes. Registry worker completed red/green tests with immutable README bytes and independently reviewed provenance. Metrics/report integration exercised red/green tests before implementation. Independent final review found repeated raw scans (P2); fixed with projected temporary tables and a regression test (failing VIEW before fix, passing table kinds afterward).

Validation after fixes: full suite 340 passed in 35.64 seconds. Synthetic offline CLI demo exported all 26 CSVs, summary, appendix and charts. A 1,000,000-row in-memory synthetic augmentation reconciled daily/project/recipient counts; it is not a production Parquet or three-month benchmark. Delivery consists only of selected changed code, tests and docs; final source-copy verification and repository whitespace checks follow transfer. Checklist above records planned actions; this execution record records completed work. No actual seven-day raw-data report was rerun locally.
