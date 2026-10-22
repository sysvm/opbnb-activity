# Independent offline implementation review

## Verdict

**Spec compliance: substantial implementation, changes required.** The requested standalone package, CLI, conservative RPC client, partitioned storage, quality gate, exact integer analytics, synthetic demonstration and reporting are present. Two verified defects prevent full acceptance of the fail-closed data-quality and standalone validation requirements.

**Code quality: changes required before relying on a collected dataset.** No Critical findings. Two Important findings are listed below. The implementation otherwise keeps scope controlled and its limitations explicit. This is an offline source review, not an endorsement of live-node correctness, throughput or 92-day operating performance.

Reviewed the supplied full addition diff, implementation brief and implementation report. Inspected additional source only to locate the two concrete risks below. Did not edit source or rerun the full suite. Accepted the implementer's reported **32 passed in 2.57s**, compile/import and CLI demo results as reported evidence, not independently rerun evidence. Executed only two narrow offline reproduction scripts, using `/private/tmp/opbnb-activity-venv/bin/python`, temporary datasets and the supplied fixture; neither used a node or network.

## Important findings

### 1. Missing required source fields become valid zero amounts or empty log sets

**Priority: P2 / Important.**

Locations:

- `/Users/zhaoyu/.codex/visualizations/2026/10/22/01a162b5-d484-7d70-8b57-9711a9107b22/opbnb-activity/src/opbnb_activity/models.py:45`
- `/Users/zhaoyu/.codex/visualizations/2026/10/22/01a162b5-d484-7d70-8b57-9711a9107b22/opbnb-activity/src/opbnb_activity/models.py:60`

`transaction.get("value", "0x0")` converts an absent native amount into a known zero. `receipt.get("logs", [])` converts an absent log field into a known empty log list. Both results are persisted and pass the existing quality gate because the original omission has already been erased. A malformed or incomplete RPC response with otherwise valid transaction/receipt identities therefore silently understates financial results rather than preventing partition completion.

Focused reproduction: deep-copy the supplied synthetic fixture, remove the first transaction's `value` or the first receipt's `logs`, then pass all six blocks through `normalize_block`, `Store.write_block` and `analyze`:

| Omission | Observed result | Quality |
|---|---|---|
| First transaction `value` | Native value becomes `74` instead of `1000000000000000000000000000000000000000000081` wei | PASS |
| First receipt `logs` | Verified token sums become `{}`; stored logs drop from 2 to 1 | PASS |

Required correction: require the mandatory source fields used for complete native/log analytics, validate their types, and reject omissions before storing a completed partition. Preserve the intentional nullable handling of genuinely optional fee components. Add focused regressions proving missing `value` and missing `logs` fail before completion, while explicit zero value and explicit empty logs remain valid.

### 2. The standalone validator unnecessarily depends on the original labels file

**Priority: P2 / Important.**

Location:

- `/Users/zhaoyu/.codex/visualizations/2026/10/22/01a162b5-d484-7d70-8b57-9711a9107b22/opbnb-activity/src/opbnb_activity/cli.py:39`

The offline command branch calls `load_labels(settings.labels or store.config.labels)` before checking whether the command is `validate`. Label paths loaded from TOML are absolute and retained in the manifest, so moving the dataset to a server without the old label file, or simply removing that external file, makes `validate --dataset ... --run-id ...` fail with `Error: FileNotFoundError`. Dataset integrity validation does not use labels and should remain available independently of them.

Focused reproduction: create a valid empty run while a labels CSV exists, remove only that external CSV, reopen the run, and compare the two validation entry points. `quality.validate(..., save=False)` returns `ok=True`, while the documented CLI `validate` returns exit 2 with `FileNotFoundError`. No data or manifest corruption is involved.

Required correction: dispatch `validate` immediately after opening the store, before loading labels/assets. Add a CLI regression with a missing configured label file. For `analyze/report`, keep explicit label provenance and make any continued reliance on external label files clear; do not silently change an attributed report to unknown labels.

## Other assessed areas

- **Recovery/idempotency:** deterministic range directories, checksums, identity-bound SQLite checkpoints, atomic finalization and recovery of a finalized directory without a checkpoint form a coherent resumable design. Corruption is detected and excluded from final analysis. Single-writer operation and absence of automatic reorg rollback are disclosed.
- **Run isolation:** collection identity includes window/timezone, system rules and partition size, with source mode and exact block range checked separately. Offline commands explicitly choose a run. Labels and assets have separate analysis versions.
- **Protocol classification:** the implementation requires deposit type plus configured L1-attributes sender and recipient to classify a system transaction. Ordinary deposits remain non-system; unknown historical recipient kind remains unknown.
- **Precision and missing fees:** raw amounts remain strings, reductions use Python integers, and scaling uses an adequately sized Decimal context. Full fees require both components; partial L2 and missing coverage remain separate. Failure fees are included.
- **Metric denominators:** direct-project grouping precedes sender deduplication; Unknown is separate from identified-project concentration; the top-1% sender share is computed against the full successful non-system population rather than the truncated sender table. Retention excludes immature and partial cohort/target days.
- **Packaging/CLI:** console and module entry points and packaged SQL/fixture declarations are present. The implementation report records the controller's installed-console exercise. This review did not duplicate that check.
- **Truthful coverage:** README/report distinguish synthetic data, unverified live coverage, absent historical code evidence, missing financial components and unsupported economic metrics. No live URL, real-node throughput or full-window performance claim was introduced.

No additional Minor issue is elevated in this review. Focused re-review should inspect the fixes and their narrow regression results; another blanket suite run is not necessary solely to repeat this review.

## Focused re-review after corrections — 2026-10-22

**Updated spec-compliance verdict: PASS for the approved offline implementation scope. Updated code-quality verdict: APPROVE. Both Important findings are resolved; no remaining Important defect was identified in the correction scope.** This verdict supersedes the original changes-required verdict above. Live-node behavior and full-window operating performance remain unverified, as already documented.

Reviewed the delta between `offline-review.diff` and `offline-review-fixed.diff`, covering the two production changes, their new regression tests and the README clarification. Read the appended red/green/full-suite evidence in `implementation-report.md`. No source edits or additional test executions were performed during re-review; the code paths and supplied focused regressions resolve the original doubts without repeating the suite.

- **Finding 1 resolved:** `models.py:33` now requires an explicit string/integer native value, rejects null, boolean, float, container, unparseable and negative values, and preserves explicit integer/hex zero. Receipt `logs` must be an explicit list of objects; an empty list remains valid. These checks occur before `normalize_block` returns anything to `Store.write_block`, so missing fields cannot complete a partition. Tests cover both missing fields with a single-block partition, malformed shapes, and zero/empty controls. Optional fee handling is unchanged.
- **Finding 2 resolved:** `cli.py:39` now dispatches `validate` immediately after opening the store, before external label loading and analysis asset selection. Regression tests move a valid dataset and either delete or corrupt its original labels CSV; validation succeeds, while both `analyze` and `report` still reject the unavailable/invalid labels. The README explains how to retain or replace the external labels path after migration, preserving explicit analysis provenance.
- **Reported verification:** focused regressions first produced **16 failures / 2 passes**, then **18 passes** after the fixes. The implementer reports **50 passed in 3.38s** for the full suite and compileall exit 0. These results are accurately described as implementation evidence, not reviewer reruns.

Previously stored data whose omissions were already normalized into zero/empty cannot be retrospectively repaired from those normalized tables; the appended implementation report correctly states that limitation. No real-chain data was collected in this task.
