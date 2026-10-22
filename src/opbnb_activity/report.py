import csv
import json
import os
from pathlib import Path

from .metrics import analyze


def generate(store, labels, assets):
    result = analyze(store, labels, assets)
    destination = store.root / "report"
    destination.mkdir(exist_ok=True)
    for name in ("daily", "contracts", "projects", "senders", "weekly", "monthly", "retention"):
        rows = result[name]
        fields = list(rows[0]) if rows else {"contracts": ["address", "project", "role", "category", "transactions", "successful_transactions", "successful_senders"],
                                           "projects": ["project", "transactions", "successful_transactions", "failed_transactions", "successful_senders", "direct_recipients"],
                                           "senders": ["sender", "transactions", "successful_transactions", "failed_transactions", "active_days"],
                                           "weekly": ["period_start", "active_senders", "all_non_system_senders"],
                                           "monthly": ["period_start", "active_senders", "all_non_system_senders"],
                                           "retention": ["cohort_day", "lag_days", "cohort_size", "retained", "rate"]}.get(name, ["day", "transactions", "active_senders"])
        with (destination / (name + ".csv")).open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)
    metadata, summary = result["metadata"], result["summary"]
    source = "SYNTHETIC / TEST DATA — 合成测试数据，非主网统计" if metadata["synthetic"] else "RPC collected dataset — 来源节点未独立认证"
    text = f"""# opBNB activity report

**{source}**

Run: `{metadata['run_id']}`. Dataset: `..` relative to this report. Timezone: **{metadata['timezone']}**.
UTC window: **[{metadata['start_utc']}, {metadata['end_utc']})**; blocks **[{store.start_block}, {store.end_block})**.
Dataset version: `{metadata['dataset_version']}`. Labels version: `{metadata['labels_version']}`.
Assets version: `{metadata['assets_version']}`. Quality: **PASS** for declared manifest range.

## Activity

| Metric | Value |
|---|---:|
| Transactions (all) | {summary['transactions']} |
| System L1 attributes transactions | {summary['system_transactions']} |
| Successful non-system transactions | {summary['successful_non_system_transactions']} |
| Failed non-system transactions | {summary['failed_non_system_transactions']} |
| Unique successful non-system sending addresses | {summary['unique_successful_senders']} |
| Unique all non-system sending addresses | {summary['unique_all_non_system_senders']} |
| Unknown project transactions (non-system) | {summary['unknown_project_transactions']} |
| Unknown historical recipient kind transactions | {summary['unknown_recipient_kind_transactions']} |
| Known project coverage | {summary['known_project_coverage']} |
| Full fee coverage | {summary['full_fee_coverage']} |
| L2 fee coverage | {summary['l2_fee_coverage']} |
| Unknown total-fee transactions | {summary['unknown_total_fee_transactions']} |
| Known total fees (wei, includes failures) | {summary['known_total_fee_wei']} |
| Partial L2 fees (wei, includes failures) | {summary['partial_l2_fee_wei']} |
| Successful top-level native value (wei) | {summary['native_value_wei']} |
| Successful top-level native value (BNB) | {summary['native_value_bnb']} |
| Ambiguous/NFT-like Transfer logs excluded | {summary['ambiguous_transfer_logs']} |
| Unverified Transfer logs excluded | {summary['unverified_transfer_logs']} |

Verified fungible token raw sums: `{json.dumps(summary['verified_token_raw_amounts'], sort_keys=True)}`.
Verified fungible token scaled sums: `{json.dumps(summary['verified_token_amounts'], sort_keys=True)}`.
Identified-project contributions / all non-system transactions: top-1={summary['project_top1_share']}, top-5={summary['project_top5_share']}, top-10={summary['project_top10_share']}, HHI={summary['project_hhi']}. Unknown share={summary['unknown_project_share']} is separate and is not treated as one project.
Top 1% successful senders (ceil, {summary['top1_percent_sender_count']} addresses) contribute {summary['top1_percent_sender_successful_tx_share']} of all successful non-system transactions; calculated from the full dataset.

## Definitions and limits

- Main active measure: distinct sending addresses of successful non-system transactions in each local calendar day; supplemental measure includes failed non-system senders. Addresses are not people.
- `daily.csv` includes exact successful top-level native value, known full fees, partial L2 fees, missing counts and fee coverage; zero-transaction days have undefined fee coverage.
- Whole-window, weekly (Monday start), monthly addresses deduplicate within each period. Partial edge periods are retained. Contract/project successful senders deduplicate after direct-recipient grouping.
- Project ranking uses direct calls only: at most one project per transaction, failed calls included, system calls excluded. Unknown remains visible. Internal calls, proxy implementation ownership and indirect routing are not inferred.
- `contracts.csv` ranks direct recipients; unknown historical code means the recipient may be a contract or EOA. Empty input never proves EOA. Technical/business categories are only supplied labels; unsupported classifications remain unknown.
- System identification requires deposit type 0x7e AND exact configured L1 attributes depositor AND configured protocol recipient. User deposits remain non-system.
- D1/D7 retention uses each address's first observed successful non-system day within this window as cohort; return is a successful non-system transaction on that exact later local day. Only complete, mature cohort/target days are included. Prior history before the window is unknown.
- Native value includes successful non-system top-level `value` only; it is not economic trading volume and may include user deposits or self-transfers. No internal native transfers are measured.
- Token amounts require explicitly verified fungible address and supplied decimals; three-topic Transfer events only. NFT-like four-topic or ambiguous events are excluded. Transfer totals are event movement, not economic volume.
- Full fee = observed L2 gasUsed × effectiveGasPrice + observed l1Fee only when BOTH components exist; unknown L1 never becomes zero. Known subtotal is a partial-coverage sum. Failure fees are included where fields are known. No unverified total-fee fields are trusted.
- Unsupported: prices, USD value, TVL, DEX/bridge volume, bots, human-user counts, trace-based attribution and comprehensive business-type classification.
- Synthetic sparse chain blocks simulate irregular timestamps. Zero days in a synthetic run describe the fixture only. Capability samples do not establish live throughput or provider coverage.

## Tables and charts

`daily.csv`, `contracts.csv`, `projects.csv`, `senders.csv`, `weekly.csv`, `monthly.csv`, `retention.csv`.

![Daily activity](daily.png)
![Top projects](projects.png)
"""
    (destination / "summary.md").write_text(text, encoding="utf-8")
    os.environ.setdefault("MPLCONFIGDIR", str(store.root / ".matplotlib"))
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plot
    prefix = "SYNTHETIC / TEST DATA | " if metadata["synthetic"] else "RPC dataset | "
    figure, axes = plot.subplots(figsize=(11, 4.5), layout="constrained")
    daily = result["daily"]
    axes.plot([row["day"] for row in daily], [row["active_senders"] for row in daily], marker="o", label="Successful non-system senders")
    axes.plot([row["day"] for row in daily], [row["transactions"] for row in daily], alpha=0.65, label="All transactions")
    axes.set_title(prefix + "Daily activity")
    axes.set_xlabel("Local date (" + store.config.timezone + ")")
    axes.set_ylabel("Count")
    axes.legend()
    axes.tick_params(axis="x", rotation=45)
    if len(daily) > 12:
        axes.set_xticks(range(0, len(daily), max(1, len(daily) // 10)))
    figure.savefig(destination / "daily.png", dpi=140)
    plot.close(figure)
    figure, axes = plot.subplots(figsize=(9, 4.5), layout="constrained")
    projects = result["projects"][:10]
    if projects:
        axes.barh([row["project"] for row in reversed(projects)], [row["transactions"] for row in reversed(projects)])
    else:
        axes.text(0.5, 0.5, "No transactions in valid window", ha="center", transform=axes.transAxes)
    axes.set_title(prefix + "Top direct-call projects")
    axes.set_xlabel("Non-system transactions (failures included)")
    figure.savefig(destination / "projects.png", dpi=140)
    plot.close(figure)
    return destination
