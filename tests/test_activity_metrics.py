from collections import Counter
from datetime import datetime, timezone
from types import SimpleNamespace

import duckdb
import pytest

from conftest import address, module


def analyze_fixture(tmp_path, synthetic, labels=None):
    config = module("config").Config(dataset=tmp_path, run_id="metrics", start="2026-10-01")
    store = module("storage").Store(config, 100, 106, synthetic=True)
    for block in synthetic["blocks"]:
        tables = module("models").normalize_block(block, synthetic["receipts"], config.system_recipients)
        store.write_block(int(block["number"], 16), tables)
    return module("metrics").analyze(store, synthetic["labels"] if labels is None else labels, synthetic["assets"])


def test_non_system_denominators_and_zero_days(tmp_path, synthetic):
    result = analyze_fixture(tmp_path, synthetic)
    summary = result["summary"]
    assert summary["non_system_transactions"] == 10
    assert summary["non_system_failure_rate"] == 0.2
    assert summary["system_share"] == 1 / 11
    daily = result["daily"][0]
    assert daily["non_system_transactions"] == 4
    assert daily["successful_non_system_transactions"] == 3
    assert daily["failed_non_system_transactions"] == 1
    assert daily["non_system_failure_rate"] == 0.25
    assert daily["system_share"] == 0.2
    empty = result["daily"][2]
    assert empty["non_system_transactions"] == 0
    assert empty["non_system_failure_rate"] is None
    assert empty["system_share"] is None


def test_fee_groups_preserve_deposit_unknowns_and_exact_money(tmp_path, synthetic):
    synthetic["receipts"][synthetic["blocks"][0]["transactions"][0]["hash"]]["l1Fee"] = hex(10**50 + 3)
    result = analyze_fixture(tmp_path, synthetic)
    groups = {row["fee_group"]: row for row in result["fee_groups"]}
    assert {name: row["transactions"] for name, row in groups.items()} == {"system": 1, "user_deposit": 1, "regular": 9}
    assert groups["system"]["known_total_fee_wei"] == "0"
    assert groups["user_deposit"]["known_total_fee_transactions"] == 0
    assert groups["user_deposit"]["missing_total_fee_transactions"] == 1
    assert groups["user_deposit"]["known_total_fee_wei"] is None
    assert groups["user_deposit"]["total_fee_coverage"] == 0
    assert groups["regular"]["known_total_fee_wei"] == str(10**50 + 336703)
    assert groups["regular"]["known_total_fee_bnb"] == "100000000000000000000000000000000.000000000000336703"
    assert groups["regular"]["total_fee_coverage"] == 8 / 9
    assert groups["regular"]["known_l2_fee_wei"] == "378000"


def test_technical_types_and_methods_count_each_transaction(tmp_path, synthetic):
    transactions = [transaction for block in synthetic["blocks"] for transaction in block["transactions"]]
    transactions[0]["input"] = "0x"
    transactions[1]["input"] = "0x12"
    transactions[2]["to"] = None
    result = analyze_fixture(tmp_path, synthetic)
    counts = {row["transaction_type"]: row["transactions"] for row in result["transaction_types"]}
    assert counts == {"system_l1_attributes": 1, "user_deposit": 1, "contract_creation": 1,
                      "empty_calldata": 1, "short_calldata": 1, "selector_call": 6}
    assert all(row["total_transactions"] == 11 for row in result["transaction_types"])
    assert sum(row["transactions"] for row in result["methods"]) == 10
    assert any(row["selector"] == "0x" and row["transactions"] == 1 for row in result["methods"])
    assert any(row["selector"] is None and row["transactions"] == 1 for row in result["methods"])
    assert all(not isinstance(value, (dict, list)) for name in ("transaction_types", "methods", "fee_groups")
               for row in result[name] for value in row.values())


def test_hourly_counts_and_daily_active_senders(tmp_path, synthetic):
    result = analyze_fixture(tmp_path, synthetic)
    first = result["hourly"][0]
    assert first["hour_utc"] == "2026-10-01T04:00:00+00:00"
    assert first["hour"] == "2026-10-01T12:00:00+08:00"
    assert first["transactions"] == 5
    assert first["active_senders"] == 2
    assert first["non_system_transactions"] == 4


def test_concentrations_rank_addresses_once_despite_label_changes(tmp_path, synthetic):
    labels = list(synthetic["labels"])
    labels[0] = dict(labels[0], valid_to="103")
    labels.append(dict(labels[0], valid_from="103", valid_to="", project="Renamed"))
    result = analyze_fixture(tmp_path, synthetic, labels)
    recipients = {row["top_n"]: row for row in result["recipient_concentration"]}
    senders = {row["top_n"]: row for row in result["sender_concentration"]}
    assert recipients[1] == {"top_n": 1, "transactions": 4, "share": 0.4}
    assert recipients[5]["transactions"] == 10
    assert senders[1]["transactions"] == 4
    assert senders[1]["share"] == 0.4
    breakdown = result["sender_breakdown"]
    sender_rows = [row for row in breakdown if row["sender"] == address(1) and row["dimension"] == "recipient"]
    assert sum(row["transactions"] for row in sender_rows) == 4
    assert all(row["total_transactions"] == 4 for row in sender_rows)
    assert {row["key"]: row["share"] for row in sender_rows}[address(102)] == 0.5


def test_period_coverage_includes_days_without_transactions(tmp_path, synthetic):
    result = analyze_fixture(tmp_path, synthetic)
    assert result["weekly"][0]["observed_days"] == 4
    assert result["weekly"][0]["expected_days"] == 7
    assert result["weekly"][0]["is_partial"] is True
    assert result["weekly"][1]["observed_days"] == 7
    assert result["weekly"][1]["is_partial"] is False
    assert result["monthly"][0]["observed_days"] == 21
    assert result["monthly"][0]["expected_days"] == 31
    assert result["monthly"][0]["is_partial"] is True


def test_unknown_project_concentration_and_empty_window(tmp_path):
    config = module("config").Config(dataset=tmp_path, run_id="empty", start="2026-10-01")
    store = module("storage").Store(config, 100, 100, synthetic=True)
    result = module("metrics").analyze(store, [], {})
    assert result["summary"]["project_top1_share"] is None
    assert result["summary"]["project_hhi"] is None
    assert result["summary"]["non_system_failure_rate"] is None
    assert result["transaction_types"] == []
    assert all(row["share"] is None for row in result["recipient_concentration"])
    assert all(row["transactions"] == 0 for row in result["fee_groups"])
    assert result["weekly"][1]["observed_days"] == 7


def augment_rows(rows, start, end, zone="UTC"):
    import pyarrow as arrow

    schema = arrow.schema([(name, kind) for name, kind in [
        ("timestamp", arrow.int64()), ("sender", arrow.string()), ("recipient", arrow.string()),
        ("selector", arrow.string()), ("input", arrow.string()), ("type", arrow.int64()),
        ("is_system", arrow.bool_()), ("status", arrow.int64()), ("day", arrow.string()),
        ("l1_fee_wei", arrow.string()), ("l2_fee_wei", arrow.string()), ("total_fee_wei", arrow.string())]])
    connection = duckdb.connect()
    connection.register("activity", arrow.Table.from_pylist(rows, schema=schema))
    store = SimpleNamespace(config=SimpleNamespace(timezone=zone), manifest={"start_utc": start, "end_utc": end})
    result = {"daily": [], "weekly": [], "monthly": [], "summary": {"transactions": len(rows)}}
    try:
        module("activity_metrics").augment(connection, result, store)
        return result
    finally:
        connection.close()


def activity_row(sender, recipient, timestamp, **changes):
    return dict(timestamp=timestamp, sender=sender, recipient=recipient, selector="0x12345678", input="0x12345678",
                type=2, is_system=False, status=1, day=datetime.fromtimestamp(timestamp, timezone.utc).date().isoformat(),
                l1_fee_wei=None, l2_fee_wei=None, total_fee_wei=None, **changes)


def test_sender_breakdown_limits_each_dimension_and_keeps_full_denominator():
    timestamp = int(datetime.fromisoformat("2026-10-01T00:00:00+00:00").timestamp())
    rows = [activity_row(address(sender), address(sender * 1000 + offset), timestamp + offset * 3600)
            for sender in range(1, 23) for offset in range(25)]
    result = augment_rows(rows, "2026-10-01T00:00:00+00:00", "2026-10-03T00:00:00+00:00")
    counts = Counter((row["sender"], row["dimension"]) for row in result["sender_breakdown"])
    assert len({row["sender"] for row in result["sender_breakdown"]}) == 20
    assert max(counts.values()) == 20
    assert len(result["sender_breakdown"]) <= 20 * 3 * 20
    assert all(row["total_transactions"] == 25 for row in result["sender_breakdown"])
    assert all(row["share"] == 1 / 25 for row in result["sender_breakdown"] if row["dimension"] == "recipient")
    assert {row["sender"] for row in result["sender_breakdown"]} == {address(sender) for sender in range(1, 21)}
    assert len(result["methods"]) == 100


@pytest.mark.parametrize("start,end", [("2026-10-05T00:00:01+00:00", "2026-10-12T00:00:00+00:00"),
                                      ("2026-10-05T00:00:00+00:00", "2026-10-11T23:59:59+00:00")])
def test_period_partial_uses_actual_timestamp_boundaries(start, end):
    result = augment_rows([], start, end)
    assert result["weekly"][0]["observed_days"] == 7
    assert result["weekly"][0]["is_partial"] is True


def test_hourly_distinguishes_repeated_dst_hour():
    timestamps = [int(datetime.fromisoformat(value).timestamp()) for value in
                  ("2026-11-01T05:30:00+00:00", "2026-11-01T06:30:00+00:00")]
    rows = [activity_row(address(1), address(101), timestamp) for timestamp in timestamps]
    result = augment_rows(rows, "2026-11-01T00:00:00-04:00", "2026-11-02T00:00:00-05:00", "America/New_York")
    assert [row["hour"] for row in result["hourly"]] == ["2026-11-01T01:00:00-04:00", "2026-11-01T01:00:00-05:00"]


def test_unknown_projects_keep_concentration_unknown(tmp_path, synthetic):
    result = analyze_fixture(tmp_path, synthetic, [])
    assert result["summary"]["project_top1_share"] is None
    assert result["summary"]["project_top5_share"] is None
    assert result["summary"]["project_top10_share"] is None
    assert result["summary"]["project_hhi"] is None


def test_recipient_concentration_ranks_addresses_and_keeps_creations_in_denominator():
    timestamp = int(datetime.fromisoformat("2026-10-01T00:00:00+00:00").timestamp())
    rows = [activity_row(address(1), None, timestamp) for _ in range(5)]
    rows.extend(activity_row(address(1), address(101), timestamp) for _ in range(2))
    result = augment_rows(rows, "2026-10-01T00:00:00+00:00", "2026-10-02T00:00:00+00:00")
    assert result["recipient_concentration"][0] == {"top_n": 1, "transactions": 2, "share": 2 / 7}
    assert result["recipient_concentration"][1]["transactions"] == 2


@pytest.mark.parametrize("calldata,stored_selector,expected_selector", [
    ("not-a-hex-input", "not-a-hex-", None),
    (None, "0x12345678", None),
    ("0x12", "0x12345678", None),
    ("0xABCDEF1200", "0x12345678", "0xabcdef12"),
])
def test_method_selectors_derive_only_from_valid_calldata(calldata, stored_selector, expected_selector):
    timestamp = int(datetime.fromisoformat("2026-10-01T00:00:00+00:00").timestamp())
    row = activity_row(address(1), address(101), timestamp)
    row.update(input=calldata, selector=stored_selector)
    result = augment_rows([row], "2026-10-01T00:00:00+00:00", "2026-10-02T00:00:00+00:00")
    assert result["methods"][0]["selector"] == expected_selector
    selectors = [entry["key"] for entry in result["sender_breakdown"] if entry["dimension"] == "selector"]
    assert selectors == [expected_selector]
