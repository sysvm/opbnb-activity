import copy
import csv
import json

import duckdb
import pyarrow as arrow
import pytest

from conftest import address, block_hash, module


DAG_SENDER = "0x8eece8cb98dc9aaddb37a8177e4615469d771631"
DAG_RECIPIENT = "0xda90000000000000000000000000000000000000"
DAG_INPUT = "0x5517ed8c" + f"{32:064x}{1:064x}" + "c0" + "00" * 31


@pytest.mark.parametrize("target,field,value,expected", [
    ("transaction", "sender", DAG_SENDER, True),
    ("transaction", "sender", address(50), False),
    ("transaction", "recipient", address(51), False),
    ("transaction", "type", 2, False), ("transaction", "type", 126, False),
    ("transaction", "value_wei", "1", False),
    ("transaction", "transaction_index", 0, False),
    ("transaction", "input", "0x12345678" + DAG_INPUT[10:], False),
    ("transaction", "input", "0x5517ed8c", False),
    ("transaction", "input", DAG_INPUT[:-2] + "zz", False),
    ("transaction", "input", None, False),
    ("transaction", "input", "0x" + DAG_INPUT[2:].upper(), True),
    ("receipt", "effective_gas_price_wei", "1", False),
    ("receipt", "effective_gas_price_wei", None, False),
    ("receipt", "status", 0, False),
])
def test_python_and_sql_rules_agree_on_positive_and_missing_evidence(target, field, value, expected):
    transaction = {"sender": DAG_SENDER, "recipient": DAG_RECIPIENT, "type": 0,
        "value_wei": "0", "input": DAG_INPUT, "transaction_index": 5}
    receipt = {"effective_gas_price_wei": "0", "status": 1}
    source = transaction if target == "transaction" else receipt
    source[field] = value
    rules = module("system_transactions")
    assert rules.is_tx_dag(transaction, receipt, 6) is expected
    transaction_schema = arrow.schema([("sender", arrow.string()), ("recipient", arrow.string()),
        ("type", arrow.int64()), ("value_wei", arrow.string()), ("input", arrow.string()),
        ("transaction_index", arrow.int64())])
    receipt_schema = arrow.schema([("effective_gas_price_wei", arrow.string()), ("status", arrow.int64())])
    with duckdb.connect() as connection:
        connection.register("transactions", arrow.Table.from_pylist([transaction], schema=transaction_schema))
        connection.register("receipts", arrow.Table.from_pylist([receipt], schema=receipt_schema))
        found = connection.execute(f"""SELECT {rules.tx_dag_sql()} FROM transactions t, receipts r,
            (SELECT 6 AS transaction_count) b""").fetchone()[0]
    assert found is expected


def append_dag(synthetic):
    block = synthetic["blocks"][0]
    transaction = dict(block["transactions"][0], hash=block_hash(900001),
        transactionIndex=hex(len(block["transactions"])), **{
            "from": DAG_SENDER, "to": DAG_RECIPIENT, "input": DAG_INPUT, "type": "0x0", "value": "0x0"})
    receipt = dict(synthetic["receipts"][block["transactions"][0]["hash"]],
        transactionHash=transaction["hash"], transactionIndex=transaction["transactionIndex"],
        effectiveGasPrice="0x0", l1Fee="0x0", logs=[])
    block["transactions"].append(transaction)
    synthetic["receipts"][transaction["hash"]] = receipt
    return block, transaction, receipt


def test_normalization_recognizes_terminal_protocol_dag_without_excluding_user_deposits(synthetic):
    block, transaction, receipt = append_dag(synthetic)
    result = module("models").normalize_block(block, synthetic["receipts"], [
        "0x4200000000000000000000000000000000000015"])
    assert result["transactions"][-1]["is_system"] is True
    assert result["transactions"][-1]["classification"] == "system_tx_dag"
    assert result["transactions"][3]["classification"] == "system_l1_attributes"
    assert result["transactions"][4]["is_system"] is False
    assert result["transactions"][4]["classification"] == "user_deposit"


@pytest.mark.parametrize("target,field,value", [
    ("transaction", "from", address(50)), ("transaction", "to", address(51)),
    ("transaction", "type", "0x2"), ("transaction", "value", "0x1"),
    ("transaction", "input", "0x12345678" + DAG_INPUT[10:]),
    ("transaction", "input", "0x5517ed8c"),
    ("transaction", "input", DAG_INPUT[:-2] + "zz"),
    ("receipt", "effectiveGasPrice", "0x1"), ("receipt", "effectiveGasPrice", None),
    ("receipt", "status", "0x0"),
])
def test_dag_like_calls_with_missing_evidence_stay_non_system(synthetic, target, field, value):
    block, transaction, receipt = append_dag(synthetic)
    source = transaction if target == "transaction" else receipt
    source[field] = value
    result = module("models").normalize_block(block, synthetic["receipts"], [])
    assert result["transactions"][-1]["is_system"] is False


def test_known_dag_call_not_at_end_stays_non_system(synthetic):
    block, transaction, receipt = append_dag(synthetic)
    transaction.update(transactionIndex="0x0")
    receipt.update(transactionIndex="0x0")
    block["transactions"].pop()
    block["transactions"].insert(0, transaction)
    for position, item in enumerate(block["transactions"]):
        item["transactionIndex"] = hex(position)
        synthetic["receipts"][item["hash"]]["transactionIndex"] = hex(position)
    assert module("models").normalize_block(block, synthetic["receipts"], [])["transactions"][0]["is_system"] is False


def legacy_dag_store(tmp_path, synthetic):
    block, transaction, receipt = append_dag(synthetic)
    log = copy.deepcopy(synthetic["receipts"][block["transactions"][0]["hash"]]["logs"][0])
    log.update(transactionHash=transaction["hash"], logIndex="0x63")
    receipt["logs"] = [log]
    config = module("config").Config(dataset=tmp_path, run_id="legacy-dag", start="2026-10-01",
        end="2026-10-22", blocks_per_partition=2)
    store = module("storage").Store(config, 100, 106, synthetic=True)
    for block in synthetic["blocks"]:
        tables = module("models").normalize_block(block, synthetic["receipts"], config.system_recipients)
        for row in tables["transactions"]:
            if row["hash"] == transaction["hash"]:
                row.update(is_system=False, classification="unknown")
        store.write_block(int(block["number"], 16), tables)
    return store


def test_report_reclassifies_legacy_dag_without_rewriting_raw_data(tmp_path, synthetic):
    store = legacy_dag_store(tmp_path, synthetic)
    originals = {path: path.read_bytes() for path in store.root.rglob("*.parquet")}
    originals[store.manifest_path] = store.manifest_path.read_bytes()
    checkpoint = store.checkpoint.get(100)
    result = module("metrics").analyze(store, synthetic["labels"], synthetic["assets"])
    assert result["quality"]["ok"] is True
    assert result["summary"]["transactions"] == 12
    assert result["summary"]["system_transactions"] == 2
    assert result["summary"]["non_system_transactions"] == 10
    assert result["summary"]["unique_successful_senders"] == 5
    assert result["summary"]["native_value_wei"] == str(10**45 + 81)
    assert result["summary"]["verified_token_raw_amounts"][address(500)] == str(10**40 + 1)
    assert sum(row["log_count"] for row in result["events"]) == 2
    assert all(row["sender"] != DAG_SENDER for row in result["senders"] + result["sender_profiles"])
    assert all(row["address"] != DAG_RECIPIENT for row in result["contracts"] + result["recipient_details"])
    assert all(row["recipient"] != DAG_RECIPIENT for row in result["methods"])
    assert sum(row["transactions"] for row in result["projects"]) == 10
    assert [row["active_senders"] for row in result["weekly"]] == [2, 2, 1, 1]
    assert result["monthly"][0]["active_senders"] == 5
    assert all(path.read_bytes() == content for path, content in originals.items())
    assert store.checkpoint.get(100) == checkpoint


def test_report_exposes_dag_subtype_and_rule_provenance(tmp_path, synthetic):
    store = legacy_dag_store(tmp_path, synthetic)
    destination = module("report").generate(store, synthetic["labels"], synthetic["assets"])
    result = json.loads((store.root / "analysis.json").read_text())
    counts = {row["transaction_type"]: row["transactions"] for row in result["transaction_types"]}
    assert counts["system_tx_dag"] == counts["system_l1_attributes"] == 1
    assert sum(counts.values()) == 12
    groups = {row["fee_group"]: row for row in result["fee_groups"]}
    assert groups["system"]["transactions"] == 2
    assert groups["user_deposit"]["transactions"] == 1
    assert groups["regular"]["transactions"] == 9
    assert result["metadata"]["system_rules_version"] == "l1_attributes_and_known_txdag_v1"
    summary = (destination / "summary.md").read_text()
    technical = (destination / "technical.md").read_text()
    assert "system_tx_dag" in summary and "TxDAG" in summary
    assert "system_rules_version" in technical and DAG_RECIPIENT in technical
    with (destination / "weekly.csv").open(encoding="utf-8-sig") as handle:
        assert [int(row["active_senders"]) for row in csv.DictReader(handle)] == [2, 2, 1, 1]
    with (destination / "monthly.csv").open(encoding="utf-8-sig") as handle:
        assert next(csv.DictReader(handle))["active_senders"] == "5"
