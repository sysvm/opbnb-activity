import json
from datetime import datetime, timezone

import pytest

from conftest import address, block_hash, module


def test_normalization_keeps_uint256_and_distinguishes_user_deposit(synthetic):
    normalize = module("models").normalize_block
    tables = normalize(synthetic["blocks"][0], synthetic["receipts"], ["0x4200000000000000000000000000000000000015"])
    assert tables["transactions"][0]["value_wei"] == str(10**45 + 7)
    assert tables["transactions"][3]["is_system"] is True
    assert tables["transactions"][4]["is_system"] is False
    assert tables["transactions"][4]["classification"] == "user_deposit"
    assert tables["receipts"][1]["total_fee_wei"] is None
    assert tables["receipts"][1]["l2_fee_wei"] == "42000"
    assert tables["receipts"][2]["total_fee_wei"] == "42100"


def test_normalization_rejects_missing_or_wrong_receipts(synthetic):
    normalize = module("models").normalize_block
    receipts = dict(synthetic["receipts"])
    first_hash = synthetic["blocks"][0]["transactions"][0]["hash"]
    receipts.pop(first_hash)
    with pytest.raises(ValueError, match="receipt"):
        normalize(synthetic["blocks"][0], receipts, [])
    receipts[first_hash] = dict(synthetic["receipts"][first_hash], blockHash=block_hash(999))
    with pytest.raises(ValueError, match="receipt"):
        normalize(synthetic["blocks"][0], receipts, [])


@pytest.mark.parametrize("field", ["value", "logs"])
def test_normalization_rejects_missing_required_fields_before_completion(tmp_path, synthetic, field):
    config = module("config").Config(dataset=tmp_path, run_id="missing", blocks_per_partition=1)
    store = module("storage").Store(config, 100, 101, synthetic=True)
    block = synthetic["blocks"][0]
    transaction = block["transactions"][0]
    source = transaction if field == "value" else synthetic["receipts"][transaction["hash"]]
    source.pop(field)
    with pytest.raises(ValueError, match=field):
        store.write_block(100, module("models").normalize_block(block, synthetic["receipts"], config.system_recipients))
    assert store.completed(100) is False
    assert store.partition_dirs() == []


@pytest.mark.parametrize("value", [None, True, 1.5, [], {}, "", "0xzz", -1])
def test_normalization_rejects_invalid_value_shapes(synthetic, value):
    block = synthetic["blocks"][0]
    block["transactions"][0]["value"] = value
    with pytest.raises(ValueError, match="value"):
        module("models").normalize_block(block, synthetic["receipts"], [])


@pytest.mark.parametrize("logs", [None, {}, "", [None]])
def test_normalization_rejects_invalid_logs_shapes(synthetic, logs):
    block = synthetic["blocks"][0]
    synthetic["receipts"][block["transactions"][0]["hash"]]["logs"] = logs
    with pytest.raises(ValueError, match="logs"):
        module("models").normalize_block(block, synthetic["receipts"], [])


@pytest.mark.parametrize("value", ["0x0", 0])
def test_normalization_preserves_explicit_zero_and_empty_logs(synthetic, value):
    block = synthetic["blocks"][0]
    transaction = block["transactions"][0]
    transaction["value"] = value
    synthetic["receipts"][transaction["hash"]]["logs"] = []
    result = module("models").normalize_block(block, synthetic["receipts"], [])
    assert result["transactions"][0]["value_wei"] == "0"
    assert not any(log["transaction_hash"] == transaction["hash"] for log in result["logs"])


def test_transfer_decoder_never_confuses_nft_and_unverified_assets(synthetic):
    decode = module("decode").decode_transfer
    receipts = list(synthetic["receipts"].values())
    fungible = decode(receipts[0]["logs"][0], synthetic["assets"])
    nft = decode(receipts[1]["logs"][0], synthetic["assets"])
    assert fungible["kind"] == "verified_erc20"
    assert fungible["raw_amount"] == str(10**40 + 1)
    assert nft["kind"] == "nft_or_ambiguous"
    assert decode(receipts[0]["logs"][0], {})["kind"] == "unverified_transfer"
    assert fungible["amount"] == "10000000000000000000000.000000000000000001"


def test_contract_creation_and_selector_are_preserved(synthetic):
    block = dict(synthetic["blocks"][1])
    transaction = dict(block["transactions"][0], to=None)
    block["transactions"] = [transaction]
    receipts = dict(synthetic["receipts"])
    receipts[transaction["hash"]] = dict(receipts[transaction["hash"]], contractAddress=address(900))
    result = module("models").normalize_block(block, receipts, [])
    assert result["transactions"][0]["classification"] == "contract_creation"
    assert result["transactions"][0]["selector"] == "0x12345678"
    assert result["receipts"][0]["contract_address"] == address(900)


def test_standard_null_contract_address_is_valid(synthetic):
    block = synthetic["blocks"][1]
    tx_hash = block["transactions"][0]["hash"]
    receipts = dict(synthetic["receipts"])
    receipts[tx_hash] = dict(receipts[tx_hash], contractAddress=None)
    result = module("models").normalize_block(block, receipts, [])
    assert result["receipts"][0]["contract_address"] is None


def test_partition_commit_is_atomic_and_bounded(tmp_path, synthetic):
    config = module("config").Config(dataset=tmp_path, run_id="shards", blocks_per_partition=2)
    store = module("storage").Store(config, 100, 106, synthetic=True)
    normalize = module("models").normalize_block
    store.write_block(100, normalize(synthetic["blocks"][0], synthetic["receipts"], config.system_recipients))
    assert store.completed(100) is False
    assert store.partition_dirs() == []
    for block in synthetic["blocks"][1:]:
        store.write_block(int(block["number"], 16), normalize(block, synthetic["receipts"], config.system_recipients))
    assert len(store.partition_dirs()) == 3
    assert len(list(store.root.glob("data/????-??-??/*-*/*.parquet"))) == 12
    assert store.completed(101) is True
    assert module("quality").validate(store)["ok"] is True


def test_range_lower_bound_equality_irregular_and_incomplete():
    resolve = module("block_range").resolve_range
    times = [10, 10, 14, 25, 25, 40]
    getter = lambda number: {"number": hex(number), "timestamp": hex(times[number])}
    assert resolve(getter, 5, 10, 25) == (0, 3)
    assert resolve(getter, 5, 11, 26) == (2, 5)
    assert resolve(getter, 5, 15, 20) == (3, 3)
    with pytest.raises(ValueError, match="incomplete"):
        resolve(getter, 5, 20, 41)
    with pytest.raises(ValueError, match="historical"):
        resolve(getter, 5, 9, 25)


def test_config_identity_and_label_ranges(tmp_path):
    config = module("config")
    labels = module("labels")
    first = config.Config(dataset=tmp_path, run_id="one")
    assert first.start_utc.isoformat() == "2026-07-21T16:00:00+00:00"
    assert first.end_utc.isoformat() == "2026-10-21T16:00:00+00:00"
    assert first.identity != config.Config(dataset=tmp_path, run_id="one", end="2026-10-21").identity
    with pytest.raises(ValueError):
        config.Config(dataset=tmp_path, run_id="../escape")
    index = labels.LabelIndex([{"address": address(1), "project": "old", "valid_from": "0", "valid_to": "10"},
                              {"address": address(1), "project": "new", "valid_from": "10", "valid_to": ""}])
    assert index.lookup(address(1), 9)["project"] == "old"
    assert index.lookup(address(1), 10)["project"] == "new"
    assert index.lookup(address(2), 10)["project"] == "Unknown"


def test_store_atomic_resume_identity_and_quality_gate(tmp_path, synthetic):
    config = module("config").Config(dataset=tmp_path, run_id="test")
    Store = module("storage").Store
    normalize = module("models").normalize_block
    quality = module("quality")
    store = Store(config, start_block=100, end_block=106, synthetic=True)
    for block in synthetic["blocks"]:
        tables = normalize(block, synthetic["receipts"], config.system_recipients)
        store.write_block(int(block["number"], 16), tables)
    assert quality.validate(store)["ok"] is True
    assert store.completed(100)
    store.checkpoint.connection.execute("DELETE FROM tasks WHERE block_number=100")
    store.checkpoint.connection.commit()
    assert store.recover(100) is True
    assert len(store.read_table("transactions")) == 11
    path = store.block_dir(100) / "transactions.parquet"
    path.write_bytes(b"corrupt")
    assert store.completed(100) is False
    assert quality.validate(store)["ok"] is False
    with pytest.raises(ValueError, match="identity"):
        Store(module("config").Config(dataset=tmp_path, run_id="test", end="2026-10-21"), 100, 106)


def test_quality_rejects_parent_mismatch_and_report_refuses_incomplete(tmp_path, synthetic):
    config = module("config").Config(dataset=tmp_path, run_id="bad")
    store = module("storage").Store(config, 100, 106, synthetic=True)
    normalize = module("models").normalize_block
    for block in synthetic["blocks"]:
        modified = dict(block)
        if block["number"] == "0x65":
            modified["parentHash"] = block_hash(999)
        store.write_block(int(block["number"], 16), normalize(modified, synthetic["receipts"], config.system_recipients))
    result = module("quality").validate(store)
    assert result["ok"] is False
    assert any("parent" in error for error in result["errors"])
    with pytest.raises(ValueError, match="quality"):
        module("report").generate(store, [], {})
