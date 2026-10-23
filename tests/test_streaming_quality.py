import json
import shutil

import pyarrow as arrow
import pyarrow.parquet as parquet
import pytest

from conftest import block_hash, module


def populated_store(tmp_path, synthetic, partition_size=6):
    config = module("config").Config(dataset=tmp_path, run_id="typed-quality", blocks_per_partition=partition_size)
    store = module("storage").Store(config, 100, 106, synthetic=True)
    for block in synthetic["blocks"]:
        tables = module("models").normalize_block(block, synthetic["receipts"], config.system_recipients)
        store.write_block(int(block["number"], 16), tables)
    return store


def rewrite_table(store, name, mutate, start=100, changed_schema=None):
    storage = module("storage")
    directory = store.block_dir(start)
    output = directory / (name + ".parquet")
    rows = parquet.ParquetFile(output).read().to_pylist()
    mutate(rows)
    parquet.write_table(arrow.Table.from_pylist(rows, schema=changed_schema or storage.schema(name)), output)
    metadata_path = directory / "_partition.json"
    metadata = json.loads(metadata_path.read_text())
    metadata["files"][name] = {"rows": len(rows), "sha256": storage.digest(output)}
    storage.atomic_json(metadata_path, metadata)
    store.checkpoint.complete(metadata["start_block"], metadata["end_block"],
                              str(directory.relative_to(store.root)), storage.digest(metadata_path))


@pytest.mark.parametrize("legacy", [False, True])
def test_quality_uses_bounded_typed_batches_without_fixture_readers(tmp_path, synthetic, monkeypatch, legacy):
    store = populated_store(tmp_path, synthetic)
    if legacy:
        current = store.block_dir(100)
        target = store.root / "data" / ("date=" + current.parent.name) / "blocks=000000000100-000000000106"
        target.parent.mkdir(parents=True)
        current.rename(target)
        task = store.checkpoint.get(100)
        store.checkpoint.complete(100, 106, str(target.relative_to(store.root)), task["checksum"])
    observed = []
    original = parquet.ParquetFile.iter_batches

    def typed_batches(reader, batch_size=65536, **options):
        observed.append(batch_size)
        assert batch_size <= 4096
        for batch in original(reader, batch_size=batch_size, **options):
            assert isinstance(batch, arrow.RecordBatch)
            assert batch.num_rows <= 4096
            yield batch

    def forbidden(*arguments, **options):
        raise AssertionError("whole-partition reader used")

    monkeypatch.setattr(parquet.ParquetFile, "iter_batches", typed_batches)
    monkeypatch.setattr(parquet.ParquetFile, "read", forbidden)
    monkeypatch.setattr(store, "iter_partitions", forbidden)
    monkeypatch.setattr(store, "read_table", forbidden)
    result = module("quality").validate(store)
    assert result["ok"] is True, result["errors"]
    assert result["counts"] == {"blocks": 6, "transactions": 11, "receipts": 11, "logs": 2}
    assert observed and set(observed) == {4096}
    assert json.loads((store.root / "quality.json").read_text()) == result


def test_quality_rejects_wrong_schema_after_checksum_refresh(tmp_path, synthetic):
    store = populated_store(tmp_path, synthetic)
    fields = list(module("storage").schema("blocks"))
    fields[0] = arrow.field("number", arrow.int32())
    rewrite_table(store, "blocks", lambda rows: None, changed_schema=arrow.schema(fields))
    result = module("quality").validate(store)
    assert result["ok"] is False
    assert "typed schema mismatch blocks" in result["errors"]
    assert result["counts"] == {"blocks": 6, "transactions": 11, "receipts": 11, "logs": 2}


@pytest.mark.parametrize("name,field", [
    ("blocks", "hash"), ("blocks", "number"), ("blocks", "timestamp"),
    ("transactions", "sender"), ("transactions", "value_wei"),
    ("receipts", "status"), ("logs", "address"), ("logs", "topics"),
])
def test_quality_rejects_required_nulls(tmp_path, synthetic, name, field):
    store = populated_store(tmp_path, synthetic)
    rewrite_table(store, name, lambda rows: rows[0].update({field: None}))
    result = module("quality").validate(store)
    assert result["ok"] is False
    assert "null required values " + name in result["errors"]


def test_quality_accepts_normalized_optional_nulls_and_exact_amounts(tmp_path, synthetic):
    store = populated_store(tmp_path, synthetic)
    rewrite_table(store, "transactions", lambda rows: rows[0].update(recipient=None, selector=None))
    rewrite_table(store, "receipts", lambda rows: rows[0].update(
        gas_used=None, effective_gas_price_wei=None, l2_fee_wei=None, l1_fee_wei=None, total_fee_wei=None))
    result = module("quality").validate(store)
    assert result["ok"] is True, result["errors"]


@pytest.mark.parametrize("rows", [0, 999, True, 6.0])
def test_quality_rejects_dishonest_or_untyped_metadata_rows(tmp_path, synthetic, rows):
    store = populated_store(tmp_path, synthetic)
    directory = store.block_dir(100)
    metadata_path = directory / "_partition.json"
    metadata = json.loads(metadata_path.read_text())
    metadata["files"]["blocks"]["rows"] = rows
    module("storage").atomic_json(metadata_path, metadata)
    result = module("quality").validate(store)
    assert result["ok"] is False
    assert "metadata row count mismatch blocks" in result["errors"]


@pytest.mark.parametrize("name,mutate,diagnostic", [
    ("blocks", lambda rows: rows.append(dict(rows[-1])), "missing/duplicate/out-of-order block"),
    ("blocks", lambda rows: rows.pop(2), "missing/duplicate/out-of-order block"),
    ("blocks", lambda rows: rows[0].update(number=99), "manifest block range incomplete or unexpected partitions"),
    ("blocks", lambda rows: rows.reverse(), "missing/duplicate/out-of-order block"),
    ("blocks", lambda rows: rows[1].update(parent_hash=block_hash(999)), "parent hash discontinuity"),
    ("blocks", lambda rows: rows[1].update(timestamp=1), "block timestamp outside manifest window"),
    ("blocks", lambda rows: rows[0].update(transaction_count=99), "block transaction count mismatch"),
    ("transactions", lambda rows: rows[0].update(block_number=999), "transaction block hash/timestamp mismatch"),
    ("transactions", lambda rows: rows[0].update(block_hash=block_hash(999)), "transaction block hash/timestamp mismatch"),
    ("transactions", lambda rows: rows[0].update(timestamp=1), "transaction block hash/timestamp mismatch"),
    ("transactions", lambda rows: rows.append(dict(rows[0])), "duplicate transaction/receipt hash"),
    ("receipts", lambda rows: rows.pop(), "receipt joins do not cover transactions"),
    ("receipts", lambda rows: rows.append(dict(rows[0], transaction_hash=block_hash(999))), "receipt joins do not cover transactions"),
    ("receipts", lambda rows: rows.append(dict(rows[0])), "duplicate transaction/receipt hash"),
    ("receipts", lambda rows: rows[0].update(transaction_index=999), "receipt hash/index mismatch"),
    ("receipts", lambda rows: rows[0].update(block_hash=block_hash(999)), "receipt hash/index mismatch"),
    ("receipts", lambda rows: rows[0].update(block_number=999), "receipt hash/index mismatch"),
    ("logs", lambda rows: rows[0].update(transaction_hash=block_hash(999)), "log hash/join/duplicate mismatch"),
    ("logs", lambda rows: rows[0].update(block_hash=block_hash(999)), "log hash/join/duplicate mismatch"),
    ("logs", lambda rows: rows[0].update(block_number=999), "log hash/join/duplicate mismatch"),
    ("logs", lambda rows: rows.append(dict(rows[0])), "log hash/join/duplicate mismatch"),
])
def test_quality_preserves_consistency_checks_after_checksum_refresh(tmp_path, synthetic, name, mutate, diagnostic):
    store = populated_store(tmp_path, synthetic)
    rewrite_table(store, name, mutate)
    result = module("quality").validate(store)
    assert result["ok"] is False
    assert diagnostic in result["errors"]


def test_quality_rejects_cross_partition_transaction_and_receipt_duplicates(tmp_path, synthetic):
    store = populated_store(tmp_path, synthetic, partition_size=2)
    first_hash = synthetic["blocks"][0]["transactions"][0]["hash"]
    rewrite_table(store, "transactions", lambda rows: rows[0].update(hash=first_hash), start=102)
    rewrite_table(store, "receipts", lambda rows: rows[0].update(transaction_hash=first_hash), start=102)
    result = module("quality").validate(store)
    assert result["ok"] is False
    assert "cross-partition duplicate transactions" in result["errors"]
    assert "cross-partition duplicate receipts" in result["errors"]


def test_quality_requires_final_coverage_and_ignores_private_fragments(tmp_path, synthetic):
    store = populated_store(tmp_path, synthetic)
    directory = store.block_dir(100)
    private = store.root / ".staging" / "100-106" / "100-106"
    private.parent.mkdir(parents=True)
    shutil.move(str(directory), str(private))
    result = module("quality").validate(store)
    assert result["ok"] is False
    assert result["counts"] == {"blocks": 0, "transactions": 0, "receipts": 0, "logs": 0}
    assert "missing/corrupt/incomplete partition [100,106)" in result["errors"]


def test_quality_converts_connection_errors_to_safe_types(tmp_path, synthetic, monkeypatch):
    store = populated_store(tmp_path, synthetic)

    def fail_connection():
        raise RuntimeError("private credential from query engine")

    monkeypatch.setattr(store, "connection", fail_connection)
    result = module("quality").validate(store)
    assert result["ok"] is False
    assert "cannot read typed partitions: RuntimeError" in result["errors"]
    assert "private credential" not in json.dumps(result)


def test_empty_window_has_valid_zero_counts(tmp_path):
    config = module("config").Config(dataset=tmp_path, run_id="typed-empty")
    store = module("storage").Store(config, 100, 100, synthetic=True)
    result = module("quality").validate(store)
    assert result["ok"] is True
    assert result["counts"] == {"blocks": 0, "transactions": 0, "receipts": 0, "logs": 0}


@pytest.mark.parametrize("fault", [None, "parent", "order"])
def test_quality_keeps_continuity_across_record_batches(tmp_path, synthetic, monkeypatch, fault):
    config = module("config").Config(dataset=tmp_path, run_id="batch-boundary", blocks_per_partition=5000)
    store = module("storage").Store(config, 100, 4200, synthetic=True)
    template = module("models").normalize_block(synthetic["blocks"][0], synthetic["receipts"], config.system_recipients)["blocks"][0]
    tables = {name: [] for name in module("models").TABLES}
    for number in range(100, 4200):
        tables["blocks"].append(dict(template, number=number, hash=block_hash(number),
                                     parent_hash=block_hash(number - 1), transaction_count=0))
    if fault == "parent":
        tables["blocks"][4096]["parent_hash"] = block_hash(999)
    if fault == "order":
        tables["blocks"][4095], tables["blocks"][4096] = tables["blocks"][4096], tables["blocks"][4095]
    store.commit_partition(100, 4200, tables)
    observed_rows = []
    original = parquet.ParquetFile.iter_batches

    def inspect_batches(reader, batch_size=65536, **options):
        for batch in original(reader, batch_size=batch_size, **options):
            assert batch_size == 4096
            observed_rows.append(batch.num_rows)
            yield batch

    monkeypatch.setattr(parquet.ParquetFile, "iter_batches", inspect_batches)
    result = module("quality").validate(store, save=False)
    assert result["ok"] is (fault is None), result["errors"]
    assert 4096 in observed_rows and 4 in observed_rows
    assert result["counts"]["blocks"] == 4100
    if fault == "parent":
        assert "parent hash discontinuity" in result["errors"]
    if fault == "order":
        assert "missing/duplicate/out-of-order block" in result["errors"]
    assert not (store.root / "quality.json").exists()


def test_quality_checks_metadata_interval_even_when_global_blocks_are_contiguous(tmp_path, synthetic):
    store = populated_store(tmp_path, synthetic, partition_size=2)
    rewrite_table(store, "blocks", lambda rows: rows.pop(), start=100)
    second = module("models").normalize_block(synthetic["blocks"][1], synthetic["receipts"], store.config.system_recipients)["blocks"][0]
    rewrite_table(store, "blocks", lambda rows: rows.insert(0, second), start=102)
    result = module("quality").validate(store)
    assert result["ok"] is False
    assert result["counts"]["blocks"] == 6
    assert "manifest block range incomplete or unexpected partitions" in result["errors"]


def test_quality_rejects_log_join_to_transaction_in_another_partition(tmp_path, synthetic):
    store = populated_store(tmp_path, synthetic, partition_size=2)
    rewrite_table(store, "logs", lambda rows: rows.pop(0), start=100)
    transaction = module("models").normalize_block(synthetic["blocks"][0], synthetic["receipts"], store.config.system_recipients)["logs"][0]
    rewrite_table(store, "logs", lambda rows: rows.append(transaction), start=102)
    result = module("quality").validate(store)
    assert result["ok"] is False
    assert "log hash/join/duplicate mismatch" in result["errors"]


def test_quality_rejects_cross_partition_block_hash_duplicates(tmp_path, synthetic):
    store = populated_store(tmp_path, synthetic, partition_size=2)
    rewrite_table(store, "blocks", lambda rows: rows[0].update(hash=block_hash(100)), start=102)
    result = module("quality").validate(store)
    assert result["ok"] is False
    assert "cross-partition duplicate blocks" in result["errors"]


def test_quality_rejects_null_topic_elements(tmp_path, synthetic):
    store = populated_store(tmp_path, synthetic)
    rewrite_table(store, "logs", lambda rows: rows[0].update(topics=[None]))
    result = module("quality").validate(store)
    assert result["ok"] is False
    assert "null required values logs" in result["errors"]
