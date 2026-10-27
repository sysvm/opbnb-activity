import json
import logging

import duckdb
import pyarrow.parquet as parquet
import pytest

from conftest import block_hash, module
from test_streaming_quality import populated_store, rewrite_table


class ObservedConnection:
    def __init__(self, connection, fail_sort=False):
        self.connection = connection
        self.fail_sort = fail_sort
        self.bindings = []
        self.queries = []
        self.fetch_sizes = []

    def from_parquet(self, filenames, **options):
        self.bindings.append(tuple(filenames))
        return self.connection.from_parquet(filenames, **options)

    def execute(self, query):
        self.queries.append(query)
        if "ORDER BY" in query:
            if self.fail_sort:
                raise self.fail_sort("private query credential and spill path")
        else:
            assert all(len(paths) == 1 for paths in self.bindings[-4:]), "all-window relational scan"
        self.connection.execute(query)
        return self

    def fetchone(self):
        return self.connection.fetchone()

    def fetchmany(self, size):
        self.fetch_sizes.append(size)
        assert 0 < size <= 4096
        return self.connection.fetchmany(size)

    def fetchall(self):
        raise AssertionError("unbounded cursor consumption")

    def close(self):
        self.connection.close()


@pytest.mark.parametrize("legacy", [False, True])
def test_relational_checks_bind_only_one_physical_partition(tmp_path, synthetic, legacy):
    store = populated_store(tmp_path, synthetic, partition_size=2)
    if legacy:
        for directory in store.partition_dirs():
            bounds = directory.name.split("-")
            target = store.root / "data" / ("date=" + directory.parent.name) / (
                "blocks=" + "-".join(f"{int(bound):012d}" for bound in bounds))
            target.parent.mkdir(parents=True, exist_ok=True)
            directory.rename(target)
    connection = ObservedConnection(store.connection())
    try:
        assert module("quality_sql").relational_errors(connection, store.partition_dirs()) == []
    finally:
        connection.close()
    assert sum("SELECT EXISTS" in query for query in connection.queries) == 3 * 6
    sorts = [query for query in connection.queries if "ORDER BY" in query]
    assert len(sorts) == 3
    assert all("GROUP BY" not in query and "JOIN" not in query for query in sorts)
    assert connection.fetch_sizes


@pytest.mark.parametrize("table,key", [("blocks", "hash"), ("transactions", "hash"),
                                       ("receipts", "transaction_hash")])
def test_global_sort_finds_cross_partition_duplicates(tmp_path, synthetic, table, key):
    store = populated_store(tmp_path, synthetic, partition_size=2)
    value = parquet.ParquetFile(store.block_dir(100) / (table + ".parquet")).read(columns=[key])[key][0].as_py()
    rewrite_table(store, table, lambda rows: rows[0].update({key: value}), start=102)
    connection = ObservedConnection(store.connection())
    try:
        errors = module("quality_sql").relational_errors(connection, store.partition_dirs())
    finally:
        connection.close()
    assert "cross-partition duplicate " + table in errors


def test_exact_duplicate_comparison_crosses_cursor_batch_boundary(tmp_path, synthetic):
    config = module("config").Config(dataset=tmp_path, run_id="sort-boundary", blocks_per_partition=2050)
    store = module("storage").Store(config, 100, 4200, synthetic=True)
    template = module("models").normalize_block(synthetic["blocks"][0], synthetic["receipts"],
                                                config.system_recipients)["blocks"][0]
    tables = {name: [] for name in module("models").TABLES}
    for number in range(100, 4200):
        tables["blocks"].append(dict(template, number=number, hash=block_hash(number),
                                     parent_hash=block_hash(number - 1), transaction_count=0))
    tables["blocks"][2049]["hash"], tables["blocks"][4095]["hash"] = (
        tables["blocks"][4095]["hash"], tables["blocks"][2049]["hash"])
    tables["blocks"][4096]["hash"] = tables["blocks"][2049]["hash"]
    store.commit_partition(100, 2150, {name: rows[:2050] for name, rows in tables.items()})
    store.commit_partition(2150, 4200, {name: rows[2050:] for name, rows in tables.items()})
    connection = ObservedConnection(store.connection())
    try:
        errors = module("quality_sql").relational_errors(connection, store.partition_dirs())
    finally:
        connection.close()
    assert errors == ["cross-partition duplicate blocks"]
    assert len(connection.fetch_sizes) >= 4


@pytest.mark.parametrize("exception", [RuntimeError, duckdb.OutOfMemoryException])
def test_validation_reports_safe_global_sort_failure_stage(tmp_path, synthetic, monkeypatch, caplog, exception):
    store = populated_store(tmp_path, synthetic, partition_size=2)
    connection = ObservedConnection(store.connection(), fail_sort=exception)
    monkeypatch.setattr(store, "connection", lambda: connection)
    with caplog.at_level(logging.INFO):
        result = module("quality").validate(store)
    assert result["ok"] is False
    assert "cannot read typed partitions: " + exception.__name__ in result["errors"]
    assert result["failure_stage"] == "global duplicate sort blocks"
    assert "private query" not in json.dumps(result) + caplog.text
    assert "global duplicate sort blocks" in caplog.text


def test_quality_logs_partition_and_check_progress(tmp_path, synthetic, caplog):
    store = populated_store(tmp_path, synthetic, partition_size=2)
    with caplog.at_level(logging.INFO):
        result = module("quality").validate(store, save=False)
    assert result["ok"] is True
    assert "Typed partition inspection complete: partitions=3" in caplog.text
    assert "Partition recovery/checksum preflight start: partitions=3" in caplog.text
    assert "Partition recovery/checksum preflight complete: partitions=3" in caplog.text
    assert "Relational partition checks complete: partitions=3" in caplog.text
    assert "Relational check start:" in caplog.text
    assert "Global duplicate sort complete: table=receipts" in caplog.text


def test_relational_partition_progress_every_25_partitions(tmp_path, synthetic, caplog):
    store = populated_store(tmp_path, synthetic)
    connection = store.connection()
    try:
        with caplog.at_level(logging.INFO):
            errors = module("quality_sql").relational_errors(connection, store.partition_dirs() * 26)
    finally:
        connection.close()
    assert "cross-partition duplicate blocks" in errors
    assert "Relational partition checks progress: partitions=25/26" in caplog.text
    assert "Relational partition checks complete: partitions=26" in caplog.text


def test_recovery_logs_progress_every_25_ranges(tmp_path, synthetic, caplog):
    config = module("config").Config(dataset=tmp_path, run_id="preflight-progress", blocks_per_partition=1)
    store = module("storage").Store(config, 100, 126, synthetic=True)
    with caplog.at_level(logging.INFO):
        result = module("quality").validate(store, save=False)
    assert result["ok"] is False
    assert "Partition recovery/checksum preflight progress: partitions=25/26" in caplog.text
    assert "Partition recovery/checksum preflight complete: partitions=26" in caplog.text
