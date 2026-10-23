import shutil
from pathlib import Path

import pytest

from conftest import module


def make_store(tmp_path, end_block=106):
    config = module("config").Config(dataset=tmp_path, run_id="layout", start="2026-10-01", blocks_per_partition=2)
    return module("storage").Store(config, 100, end_block, synthetic=True)


def write_blocks(store, synthetic, blocks):
    for block in blocks:
        tables = module("models").normalize_block(block, synthetic["receipts"], store.config.system_recipients)
        store.write_block(int(block["number"], 16), tables)


def legacy_fixture(store, start):
    current = store.block_dir(start)
    first, end = store.partition_range(start)
    day = current.parent.name.removeprefix("date=")
    legacy = store.root / "data" / ("date=" + day) / f"blocks={first:012d}-{end:012d}"
    if current != legacy:
        legacy.parent.mkdir(parents=True, exist_ok=True)
        current.rename(legacy)
    task = store.checkpoint.get(first)
    store.checkpoint.complete(first, end, str(legacy.relative_to(store.root)), task["checksum"])
    return legacy


def test_new_partition_uses_first_block_utc_day_and_plain_exclusive_range(tmp_path, synthetic):
    store = make_store(tmp_path)
    write_blocks(store, synthetic, synthetic["blocks"][:2])
    path = store.block_dir(101)
    assert path.relative_to(store.root).as_posix() == "data/2026-10-01/100-102"
    assert store.checkpoint.get(100)["path"] == "data/2026-10-01/100-102"
    assert store.completed(100) is True


@pytest.mark.parametrize("recover_checkpoint", [False, True])
def test_legacy_read_resume_quality_and_sql_leave_existing_files_untouched(tmp_path, synthetic, recover_checkpoint):
    store = make_store(tmp_path)
    write_blocks(store, synthetic, synthetic["blocks"])
    legacy_paths = [legacy_fixture(store, start) for start, _ in store.ranges()]
    original_files = {path: path.read_bytes() for directory in legacy_paths for path in directory.iterdir()}
    original_manifest = store.manifest_path.read_bytes()
    if recover_checkpoint:
        with store.checkpoint.connection:
            store.checkpoint.connection.execute("DELETE FROM tasks")
    store.checkpoint.close()
    resumed = module("storage").Store.open(tmp_path, "layout")
    for start, _ in resumed.ranges():
        if recover_checkpoint:
            assert resumed.completed(start) is False
            assert resumed.recover(start) is True
        assert resumed.completed(start) is True
        assert resumed.checkpoint.get(start)["path"] == str(resumed.block_dir(start).relative_to(resumed.root))
    assert resumed.partition_dirs() == legacy_paths
    assert [block["number"] for block in resumed.read_table("blocks")] == list(range(100, 106))
    result = module("quality").validate(resumed)
    assert result["ok"] is True
    assert result["counts"] == {"blocks": 6, "transactions": 11, "receipts": 11, "logs": 2}
    connection = resumed.connection()
    try:
        assert connection.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 11
    finally:
        connection.close()
    assert resumed.manifest_path.read_bytes() == original_manifest
    assert {path: path.read_bytes() for path in original_files} == original_files
    assert not list(resumed.root.glob("data/????-??-??/*-*"))


def test_resumed_run_keeps_legacy_partition_and_writes_plain_partitions_across_days(tmp_path, synthetic):
    store = make_store(tmp_path)
    write_blocks(store, synthetic, synthetic["blocks"][:2])
    legacy = legacy_fixture(store, 100)
    original_files = {path: path.read_bytes() for path in legacy.iterdir()}
    store.checkpoint.close()
    resumed = module("storage").Store.open(tmp_path, "layout")
    write_blocks(resumed, synthetic, synthetic["blocks"][2:])
    assert [path.relative_to(resumed.root).as_posix() for path in resumed.partition_dirs()] == [
        "data/date=2026-10-01/blocks=000000000100-000000000102",
        "data/2026-10-08/102-104",
        "data/2026-10-14/104-106",
    ]
    assert {path: path.read_bytes() for path in original_files} == original_files
    assert all(resumed.completed(start) for start, _ in resumed.ranges())
    result = module("quality").validate(resumed)
    assert result["ok"] is True
    assert result["counts"] == {"blocks": 6, "transactions": 11, "receipts": 11, "logs": 2}
    connection = resumed.connection()
    try:
        assert connection.execute("SELECT number FROM blocks ORDER BY number").fetchall() == [(number,) for number in range(100, 106)]
    finally:
        connection.close()


@pytest.mark.parametrize("legacy_layout", [False, True])
def test_corrupt_partition_rewrite_reuses_visible_path_and_preserves_backup(tmp_path, synthetic, legacy_layout):
    store = make_store(tmp_path, 102)
    write_blocks(store, synthetic, synthetic["blocks"][:2])
    original = legacy_fixture(store, 100) if legacy_layout else store.block_dir(100)
    original.joinpath("transactions.parquet").write_bytes(b"corrupt")
    assert store.completed(100) is False
    write_blocks(store, synthetic, synthetic["blocks"][:2])
    assert store.block_dir(100) == original
    assert store.partition_dirs() == [original]
    assert store.checkpoint.get(100)["path"] == str(original.relative_to(store.root))
    backups = list(original.parent.glob(".replaced-*"))
    assert len(backups) == 1
    assert backups[0].joinpath("transactions.parquet").read_bytes() == b"corrupt"
    assert not list(original.parent.glob(".pending-*"))
    assert store.completed(100) is True
    assert module("quality").validate(store)["ok"] is True


def test_duplicate_legacy_and_plain_ranges_are_rejected_before_rewriting(tmp_path, synthetic):
    store = make_store(tmp_path, 102)
    write_blocks(store, synthetic, synthetic["blocks"][:2])
    legacy = legacy_fixture(store, 100)
    plain = store.root / "data/2026-10-01/100-102"
    shutil.copytree(legacy, plain)
    original_files = {path: path.read_bytes() for directory in (legacy, plain) for path in directory.iterdir()}
    with pytest.raises(ValueError, match="missing or duplicated partition"):
        store.block_dir(100)
    assert module("quality").validate(store)["ok"] is False
    with pytest.raises(ValueError, match="missing or duplicated partition"):
        write_blocks(store, synthetic, synthetic["blocks"][:2])
    assert {path: path.read_bytes() for path in original_files} == original_files
    assert len(store.partition_dirs()) == 2
    assert not list(store.root.glob("data/*/.replaced-*"))
    assert not list(store.root.glob("data/*/.pending-*"))


def test_partition_enumeration_sorts_numeric_starts_and_ignores_unrelated_paths(tmp_path):
    store = make_store(tmp_path)
    expected = [
        "data/2026-10-21/2-4",
        "data/date=2026-10-02/blocks=000000000006-000000000008",
        "data/2026-10-01/10-12",
    ]
    unrelated = [
        "data/2026-10-01/.pending-2-4",
        "data/2026-10-01/.replaced-2-4",
        "data/date=2026-10-01/.pending-blocks=2-4",
        "data/date=2026-10-01/.replaced-blocks=2-4",
        "data/2026-10-01/notes",
        "data/2026-10-01/2-4-extra",
        "data/2026-10-01/blocks=2-4",
        "data/date=2026-10-01/notes",
        "data/date=2026-10-01/blocks=not-a-range",
        "data/date=2026-10-01/blocks=2-4-extra",
        "data/unrelated/2-4",
        "data/.hidden/2-4",
    ]
    for relative in expected + unrelated:
        store.root.joinpath(relative).mkdir(parents=True, exist_ok=True)
    store.root.joinpath("data/2026-10-01/20-22").write_text("unrelated file")
    assert [path.relative_to(store.root).as_posix() for path in store.partition_dirs()] == expected


@pytest.mark.parametrize("legacy_layout", [False, True])
def test_partition_lookup_and_rewrite_use_targeted_range_globs(tmp_path, synthetic, monkeypatch, legacy_layout):
    store = make_store(tmp_path, 102)
    write_blocks(store, synthetic, synthetic["blocks"][:2])
    expected = legacy_fixture(store, 100) if legacy_layout else store.block_dir(100)
    original_glob = Path.glob
    patterns = []

    def targeted_glob(path, pattern):
        patterns.append(pattern)
        assert "100-102" in pattern or "000000000100-000000000102" in pattern
        return original_glob(path, pattern)

    monkeypatch.setattr(Path, "glob", targeted_glob)
    assert store.block_dir(101) == expected
    write_blocks(store, synthetic, synthetic["blocks"][:2])
    assert len(patterns) == 4
