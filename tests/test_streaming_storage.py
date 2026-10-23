import hashlib
import json
import shutil
from dataclasses import asdict

import pytest

from conftest import module


def make_store(tmp_path, checkpoint=2):
    config = module("config").Config(dataset=tmp_path, run_id="stream", start="2026-10-01", blocks_per_partition=6, blocks_per_checkpoint=checkpoint)
    return module("storage").Store(config, 100, 106, synthetic=True)


def write(store, synthetic, first=0, last=6):
    for block in synthetic["blocks"][first:last]:
        tables = module("models").normalize_block(block, synthetic["receipts"], store.config.system_recipients)
        store.write_block(int(block["number"], 16), tables)


def fragments(store):
    return sorted(store.root.glob(".staging/100-106/[0-9]*-[0-9]*"))


@pytest.mark.parametrize("setting,values,message", [
    ("blocks_per_partition", [0, 100001, True, 2.5], "blocks_per_partition must be an integer in [1,100000]"),
    ("blocks_per_checkpoint", [0, 5001, False, 2.5], "blocks_per_checkpoint must be an integer in [1,5000]"),
])
def test_config_strict_bounds(setting, values, message):
    for value in values:
        with pytest.raises(ValueError) as failure:
            module("config").Config(run_id="large", **{setting: value})
        assert str(failure.value) == message


def test_large_config_preserves_identity_formula():
    config = module("config").Config(run_id="large", blocks_per_partition=100000, blocks_per_checkpoint=500)
    settings = asdict(config)
    for key in ("dataset", "run_id", "qps", "concurrency", "batch_size", "timeout_seconds", "retries", "labels", "assets", "blocks_per_checkpoint"):
        settings.pop(key)
    settings.update(schema_version=1, chain_id=204)
    assert config.identity == hashlib.sha256(json.dumps(settings, sort_keys=True).encode()).hexdigest()


def test_closed_fragments_resume_changed_checkpoint_and_match_direct(tmp_path, synthetic):
    store = make_store(tmp_path)
    write(store, synthetic, last=2)
    assert store.partition_dirs() == []
    assert not store.completed(100)
    assert store.durable_end(100) == 102
    assert len(fragments(store)) == 1
    store.checkpoint.close()
    resumed = make_store(tmp_path, checkpoint=3)
    assert resumed.resume_partition(100) == 102
    write(resumed, synthetic, first=2)
    assert resumed.completed(100)
    assert len(list(resumed.block_dir(100).iterdir())) == 5
    assert not (resumed.root / ".staging" / "100-106").exists()
    direct = make_store(tmp_path / "direct", checkpoint=6)
    write(direct, synthetic)
    for name in module("models").TABLES:
        assert resumed.read_table(name) == direct.read_table(name)


def test_old_manifest_without_checkpoint_opens(tmp_path):
    store = make_store(tmp_path)
    manifest = store.manifest
    manifest["config"].pop("blocks_per_checkpoint")
    module("storage").atomic_json(store.manifest_path, manifest)
    assert module("storage").Store.open(tmp_path, "stream").config.blocks_per_checkpoint == 500


@pytest.mark.parametrize("damage", ["corrupt", "gap", "overlap", "symlink", "metadata_symlink", "identity", "bounds", "schema", "rows", "kind", "path"])
def test_bad_fragment_suffix_quarantined(tmp_path, synthetic, damage):
    store = make_store(tmp_path)
    write(store, synthetic, last=4)
    first, second = fragments(store)
    if damage == "corrupt":
        (second / "blocks.parquet").write_bytes(b"bad")
    elif damage == "gap":
        shutil.rmtree(first)
    elif damage == "overlap":
        shutil.copytree(second, second.parent / "101-104")
    elif damage == "symlink":
        (second / "logs.parquet").unlink()
        (second / "logs.parquet").symlink_to(first / "logs.parquet")
    elif damage == "metadata_symlink":
        original = second / "_fragment.json"
        replacement = tmp_path / "metadata.json"
        original.rename(replacement)
        original.symlink_to(replacement)
    elif damage == "path":
        second.rename(second.parent / "untrusted")
    else:
        path = second / "_fragment.json"
        metadata = json.loads(path.read_text())
        if damage == "identity":
            metadata["identity"] = "wrong"
        elif damage == "bounds":
            metadata["end_block"] = 107
        elif damage == "schema":
            metadata["schema_version"] = 99
        elif damage == "kind":
            metadata["kind"] = "partition"
        else:
            metadata["files"]["blocks"]["rows"] = 999
        module("storage").atomic_json(path, metadata)
    store.abort_partition()
    resumed = make_store(tmp_path)
    expected = 100 if damage in ("gap", "overlap", "path") else 102
    assert resumed.resume_partition(100) == expected
    assert all(int(path.name.split("-")[1]) <= expected for path in fragments(resumed))
    assert list((resumed.root / ".staging" / "100-106").glob(".quarantine-*"))
    write(resumed, synthetic, first=expected - 100)
    assert resumed.completed(100)
    assert [row["number"] for row in resumed.read_table("blocks")] == list(range(100, 106))


@pytest.mark.parametrize("metadata", [[], None, "private-fragment-secret", 17, 1.5, True])
@pytest.mark.parametrize("damaged_index", [0, 1])
def test_nonobject_fragment_metadata_quarantined(tmp_path, synthetic, caplog, capsys, metadata, damaged_index):
    caplog.set_level("DEBUG")
    store = make_store(tmp_path)
    write(store, synthetic, last=4)
    published = fragments(store)
    prefix_metadata = (published[0] / "_fragment.json").read_bytes()
    (published[damaged_index] / "_fragment.json").write_text(json.dumps(metadata))
    store.abort_partition()
    resumed = make_store(tmp_path)
    expected = 100 + damaged_index * 2
    assert resumed.resume_partition(100) == expected
    assert fragments(resumed) == published[:damaged_index]
    if damaged_index:
        assert (published[0] / "_fragment.json").read_bytes() == prefix_metadata
    assert len(list((resumed.root / ".staging" / "100-106").glob(".quarantine-*"))) == 2 - damaged_index
    resumed.abort_partition()
    restarted = make_store(tmp_path)
    assert restarted.resume_partition(100) == expected
    write(restarted, synthetic, first=expected - 100)
    assert restarted.completed(100)
    assert [row["number"] for row in restarted.read_table("blocks")] == list(range(100, 106))
    output = capsys.readouterr()
    assert "private-fragment-secret" not in caplog.text + output.out + output.err


def test_pending_ignored_and_abort_keeps_published_prefix(tmp_path, synthetic):
    store = make_store(tmp_path)
    write(store, synthetic, last=3)
    (fragments(store)[0].parent / ".pending-incomplete").mkdir()
    store.abort_partition()
    assert store.buffer_start is None
    assert not any(store.buffer.values())
    assert make_store(tmp_path).resume_partition(100) == 102


def test_finalization_replays_full_prefix_in_bounded_batches(tmp_path, synthetic, monkeypatch):
    store = make_store(tmp_path)
    monkeypatch.setattr(store, "_finalize_fragments", lambda *args: (_ for _ in ()).throw(OSError("interrupted")))
    with pytest.raises(OSError):
        write(store, synthetic)
    assert store.durable_end(100) == 106
    assert len(fragments(store)) == 3
    import pyarrow.parquet as parquet
    batch_sizes = []
    original_batches = parquet.ParquetFile.iter_batches

    def batches(source, *args, **kwargs):
        batch_sizes.append(kwargs.get("batch_size"))
        return original_batches(source, *args, **kwargs)

    monkeypatch.setattr(parquet.ParquetFile, "iter_batches", batches)
    monkeypatch.setattr(parquet.ParquetFile, "read", lambda *args, **kwargs: pytest.fail("whole-file read"))
    resumed = make_store(tmp_path)
    assert resumed.resume_partition(100) == 106
    assert resumed.completed(100)
    assert not fragments(resumed)
    assert batch_sizes and all(size <= 4096 for size in batch_sizes)


def test_final_sqlite_failure_retains_fragments_and_recovers(tmp_path, synthetic, monkeypatch):
    store = make_store(tmp_path)
    monkeypatch.setattr(store.checkpoint, "complete", lambda *args: (_ for _ in ()).throw(OSError("checkpoint failed")))
    with pytest.raises(OSError):
        write(store, synthetic)
    assert len(store.partition_dirs()) == 1
    assert len(fragments(store)) == 3
    resumed = make_store(tmp_path)
    assert resumed.recover(100)
    assert resumed.completed(100)
    assert not fragments(resumed)


def test_corrupt_final_does_not_delete_recovery_prefix(tmp_path, synthetic, monkeypatch):
    store = make_store(tmp_path)
    monkeypatch.setattr(store.checkpoint, "complete", lambda *args: (_ for _ in ()).throw(OSError("checkpoint failed")))
    with pytest.raises(OSError):
        write(store, synthetic)
    (store.block_dir(100) / "blocks.parquet").write_bytes(b"broken")
    resumed = make_store(tmp_path)
    assert not resumed.recover(100)
    assert len(fragments(resumed)) == 3
    assert resumed.resume_partition(100) == 106
    assert resumed.completed(100)
    assert list(resumed.block_dir(100).parent.glob(".replaced-*"))


@pytest.mark.parametrize("boundary", ["file_fsync", "metadata", "before_rename", "after_rename", "directory_fsync"])
def test_fragment_publication_fault_preserves_prior_prefix(tmp_path, synthetic, monkeypatch, boundary):
    from pathlib import Path
    helpers = module("storage_fragments")
    store = make_store(tmp_path)
    write(store, synthetic, last=2)
    original_replace = helpers.os.replace
    original_sync = helpers.sync_directory
    original_json = helpers.atomic_json
    original_fsync = helpers.os.fsync

    def replace(source, destination):
        if Path(destination).name == "102-104":
            if boundary == "before_rename":
                raise OSError("before fragment rename")
            original_replace(source, destination)
            if boundary == "after_rename":
                raise OSError("after fragment rename")
            return
        return original_replace(source, destination)

    def sync(path):
        if boundary == "directory_fsync" and Path(path).name == "100-106":
            raise OSError("fragment parent fsync")
        return original_sync(path)

    def atomic(path, value):
        if boundary == "metadata":
            raise OSError("fragment metadata")
        return original_json(path, value)

    def fsync(descriptor):
        import stat
        if boundary == "file_fsync" and stat.S_ISREG(helpers.os.fstat(descriptor).st_mode):
            raise OSError("fragment file fsync")
        return original_fsync(descriptor)

    with monkeypatch.context() as patch:
        patch.setattr(helpers.os, "replace", replace)
        patch.setattr(helpers, "sync_directory", sync)
        patch.setattr(helpers, "atomic_json", atomic)
        patch.setattr(helpers.os, "fsync", fsync)
        with pytest.raises(OSError):
            write(store, synthetic, first=2, last=4)
    store.abort_partition()
    resumed = make_store(tmp_path)
    expected = 104 if boundary in ("after_rename", "directory_fsync") else 102
    assert resumed.resume_partition(100) == expected
    write(resumed, synthetic, first=expected - 100)
    assert resumed.completed(100)
    assert [row["number"] for row in resumed.read_table("blocks")] == list(range(100, 106))


@pytest.mark.parametrize("boundary", ["write", "before_rename", "after_rename", "verify", "after_sqlite"])
def test_final_publication_fault_keeps_all_fragments(tmp_path, synthetic, monkeypatch, boundary):
    from pathlib import Path
    helpers = module("storage_fragments")
    store = make_store(tmp_path)
    original_replace = helpers.os.replace
    original_complete = store.checkpoint.complete

    def replace(source, destination):
        if Path(destination).name == "100-106" and Path(destination).parent.name != ".staging":
            if boundary == "before_rename":
                raise OSError("before final rename")
            original_replace(source, destination)
            if boundary == "after_rename":
                raise OSError("after final rename")
            return
        return original_replace(source, destination)

    def complete(*args):
        original_complete(*args)
        raise OSError("after SQLite completion")

    with monkeypatch.context() as patch:
        patch.setattr(helpers.os, "replace", replace)
        if boundary == "write":
            patch.setattr(helpers, "concatenate", lambda *args: (_ for _ in ()).throw(OSError("final writing")))
        elif boundary == "verify":
            patch.setattr(store, "verify_partition", lambda *args: False)
        elif boundary == "after_sqlite":
            patch.setattr(store.checkpoint, "complete", complete)
        with pytest.raises((OSError, ValueError)):
            write(store, synthetic)
    assert len(fragments(store)) == 3
    resumed = make_store(tmp_path)
    if resumed.completed(100):
        assert not fragments(resumed)
    elif not resumed.recover(100):
        assert resumed.resume_partition(100) == 106
    assert resumed.completed(100)
    assert not fragments(resumed)


def test_byte_limit_estimates_only_incoming_blocks(tmp_path, synthetic, monkeypatch):
    helpers = module("storage_fragments")
    store = make_store(tmp_path, checkpoint=6)
    estimates = []
    original = helpers.estimate_bytes

    def estimate(tables):
        estimates.append(len(tables["blocks"]))
        return original(tables)

    monkeypatch.setattr(helpers, "estimate_bytes", estimate)
    monkeypatch.setattr(helpers, "CHECKPOINT_BYTES", 1)
    write(store, synthetic, last=3)
    assert estimates == [1, 1, 1]
    assert store.durable_end(100) == 103
    assert not any(store.buffer.values())
    assert len(fragments(store)) == 3


def test_cached_durable_end_does_not_hash_per_block(tmp_path, synthetic, monkeypatch):
    store = make_store(tmp_path)
    write(store, synthetic, last=2)
    monkeypatch.setattr(module("storage"), "digest", lambda *args: pytest.fail("unexpected repeated checksum"))
    monkeypatch.setattr(module("storage_fragments"), "digest", lambda *args: pytest.fail("unexpected repeated checksum"))
    assert [store.durable_end(number) for number in range(100, 106)] == [102] * 6


@pytest.mark.parametrize("parent", [".staging", ".staging/100-106"])
def test_private_staging_symlink_is_rejected_without_touching_target(tmp_path, parent):
    store = make_store(tmp_path)
    target = tmp_path / "external"
    target.mkdir()
    marker = target / "keep"
    marker.write_text("unchanged")
    link = store.root / parent
    link.parent.mkdir(parents=True, exist_ok=True)
    link.symlink_to(target, target_is_directory=True)
    with pytest.raises(ValueError, match="staging path"):
        store.resume_partition(100)
    assert marker.read_text() == "unchanged"
