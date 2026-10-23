import json
import os
import re
import shutil
import uuid

from .models import TABLES
from .storage import atomic_json, digest, schema

BATCH_ROWS = 4096
CHECKPOINT_BYTES = 64 * 1024 * 1024


def sync_directory(path):
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def write_tables(directory, tables):
    import pyarrow as arrow
    import pyarrow.parquet as parquet
    files = {}
    for name in TABLES:
        output = directory / (name + ".parquet")
        with parquet.ParquetWriter(output, schema(name), compression="zstd") as writer:
            for offset in range(0, len(tables[name]), BATCH_ROWS):
                writer.write_table(arrow.Table.from_pylist(tables[name][offset:offset + BATCH_ROWS], schema=schema(name)))
        with output.open("rb") as handle:
            os.fsync(handle.fileno())
        files[name] = {"rows": len(tables[name]), "sha256": digest(output)}
    return files


def estimate_bytes(tables):
    import pyarrow as arrow
    return sum(arrow.Table.from_pylist(tables[name][offset:offset + BATCH_ROWS], schema=schema(name)).nbytes
               for name in TABLES for offset in range(0, len(tables[name]), BATCH_ROWS))


def concatenate(directory, fragments):
    import pyarrow.parquet as parquet
    files = {}
    for name in TABLES:
        output = directory / (name + ".parquet")
        rows = 0
        with parquet.ParquetWriter(output, schema(name), compression="zstd") as writer:
            for path in fragments:
                with parquet.ParquetFile(path / (name + ".parquet")) as source:
                    for batch in source.iter_batches(batch_size=BATCH_ROWS):
                        writer.write_batch(batch)
                        rows += batch.num_rows
        with output.open("rb") as handle:
            os.fsync(handle.fileno())
        files[name] = {"rows": rows, "sha256": digest(output)}
    return files


class Fragments:
    def __init__(self, store, start, end):
        self.store, self.start, self.end = store, start, end
        self.root = store.root / ".staging" / f"{start}-{end}"
        self.paths = []

    def safe_root(self):
        if self.root.is_symlink() or self.root.parent.is_symlink():
            raise ValueError("invalid private staging path")

    def publish(self, start, end, tables):
        self.safe_root()
        self.root.mkdir(parents=True, exist_ok=True)
        sync_directory(self.root.parent.parent)
        sync_directory(self.root.parent)
        temporary = self.root / (".pending-" + uuid.uuid4().hex)
        temporary.mkdir()
        final = self.root / f"{start}-{end}"
        if final.exists() or final.is_symlink():
            raise ValueError("fragment already exists")
        try:
            metadata = {"kind": "checkpoint_fragment", "schema_version": 1,
                        "identity": self.store.config.identity, "partition_start": self.start,
                        "partition_end": self.end, "start_block": start, "end_block": end,
                        "files": write_tables(temporary, tables)}
            atomic_json(temporary / "_fragment.json", metadata)
            sync_directory(temporary)
            os.replace(temporary, final)
            sync_directory(self.root)
            self.paths.append(final)
        finally:
            if temporary.exists():
                shutil.rmtree(temporary)

    def verify(self, path, start, end):
        import pyarrow.parquet as parquet
        try:
            if path.is_symlink() or not path.is_dir() or not self.start <= start < end <= self.end:
                return False
            metadata_path = path / "_fragment.json"
            if metadata_path.is_symlink():
                return False
            metadata = json.loads(metadata_path.read_text())
            if not isinstance(metadata, dict):
                return False
            expected = {"kind": "checkpoint_fragment", "schema_version": 1,
                        "identity": self.store.config.identity, "partition_start": self.start,
                        "partition_end": self.end, "start_block": start, "end_block": end}
            if any(type(metadata.get(key)) is not type(value) or metadata[key] != value for key, value in expected.items()):
                return False
            for name in TABLES:
                file = path / (name + ".parquet")
                if file.is_symlink() or not file.is_file() or digest(file) != metadata["files"][name]["sha256"]:
                    return False
                with parquet.ParquetFile(file) as source:
                    if source.schema_arrow != schema(name) or source.metadata.num_rows != metadata["files"][name]["rows"]:
                        return False
                    number_field = "number" if name == "blocks" else "block_number"
                    next_number = start
                    for batch in source.iter_batches(batch_size=BATCH_ROWS, columns=[number_field]):
                        for value in batch.column(0):
                            number = value.as_py()
                            if number is None or not start <= number < end or (name == "blocks" and number != next_number):
                                return False
                            next_number += 1
                    if name == "blocks" and next_number != end:
                        return False
            return True
        except (OSError, KeyError, ValueError, TypeError):
            return False

    def resume(self):
        self.safe_root()
        if not self.root.exists():
            return self.start
        candidates = []
        for path in self.root.iterdir():
            if path.name.startswith("."):
                continue
            match = re.fullmatch(r"([0-9]+)-([0-9]+)", path.name)
            candidates.append((int(match[1]) if match else -1, int(match[2]) if match else -1, path))
        candidates.sort(key=lambda item: (item[0], item[1], item[2].name))
        trusted = []
        prefix = self.start
        for start, end, path in candidates:
            if start != prefix or not self.verify(path, start, end):
                while trusted and trusted[-1][1] > start:
                    trusted.pop()
                suffix = candidates[len(trusted):]
                for _, _, invalid in suffix:
                    os.replace(invalid, self.root / (".quarantine-" + uuid.uuid4().hex))
                sync_directory(self.root)
                break
            trusted.append((start, end, path))
            prefix = end
        self.paths = [path for _, _, path in trusted]
        return trusted[-1][1] if trusted else self.start

    def cleanup(self):
        self.safe_root()
        if self.root.exists():
            shutil.rmtree(self.root)
            sync_directory(self.root.parent)
            try:
                self.root.parent.rmdir()
            except OSError:
                pass
            else:
                sync_directory(self.store.root)
