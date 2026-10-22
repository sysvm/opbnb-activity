import hashlib
import json
import os
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path

from .checkpoint import Checkpoint
from .config import Config
from .models import TABLES

FIELDS = {
    "blocks": {"number": "int", "hash": "str", "parent_hash": "str", "timestamp": "int", "timestamp_utc": "str", "transaction_count": "int"},
    "transactions": {"hash": "str", "block_number": "int", "block_hash": "str", "transaction_index": "int", "timestamp": "int", "timestamp_utc": "str", "sender": "str", "recipient": "str", "value_wei": "str", "input": "str", "selector": "str", "type": "int", "is_system": "bool", "classification": "str", "recipient_kind": "str"},
    "receipts": {"transaction_hash": "str", "block_number": "int", "block_hash": "str", "transaction_index": "int", "status": "int", "gas_used": "str", "effective_gas_price_wei": "str", "l2_fee_wei": "str", "l1_fee_wei": "str", "total_fee_wei": "str", "contract_address": "str"},
    "logs": {"transaction_hash": "str", "block_number": "int", "block_hash": "str", "log_index": "int", "address": "str", "topics": "list", "data": "str"},
}


def schema(name):
    import pyarrow as arrow
    types = {"int": arrow.int64(), "str": arrow.string(), "bool": arrow.bool_(), "list": arrow.list_(arrow.string())}
    return arrow.schema([(field, types[kind]) for field, kind in FIELDS[name].items()])


def digest(path):
    checksum = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            checksum.update(chunk)
    return checksum.hexdigest()


def atomic_json(path, value):
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(value, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


class Store:
    def __init__(self, config, start_block, end_block, synthetic=False):
        if start_block < 0 or start_block > end_block:
            raise ValueError("Invalid exclusive block range")
        self.config = config
        self.root = config.dataset / config.run_id
        self.root.mkdir(parents=True, exist_ok=True)
        self.manifest_path = self.root / "manifest.json"
        self.start_block, self.end_block = start_block, end_block
        self.buffer, self.buffer_start = {name: [] for name in TABLES}, None
        self.buffer_next = None
        if self.manifest_path.exists():
            self.manifest = json.loads(self.manifest_path.read_text())
            if self.manifest["identity"] != config.identity or self.manifest["start_block"] != start_block or self.manifest["end_block"] != end_block or self.manifest["synthetic"] != synthetic:
                raise ValueError("run identity/window/source mismatch; choose a different run ID")
        else:
            settings = config.serializable()
            settings.pop("dataset")
            self.manifest = {"schema_version": 1, "run_id": config.run_id, "identity": config.identity,
                             "chain_id": 204, "synthetic": synthetic, "start_block": start_block, "end_block": end_block,
                             "start_utc": config.start_utc.isoformat(), "end_utc": config.end_utc.isoformat(),
                             "timezone": config.timezone, "config": settings, "range_semantics": "[start_block,end_block)",
                             "source": "SYNTHETIC / TEST DATA" if synthetic else "RPC collection; source URL intentionally omitted"}
            atomic_json(self.manifest_path, self.manifest)
        self.checkpoint = Checkpoint(self.root / "checkpoint.sqlite", config.identity)

    @classmethod
    def open(cls, dataset, run_id):
        root = Path(dataset).expanduser().resolve() / run_id
        manifest = json.loads((root / "manifest.json").read_text())
        settings = dict(manifest["config"], dataset=dataset, run_id=run_id)
        return cls(Config(**settings), manifest["start_block"], manifest["end_block"], manifest["synthetic"])

    def ranges(self):
        for start in range(self.start_block, self.end_block, self.config.blocks_per_partition):
            yield start, min(start + self.config.blocks_per_partition, self.end_block)

    def partition_range(self, block_number):
        if not self.start_block <= block_number < self.end_block:
            raise ValueError("block outside manifest range")
        start = self.start_block + ((block_number - self.start_block) // self.config.blocks_per_partition) * self.config.blocks_per_partition
        return start, min(start + self.config.blocks_per_partition, self.end_block)

    def partition_dirs(self):
        return sorted(self.root.glob("data/date=*/blocks=*"), key=lambda path: int(path.name.split("=")[1].split("-")[0]))

    def block_dir(self, block_number):
        start, end = self.partition_range(block_number)
        matches = list(self.root.glob(f"data/date=*/blocks={start:012d}-{end:012d}"))
        if len(matches) != 1:
            raise ValueError("missing or duplicated partition")
        return matches[0]

    def verify_partition(self, path, expected_start, expected_end):
        try:
            metadata = json.loads((path / "_partition.json").read_text())
            if metadata["identity"] != self.config.identity or metadata["start_block"] != expected_start or metadata["end_block"] != expected_end:
                return False
            for name in TABLES:
                file = path / (name + ".parquet")
                if digest(file) != metadata["files"][name]["sha256"]:
                    return False
            return True
        except (OSError, KeyError, ValueError, TypeError):
            return False

    def completed(self, block_number):
        start, end = self.partition_range(block_number)
        task = self.checkpoint.get(start)
        if not task or task["end_block"] != end:
            return False
        path = self.root / task["path"]
        return self.verify_partition(path, start, end) and digest(path / "_partition.json") == task["checksum"]

    def recover(self, block_number):
        start, end = self.partition_range(block_number)
        try:
            path = self.block_dir(block_number)
        except ValueError:
            return False
        if not self.verify_partition(path, start, end):
            return False
        self.checkpoint.complete(start, end, str(path.relative_to(self.root)), digest(path / "_partition.json"))
        return True

    def write_block(self, block_number, tables):
        start, end = self.partition_range(block_number)
        if self.buffer_start is None:
            if block_number != start:
                raise ValueError("partition must start at its first block")
            self.buffer_start, self.buffer_next = start, start
        if self.buffer_start != start or self.buffer_next != block_number or len(tables["blocks"]) != 1 or tables["blocks"][0]["number"] != block_number:
            raise ValueError("partition blocks must be ordered and contiguous")
        for name in TABLES:
            self.buffer[name].extend(tables[name])
        self.buffer_next += 1
        if self.buffer_next == end:
            self.commit_partition(start, end, self.buffer)
            self.buffer, self.buffer_start, self.buffer_next = {name: [] for name in TABLES}, None, None

    def commit_partition(self, start, end, tables):
        import pyarrow as arrow
        import pyarrow.parquet as parquet
        day = datetime.fromtimestamp(tables["blocks"][0]["timestamp"], timezone.utc).date().isoformat()
        parent = self.root / "data" / ("date=" + day)
        parent.mkdir(parents=True, exist_ok=True)
        final = parent / f"blocks={start:012d}-{end:012d}"
        temporary = parent / (".pending-" + uuid.uuid4().hex)
        temporary.mkdir()
        metadata = {"identity": self.config.identity, "start_block": start, "end_block": end, "files": {}}
        try:
            for name in TABLES:
                output = temporary / (name + ".parquet")
                parquet.write_table(arrow.Table.from_pylist(tables[name], schema=schema(name)), output, compression="zstd")
                with output.open("rb") as handle:
                    os.fsync(handle.fileno())
                metadata["files"][name] = {"rows": len(tables[name]), "sha256": digest(output)}
            atomic_json(temporary / "_partition.json", metadata)
            if final.exists():
                os.replace(final, parent / (".replaced-" + uuid.uuid4().hex))
            os.replace(temporary, final)
            directory_fd = os.open(parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
            self.checkpoint.complete(start, end, str(final.relative_to(self.root)), digest(final / "_partition.json"))
        finally:
            if temporary.exists():
                shutil.rmtree(temporary)

    def iter_partitions(self):
        import pyarrow.parquet as parquet
        for path in self.partition_dirs():
            yield path, {name: parquet.ParquetFile(path / (name + ".parquet")).read().to_pylist() for name in TABLES}

    def read_table(self, name):
        return [row for _, tables in self.iter_partitions() for row in tables[name]]

    def connection(self):
        import duckdb
        import pyarrow as arrow
        connection = duckdb.connect()
        connection.execute("SET memory_limit='512MB'")
        connection.execute("SET threads=2")
        temporary = self.root / ".duckdb-tmp"
        temporary.mkdir(exist_ok=True)
        connection.execute("SET temp_directory=?", [str(temporary)])
        for name in TABLES:
            paths = [str(path / (name + ".parquet")) for path in self.partition_dirs()]
            if paths:
                connection.from_parquet(paths, hive_partitioning=False).create_view(name)
            else:
                connection.register(name, arrow.Table.from_pylist([], schema=schema(name)))
        return connection
