import json
import logging

from .models import TABLES
from .quality_sql import relational_errors
from .storage import atomic_json, schema

BATCH_SIZE = 4096
OPTIONAL_FIELDS = {
    "blocks": set(),
    "transactions": {"recipient", "selector"},
    "receipts": {"gas_used", "effective_gas_price_wei", "l2_fee_wei", "l1_fee_wei", "total_fee_wei", "contract_address"},
    "logs": set(),
}
RANGE_ERROR = "manifest block range incomplete or unexpected partitions"
logger = logging.getLogger(__name__)


def _inspect_blocks(batch, previous_number, previous_hash, start, end, timestamps, errors):
    columns = [batch.column(name) for name in ("number", "hash", "parent_hash", "timestamp")]
    for position in range(batch.num_rows):
        number, block_hash, parent_hash, timestamp = [column[position].as_py() for column in columns]
        if previous_number is None or number != previous_number + 1:
            errors.add("missing/duplicate/out-of-order block")
        if number is None or not start <= number < end:
            errors.add(RANGE_ERROR)
        if previous_hash is not None and parent_hash != previous_hash:
            errors.add("parent hash discontinuity")
        if timestamp is None or not timestamps[0] <= timestamp < timestamps[1]:
            errors.add("block timestamp outside manifest window")
        previous_number, previous_hash = number, block_hash
    return previous_number, previous_hash


def _inspect_partition(path, start, end, store, counts, previous, timestamps, errors):
    import pyarrow.compute as compute
    import pyarrow.parquet as parquet

    metadata = json.loads((path / "_partition.json").read_text())
    if (metadata["identity"] != store.config.identity
            or type(metadata["start_block"]) is not int or metadata["start_block"] != start
            or type(metadata["end_block"]) is not int or metadata["end_block"] != end):
        errors.add(RANGE_ERROR)
    for name in TABLES:
        with parquet.ParquetFile(path / (name + ".parquet")) as reader:
            schema_matches = reader.schema_arrow.equals(schema(name), check_metadata=False)
            if not schema_matches:
                errors.add("typed schema mismatch " + name)
            observed_rows = 0
            for batch in reader.iter_batches(batch_size=BATCH_SIZE):
                if not schema_matches:
                    observed_rows += batch.num_rows
                    continue
                for field in batch.schema.names:
                    if field not in OPTIONAL_FIELDS[name] and batch.column(field).null_count:
                        errors.add("null required values " + name)
                if name == "logs" and compute.list_flatten(batch.column("topics")).null_count:
                    errors.add("null required values logs")
                if name == "blocks":
                    if observed_rows == 0 and batch.num_rows and batch.column("number")[0].as_py() != start:
                        errors.add(RANGE_ERROR)
                    previous = _inspect_blocks(batch, *previous, start, end, timestamps, errors)
                observed_rows += batch.num_rows
            counts[name] += observed_rows
            expected_rows = metadata["files"][name]["rows"]
            if type(expected_rows) is not int or expected_rows != observed_rows or reader.metadata.num_rows != observed_rows:
                errors.add("metadata row count mismatch " + name)
            if name == "blocks":
                if not observed_rows:
                    errors.add("empty partition " + path.name)
                if observed_rows != end - start or previous[0] != end - 1:
                    errors.add(RANGE_ERROR)
    return previous


def validate(store, save=True):
    errors = set()
    counts = {name: 0 for name in TABLES}
    expected_ranges = dict(store.ranges())
    seen_starts = set()
    previous = store.start_block - 1, None
    timestamps = int(store.config.start_utc.timestamp()), int(store.config.end_utc.timestamp())
    paths = []
    stage = "partition recovery"
    failure_stage = None

    def set_stage(value):
        nonlocal stage
        stage = value

    try:
        logger.info("Partition recovery/checksum preflight start: partitions=%d", len(expected_ranges))
        for position, (start, end) in enumerate(expected_ranges.items(), 1):
            stage = f"partition recovery/checksum preflight {position}"
            if not store.completed(start) and not store.recover(start):
                errors.add(f"missing/corrupt/incomplete partition [{start},{end})")
            if position % 25 == 0:
                logger.info("Partition recovery/checksum preflight progress: partitions=%d/%d",
                            position, len(expected_ranges))
        logger.info("Partition recovery/checksum preflight complete: partitions=%d", len(expected_ranges))
        stage = "partition discovery"
        paths = store.partition_dirs()
        for position, path in enumerate(paths, 1):
            stage = f"typed partition inspection {position}"
            start, end = [int(bound) for bound in path.name.removeprefix("blocks=").split("-")]
            if start in seen_starts or expected_ranges.get(start) != end:
                errors.add("unexpected partition " + path.name)
                errors.add(RANGE_ERROR)
            seen_starts.add(start)
            previous = _inspect_partition(path, start, end, store, counts, previous, timestamps, errors)
            if position % 25 == 0:
                logger.info("Typed partition inspection progress: partitions=%d/%d", position, len(paths))
        logger.info("Typed partition inspection complete: partitions=%d", len(paths))
        if paths and not any(error.startswith("typed schema mismatch") for error in errors):
            stage = "quality connection setup"
            connection = store.connection()
            try:
                errors.update(relational_errors(connection, paths, set_stage=set_stage))
            finally:
                connection.close()
    except Exception as error:
        errors.add("cannot read typed partitions: " + type(error).__name__)
        failure_stage = stage
        logger.error("Quality validation failed: stage=%s error_type=%s", stage, type(error).__name__)
    if seen_starts != set(expected_ranges) or counts["blocks"] != store.end_block - store.start_block:
        errors.add(RANGE_ERROR)
    result = {"ok": not errors, "errors": sorted(errors), "counts": counts,
              "identity": store.config.identity, "start_block": store.start_block, "end_block": store.end_block}
    if failure_stage is not None:
        result["failure_stage"] = failure_stage
    if save:
        atomic_json(store.root / "quality.json", result)
    return result
