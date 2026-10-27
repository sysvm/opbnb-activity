import logging

from .models import TABLES

logger = logging.getLogger(__name__)
HASH_BATCH_SIZE = 4096

CHECKS = (
    ("duplicate transaction/receipt hash", """
        SELECT partition_path, hash FROM quality_transactions GROUP BY partition_path, hash HAVING COUNT(*) > 1
        UNION ALL
        SELECT partition_path, transaction_hash FROM quality_receipts
        GROUP BY partition_path, transaction_hash HAVING COUNT(*) > 1
    """),
    ("receipt joins do not cover transactions", """
        SELECT 1 FROM quality_transactions AS transactions
        FULL OUTER JOIN quality_receipts AS receipts
          ON transactions.partition_path = receipts.partition_path AND transactions.hash = receipts.transaction_hash
        WHERE transactions.hash IS NULL OR receipts.transaction_hash IS NULL
    """),
    ("transaction block hash/timestamp mismatch", """
        SELECT 1 FROM quality_transactions AS transactions
        LEFT JOIN quality_blocks AS blocks
          ON transactions.partition_path = blocks.partition_path AND transactions.block_number = blocks.number
        WHERE blocks.number IS NULL OR transactions.block_hash IS DISTINCT FROM blocks.hash
           OR transactions.timestamp IS DISTINCT FROM blocks.timestamp
    """),
    ("receipt hash/index mismatch", """
        SELECT 1 FROM quality_transactions AS transactions
        LEFT JOIN quality_receipts AS receipts
          ON transactions.partition_path = receipts.partition_path AND transactions.hash = receipts.transaction_hash
        WHERE receipts.transaction_hash IS NULL OR receipts.block_hash IS DISTINCT FROM transactions.block_hash
           OR receipts.block_number IS DISTINCT FROM transactions.block_number
           OR receipts.transaction_index IS DISTINCT FROM transactions.transaction_index
    """),
    ("block transaction count mismatch", """
        SELECT 1 FROM quality_blocks AS blocks
        LEFT JOIN (
            SELECT partition_path, block_number, COUNT(*) AS transaction_count
            FROM quality_transactions GROUP BY partition_path, block_number
        ) AS transactions ON transactions.partition_path = blocks.partition_path AND transactions.block_number = blocks.number
        WHERE blocks.transaction_count IS DISTINCT FROM COALESCE(transactions.transaction_count, 0)
    """),
    ("log hash/join/duplicate mismatch", """
        SELECT 1 FROM quality_logs AS logs
        LEFT JOIN quality_transactions AS transactions
          ON logs.partition_path = transactions.partition_path AND logs.transaction_hash = transactions.hash
        WHERE transactions.hash IS NULL OR logs.block_hash IS DISTINCT FROM transactions.block_hash
           OR logs.block_number IS DISTINCT FROM transactions.block_number
        UNION ALL
        SELECT 1 FROM quality_logs GROUP BY partition_path, transaction_hash, log_index HAVING COUNT(*) > 1
    """),
)


def _has_duplicate_hash(connection, table, key, paths):
    filenames = [str(path / (table + ".parquet")) for path in paths]
    connection.from_parquet(filenames, hive_partitioning=False).project(f'"{key}"').create_view(
        "quality_hashes", replace=True
    )
    cursor = connection.execute(f'SELECT "{key}" FROM quality_hashes ORDER BY "{key}"')
    previous = object()
    duplicate = False
    while True:
        batch = cursor.fetchmany(HASH_BATCH_SIZE)
        if not batch:
            return duplicate
        for (value,) in batch:
            if value == previous:
                duplicate = True
            previous = value


def relational_errors(connection, paths, set_stage=None):
    errors = set()
    for position, path in enumerate(paths, 1):
        if set_stage is not None:
            set_stage(f"relational partition {position} views")
        for name in TABLES:
            connection.from_parquet([str(path / (name + ".parquet"))], filename=True,
                                    hive_partitioning=False).project(
                "*, regexp_replace(filename, '/[^/]+$', '') AS partition_path"
            ).create_view("quality_" + name, replace=True)
        for check_number, (message, query) in enumerate(CHECKS, 1):
            if set_stage is not None:
                set_stage(f"relational partition {position} check {check_number}")
            if position == 1 or position % 25 == 0:
                logger.info("Relational check start: partition=%d/%d check=%s", position, len(paths), message)
            cursor = connection.execute("SELECT EXISTS (" + query + ")")
            exists = cursor.fetchone()[0]
            cursor.fetchone()
            if exists:
                errors.add(message)
        if position % 25 == 0:
            logger.info("Relational partition checks progress: partitions=%d/%d", position, len(paths))
    logger.info("Relational partition checks complete: partitions=%d", len(paths))
    if not paths:
        return []
    for table, key in (("blocks", "hash"), ("transactions", "hash"), ("receipts", "transaction_hash")):
        if set_stage is not None:
            set_stage("global duplicate sort " + table)
        logger.info("Global duplicate sort start: table=%s", table)
        if _has_duplicate_hash(connection, table, key, paths):
            errors.add("cross-partition duplicate " + table)
        logger.info("Global duplicate sort complete: table=%s", table)
    return sorted(errors)
