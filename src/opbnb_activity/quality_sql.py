from .models import TABLES

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


def relational_errors(connection, paths):
    for name in TABLES:
        filenames = [str(path / (name + ".parquet")) for path in paths]
        connection.from_parquet(filenames, filename=True, hive_partitioning=False).project(
            "*, regexp_replace(filename, '/[^/]+$', '') AS partition_path"
        ).create_view("quality_" + name)
    errors = []
    for message, query in CHECKS:
        if connection.execute("SELECT EXISTS (" + query + ")").fetchone()[0]:
            errors.append(message)
    for table, key in (("blocks", "hash"), ("transactions", "hash"), ("receipts", "transaction_hash")):
        if connection.execute(
            f'SELECT EXISTS (SELECT "{key}" FROM quality_{table} GROUP BY "{key}" HAVING COUNT(*) > 1)'
        ).fetchone()[0]:
            errors.append("cross-partition duplicate " + table)
    return errors
