from .storage import atomic_json


def validate(store, save=True):
    errors = []
    expected_ranges = list(store.ranges())
    expected_starts = {start for start, _ in expected_ranges}
    seen_starts = set()
    previous_hash = None
    previous_number = store.start_block - 1
    counts = {"blocks": 0, "transactions": 0, "receipts": 0, "logs": 0}
    start_timestamp = int(store.config.start_utc.timestamp())
    end_timestamp = int(store.config.end_utc.timestamp())
    for start, end in expected_ranges:
        if not store.completed(start) and not store.recover(start):
            errors.append(f"missing/corrupt/incomplete partition [{start},{end})")
    try:
        for path, tables in store.iter_partitions():
            blocks = tables["blocks"]
            if not blocks:
                errors.append("empty partition " + path.name)
                continue
            partition_start = blocks[0]["number"]
            seen_starts.add(partition_start)
            if partition_start not in expected_starts:
                errors.append("unexpected partition " + path.name)
            block_by_number = {block["number"]: block for block in blocks}
            transaction_by_hash = {transaction["hash"]: transaction for transaction in tables["transactions"]}
            receipt_by_hash = {receipt["transaction_hash"]: receipt for receipt in tables["receipts"]}
            if len(transaction_by_hash) != len(tables["transactions"]) or len(receipt_by_hash) != len(tables["receipts"]):
                errors.append("duplicate transaction/receipt hash")
            if set(transaction_by_hash) != set(receipt_by_hash):
                errors.append("receipt joins do not cover transactions")
            transaction_counts = {}
            for transaction in tables["transactions"]:
                number = transaction["block_number"]
                block = block_by_number.get(number)
                receipt = receipt_by_hash.get(transaction["hash"])
                transaction_counts[number] = transaction_counts.get(number, 0) + 1
                if not block or transaction["block_hash"] != block["hash"] or transaction["timestamp"] != block["timestamp"]:
                    errors.append("transaction block hash/timestamp mismatch")
                if not receipt or receipt["block_hash"] != transaction["block_hash"] or receipt["block_number"] != number or receipt["transaction_index"] != transaction["transaction_index"]:
                    errors.append("receipt hash/index mismatch")
            for block in blocks:
                if block["number"] != previous_number + 1:
                    errors.append("missing/duplicate/out-of-order block")
                if previous_hash is not None and block["parent_hash"] != previous_hash:
                    errors.append("parent hash discontinuity")
                if not start_timestamp <= block["timestamp"] < end_timestamp:
                    errors.append("block timestamp outside manifest window")
                if transaction_counts.get(block["number"], 0) != block["transaction_count"]:
                    errors.append("block transaction count mismatch")
                previous_number, previous_hash = block["number"], block["hash"]
            seen_logs = set()
            for log in tables["logs"]:
                transaction = transaction_by_hash.get(log["transaction_hash"])
                key = (log["transaction_hash"], log["log_index"])
                if not transaction or log["block_hash"] != transaction["block_hash"] or log["block_number"] != transaction["block_number"] or key in seen_logs:
                    errors.append("log hash/join/duplicate mismatch")
                seen_logs.add(key)
            for name in counts:
                counts[name] += len(tables[name])
    except Exception as error:
        errors.append("cannot read typed partitions: " + type(error).__name__)
    if seen_starts != expected_starts or counts["blocks"] != store.end_block - store.start_block:
        errors.append("manifest block range incomplete or unexpected partitions")
    if not errors:
        connection = store.connection()
        try:
            for table, key in (("blocks", "hash"), ("transactions", "hash"), ("receipts", "transaction_hash")):
                if connection.execute(f'SELECT COUNT(*) FROM (SELECT "{key}" FROM {table} GROUP BY "{key}" HAVING COUNT(*)>1)').fetchone()[0]:
                    errors.append("cross-partition duplicate " + table)
        finally:
            connection.close()
    result = {"ok": not errors, "errors": sorted(set(errors)), "counts": counts,
              "identity": store.config.identity, "start_block": store.start_block, "end_block": store.end_block}
    if save:
        atomic_json(store.root / "quality.json", result)
    return result
