import logging
from time import monotonic

from .models import normalize_block
from .probe import require_chain
from .rpc import RpcError

logger = logging.getLogger(__name__)


def collect(client, store, block_receipts=True, batch_supported=True):
    started = last_report = monotonic()
    total = store.end_block - store.start_block
    processed = committed = new_blocks = new_transactions = 0

    def progress(event, start=None, end=None):
        nonlocal last_report
        last_report = monotonic()
        elapsed = max(0.0, last_report - started)
        rate = new_blocks / elapsed if elapsed > 0 else 0.0
        remaining = total - processed
        eta = "0.0" if remaining == 0 else f"{remaining / rate:.1f}" if rate > 0 else "unknown"
        partition = f"partition=[{start},{end}) " if start is not None else ""
        logger.info(
            "Collection %s: %sprocessed_blocks=%d/%d committed_blocks=%d/%d "
            "processed_percent=%.1f committed_percent=%.1f new_blocks=%d new_transactions=%d "
            "elapsed_seconds=%.1f rate_blocks_per_second=%.3f eta_seconds=%s",
            event, partition, processed, total, committed, total,
            100 * processed / total if total else 100.0,
            100 * committed / total if total else 100.0,
            new_blocks, new_transactions, elapsed, rate, eta,
        )

    logger.info("Collection start: blocks=[%d,%d)", store.start_block, store.end_block)
    require_chain(client)
    for start, end in store.ranges():
        completed = store.completed(start)
        if completed or store.recover(start):
            processed += end - start
            committed += end - start
            progress("partition skipped" if completed else "partition recovered", start, end)
            continue
        for number in range(start, end):
            block = client.call("eth_getBlockByNumber", [hex(number), True])
            if block is None:
                raise ValueError("historical full block unavailable")
            receipts = None
            if block_receipts:
                try:
                    receipts = client.call("eth_getBlockReceipts", [hex(number)])
                except RpcError as error:
                    if error.kind != "unsupported":
                        raise
                    block_receipts = False
                    logger.info("Optional eth_getBlockReceipts unsupported; falling back to transaction receipts")
            if receipts is None:
                calls = [("eth_getTransactionReceipt", [transaction["hash"]]) for transaction in block["transactions"]]
                receipts = client.batch(calls) if batch_supported else [client.call(method, params) for method, params in calls]
            if len(receipts) != len(block["transactions"]) or any(not receipt for receipt in receipts):
                raise ValueError("missing receipt prevents partition completion")
            by_hash = {receipt["transactionHash"].lower(): receipt for receipt in receipts}
            if len(by_hash) != len(receipts):
                raise ValueError("duplicate receipt prevents partition completion")
            normalized = normalize_block(block, by_hash, store.config.system_recipients, store.config.system_depositor)
            store.write_block(number, normalized)
            processed += 1
            new_blocks += 1
            new_transactions += len(normalized["transactions"])
            if number + 1 == end:
                committed += end - start
                progress("partition committed", start, end)
            elif monotonic() - last_report >= 30:
                progress("partition in progress", start, end)
    progress("complete")
    return store
