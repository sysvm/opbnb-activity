import logging
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from threading import Event, Lock
from time import monotonic

from .models import normalize_block
from .probe import require_chain
from .rpc import RpcError

logger = logging.getLogger(__name__)


def _ordered_blocks(fetch, start, end, concurrency, pending):
    if concurrency == 1:
        for number in range(start, end):
            yield number, fetch(number)
        return
    executor = ThreadPoolExecutor(max_workers=concurrency, thread_name_prefix="activity-fetch")
    state_lock = Lock()
    outstanding = set()
    failure = None
    def fetch_block(number):
        nonlocal failure
        with state_lock:
            if failure is not None:
                raise failure
        try:
            return fetch(number)
        except BaseException as error:
            with state_lock:
                if failure is None:
                    failure = error
                    queued = tuple(outstanding)
                else:
                    queued = ()
            for future in queued:
                future.cancel()
            raise
    def submit_block(number):
        with state_lock:
            if failure is not None:
                return False
            future = executor.submit(fetch_block, number)
            outstanding.add(future)
            pending.append((number, future))
            return True
    next_number = start
    try:
        while next_number < end and len(pending) < 2 * concurrency and submit_block(next_number):
            next_number += 1
        while pending:
            number, future = pending.popleft()
            try:
                result = future.result()
            except BaseException:
                with state_lock:
                    error = failure
                if error is not None:
                    raise error
                raise
            with state_lock:
                outstanding.discard(future)
                error = failure
            if error is not None:
                raise error
            yield number, result
            if next_number < end and submit_block(next_number):
                next_number += 1
    finally:
        for number, future in pending:
            future.cancel()
        executor.shutdown(wait=True, cancel_futures=True)
        pending.clear()


def collect(client, store, block_receipts=True, batch_supported=True):
    started = last_report = monotonic()
    total = store.end_block - store.start_block
    processed = committed = new_blocks = new_transactions = 0
    concurrency = store.config.concurrency
    pending = deque()
    unsupported_receipts = Event()
    capability_lock = Lock()
    if not block_receipts:
        unsupported_receipts.set()

    def fetch(number):
        block = client.call("eth_getBlockByNumber", [hex(number), True])
        if block is None:
            raise ValueError("historical full block unavailable")
        receipts = None
        if not unsupported_receipts.is_set():
            try:
                receipts = client.call("eth_getBlockReceipts", [hex(number)])
            except RpcError as error:
                if error.kind != "unsupported":
                    raise
                with capability_lock:
                    if not unsupported_receipts.is_set():
                        unsupported_receipts.set()
                        logger.info("Optional eth_getBlockReceipts unsupported; falling back to transaction receipts")
        if receipts is None:
            calls = [("eth_getTransactionReceipt", [transaction["hash"]]) for transaction in block["transactions"]]
            receipts = client.batch(calls) if batch_supported else [client.call(method, params) for method, params in calls]
        return block, receipts

    def progress(event, start=None, end=None):
        nonlocal last_report
        last_report = monotonic()
        elapsed = max(0.0, last_report - started)
        rate = new_blocks / elapsed if elapsed > 0 else 0.0
        remaining = total - processed
        eta = "0.0" if remaining == 0 else f"{remaining / rate:.1f}" if rate > 0 else "unknown"
        partition = f"partition=[{start},{end}) " if start is not None else ""
        snapshot = getattr(client, "snapshot", None)
        counters = snapshot() if callable(snapshot) else {}
        rpc_fields = "".join(f" {name}={counters[name]}" for name in (
            "rpc_elements", "http_attempts", "retries", "http_429", "in_flight", "peak_in_flight",
            "rpc_elements_per_second") if name in counters)
        logger.info(
            "Collection %s: %sprocessed_blocks=%d/%d committed_blocks=%d/%d "
            "processed_percent=%.1f committed_percent=%.1f new_blocks=%d new_transactions=%d "
            "elapsed_seconds=%.1f rate_blocks_per_second=%.3f eta_seconds=%s "
            "concurrency=%d scheduled_blocks=%d buffered_blocks=%d%s",
            event, partition, processed, total, committed, total,
            100 * processed / total if total else 100.0,
            100 * committed / total if total else 100.0,
            new_blocks, new_transactions, elapsed, rate, eta, concurrency, len(pending),
            sum(future.done() for number, future in pending), rpc_fields,
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
        blocks = _ordered_blocks(fetch, start, end, concurrency, pending)
        try:
            for number, (block, receipts) in blocks:
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
        except BaseException:
            store.buffer = {name: [] for name in store.buffer}
            store.buffer_start = store.buffer_next = None
            raise
        finally:
            blocks.close()
    progress("complete")
    return store
