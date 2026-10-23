import logging

from .block_range import quantity, resolve_range
from .rpc import RpcError

logger = logging.getLogger(__name__)


def require_chain(client):
    if quantity(client.call("eth_chainId", [])) != 204:
        raise ValueError("Expected opBNB chain ID 204")


def capability(operation):
    try:
        result = operation()
        return {"status": "supported" if result is not None else "unavailable"}
    except RpcError as error:
        return {"status": error.kind, "code": error.code}


def probe(client, start_timestamp, end_timestamp):
    logger.info("Probe chain check start")
    require_chain(client)
    logger.info("Probe chain check complete: chain_id=204")
    latest = client.call("eth_getBlockByNumber", ["latest", False])
    if not latest:
        raise ValueError("Head block unavailable")
    latest_number = quantity(latest["number"])
    result = {"chain_id": 204, "head_block": latest_number, "throughput_tested": False,
              "note": "Bounded capability samples only; tracing is optional and archive access does not imply tracing."}
    try:
        logger.info("Probe range resolution start")
        start_block, end_block = resolve_range(lambda number: client.call("eth_getBlockByNumber", [hex(number), False]), latest_number, start_timestamp, end_timestamp)
    except RpcError as error:
        result["historical_coverage"] = {"status": error.kind, "code": error.code}
        logger.info("Probe range resolution complete: status=%s", error.kind)
        return result
    logger.info("Probe range resolution complete: blocks=[%d,%d)", start_block, end_block)
    result.update(start_block=start_block, end_block=end_block, historical_coverage={"status": "supported"})
    representative = min(start_block, latest_number)
    header = client.call("eth_getBlockByNumber", [hex(representative), False])
    block_tag = hex(representative)
    logger.info("Probe capability samples start")
    result["batch"] = capability(client.probe_batch)
    result["block_receipts"] = capability(lambda: client.call("eth_getBlockReceipts", [block_tag]))
    hashes = header.get("transactions", [])
    result["transaction_receipts"] = capability(lambda: client.call("eth_getTransactionReceipt", [hashes[0] if isinstance(hashes[0], str) else hashes[0]["hash"]])) if hashes else {"status": "not_sampled_empty_block"}
    result["logs"] = capability(lambda: client.call("eth_getLogs", [{"fromBlock": block_tag, "toBlock": block_tag}]))
    logger.info("Probe capability samples complete: batch=%s block_receipts=%s transaction_receipts=%s logs=%s",
                result["batch"]["status"], result["block_receipts"]["status"],
                result["transaction_receipts"]["status"], result["logs"]["status"])
    logger.info("Probe optional trace start")
    result["trace"] = capability(lambda: client.call("debug_traceBlockByNumber", [block_tag, {"tracer": "callTracer", "timeout": "5s"}]))
    logger.info("Probe optional trace complete: status=%s", result["trace"]["status"])
    return result
