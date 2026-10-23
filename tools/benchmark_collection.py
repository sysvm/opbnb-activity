import argparse
import copy
import json
import math
import threading
import time
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx
import pyarrow.parquet

from opbnb_activity import fixtures
from opbnb_activity.collect import collect
from opbnb_activity.config import Config
from opbnb_activity.models import TABLES
from opbnb_activity.quality import validate
from opbnb_activity.rpc import RpcClient
from opbnb_activity.storage import Store, schema


def bounded_integer(minimum, maximum):
    def parse(value):
        parsed = int(value)
        if not minimum <= parsed <= maximum:
            raise argparse.ArgumentTypeError(f"must be in [{minimum},{maximum}]")
        return parsed
    return parse


def bounded_float(minimum, maximum):
    def parse(value):
        parsed = float(value)
        if not math.isfinite(parsed) or not minimum <= parsed <= maximum:
            raise argparse.ArgumentTypeError(f"must be finite and in [{minimum},{maximum}]")
        return parsed
    return parse


def synthetic_blocks(count):
    fixture = json.loads((Path(fixtures.__path__[0]) / "synthetic.json").read_text())
    blocks = {}
    receipts = {}
    transaction_number = 1000000
    timestamp = int(fixture["blocks"][0]["timestamp"], 16)
    for offset in range(count):
        source = fixture["blocks"][offset % len(fixture["blocks"])]
        block = copy.deepcopy(source)
        number = 100 + offset
        block.update(number=hex(number), hash=f"0x{number:064x}", parentHash=f"0x{number - 1:064x}", timestamp=hex(timestamp + offset))
        block_receipts = []
        for transaction in block["transactions"]:
            receipt = copy.deepcopy(fixture["receipts"][transaction["hash"]])
            transaction_number += 1
            transaction.update(hash=f"0x{transaction_number:064x}", blockHash=block["hash"], blockNumber=block["number"])
            receipt.update(transactionHash=transaction["hash"], blockHash=block["hash"], blockNumber=block["number"])
            for log in receipt["logs"]:
                log.update(transactionHash=transaction["hash"], blockHash=block["hash"], blockNumber=block["number"])
            block_receipts.append(receipt)
        blocks[number] = block
        receipts[number] = block_receipts
    return fixture, blocks, receipts


class SimulatedTransport:
    def __init__(self, blocks, receipts, latency):
        self.blocks, self.receipts, self.latency = blocks, receipts, latency
        self.lock = threading.Lock()
        self.attempts = self.in_flight = self.peak = 0

    def __call__(self, request):
        with self.lock:
            self.attempts += 1
            self.in_flight += 1
            self.peak = max(self.peak, self.in_flight)
        try:
            time.sleep(self.latency)
            payload = json.loads(request.content)
            method = payload["method"]
            if method == "eth_chainId":
                result = "0xcc"
            elif method == "eth_getBlockByNumber":
                result = self.blocks[int(payload["params"][0], 16)]
            elif method == "eth_getBlockReceipts":
                result = self.receipts[int(payload["params"][0], 16)]
            else:
                raise AssertionError("unexpected synthetic RPC method")
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": payload["id"], "result": result})
        finally:
            with self.lock:
                self.in_flight -= 1


def run(dataset, name, concurrency, arguments, fixture, blocks, receipts):
    config = Config(dataset=dataset, run_id=name, start=fixture["start"], end=fixture["end"],
                    qps=arguments.qps, concurrency=concurrency, blocks_per_partition=min(500, arguments.blocks))
    store = Store(config, 100, 100 + arguments.blocks, synthetic=True)
    transport = SimulatedTransport(blocks, receipts, arguments.latency_ms / 1000)
    client = RpcClient("https://benchmark.invalid", qps=arguments.qps, concurrency=concurrency,
                       retries=0, transport=httpx.MockTransport(transport))
    try:
        started = time.monotonic()
        collect(client, store)
        elapsed = time.monotonic() - started
        counters = client.snapshot()
        validation_started = time.monotonic()
        quality = validate(store)
        validation_seconds = time.monotonic() - validation_started
        if not quality["ok"]:
            raise AssertionError("synthetic quality validation failed")
        contents = {name: store.read_table(name) for name in TABLES}
        if counters["http_attempts"] != transport.attempts:
            raise AssertionError("HTTP counters disagree with simulated transport")
        if (counters["http_attempts"] != 1 + 2 * arguments.blocks
                or not 1 <= counters["peak_in_flight"] <= concurrency or not 1 <= transport.peak <= concurrency):
            raise AssertionError("unexpected synthetic request count or concurrency")
        result = {"concurrency": concurrency, "elapsed_seconds": elapsed,
                  "blocks_per_second": arguments.blocks / elapsed, "validation_seconds": validation_seconds,
                  "http_attempts": transport.attempts, "peak_in_flight": counters["peak_in_flight"],
                  "transport_peak_in_flight": transport.peak,
                  "rpc_elements": counters["rpc_elements"], "rpc_elements_per_second": counters["rpc_elements_per_second"]}
        return result, contents, quality["counts"]
    finally:
        client.close()
        store.checkpoint.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description="Offline synthetic MockTransport benchmark; no production URL accepted")
    parser.add_argument("--blocks", type=bounded_integer(1, 10000), default=120)
    parser.add_argument("--latency-ms", type=bounded_float(0, 1000), default=10)
    parser.add_argument("--concurrency", type=bounded_integer(1, 128), default=16)
    parser.add_argument("--qps", type=bounded_float(0.001, 1000000), default=10000)
    arguments = parser.parse_args(argv)
    fixture, blocks, receipts = synthetic_blocks(arguments.blocks)
    schema("blocks")
    with TemporaryDirectory(prefix="opbnb-activity-benchmark-") as directory:
        serial, serial_contents, counts = run(Path(directory), "serial", 1, arguments, fixture, blocks, receipts)
        concurrent, concurrent_contents, concurrent_counts = run(Path(directory), "concurrent", arguments.concurrency,
                                                                arguments, fixture, blocks, receipts)
        expected = {"blocks": arguments.blocks, "transactions": sum(len(block["transactions"]) for block in blocks.values()),
                    "receipts": sum(len(values) for values in receipts.values()),
                    "logs": sum(len(receipt["logs"]) for values in receipts.values() for receipt in values)}
        if serial_contents != concurrent_contents or counts != concurrent_counts or counts != expected:
            raise AssertionError("synthetic normalized outputs differ or have incorrect counts")
        output = {"synthetic": True, "equal_outputs": True, "blocks": arguments.blocks,
                  "latency_ms": arguments.latency_ms, "qps": arguments.qps, "counts": counts,
                  "serial": serial, "concurrent": concurrent,
                  "synthetic_speedup": serial["elapsed_seconds"] / concurrent["elapsed_seconds"]}
        print(json.dumps(output, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
