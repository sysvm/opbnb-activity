import argparse
import copy
import hashlib
import json
import logging
import os
import resource
import subprocess
import sys
import threading
import time
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow.parquet as parquet

from opbnb_activity import fixtures
from opbnb_activity.collect import collect
from opbnb_activity.config import Config
from opbnb_activity.models import TABLES
from opbnb_activity.quality import validate
from opbnb_activity.storage import Store


class SyntheticChain:
    def __init__(self, mode):
        self.mode = mode
        self.fixture = json.loads((Path(fixtures.__path__[0]) / "synthetic.json").read_text())
        self.timestamp = int(self.fixture["blocks"][0]["timestamp"], 16)

    def block(self, number):
        offset = number - 100
        block = copy.deepcopy(self.fixture["blocks"][offset % len(self.fixture["blocks"])])
        block.update(number=hex(number), hash=f"0x{number:064x}", parentHash=f"0x{number - 1:064x}",
                     timestamp=hex(self.timestamp + offset))
        if self.mode == "empty":
            block["transactions"] = []
        receipts = []
        log_index = 0
        for position, transaction in enumerate(block["transactions"]):
            receipt = copy.deepcopy(self.fixture["receipts"][transaction["hash"]])
            transaction_hash = f"0x{1000000 + offset * 100 + position:064x}"
            transaction.update(hash=transaction_hash, blockHash=block["hash"], blockNumber=block["number"],
                               transactionIndex=hex(position))
            receipt.update(transactionHash=transaction_hash, blockHash=block["hash"], blockNumber=block["number"],
                           transactionIndex=hex(position))
            for log in receipt["logs"]:
                log.update(transactionHash=transaction_hash, blockHash=block["hash"], blockNumber=block["number"],
                           logIndex=hex(log_index))
                log_index += 1
            receipts.append(receipt)
        return block, receipts

    def expected_counts(self, count):
        result = {name: 0 for name in TABLES}
        result["blocks"] = count
        if self.mode == "empty":
            return result
        periods, remainder = divmod(count, len(self.fixture["blocks"]))
        for offset, block in enumerate(self.fixture["blocks"]):
            repetitions = periods + (offset < remainder)
            result["transactions"] += repetitions * len(block["transactions"])
            result["logs"] += repetitions * sum(len(self.fixture["receipts"][transaction["hash"]]["logs"])
                                                   for transaction in block["transactions"])
        result["receipts"] = result["transactions"]
        return result


class SyntheticInterruption(Exception):
    pass


class SyntheticClient:
    def __init__(self, chain, interrupt_after=None):
        self.chain = chain
        self.interrupt_after = interrupt_after
        self.counts = {"eth_chainId": 0, "eth_getBlockByNumber": 0, "eth_getBlockReceipts": 0}
        self.minimum_block = None

    def call(self, method, params):
        self.counts[method] += 1
        if method == "eth_chainId":
            return "0xcc"
        number = int(params[0], 16)
        if method == "eth_getBlockByNumber":
            self.minimum_block = number if self.minimum_block is None else min(self.minimum_block, number)
            if self.interrupt_after is not None and number == 100 + self.interrupt_after:
                raise SyntheticInterruption()
        block, receipts = self.chain.block(number)
        return block if method == "eth_getBlockByNumber" else receipts


def rss_bytes(value, platform):
    return int(value if platform == "darwin" else value * 1024)


def tree_bytes(path):
    total = 0
    try:
        for entry in path.rglob("*"):
            try:
                if entry.is_file():
                    total += entry.stat().st_size
            except FileNotFoundError:
                pass
    except FileNotFoundError:
        pass
    return total


class DiskSampler:
    def __init__(self, root):
        self.root = root
        self.staging_peak = 0
        self.data_peak = 0
        self.finished = threading.Event()
        self.lock = threading.Lock()
        self.thread = threading.Thread(target=self.run, name="benchmark-disk-sampler")

    def sample(self):
        with self.lock:
            staging = tree_bytes(self.root / ".staging")
            self.staging_peak = max(self.staging_peak, staging)
            self.data_peak = max(self.data_peak, staging + tree_bytes(self.root / "data"))

    def run(self):
        while not self.finished.wait(0.05):
            self.sample()

    def start(self):
        self.sample()
        self.thread.start()

    def stop(self):
        self.finished.set()
        self.thread.join()
        self.sample()


class FinalizationTimer(logging.Handler):
    def __init__(self, sampler):
        super().__init__()
        self.sampler = sampler
        self.started = None
        self.seconds = 0.0
        self.completed = 0

    def emit(self, record):
        message = record.getMessage().lower()
        if "finalization start" in message:
            self.started = time.monotonic()
            self.sampler.sample()
        elif "finalization complete" in message and self.started is not None:
            self.seconds += time.monotonic() - self.started
            self.completed += 1
            self.started = None
        elif "fragment committed" in message:
            self.sampler.sample()


def fingerprints(store):
    result = {}
    counts = {}
    for name in TABLES:
        checksum = hashlib.sha256()
        count = 0
        for directory in store.partition_dirs():
            for batch in parquet.ParquetFile(directory / (name + ".parquet")).iter_batches(batch_size=4096):
                for row in batch.to_pylist():
                    checksum.update(json.dumps(row, sort_keys=True, separators=(",", ":")).encode())
                    checksum.update(b"\n")
                count += batch.num_rows
        result[name] = checksum.hexdigest()
        counts[name] = count
    return result, counts


def run_worker(arguments):
    chain = SyntheticChain(arguments.mode)
    name = f"{arguments.mode}-{arguments.partition_blocks}" + ("-resume" if arguments.interrupted else "")
    config = Config(dataset=arguments.output_directory, run_id=name, start=chain.fixture["start"],
                    end=chain.fixture["end"], blocks_per_partition=arguments.partition_blocks,
                    blocks_per_checkpoint=arguments.checkpoint_blocks)
    store = Store(config, 100, 100 + arguments.blocks, synthetic=True)
    sampler = DiskSampler(store.root)
    timer = FinalizationTimer(sampler)
    package_logger = logging.getLogger("opbnb_activity")
    package_logger.setLevel(logging.INFO)
    package_logger.addHandler(timer)
    sampler.start()
    started = time.monotonic()
    interruption = None
    try:
        if arguments.interrupted:
            client = SyntheticClient(chain, arguments.interrupt_after)
            try:
                collect(client, store)
            except SyntheticInterruption:
                pass
            else:
                raise AssertionError("requested synthetic interruption did not occur")
            durable_end = store.durable_end(100)
            interruption = {"after_normalized_blocks": arguments.interrupt_after,
                            "durable_blocks": durable_end - 100, "before_restart_rpc_calls": client.counts}
            store.checkpoint.close()
            store = Store.open(arguments.output_directory, name)
        client = SyntheticClient(chain)
        collect(client, store)
        collection_seconds = time.monotonic() - started
        if interruption is not None:
            avoided = interruption["durable_blocks"]
            expected_new = arguments.blocks - avoided
            if client.minimum_block != 100 + avoided or client.counts["eth_getBlockByNumber"] != expected_new or client.counts["eth_getBlockReceipts"] != expected_new:
                raise AssertionError("resume refetched durable blocks or missed new blocks")
            if arguments.checkpoint_blocks == 500 and arguments.interrupt_after == 1250 and avoided != 1000:
                raise AssertionError("fixture interruption did not preserve expected 1000-block prefix")
            interruption.update(rpc_blocks_avoided=avoided, rpc_calls_avoided=2 * avoided,
                                first_resumed_rpc_block=client.minimum_block)
        elif client.counts["eth_getBlockByNumber"] != arguments.blocks or client.counts["eth_getBlockReceipts"] != arguments.blocks:
            raise AssertionError("unexpected synthetic RPC counts")
        validation_started = time.monotonic()
        quality = validate(store)
        validation_seconds = time.monotonic() - validation_started
        if not quality["ok"] or quality["counts"] != chain.expected_counts(arguments.blocks):
            raise AssertionError("synthetic quality/count validation failed")
        fingerprint_started = time.monotonic()
        checksums, counts = fingerprints(store)
        fingerprint_seconds = time.monotonic() - fingerprint_started
        if counts != quality["counts"]:
            raise AssertionError("bounded fingerprint counts disagree with quality")
        directories = store.partition_dirs()
        files = sum(len(list(directory.iterdir())) for directory in directories)
        if files != 5 * len(directories):
            raise AssertionError("unexpected final partition file count")
        if tree_bytes(store.root / ".staging"):
            raise AssertionError("successful run retained private fragment files")
        sampler.stop()
        return {"mode": arguments.mode, "blocks_per_partition": arguments.partition_blocks,
                "process_id": os.getpid(),
                "blocks_per_checkpoint": arguments.checkpoint_blocks, "quality_ok": True, "counts": counts,
                "fingerprints": checksums, "collection_seconds": collection_seconds,
                "finalization_seconds": timer.seconds, "finalization_events": timer.completed,
                "validation_seconds": validation_seconds, "fingerprint_seconds": fingerprint_seconds,
                "elapsed_seconds": time.monotonic() - started,
                "peak_rss_bytes": rss_bytes(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss, sys.platform),
                "peak_staging_disk_bytes": sampler.staging_peak, "peak_data_disk_bytes": sampler.data_peak,
                "disk_sampling_interval_seconds": 0.05, "final_directories": len(directories),
                "final_files": files, "final_parquet_files": 4 * len(directories),
                "final_disk_bytes": tree_bytes(store.root / "data"), "remaining_staging_bytes": 0,
                "rpc_calls": client.counts, "interruption": interruption}
    finally:
        if sampler.thread.is_alive():
            sampler.stop()
        package_logger.removeHandler(timer)
        store.checkpoint.close()


def bounded_integer(minimum, maximum):
    def parse(value):
        parsed = int(value)
        if not minimum <= parsed <= maximum:
            raise argparse.ArgumentTypeError(f"must be in [{minimum},{maximum}]")
        return parsed
    return parse


def parser():
    result = argparse.ArgumentParser(description="Offline lazy synthetic partition benchmark; isolated RSS, no endpoint input")
    result.add_argument("--blocks", type=bounded_integer(1, 100000), default=120)
    result.add_argument("--checkpoint-blocks", type=bounded_integer(1, 5000), default=500)
    result.add_argument("--interrupt-after", type=bounded_integer(1, 100000), default=1250)
    result.add_argument("--mode", choices=("both", "representative", "empty"), default="both")
    result.add_argument("--output-directory", type=Path)
    result.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    result.add_argument("--partition-blocks", type=int, choices=(500, 100000), default=100000, help=argparse.SUPPRESS)
    result.add_argument("--interrupted", action="store_true", help=argparse.SUPPRESS)
    return result


def run_isolated(arguments, directory, mode, partition_blocks, interrupted=False):
    command = [sys.executable, str(Path(__file__).resolve()), "--worker", "--blocks", str(arguments.blocks),
               "--checkpoint-blocks", str(arguments.checkpoint_blocks), "--interrupt-after", str(arguments.interrupt_after),
               "--mode", mode, "--partition-blocks", str(partition_blocks), "--output-directory", str(directory)]
    if interrupted:
        command.append("--interrupted")
    completed = subprocess.run(command, text=True, capture_output=True)
    if completed.returncode:
        raise RuntimeError("isolated synthetic benchmark failed:\n" + completed.stderr)
    return json.loads(completed.stdout)


def compare(arguments, directory):
    comparisons = {}
    for mode in (("representative", "empty") if arguments.mode == "both" else (arguments.mode,)):
        small = run_isolated(arguments, directory, mode, 500)
        large = run_isolated(arguments, directory, mode, 100000)
        resumed = run_isolated(arguments, directory, mode, 100000, interrupted=True) if arguments.interrupt_after < arguments.blocks else None
        for other in (large, resumed):
            if other is not None and (small["counts"] != other["counts"] or small["fingerprints"] != other["fingerprints"]):
                raise AssertionError("partition sizes/resume produced different normalized rows")
        comparisons[mode] = {"equal_outputs": True, "partition_500": small, "partition_100000": large,
                             "interrupted_resume": resumed}
    return {"synthetic": True, "blocks": arguments.blocks, "isolated_processes": True, "comparisons": comparisons,
            "limits": "Fixture-derived small payloads; sampled logical disk peaks; RSS includes validation. No production throughput or 16GB capacity claim."}


def main(argv=None):
    arguments = parser().parse_args(argv)
    if arguments.worker:
        if arguments.output_directory is None or arguments.mode == "both":
            raise ValueError("worker requires output directory and one mode")
        output = run_worker(arguments)
    elif arguments.output_directory is not None:
        arguments.output_directory.mkdir(parents=True, exist_ok=True)
        output = compare(arguments, arguments.output_directory)
    else:
        with TemporaryDirectory(prefix="opbnb-partitions-benchmark-") as directory:
            output = compare(arguments, Path(directory))
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
