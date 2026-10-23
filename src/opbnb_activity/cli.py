import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path

from .collect import collect
from .config import load
from .enrichment import ContractMetadataClient, enrich
from .labels import load_labels
from .metrics import analyze
from .models import normalize_block
from .probe import probe
from .quality import validate
from .report import generate
from .rpc import RpcClient, RpcError
from .storage import Store, atomic_json

logger = logging.getLogger(__name__)

SAFE_VALUE_ERRORS = {
    "start must precede end",
    "historical block unavailable",
    "historical coverage does not include window start",
    "incomplete window: head has not reached requested exclusive end",
    "invalid decimals",
    "duplicate transaction hash",
    "missing or mismatched receipt",
    "transaction block hash/index mismatch",
    "transaction value is required and must be an integer quantity",
    "transaction value must be an integer quantity",
    "transaction value must be nonnegative",
    "receipt logs is required and must be a list of log objects",
    "receipt status must be 0 or 1",
    "log receipt/hash/index mismatch",
    "Explicit safe run_id required (letters, numbers, underscore, dot, dash)",
    "Invalid bounded RPC settings",
    "Asset decimals must be an integer in [0,255]",
    "invalid label validity range",
    "overlapping labels require explicit reconciliation",
    "labels CSV missing required provenance/range columns",
    "checkpoint identity mismatch",
    "historical full block unavailable",
    "missing receipt prevents partition completion",
    "duplicate receipt prevents partition completion",
    "Invalid RPC settings",
    "Invalid exclusive block range",
    "run identity/window/source mismatch; choose a different run ID",
    "block outside manifest range",
    "missing or duplicated partition",
    "partition must start at its first block",
    "partition blocks must be ordered and contiguous",
    "Expected opBNB chain ID 204",
    "Head block unavailable",
    "quality checks failed; incomplete data cannot produce final analytics",
    "OPBNB_RPC_URL is required only for probe/collect",
    "Historical coverage unavailable; collection not started",
    "OPBNB_RPC_URL is required for enrich",
    "synthetic enrichment refuses live network",
    "top must be in [1,100]",
}


def configure_logging():
    package_logger = logging.getLogger("opbnb_activity")
    for handler in list(package_logger.handlers):
        if handler.get_name() == "opbnb_activity_cli":
            package_logger.removeHandler(handler)
            handler.close()
    handler = logging.StreamHandler(sys.stderr)
    handler.set_name("opbnb_activity_cli")
    formatter = logging.Formatter("%(asctime)s UTC %(levelname)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")
    formatter.converter = time.gmtime
    handler.setFormatter(formatter)
    package_logger.addHandler(handler)
    package_logger.setLevel(logging.INFO)


def log_start(arguments, settings):
    logger.info("Run start: run_id=%s action=%s start_utc=%s end_utc=%s qps=%s batch_size=%d blocks_per_partition=%d",
                settings.run_id, arguments.command, settings.start_utc.isoformat(), settings.end_utc.isoformat(),
                settings.qps, settings.batch_size, settings.blocks_per_partition)


def validate_with_logging(store):
    logger.info("Integrity validation start")
    result = validate(store)
    logger.info("Integrity validation complete: ok=%s", result["ok"])
    return result


def parser():
    result = argparse.ArgumentParser(description="Auditable opBNB activity analytics; demo uses SYNTHETIC data")
    commands = result.add_subparsers(dest="command", required=True)
    for name in ("probe", "collect", "validate", "analyze", "report", "demo", "enrich"):
        command = commands.add_parser(name)
        command.add_argument("--config", type=Path)
        command.add_argument("--dataset", type=Path)
        command.add_argument("--run-id", required=True)
        if name in ("probe", "collect"):
            command.add_argument("--start")
            command.add_argument("--end")
        if name == "enrich":
            command.add_argument("--top", type=int, choices=range(1, 101), default=20, metavar="1..100", help="maximum recipients and Transfer emitters each (default: 20)")
            command.add_argument("--refresh", action="store_true", help="redo successful cached observations; failures always retry")
    return result


def main(argv=None):
    arguments = parser().parse_args(argv)
    configure_logging()
    try:
        if arguments.command == "enrich":
            settings = load(arguments.config, dataset=arguments.dataset, run_id=arguments.run_id)
            store = Store.open(settings.dataset, arguments.run_id)
            log_start(arguments, settings)
            if store.manifest["synthetic"]:
                raise ValueError("synthetic enrichment refuses live network")
            endpoint = os.environ.get("OPBNB_RPC_URL")
            if not endpoint:
                raise ValueError("OPBNB_RPC_URL is required for enrich")
            client = RpcClient(endpoint, qps=settings.qps, timeout=settings.timeout_seconds, retries=settings.retries, batch_size=settings.batch_size)
            metadata = None
            try:
                if os.environ.get("OPBNB_CONTRACT_API_URL"):
                    metadata = ContractMetadataClient(os.environ["OPBNB_CONTRACT_API_URL"], qps=settings.qps,
                        timeout=settings.timeout_seconds, retries=settings.retries)
                result = enrich(store, client, top=arguments.top, refresh=arguments.refresh, metadata=metadata)
                print(json.dumps({"status": result["status"], "recipients": len(result["targets"]["recipients"]), "tokens": len(result["targets"]["tokens"])}))
                return 0
            finally:
                client.close()
                if metadata:
                    metadata.close()
        if arguments.command in ("validate", "analyze", "report"):
            settings = load(arguments.config, dataset=arguments.dataset, run_id=arguments.run_id)
            store = Store.open(settings.dataset, arguments.run_id)
            log_start(arguments, store.config)
            if arguments.command == "validate":
                result = validate_with_logging(store)
                print(json.dumps(result, indent=2))
                return 0 if result["ok"] else 2
            labels = load_labels(settings.labels or store.config.labels)
            if not labels and store.manifest["synthetic"] and (store.root / "fixture-labels.json").exists():
                labels = json.loads((store.root / "fixture-labels.json").read_text())
            assets = settings.assets or store.config.assets
            if arguments.command == "analyze":
                result = analyze(store, labels, assets)
                print(json.dumps(result["summary"], indent=2))
            else:
                print(generate(store, labels, assets))
            return 0
        settings = load(arguments.config, dataset=arguments.dataset, run_id=arguments.run_id,
                        start=getattr(arguments, "start", None), end=getattr(arguments, "end", None))
        if arguments.command == "demo":
            fixture = json.loads((Path(__file__).parent / "fixtures" / "synthetic.json").read_text())
            settings.start, settings.end = fixture["start"], fixture["end"]
            settings.labels, settings.assets = None, fixture["assets"]
            log_start(arguments, settings)
            store = Store(settings, 100, 106, synthetic=True)
            atomic_json(store.root / "fixture-labels.json", fixture["labels"])
            for start, end in store.ranges():
                if store.completed(start) or store.recover(start):
                    continue
                for block in fixture["blocks"]:
                    number = int(block["number"], 16)
                    if start <= number < end:
                        store.write_block(number, normalize_block(block, fixture["receipts"], settings.system_recipients, settings.system_depositor))
            print("SYNTHETIC / TEST DATA: " + str(generate(store, fixture["labels"], fixture["assets"])))
            return 0
        log_start(arguments, settings)
        endpoint = os.environ.get("OPBNB_RPC_URL")
        if not endpoint:
            raise ValueError("OPBNB_RPC_URL is required only for probe/collect")
        client = RpcClient(endpoint, qps=settings.qps, timeout=settings.timeout_seconds, retries=settings.retries, batch_size=settings.batch_size)
        try:
            logger.info("Probe start")
            capabilities = probe(client, int(settings.start_utc.timestamp()), int(settings.end_utc.timestamp()))
            logger.info("Probe complete: historical_coverage=%s", capabilities["historical_coverage"]["status"])
            run_path = settings.dataset / settings.run_id
            run_path.mkdir(parents=True, exist_ok=True)
            atomic_json(run_path / "probe.json", dict(capabilities, identity=settings.identity,
                        start_utc=settings.start_utc.isoformat(), end_utc=settings.end_utc.isoformat()))
            if arguments.command == "probe":
                print(json.dumps(capabilities, indent=2))
                return 0
            if capabilities["historical_coverage"]["status"] != "supported":
                raise ValueError("Historical coverage unavailable; collection not started")
            store = Store(settings, capabilities["start_block"], capabilities["end_block"])
            logger.info("Collection stage start")
            collect(client, store, block_receipts=capabilities["block_receipts"]["status"] == "supported",
                    batch_supported=capabilities["batch"]["status"] == "supported")
            logger.info("Collection stage complete")
            result = validate_with_logging(store)
            print(json.dumps(result, indent=2))
            return 0 if result["ok"] else 2
        finally:
            client.close()
    except (ValueError, OSError, RpcError, KeyError) as error:
        message = str(error) if isinstance(error, RpcError) else type(error).__name__
        if isinstance(error, ValueError) and str(error) in SAFE_VALUE_ERRORS:
            message = str(error)
        logger.error("Error: %s", message)
        return 2
