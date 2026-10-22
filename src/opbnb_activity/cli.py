import argparse
import json
import os
import sys
from pathlib import Path

from .collect import collect
from .config import load
from .labels import load_labels
from .metrics import analyze
from .models import normalize_block
from .probe import probe
from .quality import validate
from .report import generate
from .rpc import RpcClient, RpcError
from .storage import Store, atomic_json


def parser():
    result = argparse.ArgumentParser(description="Auditable opBNB activity analytics; demo uses SYNTHETIC data")
    commands = result.add_subparsers(dest="command", required=True)
    for name in ("probe", "collect", "validate", "analyze", "report", "demo"):
        command = commands.add_parser(name)
        command.add_argument("--config", type=Path)
        command.add_argument("--dataset", type=Path)
        command.add_argument("--run-id", required=True)
        if name in ("probe", "collect"):
            command.add_argument("--start")
            command.add_argument("--end")
    return result


def main(argv=None):
    arguments = parser().parse_args(argv)
    try:
        if arguments.command in ("validate", "analyze", "report"):
            settings = load(arguments.config, dataset=arguments.dataset, run_id=arguments.run_id)
            store = Store.open(settings.dataset, arguments.run_id)
            if arguments.command == "validate":
                result = validate(store)
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
        endpoint = os.environ.get("OPBNB_RPC_URL")
        if not endpoint:
            raise ValueError("OPBNB_RPC_URL is required only for probe/collect")
        client = RpcClient(endpoint, qps=settings.qps, timeout=settings.timeout_seconds, retries=settings.retries, batch_size=settings.batch_size)
        try:
            capabilities = probe(client, int(settings.start_utc.timestamp()), int(settings.end_utc.timestamp()))
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
            collect(client, store, block_receipts=capabilities["block_receipts"]["status"] == "supported",
                    batch_supported=capabilities["batch"]["status"] == "supported")
            result = validate(store)
            print(json.dumps(result, indent=2))
            return 0 if result["ok"] else 2
        finally:
            client.close()
    except (ValueError, OSError, RpcError, KeyError) as error:
        message = str(error) if isinstance(error, (ValueError, RpcError)) else type(error).__name__
        print("Error: " + message, file=sys.stderr)
        return 2
