import json
import re
from pathlib import Path

from .labels import LabelIndex
from .storage import digest


def prepare_inputs(store, labels, assets):
    if store.manifest["chain_id"] != 204:
        raise ValueError("registry requires chain ID 204")
    explicit = LabelIndex(labels)
    for row in explicit.rows:
        if row.get("chain_id") is not None and int(row["chain_id"]) != 204:
            raise ValueError("label chain ID mismatch")
    merged_assets = {address.lower(): dict(asset) for address, asset in assets.items()}
    provenance = {"registry_hash": None, "registry_status": "synthetic_skipped", "registry_temporal_scope": None}
    if store.manifest["synthetic"]:
        return explicit.rows, merged_assets, provenance
    path = Path(__file__).parent / "fixtures" / "address-registry.json"
    snapshot = json.loads(path.read_text())
    if snapshot["chain_id"] != 204 or snapshot["schema_version"] != 1:
        raise ValueError("registry chain/schema mismatch")
    fallback = []
    for row in LabelIndex(snapshot["labels"]).rows:
        if row["chain_id"] != 204 or not re.fullmatch(r"0x[0-9a-f]{40}", row["address"]):
            raise ValueError("invalid registry address/chain")
        ranges = [(row["valid_from"], row["valid_to"])]
        for override in explicit.addresses.get(row["address"], []):
            remaining = []
            for start, end in ranges:
                if override["valid_to"] <= start or override["valid_from"] >= end:
                    remaining.append((start, end))
                else:
                    if start < override["valid_from"]:
                        remaining.append((start, override["valid_from"]))
                    if override["valid_to"] < end:
                        remaining.append((override["valid_to"], end))
            ranges = remaining
        fallback.extend(dict(row, valid_from=start, valid_to=end) for start, end in ranges)
    provenance.update(registry_hash=digest(path), registry_status="bundled", registry_temporal_scope=snapshot["temporal_scope"])
    return fallback + explicit.rows, dict(snapshot["assets"], **merged_assets), provenance
