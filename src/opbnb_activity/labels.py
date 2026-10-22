import csv
import hashlib
import json
from collections import defaultdict


class LabelIndex:
    def __init__(self, rows):
        self.rows = [dict(row) for row in rows]
        self.addresses = defaultdict(list)
        for row in self.rows:
            row["address"] = row["address"].lower()
            row["valid_from"] = int(row.get("valid_from") or 0)
            row["valid_to"] = int(row["valid_to"]) if row.get("valid_to") else 2**63 - 1
            if row["valid_from"] >= row["valid_to"]:
                raise ValueError("invalid label validity range")
            for existing in self.addresses[row["address"]]:
                if max(existing["valid_from"], row["valid_from"]) < min(existing["valid_to"], row["valid_to"]):
                    raise ValueError("overlapping labels require explicit reconciliation")
            self.addresses[row["address"]].append(row)

    def lookup(self, address, block_number):
        for row in self.addresses.get(address or "", []):
            if row["valid_from"] <= block_number < row["valid_to"]:
                return row
        return {"project": "Unknown", "category": "unknown", "role": "unknown", "confidence": "unknown", "source_url": ""}

    @property
    def version(self):
        return hashlib.sha256(json.dumps(self.rows, sort_keys=True).encode()).hexdigest()


def load_labels(path):
    if not path:
        return []
    with open(path, newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        required = {"address", "project", "category", "role", "source_url", "confidence", "valid_from", "valid_to"}
        if not required.issubset(reader.fieldnames or []):
            raise ValueError("labels CSV missing required provenance/range columns")
        rows = list(reader)
    LabelIndex(rows)
    return rows
