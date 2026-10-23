import json
import re
from datetime import datetime, timezone

from .abi import MAX_ABI_ENTRIES, canonical_type, selector, topic

CURRENT_SCOPE = "current verified explorer metadata; historical implementation unproven"
METADATA_SOURCE = "https://docs.bnbchain.org/bnb-opbnb/faq/build-on-opbnb-faqs/"
RPC_FAILURES = {"unsupported", "unsupported_block_reference", "pruned", "transient", "permanent", "rpc_failed"}
CONTRACT_FAILURES = {"auth", "rate_limited", "unsupported", "http_rejected", "unsupported_encoding",
                     "oversize", "transport", "malformed", "unavailable", "unavailable_no_api"}


def signature_parameters(text, depth=0):
    if depth > 8:
        raise ValueError("invalid cached ABI depth")
    if not text:
        return []
    parts, start, nesting = [], 0, 0
    for position, character in enumerate(text):
        if character == "(":
            nesting += 1
        elif character == ")":
            nesting -= 1
            if nesting < 0:
                raise ValueError("invalid cached ABI tuple")
        elif character == "," and nesting == 0:
            parts.append(text[start:position])
            start = position + 1
    if nesting != 0:
        raise ValueError("invalid cached ABI tuple")
    parts.append(text[start:])
    if len(parts) > 100:
        raise ValueError("invalid cached ABI inputs")
    parameters = []
    for part in parts:
        if part.startswith("("):
            nesting = 0
            end = None
            for position, character in enumerate(part):
                nesting += (character == "(") - (character == ")")
                if nesting == 0:
                    end = position
                    break
            if end is None:
                raise ValueError("invalid cached ABI tuple")
            parameters.append({"type": "tuple" + part[end + 1:], "components": signature_parameters(part[1:end], depth + 1)})
        else:
            parameters.append({"type": part})
    return parameters


def validate_signature_maps(row):
    count = 0
    for field, hash_signature, key_length in (("functions", selector, 8), ("events", topic, 64)):
        mapping = row.get(field)
        if not isinstance(mapping, dict) or len(mapping) > MAX_ABI_ENTRIES:
            raise ValueError("invalid cached ABI map")
        for key, signatures in mapping.items():
            if not isinstance(key, str) or not re.fullmatch(r"0x[0-9a-f]{" + str(key_length) + "}", key):
                raise ValueError("invalid cached ABI key")
            if not isinstance(signatures, list) or not 1 <= len(signatures) <= MAX_ABI_ENTRIES:
                raise ValueError("invalid cached ABI candidates")
            for signature in signatures:
                count += 1
                if count > MAX_ABI_ENTRIES or not isinstance(signature, str) or len(signature) > 4096:
                    raise ValueError("invalid cached ABI signature")
                match = re.fullmatch(r"([A-Za-z_$][A-Za-z0-9_$]{0,127})\((.*)\)", signature)
                if not match:
                    raise ValueError("invalid cached ABI signature")
                canonical = match[1] + "(" + ",".join(canonical_type(parameter) for parameter in signature_parameters(match[2])) + ")"
                if canonical != signature or hash_signature(signature) != key:
                    raise ValueError("inconsistent cached ABI signature")
            if signatures != sorted(set(signatures)):
                raise ValueError("invalid cached ABI candidates")
    if len(json.dumps({field: row[field] for field in ("functions", "events")})) > 64 * 1024:
        raise ValueError("cached ABI maps too large")


def validate_contract_payload(row):
    name = row.get("contract_name")
    if not isinstance(name, str) or len(name) > 128 or (name and not re.fullmatch(r"[A-Za-z0-9_$ .:-]+", name)):
        raise ValueError("invalid cached contract name")
    if row.get("abi_status") != "current_verified_candidate" or not isinstance(row.get("abi_sha256"), str) or not re.fullmatch(r"[0-9a-f]{64}", row["abi_sha256"]):
        raise ValueError("invalid cached ABI evidence")
    validate_signature_maps(row)


def validate_contract_row(row):
    fields = {"address", "status", "source_url", "temporal_scope", "observed_at"}
    if row["status"] == "ok":
        fields.update(("contract_name", "abi_status", "abi_sha256", "functions", "events"))
        validate_contract_payload(row)
    elif row["status"] not in CONTRACT_FAILURES:
        raise ValueError("invalid cached contract status")
    if set(row) != fields or row.get("source_url") != METADATA_SOURCE or row.get("temporal_scope") != CURRENT_SCOPE:
        raise ValueError("invalid cached contract provenance")
    observed_at = row.get("observed_at")
    if not isinstance(observed_at, str) or len(observed_at) > 40 or datetime.fromisoformat(observed_at).utcoffset() != timezone.utc.utcoffset(None):
        raise ValueError("invalid cached contract timestamp")


def validate_token_row(row):
    fields = {"address", "status", "block_number", "block_hash", "verification", "source", "decimals", "decimals_status", "symbol", "symbol_status"}
    required = {"address", "status", "block_number", "block_hash", "verification", "source"}
    if not required.issubset(row) or not set(row).issubset(fields) or row.get("verification") != "observed_unverified" or row.get("source") != "historical eth_call":
        raise ValueError("invalid cached token provenance")
    for field in ("decimals", "symbol"):
        status, value = row.get(field + "_status", "not_queried"), row.get(field)
        if status == "ok":
            if field == "decimals" and (type(value) is not int or not 0 <= value <= 255):
                raise ValueError("invalid cached token decimals")
            if field == "symbol" and (not isinstance(value, str) or not 1 <= len(value) <= 128 or not value.isprintable()):
                raise ValueError("invalid cached token symbol")
        elif not isinstance(status, str) or status not in RPC_FAILURES | {"not_queried"} or value is not None:
            raise ValueError("inconsistent cached token field")
    expected_status = "ok" if all(row.get(field + "_status") == "ok" for field in ("decimals", "symbol")) else "partial"
    if row["status"] != expected_status:
        raise ValueError("inconsistent cached token status")


def validate_code_row(row):
    fields = {"address", "status", "block_number", "block_hash", "kind", "code_hash"}
    if row["status"] == "ok":
        fields.add("code_hash_algorithm")
        if row.get("kind") not in ("code_present", "no_code_observed") or row.get("code_hash_algorithm") != "sha256":
            raise ValueError("invalid cached code evidence")
        if not isinstance(row.get("code_hash"), str) or not re.fullmatch(r"0x[0-9a-f]{64}", row["code_hash"]):
            raise ValueError("invalid cached code hash")
    elif row["status"] not in RPC_FAILURES or row.get("kind") != "unknown" or row.get("code_hash") is not None:
        raise ValueError("inconsistent cached code failure")
    if set(row) != fields:
        raise ValueError("invalid cached code fields")
