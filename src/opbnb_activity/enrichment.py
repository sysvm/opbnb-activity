import hashlib
import json
import logging
import re
import time
from datetime import datetime, timezone

import httpx

from .abi import STANDARD_EVENTS, STANDARD_FUNCTIONS, parse_abi
from .block_range import quantity
from .decode import TRANSFER_TOPIC, decimal_units
from .enrichment_schema import CURRENT_SCOPE, METADATA_SOURCE, validate_code_row, validate_contract_payload, validate_contract_row, validate_token_row
from .labels import LabelIndex
from .quality import validate
from .rpc import RpcClient, RpcError
from .storage import atomic_json, digest
from .analysis_stages import materialize, run_query

logger = logging.getLogger(__name__)
SCHEMA_VERSION = 1
HEX_ADDRESS = re.compile(r"0x[0-9a-f]{40}")
HEX_HASH = re.compile(r"0x[0-9a-f]{64}")
MAX_RESPONSE = 2 * 1024 * 1024
MAX_CACHE = 32 * 1024 * 1024


def bounded_top(value):
    if type(value) is not int or not 1 <= value <= 100:
        raise ValueError("top must be in [1,100]")
    return value


def rows(cursor):
    names = [column[0] for column in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def targets(connection, top):
    staged = connection.execute("SELECT count(*) FROM information_schema.tables WHERE table_name='eligible_logs'").fetchall()[0][0] > 0
    activity_source = 'activity' if staged else 'transactions'
    recipients = rows(run_query(connection, 'enrichment_recipients', f"""SELECT recipient AS address, count(*) AS transactions,
        min(block_number) AS first_block, max(block_number) AS last_block,
        arg_min(block_hash, block_number) AS first_hash, arg_max(block_hash, block_number) AS last_hash
        FROM {activity_source} WHERE NOT is_system AND recipient IS NOT NULL
        GROUP BY recipient ORDER BY transactions DESC, recipient LIMIT ?""", [top]))
    if staged:
        materialize(connection, 'report_transfer_keys', """SELECT address,transaction_hash,count(*) AS log_count,
            max(block_number) AS last_block,arg_max(block_hash,block_number) AS last_hash
            FROM eligible_logs WHERE lower(topics[1])=? GROUP BY address,transaction_hash""", [TRANSFER_TOPIC])
        tokens = rows(run_query(connection, 'enrichment_transfer_targets', """SELECT address,sum(log_count)::BIGINT AS log_count,
            count(*) AS transactions,max(last_block) AS last_block,arg_max(last_hash,last_block) AS last_hash
            FROM report_transfer_keys GROUP BY address ORDER BY log_count DESC,address LIMIT ?""", [top]))
    else:
        tokens = rows(connection.execute("""SELECT logs.address, count(*) AS log_count,
        count(DISTINCT logs.transaction_hash) AS transactions, max(logs.block_number) AS last_block,
        arg_max(logs.block_hash,logs.block_number) AS last_hash
        FROM logs JOIN transactions ON logs.transaction_hash=transactions.hash
        JOIN receipts ON logs.transaction_hash=receipts.transaction_hash
        WHERE NOT transactions.is_system AND receipts.status=1 AND lower(logs.topics[1])=?
            GROUP BY logs.address ORDER BY log_count DESC, logs.address LIMIT ?""", [TRANSFER_TOPIC, top]))
    for row in recipients + tokens:
        if not HEX_ADDRESS.fullmatch(row["address"]):
            raise ValueError("invalid enrichment target")
    return {"recipients": recipients, "tokens": tokens}


def observation_key(address, block_hash):
    return address + "@" + block_hash


def decode_hex(value, max_bytes):
    if not isinstance(value, str) or not value.startswith("0x") or len(value) > 2 + max_bytes * 2 or len(value) % 2:
        raise ValueError("invalid bounded hex result")
    return bytes.fromhex(value[2:])


def decode_decimals(value):
    data = decode_hex(value, 32)
    if len(data) != 32 or int.from_bytes(data) > 255:
        raise ValueError("invalid token decimals")
    return int.from_bytes(data)


def decode_symbol(value):
    data = decode_hex(value, 192)
    if len(data) == 32:
        data = data.rstrip(b"\0")
    else:
        if len(data) < 64 or int.from_bytes(data[:32]) != 32:
            raise ValueError("invalid token symbol")
        length = int.from_bytes(data[32:64])
        if not 1 <= length <= 128 or len(data) < 64 + length:
            raise ValueError("invalid token symbol")
        data = data[64:64 + length]
    value = data.decode("utf-8")
    if not value or not value.isprintable():
        raise ValueError("invalid token symbol")
    return value


class ContractMetadataClient:
    def __init__(self, endpoint, *, qps=1, timeout=30, retries=3, transport=None, sleep=time.sleep):
        try:
            parsed = httpx.URL(endpoint)
            if parsed.scheme not in ("http", "https") or not parsed.host:
                raise ValueError()
        except (httpx.InvalidURL, ValueError):
            raise ValueError("invalid contract API URL") from None
        self.endpoint, self.sleep = endpoint, sleep
        self.is_unit_transport = isinstance(transport, httpx.MockTransport)
        self.qps, self.retries = min(qps, 1), retries
        self.stopped = None
        self.client = httpx.Client(timeout=timeout, transport=transport, follow_redirects=False, trust_env=False,
                                   limits=httpx.Limits(max_connections=1, max_keepalive_connections=1))

    def close(self):
        self.client.close()

    def fetch(self, address):
        if self.stopped:
            return {"status": self.stopped}
        endpoint = httpx.URL(self.endpoint).copy_merge_params({"action": "getsourcecode", "address": address})
        for attempt in range(self.retries + 1):
            self.sleep(1 / self.qps)
            try:
                with self.client.stream("GET", endpoint, headers={"Accept-Encoding": "identity"}) as response:
                    if response.status_code in (401, 403, 429):
                        self.stopped = "rate_limited" if response.status_code == 429 else "auth"
                        return {"status": self.stopped}
                    if response.status_code in (404, 405, 501):
                        self.stopped = "unsupported"
                        return {"status": self.stopped}
                    if response.status_code >= 500 and attempt < self.retries:
                        self.sleep(min(2**attempt, 30))
                        continue
                    if response.status_code != 200:
                        return {"status": "http_rejected"}
                    if response.headers.get("Content-Encoding", "identity").lower() != "identity":
                        return {"status": "unsupported_encoding"}
                    buffer = bytearray()
                    for chunk in response.iter_bytes():
                        if len(buffer) + len(chunk) > MAX_RESPONSE:
                            return {"status": "oversize"}
                        buffer.extend(chunk)
                return self._parse(buffer)
            except httpx.HTTPError:
                if attempt < self.retries:
                    self.sleep(min(2**attempt, 30))
                    continue
                return {"status": "transport"}
            except (ValueError, TypeError, KeyError, UnicodeError, RecursionError):
                return {"status": "malformed"}
        return {"status": "transport"}

    def _parse(self, payload):
        value = json.loads(payload)
        if isinstance(value, dict) and str(value.get("status")) == "0":
            message = str(value.get("result", "")) + str(value.get("message", ""))
            if any(word in message.lower() for word in ("rate limit", "too many", "max rate")):
                self.stopped = "rate_limited"
            elif any(word in message.lower() for word in ("api key", "unauthorized", "access denied")):
                self.stopped = "auth"
            return {"status": self.stopped or "unavailable"}
        for _ in range(4):
            if isinstance(value, list):
                value = value[0] if len(value) == 1 else None
            elif isinstance(value, dict) and "ABI" not in value:
                value = value.get("result", value.get("data"))
            else:
                break
        if not isinstance(value, dict):
            raise ValueError()
        name = value.get("ContractName", "")
        if not isinstance(name, str) or len(name) > 128 or (name and not re.fullmatch(r"[A-Za-z0-9_$ .:-]+", name)):
            raise ValueError()
        parsed = parse_abi(value.get("ABI"))
        if len(json.dumps(parsed)) > 64 * 1024:
            raise ValueError()
        result = dict(status="ok", contract_name=name, abi_status="current_verified_candidate", **parsed)
        validate_contract_payload(result)
        return result


def cache_identity(store):
    return {"schema_version": SCHEMA_VERSION, "identity": store.config.identity, "run_id": store.config.run_id,
            "chain_id": store.manifest["chain_id"], "synthetic": store.manifest["synthetic"],
            "start_block": store.start_block, "end_block": store.end_block}


def load_cache(store):
    path = store.root / "enrichment.json"
    if not path.exists():
        return None, "missing"
    try:
        if path.stat().st_size > MAX_CACHE:
            raise ValueError()
        value = json.loads(path.read_text())
        if not isinstance(value, dict) or any(value.get(key) != expected for key, expected in cache_identity(store).items()):
            raise ValueError()
        if value.get("status") not in ("complete", "partial", "in_progress"):
            raise ValueError()
        for name, limit in (("code", 200), ("tokens", 100), ("contracts", 200)):
            mapping = value.get(name)
            if not isinstance(mapping, dict) or len(mapping) > limit:
                raise ValueError()
            for key, row in mapping.items():
                if not isinstance(row, dict) or not HEX_ADDRESS.fullmatch(row.get("address", "")) or not isinstance(row.get("status"), str):
                    raise ValueError()
                if name in ("code", "tokens"):
                    if not HEX_HASH.fullmatch(row.get("block_hash", "")) or key != observation_key(row["address"], row["block_hash"]) or type(row.get("block_number")) is not int:
                        raise ValueError()
                    if not store.start_block <= row["block_number"] < store.end_block:
                        raise ValueError()
                elif key != row["address"]:
                    raise ValueError()
                if name == "code":
                    validate_code_row(row)
                if name == "tokens":
                    validate_token_row(row)
                if name == "contracts":
                    validate_contract_row(row)
        if not isinstance(value.get("targets"), dict) or any(not isinstance(value["targets"].get(name), list) or len(value["targets"][name]) > 100 for name in ("recipients", "tokens")):
            raise ValueError()
        expected_status = "complete" if all(row["status"] == "ok" for name in ("code", "tokens", "contracts") for row in value[name].values()) else "partial"
        if value["status"] not in ("in_progress", expected_status):
            raise ValueError()
        observations = list(value["code"].values()) + list(value["tokens"].values())
        if observations:
            connection = store.connection()
            try:
                numbers = sorted({row["block_number"] for row in observations})
                observed_hashes = dict(connection.execute("SELECT number,hash FROM blocks WHERE number IN (" + ",".join("?" for _ in numbers) + ")", numbers).fetchall())
                if any(observed_hashes.get(row["block_number"]) != row["block_hash"] for row in observations):
                    raise ValueError()
            finally:
                connection.close()
        return value, value["status"]
    except (OSError, ValueError, KeyError, TypeError, RecursionError):
        logger.warning("Enrichment cache ignored: invalid schema or run identity")
        return None, "invalid"


def safe_rpc_error(error):
    if isinstance(error, RpcError) and error.code == -32602:
        return "unsupported_block_reference"
    if isinstance(error, RpcError) and error.kind in ("unsupported", "pruned", "transient", "permanent"):
        return error.kind
    return "rpc_failed"


def enrich(store, rpc, *, top=20, refresh=False, metadata=None, allow_synthetic=False):
    bounded_top(top)
    if store.manifest["synthetic"] and (not allow_synthetic or isinstance(rpc, RpcClient)
            or (isinstance(metadata, ContractMetadataClient) and not metadata.is_unit_transport)):
        raise ValueError("synthetic enrichment refuses live network")
    if not validate(store)["ok"]:
        raise ValueError("quality checks failed; incomplete data cannot produce final analytics")
    if store.manifest["chain_id"] != 204 or quantity(rpc.call("eth_chainId", [])) != 204:
        raise ValueError("Expected opBNB chain ID 204")
    connection = store.connection()
    try:
        selected = targets(connection, top)
    finally:
        connection.close()
    previous, _ = load_cache(store)
    cache = dict(cache_identity(store), targets=selected, code={}, tokens={}, contracts={}, status="in_progress")
    if previous and not refresh:
        recipient_addresses = {row["address"] for row in selected["recipients"]}
        token_addresses = {row["address"] for row in selected["tokens"]}
        for name, addresses in (("code", recipient_addresses), ("tokens", token_addresses), ("contracts", recipient_addresses | token_addresses)):
            cache[name] = {key: row for key, row in previous[name].items() if row["address"] in addresses}
    path = store.root / "enrichment.json"
    atomic_json(path, cache)
    unsupported = set()
    for target in selected["recipients"]:
        address = target["address"]
        logger.info("Enrichment historical code: address=%s", address)
        for prefix in ("first", "last"):
            number, block_hash = target[prefix + "_block"], target[prefix + "_hash"]
            key = observation_key(address, block_hash)
            if cache["code"].get(key, {}).get("status") == "ok":
                continue
            row = dict(address=address, block_number=number, block_hash=block_hash, kind="unknown", code_hash=None)
            try:
                if "eth_getCode" in unsupported:
                    raise RpcError("unsupported")
                data = decode_hex(rpc.call("eth_getCode", [address, {"blockHash": block_hash, "requireCanonical": True}]), 128 * 1024)
                row.update(status="ok", kind="code_present" if data else "no_code_observed", code_hash="0x" + hashlib.sha256(data).hexdigest(), code_hash_algorithm="sha256")
            except (RpcError, ValueError, TypeError) as error:
                row["status"] = safe_rpc_error(error)
                if row["status"] in ("unsupported", "unsupported_block_reference"):
                    unsupported.add("eth_getCode")
            cache["code"][key] = row
            atomic_json(path, cache)
    for target in selected["tokens"]:
        address, number, block_hash = target["address"], target["last_block"], target["last_hash"]
        key = observation_key(address, block_hash)
        logger.info("Enrichment token observation: address=%s", address)
        row = dict(cache["tokens"].get(key, {}), address=address, block_number=number, block_hash=block_hash, verification="observed_unverified", source="historical eth_call")
        for field, selector, decoder in (("decimals", "0x313ce567", decode_decimals), ("symbol", "0x95d89b41", decode_symbol)):
            if row.get(field + "_status") == "ok":
                continue
            try:
                if "eth_call" in unsupported:
                    raise RpcError("unsupported")
                row[field] = decoder(rpc.call("eth_call", [{"to": address, "data": selector}, {"blockHash": block_hash, "requireCanonical": True}]))
                row[field + "_status"] = "ok"
            except (RpcError, ValueError, TypeError, UnicodeError) as error:
                row[field] = None
                row[field + "_status"] = safe_rpc_error(error)
                if row[field + "_status"] in ("unsupported", "unsupported_block_reference"):
                    unsupported.add("eth_call")
            row["status"] = "ok" if all(row.get(name + "_status") == "ok" for name in ("decimals", "symbol")) else "partial"
            cache["tokens"][key] = row
            atomic_json(path, cache)
    for address in sorted({row["address"] for row in selected["recipients"] + selected["tokens"]}):
        if cache["contracts"].get(address, {}).get("status") == "ok":
            continue
        logger.info("Enrichment current contract metadata: address=%s", address)
        row = metadata.fetch(address) if metadata else {"status": "unavailable_no_api"}
        cache["contracts"][address] = dict(row, address=address, source_url=METADATA_SOURCE,
                                          temporal_scope=CURRENT_SCOPE, observed_at=datetime.now(timezone.utc).isoformat())
        atomic_json(path, cache)
    cache["status"] = "complete" if all(row["status"] == "ok" for name in ("code", "tokens", "contracts") for row in cache[name].values()) else "partial"
    atomic_json(path, cache)
    logger.info("Enrichment complete: status=%s recipients=%d tokens=%d", cache["status"], len(selected["recipients"]), len(selected["tokens"]))
    return cache


def signature_candidate(cache, address, key, dimension):
    entry = cache.get("contracts", {}).get(address, {})
    mapping = entry.get(dimension, {}) if entry.get("status") == "ok" else {}
    if key in mapping:
        return "; ".join(mapping[key]), "current verified ABI candidate; historical implementation unproven"
    standard = STANDARD_FUNCTIONS if dimension == "functions" else STANDARD_EVENTS
    if key in standard:
        return "; ".join(standard[key]), "standard signature candidate; unverified"
    return "", "unknown"


def augment_result(connection, result, store, provenance, assets):
    if not connection.execute("SELECT count(*) FROM information_schema.tables WHERE table_name='eligible_logs'").fetchall()[0][0]:
        materialize(connection, 'eligible_logs', """SELECT logs.* FROM logs JOIN activity
            ON logs.transaction_hash=activity.hash WHERE activity.status=1 AND NOT activity.is_system""")
    cache, status = load_cache(store)
    cache = cache or {}
    result["metadata"].update(provenance, enrichment_status=status, enrichment_schema=SCHEMA_VERSION,
                               enrichment_hash=digest(store.root / "enrichment.json") if status not in ("missing", "invalid") else None)
    selected = targets(connection, 100)
    index = LabelIndex(provenance.get("labels", []))
    result["metadata"].pop("labels", None)
    result["recipient_details"], result["token_metadata"], result["unresolved"] = [], [], []
    recipient_labels = {}
    addresses = [row["address"] for row in selected["recipients"]]
    if addresses:
        label_rows = rows(connection.execute("""SELECT recipient,project,category,role,count(*) AS transactions FROM activity
            WHERE NOT is_system AND recipient IN (""" + ",".join("?" for _ in addresses) + """)
            GROUP BY recipient,project,category,role ORDER BY recipient,transactions DESC,project,category,role""", addresses))
        for row in label_rows:
            recipient_labels.setdefault(row["recipient"], []).append(row)
    for target in selected["recipients"]:
        address = target["address"]
        labels = recipient_labels.get(address, [])
        label = index.lookup(address, target["first_block"])
        detail = dict(address=address, transactions=target["transactions"], historical_kind="unknown",
                      project="; ".join(sorted({row["project"] for row in labels})),
                      category="; ".join(sorted({row["category"] for row in labels})), role="; ".join(sorted({row["role"] for row in labels})),
                      source_url=label.get("source_url", ""), confidence=label.get("confidence", "unknown"),
                      temporal_scope=label.get("temporal_scope", "explicit label validity interval" if label["project"] != "Unknown" else "unknown"),
                      label_scope="project/category/role include all observed label intervals; source fields sample first observed block")
        for prefix in ("first", "last"):
            observation = cache.get("code", {}).get(observation_key(address, target[prefix + "_hash"]), {})
            for name, value in (("block", target[prefix + "_block"]), ("block_hash", target[prefix + "_hash"]),
                                ("kind", observation.get("kind", "unknown")), ("status", observation.get("status", "not_queried")),
                                ("code_hash", observation.get("code_hash"))):
                detail["sample_" + prefix + "_" + name] = value
        endpoints = [detail["sample_" + prefix + "_kind"] for prefix in ("first", "last")]
        statuses = [detail["sample_" + prefix + "_status"] for prefix in ("first", "last")]
        detail["code_observation_scope"] = (
            "endpoints_code_present" if statuses == ["ok", "ok"] and endpoints == ["code_present", "code_present"] else
            "endpoints_no_code_observed" if statuses == ["ok", "ok"] and endpoints == ["no_code_observed", "no_code_observed"] else
            "unavailable" if not any(status == "ok" for status in statuses) else "endpoints_mixed_or_incomplete")
        contract = cache.get("contracts", {}).get(address, {})
        detail.update(current_contract_name=contract.get("contract_name", ""), abi_status=contract.get("abi_status", contract.get("status", "not_queried")))
        result["recipient_details"].append(detail)
        if any(row["project"] == "Unknown" for row in labels):
            result["unresolved"].append(dict(address=address, transactions=sum(row["transactions"] for row in labels if row["project"] == "Unknown"),
                reason="project attribution unavailable", status=contract.get("status", "not_queried"), explorer_url="https://opbnb.bscscan.com/address/" + address))
    result["unresolved"].sort(key=lambda row: (-row["transactions"], row["address"]))
    for target in selected["tokens"]:
        address = target["address"]
        asset = assets.get(address, {})
        token = cache.get("tokens", {}).get(observation_key(address, target["last_hash"]), {})
        trusted = asset.get("verified") is True
        result["token_metadata"].append(dict(address=address, log_count=target["log_count"], transactions=target["transactions"],
            symbol=asset.get("symbol", token.get("symbol")), decimals=asset.get("decimals", token.get("decimals")),
            verification="verified_fungible" if trusted else ("observed_unverified" if token else "unknown"),
            source_url=asset.get("source_url", "explicit config" if asset else token.get("source", "")),
            confidence=asset.get("confidence", "explicit" if asset else "unverified"),
            acquired_date=asset.get("acquired_date"), source_timestamp=asset.get("source_timestamp"),
            observed_block=token.get("block_number"), observed_block_hash=token.get("block_hash"),
            observation_status=token.get("status", "not_queried"),
            raw_amount=result["summary"]["verified_token_raw_amounts"].get(address) if trusted else None,
            scaled_amount=result["summary"]["verified_token_amounts"].get(address) if trusted else None))
    for row in result["methods"]:
        row["signature_candidates"], row["signature_evidence"] = signature_candidate(cache, row["recipient"], row["selector"], "functions")
    denominator = connection.execute('SELECT count(*) FROM eligible_logs').fetchall()[0][0]
    materialize(connection, 'report_event_keys', """SELECT address,lower(topics[1]) AS topic0,transaction_hash,count(*) AS log_count
        FROM eligible_logs GROUP BY address,lower(topics[1]),transaction_hash""")
    result["events"] = rows(run_query(connection, 'enrichment_events', """SELECT address,topic0,sum(log_count)::BIGINT AS log_count,
        count(*) AS transactions FROM report_event_keys GROUP BY address,topic0
        ORDER BY log_count DESC,address,topic0 NULLS LAST LIMIT 100"""))
    for row in result["events"]:
        row.update(eligible_log_count=denominator, share=row["log_count"] / denominator if denominator else None)
        row["signature_candidates"], row["signature_evidence"] = signature_candidate(cache, row["address"], row["topic0"], "events")
