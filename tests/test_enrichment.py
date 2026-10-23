import hashlib
import json
import logging
from types import SimpleNamespace

import httpx
import pytest

from conftest import address, module


OPTIMAI = "0xa596e82b0e7d9f5c3e7841cf3f53f66b34d2c1d2"
USDT = "0x9e5aac1ba1a2e6aed6b32689dfcf62a509ca96f3"


@pytest.fixture(autouse=True)
def restore_package_logging():
    logger = logging.getLogger("opbnb_activity")
    handlers, level = list(logger.handlers), logger.level
    yield
    for handler in list(logger.handlers):
        if handler not in handlers:
            logger.removeHandler(handler)
            handler.close()
    logger.setLevel(level)


def make_store(tmp_path, synthetic, live=False):
    config = module("config").Config(dataset=tmp_path, run_id="enrichment", start="2026-10-01")
    store = module("storage").Store(config, 100, 106, synthetic=not live)
    for block in synthetic["blocks"]:
        store.write_block(int(block["number"], 16), module("models").normalize_block(block, synthetic["receipts"], config.system_recipients))
    return store


def test_registry_known_addresses_provenance_assets_and_synthetic_isolation():
    registry = module("registry")
    store = SimpleNamespace(manifest={"chain_id": 204, "synthetic": False})
    labels, assets, provenance = registry.prepare_inputs(store, [], {})
    index = module("labels").LabelIndex(labels)
    assert index.lookup(OPTIMAI, 100)["project"] == "OptimAI Network"
    assert index.lookup("0xd2042ed6021d3c29d8605eb477b45dfcb0a2caf6", 100)["project"] == "Daokraft"
    assert index.lookup("0xb89a6778d1efe7a5b7096757a21b810cc2886fa1", 100)["project"] == "PancakeSwap"
    assert {row["symbol"] for row in assets.values()} == {"BTCB", "ETH", "FDUSD", "USDT", "XCAD"}
    assert assets[USDT]["verified"] is True
    assert all(row["source_sha256"] and row["acquired_date"] == "2026-10-23" for row in labels)
    assert "historical" in provenance["registry_temporal_scope"]
    assert registry.prepare_inputs(SimpleNamespace(manifest={"chain_id": 204, "synthetic": True}), [], {})[:2] == ([], {})
    with pytest.raises(ValueError, match="chain"):
        registry.prepare_inputs(SimpleNamespace(manifest={"chain_id": 56, "synthetic": False}), [], {})


def test_registry_explicit_intervals_split_fallback_and_reject_overlap():
    registry = module("registry")
    store = SimpleNamespace(manifest={"chain_id": 204, "synthetic": False})
    row = dict(address=OPTIMAI.upper(), project="Override", category="test", role="test", valid_from=100, valid_to=110)
    labels, assets, _ = registry.prepare_inputs(store, [row], {USDT.upper(): {"symbol": "USER", "decimals": 6, "verified": False}})
    index = module("labels").LabelIndex(labels)
    assert [index.lookup(OPTIMAI, number)["project"] for number in (99, 100, 109, 110)] == ["OptimAI Network", "Override", "Override", "OptimAI Network"]
    assert assets[USDT]["decimals"] == 6 and assets[USDT]["verified"] is False
    with pytest.raises(ValueError, match="overlapping"):
        registry.prepare_inputs(store, [row, dict(row, valid_from=105)], {})


def test_keccak_tuple_overloads_and_real_selector_collision():
    abi = module("abi")
    entries = [{"type": "function", "name": "transfer", "inputs": [{"type": "address"}, {"type": "uint256"}]},
               {"type": "function", "name": "foo", "inputs": [{"type": "tuple[]", "components": [{"type": "uint"}, {"type": "tuple", "components": [{"type": "address"}]}]}]},
               {"type": "function", "name": "burn", "inputs": [{"type": "uint256"}]},
               {"type": "function", "name": "collate_propagate_storage", "inputs": [{"type": "bytes16"}]}]
    parsed = abi.parse_abi(entries)
    assert parsed["functions"]["0xa9059cbb"] == ["transfer(address,uint256)"]
    assert parsed["functions"]["0x42966c68"] == ["burn(uint256)", "collate_propagate_storage(bytes16)"]
    assert "foo((uint256,(address))[])" in sum(parsed["functions"].values(), [])
    assert abi.selector("checkIn()") == "0x183ff085"
    assert parsed["abi_sha256"]
    with pytest.raises(ValueError):
        abi.parse_abi(entries * 1001)


class FakeRpc:
    def __init__(self, failure=False, chain=204):
        self.calls, self.failure, self.chain = [], failure, chain

    def call(self, method, params):
        self.calls.append((method, params))
        if method == "eth_chainId":
            return hex(self.chain)
        if self.failure:
            raise module("rpc").RpcError("pruned", method)
        if method == "eth_getCode":
            return "0x" if params[1] == {"blockHash": "0x" + (100).to_bytes(32).hex(), "requireCanonical": True} else "0x6000"
        if params[0]["data"] == "0x313ce567":
            return "0x" + (18).to_bytes(32).hex()
        return "0x" + b"TOKEN".ljust(32, b"\0").hex()


def test_enrichment_bounded_historical_cache_refresh_retry_and_raw_unchanged(tmp_path, synthetic):
    store = make_store(tmp_path, synthetic)
    original = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in store.root.rglob("*.parquet")}
    enrich = module("enrichment").enrich
    rpc = FakeRpc()
    cache = enrich(store, rpc, top=2, allow_synthetic=True)
    assert len(cache["targets"]["recipients"]) == 2
    assert len(cache["targets"]["tokens"]) == 1
    assert all(row["address"] != module("config").SYSTEM_RECIPIENT for row in cache["code"].values())
    assert {row["kind"] for row in cache["code"].values()} == {"code_present", "no_code_observed"}
    assert all(row["block_hash"] for row in cache["code"].values())
    code_calls = [params for method, params in rpc.calls if method == "eth_getCode"]
    assert [address(103), {"blockHash": "0x" + (100).to_bytes(32).hex(), "requireCanonical": True}] in code_calls
    assert [address(103), {"blockHash": "0x" + (105).to_bytes(32).hex(), "requireCanonical": True}] in code_calls
    assert all(isinstance(params[1], dict) and params[1]["requireCanonical"] for method, params in rpc.calls if method == "eth_call")
    prior = len(rpc.calls)
    enrich(store, rpc, top=2, allow_synthetic=True)
    assert len(rpc.calls) == prior + 1
    enrich(store, rpc, top=2, refresh=True, allow_synthetic=True)
    assert len(rpc.calls) > prior + 2
    rpc.failure = True
    partial = enrich(store, rpc, top=2, refresh=True, allow_synthetic=True)
    assert partial["status"] == "partial"
    assert all(row["kind"] == "unknown" for row in partial["code"].values())
    rpc.failure = False
    assert enrich(store, rpc, top=2, allow_synthetic=True)["code"] != partial["code"]
    assert original == {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in store.root.rglob("*.parquet")}


def test_enrichment_live_synthetic_guard_and_chain_validation(tmp_path, synthetic):
    store = make_store(tmp_path, synthetic)
    with pytest.raises(ValueError, match="synthetic"):
        module("enrichment").enrich(store, FakeRpc())
    with pytest.raises(ValueError, match="204"):
        module("enrichment").enrich(store, FakeRpc(chain=56), allow_synthetic=True)
    for top in (0, 101):
        with pytest.raises(ValueError, match="top"):
            module("enrichment").enrich(store, FakeRpc(), top=top, allow_synthetic=True)


def test_offline_details_unknown_scope_unverified_token_and_wrong_cache(tmp_path, synthetic):
    store = make_store(tmp_path, synthetic)
    enrichment = module("enrichment")
    enrichment.enrich(store, FakeRpc(), top=2, allow_synthetic=True)
    result = module("metrics").analyze(store, [], {})
    assert len(result["recipient_details"]) == 4
    detail = next(row for row in result["recipient_details"] if row["address"] == address(103))
    assert detail["project"] == "Unknown"
    assert detail["sample_first_kind"] == "no_code_observed"
    assert detail["sample_last_kind"] == "code_present"
    assert detail["historical_kind"] == "unknown"
    assert result["summary"]["verified_token_raw_amounts"] == {}
    assert result["token_metadata"][0]["verification"] == "observed_unverified"
    assert result["token_metadata"][0]["raw_amount"] is None
    assert all(not isinstance(value, (list, dict)) for name in ("recipient_details", "token_metadata", "unresolved", "methods") for row in result[name] for value in row.values())
    path = store.root / "enrichment.json"
    cache = json.loads(path.read_text())
    cache["identity"] = "wrong"
    path.write_text(json.dumps(cache))
    result = module("metrics").analyze(store, [], {})
    assert result["metadata"]["enrichment_status"] == "invalid"
    assert all(row["sample_first_kind"] == "unknown" for row in result["recipient_details"])


def test_standard_signatures_remain_candidates_and_registry_matching(tmp_path, synthetic):
    synthetic["blocks"][0]["transactions"][0].update(to=OPTIMAI, input="0xa9059cbb")
    store = make_store(tmp_path, synthetic, live=True)
    result = module("metrics").analyze(store, [], {})
    detail = next(row for row in result["recipient_details"] if row["address"] == OPTIMAI)
    assert detail["project"] == "OptimAI Network"
    method = next(row for row in result["methods"] if row["selector"] == "0xa9059cbb")
    assert method["signature_candidates"] == "transfer(address,uint256)"
    assert "unverified" in method["signature_evidence"]
    assert result["events"][0]["signature_candidates"] == "Transfer(address,address,uint256)"
    assert result["events"][0]["eligible_log_count"] == 2
    assert result["events"][0]["share"] == 1


def metadata_client(handler):
    return module("enrichment").ContractMetadataClient("https://secret.example/key-SECRET/contract/", transport=httpx.MockTransport(handler), sleep=lambda seconds: None)


def test_metadata_preserves_endpoint_query_and_overrides_action_address(tmp_path, synthetic, caplog):
    requests = []
    def handler(request):
        requests.append(request)
        return httpx.Response(200, json={"result": {"ContractName": "Example", "ABI": []}})
    client = module("enrichment").ContractMetadataClient(
        "https://example.invalid/api?apikey=PUBLIC%2BQUERY%26TOKEN%3D%252F&chainid=204"
        "&route=first&route=second&action=old&action=other&address=old&address=other",
        transport=httpx.MockTransport(handler), sleep=lambda seconds: None)
    store = make_store(tmp_path, synthetic)
    try:
        cache = module("enrichment").enrich(store, FakeRpc(), top=1, metadata=client, allow_synthetic=True)
        assert len(requests) == 2
        for request in requests:
            assert request.url.params.get_list("apikey") == ["PUBLIC+QUERY&TOKEN=%2F"]
            assert request.url.params.get_list("chainid") == ["204"]
            assert request.url.params.get_list("route") == ["first", "second"]
            assert request.url.params.get_list("action") == ["getsourcecode"]
            assert request.url.params.get_list("address") == [request.url.params["address"]]
            assert request.url.params["address"] in cache["contracts"]
        assert all(row["status"] == "ok" for row in cache["contracts"].values())
        assert "PUBLIC" not in (store.root / "enrichment.json").read_text()
        assert "PUBLIC" not in caplog.text
        assert "PUBLIC" not in json.dumps(module("metrics").analyze(store, [], {}, save=False))
    finally:
        client.close()


def test_metadata_rest_wrappers_abi_hash_no_raw_source_and_success_cache(tmp_path, synthetic):
    requests = []
    def handler(request):
        requests.append(request)
        return httpx.Response(200, json={"status": "1", "result": [{"ContractName": "Example", "SourceCode": "SECRET SOURCE", "ABI": json.dumps([{"type": "function", "name": "checkIn", "inputs": []}])}]})
    client = metadata_client(handler)
    store = make_store(tmp_path, synthetic)
    cache = module("enrichment").enrich(store, FakeRpc(), top=1, metadata=client, allow_synthetic=True)
    assert requests[0].method == "GET"
    assert requests[0].url.params["action"] == "getsourcecode"
    assert len(requests) == 2
    assert all(row["temporal_scope"] == "current verified explorer metadata; historical implementation unproven" for row in cache["contracts"].values())
    assert "SECRET" not in (store.root / "enrichment.json").read_text()
    module("enrichment").enrich(store, FakeRpc(), top=1, metadata=client, allow_synthetic=True)
    assert len(requests) == 2
    client.close()


@pytest.mark.parametrize("response,expected", [(httpx.Response(401, text="SECRET"), "auth"), (httpx.Response(429, text="SECRET"), "rate_limited"), (httpx.Response(200, text="bad SECRET"), "malformed"), (httpx.Response(200, content=b"x" * (2 * 1024 * 1024 + 1)), "oversize")])
def test_metadata_failures_sanitized_and_global_failures_stop(response, expected, tmp_path, synthetic):
    requests = []
    client = metadata_client(lambda request: (requests.append(request), response)[1])
    cache = module("enrichment").enrich(make_store(tmp_path, synthetic), FakeRpc(), top=2, metadata=client, allow_synthetic=True)
    assert cache["status"] == "partial"
    assert expected in {row["status"] for row in cache["contracts"].values()}
    if expected in ("auth", "rate_limited"):
        assert len(requests) == 1
    assert "SECRET" not in json.dumps(cache)
    client.close()


def test_token_decoders_bound_uint8_dynamic_and_bytes32():
    enrichment = module("enrichment")
    assert enrichment.decode_decimals("0x" + (255).to_bytes(32).hex()) == 255
    with pytest.raises(ValueError):
        enrichment.decode_decimals("0x" + (256).to_bytes(32).hex())
    assert enrichment.decode_symbol("0x" + b"ABC".ljust(32, b"\0").hex()) == "ABC"
    dynamic = (32).to_bytes(32) + (3).to_bytes(32) + b"ABC".ljust(32, b"\0")
    assert enrichment.decode_symbol("0x" + dynamic.hex()) == "ABC"
    with pytest.raises(ValueError):
        enrichment.decode_symbol("0x" + (2048).to_bytes(32).hex())


def test_cli_enrich_help_and_top_bounds(capsys):
    cli = module("cli")
    arguments = cli.parser().parse_args(["enrich", "--run-id", "test", "--top", "10", "--refresh"])
    assert arguments.top == 10 and arguments.refresh
    with pytest.raises(SystemExit):
        cli.parser().parse_args(["enrich", "--run-id", "test", "--top", "101"])
    with pytest.raises(SystemExit):
        cli.parser().parse_args(["enrich", "--help"])
    assert "--refresh" in capsys.readouterr().out


@pytest.mark.parametrize("mutation", [
    lambda cache: cache.update(chain_id=56),
    lambda cache: cache.update(schema_version=2),
    lambda cache: next(iter(cache["code"].values())).update(block_number=999999),
    lambda cache: next(iter(cache["tokens"].values())).update(decimals=999),
    lambda cache: next(iter(cache["contracts"].values())).update(status="ok", functions={"0x1234": "bad"}, events={}),
])
def test_malformed_observation_cache_never_used(tmp_path, synthetic, mutation):
    store = make_store(tmp_path, synthetic)
    enrichment = module("enrichment")
    cache = enrichment.enrich(store, FakeRpc(), top=1, allow_synthetic=True)
    mutation(cache)
    (store.root / "enrichment.json").write_text(json.dumps(cache))
    result = module("metrics").analyze(store, [], {})
    assert result["metadata"]["enrichment_status"] == "invalid"


def test_unsupported_methods_stop_once_and_resume_flushes_completed_observations(tmp_path, synthetic):
    store = make_store(tmp_path, synthetic)
    class Unsupported(FakeRpc):
        def call(self, method, params):
            if method == "eth_chainId":
                return super().call(method, params)
            self.calls.append((method, params))
            raise module("rpc").RpcError("unsupported", method)
    rpc = Unsupported()
    module("enrichment").enrich(store, rpc, top=2, allow_synthetic=True)
    assert len([call for call in rpc.calls if call[0] == "eth_getCode"]) == 1
    assert len([call for call in rpc.calls if call[0] == "eth_call"]) == 1
    class Interrupted(FakeRpc):
        def call(self, method, params):
            if len(self.calls) == 2:
                raise KeyboardInterrupt()
            return super().call(method, params)
    with pytest.raises(KeyboardInterrupt):
        module("enrichment").enrich(store, Interrupted(), top=2, refresh=True, allow_synthetic=True)
    cache = json.loads((store.root / "enrichment.json").read_text())
    assert len(cache["code"]) == 1 and next(iter(cache["code"].values()))["status"] == "ok"
    resumed = FakeRpc()
    module("enrichment").enrich(store, resumed, top=2, allow_synthetic=True)
    assert len([call for call in resumed.calls if call[0] == "eth_getCode"]) == 3


def test_events_are_bounded_have_full_denominator_and_anonymous_unknown(tmp_path, synthetic):
    receipt = synthetic["receipts"][synthetic["blocks"][0]["transactions"][0]["hash"]]
    template = receipt["logs"][0]
    receipt["logs"] = [dict(template, address=address(800 + number), topics=[], logIndex=hex(number)) for number in range(110)]
    store = make_store(tmp_path, synthetic)
    result = module("metrics").analyze(store, [], {})
    assert len(result["events"]) == 100
    assert all(row["eligible_log_count"] == 111 for row in result["events"])
    anonymous = next(row for row in result["events"] if row["topic0"] is None)
    assert anonymous["signature_candidates"] == "" and anonymous["signature_evidence"] == "unknown"


def test_rest_object_wrapper_abi_event_and_no_redirects():
    entry = {"ContractName": "Observed", "ABI": [{"type": "event", "name": "Event", "anonymous": False, "inputs": [{"type": "address"}]},
                {"type": "event", "name": "Anonymous", "anonymous": True, "inputs": []}]}
    client = metadata_client(lambda request: httpx.Response(200, json={"result": entry}))
    result = client.fetch(address(1))
    assert result["status"] == "ok" and len(result["events"]) == 1
    assert list(result["events"].values()) == [["Event(address)"]]
    client.close()
    requests = []
    client = metadata_client(lambda request: (requests.append(request), httpx.Response(302, headers={"Location": "https://evil.example"}))[1])
    assert client.fetch(address(1))["status"] == "http_rejected"
    assert len(requests) == 1
    client.close()


def test_metadata_optional_url_absent_is_partial_and_cli_no_endpoint_safe(tmp_path, synthetic, monkeypatch, capsys):
    store = make_store(tmp_path, synthetic, live=True)
    cache = module("enrichment").enrich(store, FakeRpc(), top=1)
    assert cache["status"] == "partial"
    assert {entry["status"] for entry in cache["contracts"].values()} == {"unavailable_no_api"}
    monkeypatch.delenv("OPBNB_RPC_URL", raising=False)
    assert module("cli").main(["enrich", "--dataset", str(tmp_path), "--run-id", store.config.run_id]) == 2
    assert "OPBNB_RPC_URL is required for enrich" in capsys.readouterr().err


def test_official_asset_exact_sums_and_explicit_override(tmp_path, synthetic):
    for receipt in synthetic["receipts"].values():
        for log in receipt["logs"]:
            log["address"] = USDT
    store = make_store(tmp_path, synthetic, live=True)
    result = module("metrics").analyze(store, [], {})
    token = result["token_metadata"][0]
    assert token["verification"] == "verified_fungible"
    assert token["raw_amount"] == str(10**40 + 1)
    assert token["scaled_amount"] == "10000000000000000000000.000000000000000001"
    assert token["source_timestamp"] == "2024-03-29T09:21:13.199Z"
    override = {USDT: {"verified": False, "symbol": "USER", "decimals": 6}}
    result = module("metrics").analyze(store, [], override)
    assert result["summary"]["verified_token_raw_amounts"] == {}
    assert result["token_metadata"][0]["verification"] == "unknown"


def test_enrich_refuses_incomplete_dataset_before_rpc(tmp_path):
    config = module("config").Config(dataset=tmp_path, run_id="incomplete")
    store = module("storage").Store(config, 100, 106)
    rpc = FakeRpc()
    with pytest.raises(ValueError, match="quality checks failed"):
        module("enrichment").enrich(store, rpc)
    assert rpc.calls == []


def test_cache_observation_block_hash_must_match_raw_block(tmp_path, synthetic):
    store = make_store(tmp_path, synthetic)
    cache = module("enrichment").enrich(store, FakeRpc(), top=1, allow_synthetic=True)
    key, row = next(iter(cache["code"].items()))
    row["block_number"] += 1
    (store.root / "enrichment.json").write_text(json.dumps(cache))
    assert module("metrics").analyze(store, [], {})["metadata"]["enrichment_status"] == "invalid"


def test_provider_rejects_eip1898_without_number_fallback_or_spam(tmp_path, synthetic):
    class NoHashSupport(FakeRpc):
        def call(self, method, params):
            if method == "eth_chainId":
                return super().call(method, params)
            self.calls.append((method, params))
            raise module("rpc").RpcError("permanent", method, -32602)
    rpc = NoHashSupport()
    cache = module("enrichment").enrich(make_store(tmp_path, synthetic), rpc, top=2, allow_synthetic=True)
    assert len([call for call in rpc.calls if call[0] == "eth_getCode"]) == 1
    assert len([call for call in rpc.calls if call[0] == "eth_call"]) == 1
    assert all(row["kind"] == "unknown" for row in cache["code"].values())


def test_metadata_unsupported_endpoint_stops_and_decoding_failure_sanitized():
    requests = []
    client = metadata_client(lambda request: (requests.append(request), httpx.Response(404, text="SECRET"))[1])
    assert client.fetch(address(1))["status"] == "unsupported"
    assert client.fetch(address(2))["status"] == "unsupported"
    assert len(requests) == 1
    client.close()
    client = metadata_client(lambda request: httpx.Response(200, headers={"Content-Encoding": "gzip"}, content=b"SECRET"))
    assert client.fetch(address(1))["status"] == "transport"
    client.close()


def test_recipient_label_aggregation_uses_one_scan(tmp_path, synthetic):
    store = make_store(tmp_path, synthetic)
    connection = store.connection()
    connection.execute("CREATE VIEW activity AS SELECT transactions.*, receipts.status,'Unknown' AS project,'unknown' AS category,'unknown' AS role FROM transactions JOIN receipts ON transactions.hash=receipts.transaction_hash")
    class CountedConnection:
        def __init__(self):
            self.scans = 0
        def execute(self, query, *arguments):
            if "GROUP BY project,category,role" in query or "GROUP BY recipient,project,category,role" in query:
                self.scans += 1
            return connection.execute(query, *arguments)
    counted = CountedConnection()
    result = {"metadata": {}, "summary": {"verified_token_raw_amounts": {}, "verified_token_amounts": {}}, "methods": []}
    try:
        module("enrichment").augment_result(counted, result, store, {}, {})
        assert len(result["recipient_details"]) == 4
        assert counted.scans == 1
    finally:
        connection.close()


def test_synthetic_unit_fake_override_cannot_enable_real_rpc_client(tmp_path, synthetic):
    requests = []
    transport = httpx.MockTransport(lambda request: (requests.append(request), httpx.Response(200, json={"jsonrpc": "2.0", "id": json.loads(request.content)["id"], "result": "0xcc"}))[1])
    rpc = module("rpc").RpcClient("https://example.invalid/SECRET", transport=transport, sleep=lambda seconds: None)
    try:
        with pytest.raises(ValueError, match="synthetic"):
            module("enrichment").enrich(make_store(tmp_path, synthetic), rpc, allow_synthetic=True)
        assert requests == []
    finally:
        rpc.close()


def test_cache_failed_code_cannot_claim_observed_kind(tmp_path, synthetic):
    store = make_store(tmp_path, synthetic)
    cache = module("enrichment").enrich(store, FakeRpc(), top=1, allow_synthetic=True)
    next(iter(cache["code"].values())).update(status="pruned", kind="code_present")
    (store.root / "enrichment.json").write_text(json.dumps(cache))
    assert module("metrics").analyze(store, [], {})["metadata"]["enrichment_status"] == "invalid"


def valid_contract_cache(store):
    client = metadata_client(lambda request: httpx.Response(200, json={"result": {"ContractName": "Example", "ABI": [
        {"type": "function", "name": "transfer", "inputs": [{"type": "address"}, {"type": "uint256"}]},
        {"type": "event", "name": "Transfer", "inputs": [{"type": "address"}, {"type": "address"}, {"type": "uint256"}]}]}}))
    try:
        return module("enrichment").enrich(store, FakeRpc(), top=1, metadata=client, allow_synthetic=True)
    finally:
        client.close()


@pytest.mark.parametrize("changes", [
    {"functions": {"0xa9059cbb": ["invented()"]}},
    {"contract_name": {"not": "a string"}},
    {"contract_name": "X" * 129},
    {"abi_status": {}},
    {"abi_status": "historic_verified"},
    {"abi_sha256": "bad"},
    {"temporal_scope": "historically verified"},
    {"source_url": ["invalid"]},
    {"observed_at": "invalid"},
    {"status": "invented_status"},
    {"events": {"0x1234": ["Transfer(address,address,uint256)"]}},
    {"functions": {"0xa9059cbb": []}},
    {"functions": {"0xa9059cbb": ["transfer(address,uint256)"] * 1001}},
    {"functions": {"0xa9059cbb": ["x" * 4097]}},
    {"functions": None},
    {"extra": {"unbounded": "nested"}},
])
def test_cache_contract_evidence_schema_rejects_corruption(tmp_path, synthetic, changes):
    store = make_store(tmp_path, synthetic)
    cache = valid_contract_cache(store)
    assert module("enrichment").load_cache(store)[1] == "complete"
    recipient = cache["targets"]["recipients"][0]["address"]
    cache["contracts"][recipient].update(changes)
    (store.root / "enrichment.json").write_text(json.dumps(cache))
    result = module("metrics").analyze(store, [], {}, save=False)
    assert result["metadata"]["enrichment_status"] == "invalid"
    assert all(isinstance(row["current_contract_name"], str) for row in result["recipient_details"])


@pytest.mark.parametrize("changes", [
    {"status": "ok", "symbol": None},
    {"status": "ok", "decimals_status": "pruned"},
    {"status": "partial"},
    {"status": "invented_status"},
    {"decimals": True},
    {"symbol_status": {}},
    {"source": {}},
    {"verification": "verified_fungible"},
    {"extra": ["unexpected"]},
])
def test_cache_token_evidence_schema_rejects_inconsistent_success(tmp_path, synthetic, changes):
    store = make_store(tmp_path, synthetic)
    cache = module("enrichment").enrich(store, FakeRpc(), top=1, allow_synthetic=True)
    next(iter(cache["tokens"].values())).update(changes)
    (store.root / "enrichment.json").write_text(json.dumps(cache))
    assert module("enrichment").load_cache(store)[1] == "invalid"


def test_synthetic_unit_override_rejects_live_metadata_transport_before_dispatch(tmp_path, synthetic, monkeypatch):
    dispatches = []
    def block_request(transport, request):
        dispatches.append(request)
        raise httpx.ConnectError("blocked SECRET")
    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", block_request)
    client = module("enrichment").ContractMetadataClient("https://example.invalid/SECRET/contract/", retries=0, sleep=lambda seconds: None)
    try:
        with pytest.raises(ValueError, match="synthetic"):
            module("enrichment").enrich(make_store(tmp_path, synthetic), FakeRpc(), metadata=client, allow_synthetic=True)
        assert dispatches == []
    finally:
        client.close()


def test_enrichment_normalizes_valid_uppercase_chain_quantity(tmp_path, synthetic):
    class UppercaseChain(FakeRpc):
        def call(self, method, params):
            return "0xCC" if method == "eth_chainId" else super().call(method, params)
    result = module("enrichment").enrich(make_store(tmp_path, synthetic), UppercaseChain(), top=1, allow_synthetic=True)
    assert result["chain_id"] == 204


@pytest.mark.parametrize("changes", [{"code_hash": {}}, {"status": "invented"}, {"extra": []}])
def test_cache_code_failure_fields_stay_flat_and_typed(tmp_path, synthetic, changes):
    store = make_store(tmp_path, synthetic)
    cache = module("enrichment").enrich(store, FakeRpc(failure=True), top=1, allow_synthetic=True)
    next(iter(cache["code"].values())).update(changes)
    (store.root / "enrichment.json").write_text(json.dumps(cache))
    assert module("enrichment").load_cache(store)[1] == "invalid"


def test_cache_complete_status_requires_successful_rows(tmp_path, synthetic):
    store = make_store(tmp_path, synthetic)
    cache = module("enrichment").enrich(store, FakeRpc(), top=1, allow_synthetic=True)
    cache["status"] = "complete"
    (store.root / "enrichment.json").write_text(json.dumps(cache))
    assert module("enrichment").load_cache(store)[1] == "invalid"


def test_valid_cached_tuple_collision_candidates_and_partial_token_roundtrip(tmp_path, synthetic):
    store = make_store(tmp_path, synthetic)
    cache = valid_contract_cache(store)
    entries = [{"type": "function", "name": "foo", "inputs": [{"type": "tuple[]", "components": [{"type": "uint256"}, {"type": "tuple", "components": [{"type": "address"}]}]}]},
               {"type": "function", "name": "burn", "inputs": [{"type": "uint256"}]},
               {"type": "function", "name": "collate_propagate_storage", "inputs": [{"type": "bytes16"}]}]
    next(iter(cache["contracts"].values())).update(module("abi").parse_abi(entries))
    token = next(iter(cache["tokens"].values()))
    token.pop("symbol")
    token.pop("symbol_status")
    token["status"], cache["status"] = "partial", "in_progress"
    (store.root / "enrichment.json").write_text(json.dumps(cache))
    validated, status = module("enrichment").load_cache(store)
    assert status == "in_progress" and validated["tokens"] == cache["tokens"]
