import json

import httpx
import pytest

from conftest import block_hash, module


def rpc_client(endpoint, *, sleep=lambda seconds: None, **settings):
    now = [0.0]

    def advance(seconds):
        now[0] += seconds
        sleep(seconds)

    return module("rpc").RpcClient(endpoint, clock=lambda: now[0], sleep=advance, **settings)


def test_batch_matches_ids_and_retries_only_failed_items():
    calls, sleeps = [], []
    def handler(request):
        payload = json.loads(request.content)
        calls.append(payload)
        if len(calls) == 1:
            return httpx.Response(200, json=[{"jsonrpc": "2.0", "id": payload[1]["id"], "error": {"code": -32005, "message": "busy"}},
                                            {"jsonrpc": "2.0", "id": payload[0]["id"], "result": "first"}])
        return httpx.Response(200, json=[{"jsonrpc": "2.0", "id": payload[0]["id"], "result": "second"}])
    client = rpc_client("https://secret:password@node.invalid/private?key=hidden", transport=httpx.MockTransport(handler), sleep=sleeps.append, batch_size=20)
    assert client.batch([("a", []), ("b", [])]) == ["first", "second"]
    assert len(calls[1]) == 1
    assert calls[1][0]["method"] == "b"
    assert sum(sleeps) >= 3


@pytest.mark.parametrize("kind", ["missing", "duplicate"])
def test_batch_rejects_missing_and_duplicate_ids(kind):
    def handler(request):
        payload = json.loads(request.content)
        output = [{"id": payload[0]["id"], "result": "ok"}]
        if kind == "duplicate":
            output.append(output[0])
        return httpx.Response(200, json=output)
    client = rpc_client("https://node.invalid", transport=httpx.MockTransport(handler), sleep=lambda seconds: None, batch_size=20)
    with pytest.raises(module("rpc").RpcProtocolError, match="IDs"):
        client.batch([("a", []), ("b", [])])


def test_429_retry_after_and_secret_redaction():
    calls, sleeps = [], []
    def handler(request):
        calls.append(1)
        if len(calls) == 1:
            return httpx.Response(429, headers={"Retry-After": "7"})
        payload = json.loads(request.content)
        return httpx.Response(200, json={"id": payload["id"], "result": "ok"})
    client = rpc_client("https://node.invalid/secret", transport=httpx.MockTransport(handler), sleep=sleeps.append)
    assert client.call("a", []) == "ok"
    assert 7 in sleeps
    def failure(request):
        raise httpx.ConnectError("https://user:password@node.invalid/key=secret", request=request)
    client = rpc_client("https://user:password@node.invalid/key=secret", transport=httpx.MockTransport(failure), sleep=lambda seconds: None, retries=1)
    with pytest.raises(module("rpc").RpcError) as caught:
        client.call("a", [])
    assert "password" not in str(caught.value)
    assert "secret" not in str(caught.value)


def test_retry_after_beyond_wait_budget_stops_instead_of_retrying_early():
    calls = []
    def handler(request):
        calls.append(1)
        return httpx.Response(429, headers={"Retry-After": "600"})
    client = rpc_client("https://node.invalid", transport=httpx.MockTransport(handler), sleep=lambda seconds: None)
    with pytest.raises(module("rpc").RpcError, match="retry-after"):
        client.call("eth_chainId", [])
    assert len(calls) == 1


def test_invalid_endpoint_is_redacted_at_construction():
    with pytest.raises(module("rpc").RpcError) as caught:
        module("rpc").RpcClient("https://node.invalid:SECRET_PORT")
    assert "SECRET_PORT" not in str(caught.value)


def test_malformed_error_envelope_is_a_protocol_failure():
    def handler(request):
        payload = json.loads(request.content)
        return httpx.Response(200, json={"id": payload["id"], "error": "invalid"})
    client = rpc_client("https://node.invalid", transport=httpx.MockTransport(handler), sleep=lambda seconds: None)
    with pytest.raises(module("rpc").RpcProtocolError):
        client.call("eth_chainId", [])


def test_missing_receipt_never_marks_partition_complete(tmp_path, synthetic):
    class MissingReceipt:
        def call(self, method, params):
            if method == "eth_chainId":
                return "0xcc"
            if method == "eth_getBlockByNumber":
                return synthetic["blocks"][0]
            return None
        def batch(self, calls):
            return [None for _ in calls]
    config = module("config").Config(dataset=tmp_path, run_id="missing")
    store = module("storage").Store(config, 100, 101)
    with pytest.raises(ValueError, match="receipt"):
        module("collect").collect(MissingReceipt(), store)
    assert store.completed(100) is False
    assert store.partition_dirs() == []


def test_unsupported_is_distinct_from_pruned_and_transient():
    rpc = module("rpc")
    assert rpc.error_kind(-32601, "method not found") == "unsupported"
    assert rpc.error_kind(-32000, "missing trie node") == "pruned"
    assert rpc.error_kind(-32005, "rate limit") == "transient"
    assert rpc.error_kind(-32000, "execution reverted") == "permanent"


def test_probe_checks_chain_before_costly_work():
    calls = []
    class WrongChain:
        def call(self, method, params):
            calls.append(method)
            return "0x1"
    with pytest.raises(ValueError, match="204"):
        module("probe").probe(WrongChain(), 0, 1)
    assert calls == ["eth_chainId"]


def test_probe_uses_one_small_real_batch_with_default_batch_size():
    calls = []
    def handler(request):
        payload = json.loads(request.content)
        calls.append(payload)
        return httpx.Response(200, json=[{"id": item["id"], "result": "0xcc"} for item in payload])
    client = rpc_client("https://node.invalid", transport=httpx.MockTransport(handler), sleep=lambda seconds: None)
    assert client.probe_batch() == ["0xcc", "0xcc"]
    assert len(calls) == 1
    assert len(calls[0]) == 2


def test_collector_falls_back_to_transaction_receipts_and_resumes(tmp_path, synthetic):
    rpc = module("rpc")
    requests = []
    block_by_number = {int(block["number"], 16): block for block in synthetic["blocks"]}
    def handler(request):
        payload = json.loads(request.content)
        single = isinstance(payload, dict)
        payload = [payload] if single else payload
        answers = []
        for item in payload:
            method, params = item["method"], item["params"]
            requests.append(method)
            if method == "eth_getBlockByNumber":
                result = block_by_number[int(params[0], 16)]
            elif method == "eth_getBlockReceipts":
                answers.append({"id": item["id"], "error": {"code": -32601, "message": "not supported"}})
                continue
            elif method == "eth_getTransactionReceipt":
                result = synthetic["receipts"][params[0]]
            elif method == "eth_chainId":
                result = "0xcc"
            else:
                raise AssertionError(method)
            answers.append({"id": item["id"], "result": result})
        return httpx.Response(200, json=answers[0] if single else list(reversed(answers)))
    client = rpc_client("https://node.invalid", transport=httpx.MockTransport(handler), sleep=lambda seconds: None)
    config = module("config").Config(dataset=tmp_path, run_id="mock")
    store = module("storage").Store(config, 100, 106)
    module("collect").collect(client, store, block_receipts=True)
    assert module("quality").validate(store)["ok"] is True
    assert requests.count("eth_getBlockReceipts") == 1
    first_requests = len(requests)
    module("collect").collect(client, store, block_receipts=False)
    assert len(requests) == first_requests + 1
    assert len(store.read_table("transactions")) == 11
