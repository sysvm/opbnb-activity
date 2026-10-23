import json
import logging
import os
import subprocess
import sys
import time

import httpx
import pytest

from conftest import module


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


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


class FixtureClient:
    def __init__(self, fixture, clock, seconds_per_block=2):
        self.fixture, self.clock = fixture, clock
        self.seconds_per_block = seconds_per_block

    def call(self, method, params):
        if method == "eth_chainId":
            return "0xcc"
        if method == "eth_getBlockByNumber":
            self.clock.now += self.seconds_per_block
            return self.fixture["blocks"][int(params[0], 16) - 100]
        if method == "eth_getBlockReceipts":
            block = self.fixture["blocks"][int(params[0], 16) - 100]
            return [self.fixture["receipts"][transaction["hash"]] for transaction in block["transactions"]]
        raise AssertionError(method)


def messages(caplog):
    return [record.getMessage() for record in caplog.records if record.name.startswith("opbnb_activity")]


@pytest.mark.parametrize("recovered", [False, True])
def test_resumed_partitions_count_completion_without_inflating_new_rate(tmp_path, synthetic, monkeypatch, caplog, recovered):
    collector = module("collect")
    clock = Clock()
    monkeypatch.setattr(collector, "monotonic", clock, raising=False)
    config = module("config").Config(dataset=tmp_path, run_id="progress", blocks_per_partition=2)
    store = module("storage").Store(config, 100, 106)
    for block in synthetic["blocks"][:2]:
        store.write_block(int(block["number"], 16), module("models").normalize_block(block, synthetic["receipts"], config.system_recipients))
    if recovered:
        with store.checkpoint.connection:
            store.checkpoint.connection.execute("DELETE FROM tasks")
    caplog.clear()
    caplog.set_level(logging.INFO, logger="opbnb_activity")
    collector.collect(FixtureClient(synthetic, clock), store)
    output = messages(caplog)
    resumed = next(message for message in output if ("recovered" if recovered else "skipped") in message)
    assert "partition=[100,102)" in resumed
    assert "processed_blocks=2/6 committed_blocks=2/6" in resumed
    assert "new_blocks=0 new_transactions=0" in resumed
    assert "rate_blocks_per_second=0.000 eta_seconds=unknown" in resumed
    committed = [message for message in output if "partition committed" in message]
    assert len(committed) == 2
    assert "partition=[102,104)" in committed[0]
    assert "processed_blocks=4/6 committed_blocks=4/6" in committed[0]
    assert "processed_percent=66.7 committed_percent=66.7" in committed[0]
    assert "new_blocks=2 new_transactions=2 elapsed_seconds=4.0" in committed[0]
    assert "rate_blocks_per_second=0.500 eta_seconds=4.0" in committed[0]
    assert "processed_blocks=6/6 committed_blocks=6/6" in committed[-1]
    assert "new_blocks=4 new_transactions=5" in committed[-1]
    assert "processed_percent=100.0 committed_percent=100.0" in committed[-1]
    assert "eta_seconds=0.0" in committed[-1]
    assert "Collection start" in output[0]
    assert "Collection complete" in output[-1]
    assert module("quality").validate(store)["ok"] is True


def test_unfinished_partition_progress_uses_clock_and_block_boundaries(tmp_path, synthetic, monkeypatch, caplog):
    collector = module("collect")
    clock = Clock()
    monkeypatch.setattr(collector, "monotonic", clock, raising=False)
    store = module("storage").Store(module("config").Config(dataset=tmp_path, run_id="timed"), 100, 106)
    caplog.set_level(logging.INFO, logger="opbnb_activity")
    collector.collect(FixtureClient(synthetic, clock, seconds_per_block=10), store)
    output = messages(caplog)
    ongoing = [message for message in output if "partition in progress" in message]
    assert len(ongoing) == 1
    assert "partition=[100,106)" in ongoing[0]
    assert "processed_blocks=3/6 committed_blocks=0/6" in ongoing[0]
    assert "processed_percent=50.0 committed_percent=0.0" in ongoing[0]
    assert "new_blocks=3 new_transactions=7 elapsed_seconds=30.0" in ongoing[0]
    assert "rate_blocks_per_second=0.100 eta_seconds=30.0" in ongoing[0]
    assert len([message for message in output if "partition committed" in message]) == 1


def test_all_resumed_and_empty_runs_have_no_measured_new_rate(tmp_path, synthetic, monkeypatch, caplog):
    collector = module("collect")
    clock = Clock()
    monkeypatch.setattr(collector, "monotonic", clock, raising=False)
    store = module("storage").Store(module("config").Config(dataset=tmp_path, run_id="resumed"), 100, 101)
    collector.collect(FixtureClient(synthetic, clock), store)
    caplog.set_level(logging.INFO, logger="opbnb_activity")
    collector.collect(FixtureClient(synthetic, clock), store)
    final = messages(caplog)[-1]
    assert "processed_percent=100.0 committed_percent=100.0" in final
    assert "new_blocks=0 new_transactions=0" in final
    assert "rate_blocks_per_second=0.000 eta_seconds=0.0" in final
    empty = module("storage").Store(module("config").Config(dataset=tmp_path, run_id="empty"), 100, 100)
    collector.collect(FixtureClient(synthetic, clock), empty)
    assert "processed_blocks=0/0 committed_blocks=0/0 processed_percent=100.0" in messages(caplog)[-1]


@pytest.mark.parametrize("failure", ["transport", "http", "remote"])
def test_retry_logs_only_method_attempt_safe_reason_and_delay(caplog, failure):
    requests = []
    def handler(request):
        requests.append(request)
        if len(requests) == 1:
            if failure == "transport":
                raise httpx.ConnectError("SECRET password https://node.invalid/private", request=request)
            if failure == "http":
                return httpx.Response(429, text="SECRET", headers={"Retry-After": "7"})
            payload = json.loads(request.content)
            return httpx.Response(200, json={"id": payload["id"], "error": {"code": -32005, "message": "SECRET busy"}})
        payload = json.loads(request.content)
        return httpx.Response(200, json={"id": payload["id"], "result": "ok"})
    caplog.set_level(logging.WARNING, logger="opbnb_activity")
    clock = Clock()
    client = module("rpc").RpcClient("https://user:password@node.invalid/private?key=SECRET", transport=httpx.MockTransport(handler),
                                     clock=clock, sleep=lambda seconds: setattr(clock, "now", clock.now + seconds))
    try:
        assert client.call("eth_chainId", []) == "ok"
    finally:
        client.close()
    output = messages(caplog)
    assert len(output) == 1
    assert "method=eth_chainId attempt=1/4" in output[0]
    assert ("reason=HTTP_429" if failure == "http" else "reason=transport" if failure == "transport" else "reason=transient") in output[0]
    assert ("delay_seconds=7.0" if failure == "http" else "delay_seconds=1.0") in output[0]
    assert all(secret not in output[0] for secret in ("SECRET", "password", "node.invalid", "private"))


@pytest.mark.parametrize("action", ["probe", "collect"])
def test_cli_stage_logs_preserve_stdout_json_and_existing_handlers(tmp_path, monkeypatch, caplog, capsys, action):
    cli = module("cli")
    capabilities = {"start_block": 100, "end_block": 100, "historical_coverage": {"status": "supported"},
                    "block_receipts": {"status": "supported"}, "batch": {"status": "supported"}}
    class Client:
        def __init__(self, *args, **kwargs):
            pass
        def close(self):
            pass
    monkeypatch.setenv("OPBNB_RPC_URL", "https://user:password@node.invalid/SECRET")
    monkeypatch.setattr(cli, "RpcClient", Client)
    monkeypatch.setattr(cli, "probe", lambda *args: capabilities)
    monkeypatch.setattr(cli, "collect", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli, "validate", lambda store: {"ok": True})
    caplog.set_level(logging.INFO, logger="opbnb_activity")
    original_handlers = list(logging.getLogger().handlers)
    for _ in range(2):
        assert cli.main([action, "--dataset", str(tmp_path), "--run-id", "stage-test"]) == 0
        captured = capsys.readouterr()
        assert json.loads(captured.out) == (capabilities if action == "probe" else {"ok": True})
        assert captured.err.count("Run start") == 1
        assert "UTC INFO" in captured.err
        assert "run_id=stage-test action=" + action in captured.err
        assert "qps=1.0 batch_size=1 blocks_per_partition=500" in captured.err
        assert "start_utc=2026-07-21T16:00:00+00:00 end_utc=2026-10-21T16:00:00+00:00" in captured.err
        assert captured.err.index("Probe start") < captured.err.index("Probe complete")
        if action == "collect":
            assert captured.err.index("Probe complete") < captured.err.index("Collection stage start") < captured.err.index("Collection stage complete")
            assert captured.err.index("Integrity validation start") < captured.err.index("Integrity validation complete: ok=True")
        assert all(secret not in captured.err for secret in ("SECRET", "password", "node.invalid"))
    assert logging.getLogger().handlers == original_handlers
    assert any("Probe start" in message for message in messages(caplog))


def test_probe_logs_range_and_optional_trace_stages(monkeypatch, caplog):
    probe = module("probe")
    monkeypatch.setattr(probe, "resolve_range", lambda *args: (100, 101))
    class Client:
        def call(self, method, params):
            if method == "eth_chainId":
                return "0xcc"
            if method == "eth_getBlockByNumber":
                return {"number": "0x65", "transactions": []}
            if method == "debug_traceBlockByNumber":
                raise module("rpc").RpcError("unsupported", method, -32601)
            return []
        def probe_batch(self):
            return ["0xcc", "0xcc"]
    caplog.set_level(logging.INFO, logger="opbnb_activity")
    result = probe.probe(Client(), 0, 1)
    output = messages(caplog)
    assert "Probe chain check start" in output
    assert "Probe range resolution start" in output
    assert "Probe range resolution complete: blocks=[100,101)" in output
    assert "Probe optional trace start" in output
    assert "Probe optional trace complete: status=unsupported" in output
    assert result["trace"]["status"] == "unsupported"


def test_cli_redacts_untrusted_value_errors(tmp_path, monkeypatch, capsys):
    cli = module("cli")
    class Client:
        def __init__(self, *args, **kwargs):
            pass
        def close(self):
            pass
    def failure(*args):
        raise ValueError("invalid literal: SECRET https://user:password@node.invalid")
    monkeypatch.setenv("OPBNB_RPC_URL", "https://node.invalid/SECRET")
    monkeypatch.setattr(cli, "RpcClient", Client)
    monkeypatch.setattr(cli, "probe", failure)
    assert cli.main(["probe", "--dataset", str(tmp_path), "--run-id", "error"]) == 2
    captured = capsys.readouterr()
    assert "Error:" in captured.err
    assert all(secret not in captured.err for secret in ("SECRET", "password", "node.invalid"))


@pytest.mark.parametrize("message", [
    "Expected opBNB chain ID 204",
    "run identity/window/source mismatch; choose a different run ID",
    "OPBNB_RPC_URL is required only for probe/collect",
    "start must precede end",
    "missing receipt prevents partition completion",
])
def test_cli_preserves_fixed_actionable_diagnostics(tmp_path, monkeypatch, capsys, message):
    cli = module("cli")
    def failure(*args, **kwargs):
        raise ValueError(message)
    monkeypatch.setattr(cli, "load", failure)
    assert cli.main(["collect", "--dataset", str(tmp_path), "--run-id", "diagnostic"]) == 2
    assert "Error: " + message in capsys.readouterr().err


def test_offline_validation_logs_manifest_window(tmp_path, capsys):
    config = module("config").Config(dataset=tmp_path, run_id="actual-window", start="2026-10-01")
    module("storage").Store(config, 100, 100, synthetic=True)
    assert module("cli").main(["validate", "--dataset", str(tmp_path), "--run-id", "actual-window"]) == 0
    captured = capsys.readouterr()
    assert json.loads(captured.out)["ok"] is True
    assert "start_utc=2026-09-30T16:00:00+00:00" in captured.err


def test_imports_do_not_configure_logging():
    script = """import logging
root = logging.getLogger()
package = logging.getLogger('opbnb_activity')
before = (list(root.handlers), root.level, list(package.handlers), package.level)
import opbnb_activity.cli, opbnb_activity.collect, opbnb_activity.probe, opbnb_activity.rpc
assert before == (list(root.handlers), root.level, list(package.handlers), package.level)
"""
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_cli_flushes_redirected_stderr_before_probe_returns(tmp_path):
    script = """import sys
from opbnb_activity import cli
def probe(*args):
    sys.stdin.readline()
    return {'historical_coverage': {'status': 'supported'}}
cli.probe = probe
raise SystemExit(cli.main(['probe', '--dataset', sys.argv[1], '--run-id', 'flush']))
"""
    environment = dict(os.environ, OPBNB_RPC_URL="https://node.invalid/SECRET")
    log = tmp_path / "progress.log"
    with log.open("w") as stderr:
        process = subprocess.Popen([sys.executable, "-c", script, str(tmp_path)], stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE, stderr=stderr, text=True, env=environment)
        try:
            deadline = time.monotonic() + 10
            while "Probe start" not in log.read_text() and process.poll() is None and time.monotonic() < deadline:
                time.sleep(0.02)
            assert process.poll() is None
            assert "UTC INFO" in log.read_text()
            assert "Probe start" in log.read_text()
            stdout, _ = process.communicate("\n", timeout=10)
            assert process.returncode == 0
            assert json.loads(stdout)["historical_coverage"]["status"] == "supported"
            assert "SECRET" not in log.read_text()
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate()
