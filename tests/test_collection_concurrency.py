import hashlib
import json
import logging
import subprocess
import sys
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from pathlib import Path

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


def settings(tmp_path, run_id="concurrent", **overrides):
    return module("config").Config(dataset=tmp_path, run_id=run_id, **overrides)


class FixtureClient:
    def __init__(self, fixture, failure=None, unsupported=False, first_gate=None):
        self.fixture = fixture
        self.failure = failure
        self.unsupported = unsupported
        self.first_gate = first_gate
        self.worker_ids = set()
        self.receipt_probes = []
        self.finished = []
        self.later_finished = threading.Event()
        self.lock = threading.Lock()

    def call(self, method, params):
        if method == "eth_chainId":
            return "0xcc"
        if method == "eth_getTransactionReceipt":
            return self.fixture["receipts"][params[0]]
        number = int(params[0], 16)
        with self.lock:
            self.worker_ids.add(threading.get_ident())
        if method == "eth_getBlockByNumber":
            if number == 100 and self.first_gate is not None:
                assert self.first_gate.wait(5), "first block was never released"
            if self.failure == (number, "worker"):
                raise module("rpc").RpcError("transport", method)
            return self.fixture["blocks"][number - 100]
        if method == "eth_getBlockReceipts":
            with self.lock:
                self.receipt_probes.append(number)
            if self.unsupported:
                raise module("rpc").RpcError("unsupported", method, -32601)
            if self.failure == (number, "missing"):
                return []
            block = self.fixture["blocks"][number - 100]
            with self.lock:
                self.finished.append(number)
            if number != 100:
                self.later_finished.set()
            return [self.fixture["receipts"][transaction["hash"]] for transaction in block["transactions"]]
        raise AssertionError(method)

    def batch(self, calls):
        return [self.call(method, params) for method, params in calls]

    def snapshot(self):
        return {"rpc_elements": 13, "http_attempts": 13, "retries": 0, "http_429": 0,
                "in_flight": 0, "peak_in_flight": 2, "rpc_elements_per_second": 6.5}


def test_config_concurrency_default_and_old_identity(tmp_path):
    config = settings(tmp_path)
    assert config.concurrency == 1
    old = asdict(config)
    for key in ("dataset", "run_id", "qps", "batch_size", "timeout_seconds", "retries", "labels", "assets", "concurrency", "blocks_per_checkpoint",
                "analysis_memory_limit_mb", "analysis_threads", "analysis_temp_directory"):
        old.pop(key)
    old.update(schema_version=1, chain_id=204)
    expected = hashlib.sha256(json.dumps(old, sort_keys=True).encode()).hexdigest()
    assert config.identity == expected
    assert settings(tmp_path, concurrency=128, qps=500).identity == expected


@pytest.mark.parametrize("concurrency", [True, False, 0, 129, 1.5, "2", None])
def test_config_rejects_invalid_concurrency(tmp_path, concurrency):
    with pytest.raises(ValueError, match="Invalid bounded RPC settings"):
        settings(tmp_path, concurrency=concurrency)


@pytest.mark.parametrize("qps", [float("nan"), float("inf"), float("-inf")])
def test_config_rejects_nonfinite_qps(tmp_path, qps):
    with pytest.raises(ValueError, match="Invalid bounded RPC settings"):
        settings(tmp_path, qps=qps)


def test_old_manifest_and_runtime_resume_preserve_partition_bytes(tmp_path, synthetic):
    store = module("storage").Store(settings(tmp_path, blocks_per_partition=2), 100, 106)
    for block in synthetic["blocks"][:2]:
        store.write_block(int(block["number"], 16), module("models").normalize_block(block, synthetic["receipts"], store.config.system_recipients))
    originals = {path: path.read_bytes() for path in store.block_dir(100).iterdir()}
    manifest = json.loads(store.manifest_path.read_text())
    manifest["config"].pop("concurrency", None)
    store.manifest_path.write_text(json.dumps(manifest))
    opened = module("storage").Store.open(tmp_path, "concurrent")
    assert opened.config.concurrency == 1
    resumed = module("storage").Store(settings(tmp_path, blocks_per_partition=2, qps=500, concurrency=3), 100, 106)
    client = FixtureClient(synthetic)
    module("collect").collect(client, resumed)
    assert min(client.receipt_probes) == 102
    assert all(path.read_bytes() == content for path, content in originals.items())
    assert module("quality").validate(resumed)["ok"] is True


def test_out_of_order_is_bounded_ordered_and_coordinator_writes(tmp_path, synthetic, monkeypatch, caplog):
    collector = module("collect")
    gate = threading.Event()
    window_full = threading.Event()
    awaiting_first = threading.Event()
    submitted = []
    observed = []
    class TrackingExecutor(ThreadPoolExecutor):
        def submit(self, function, *args, **kwargs):
            future = super().submit(function, *args, **kwargs)
            submitted.append(future)
            if len(submitted) == 4:
                window_full.set()
            if args[0] == 100:
                original_result = future.result
                def result(*result_args, **result_kwargs):
                    awaiting_first.set()
                    return original_result(*result_args, **result_kwargs)
                future.result = result
            return future
    monkeypatch.setattr(collector, "ThreadPoolExecutor", TrackingExecutor, raising=False)
    concurrent = module("storage").Store(settings(tmp_path, concurrency=2), 100, 106)
    coordinator = threading.get_ident()
    original_write = concurrent.write_block
    writes = []
    def write(number, tables):
        writes.append((number, threading.get_ident()))
        return original_write(number, tables)
    monkeypatch.setattr(concurrent, "write_block", write)
    client = FixtureClient(synthetic, first_gate=gate)
    def release():
        try:
            observed.append(window_full.wait(2) and awaiting_first.wait(2))
            observed.append(len(submitted))
            observed.append(client.later_finished.wait(2))
        finally:
            gate.set()
    monitor = threading.Thread(target=release)
    monitor.start()
    caplog.set_level(logging.INFO, logger="opbnb_activity")
    try:
        collector.collect(client, concurrent)
    finally:
        gate.set()
        monitor.join(5)
    assert observed == [True, 4, True]
    assert writes == [(number, coordinator) for number in range(100, 106)]
    assert coordinator not in client.worker_ids
    assert len(client.worker_ids) <= 2
    serial = module("storage").Store(settings(tmp_path, "serial"), 100, 106)
    collector.collect(FixtureClient(synthetic), serial)
    for name in module("models").TABLES:
        assert concurrent.read_table(name) == serial.read_table(name)
    quality = module("quality").validate(concurrent)
    assert quality["ok"] is True
    assert quality["counts"] == {"blocks": 6, "transactions": 11, "receipts": 11, "logs": 2}
    progress = [record.getMessage() for record in caplog.records if "Collection complete:" in record.getMessage()][0]
    assert "concurrency=2" in progress
    assert "scheduled_blocks=0 buffered_blocks=0" in progress
    assert "http_attempts=13" in progress


@pytest.mark.parametrize("failure", ["missing", "worker", "storage"])
def test_failed_partition_resumes_without_duplicates(tmp_path, synthetic, monkeypatch, failure):
    config = settings(tmp_path, concurrency=3, blocks_per_partition=2)
    store = module("storage").Store(config, 100, 106)
    original_write = store.write_block
    if failure == "storage":
        def write(number, tables):
            if number == 103:
                raise OSError("disk unavailable")
            return original_write(number, tables)
        monkeypatch.setattr(store, "write_block", write)
    client = FixtureClient(synthetic, failure=(103, failure))
    with pytest.raises((ValueError, OSError, module("rpc").RpcError)):
        module("collect").collect(client, store)
    assert store.completed(100)
    assert not store.completed(102)
    assert not store.completed(104)
    assert store.buffer_start is None
    assert all(not rows for rows in store.buffer.values())
    original = {path: path.read_bytes() for path in store.block_dir(100).iterdir()}
    resumed = module("storage").Store(config, 100, 106)
    module("collect").collect(FixtureClient(synthetic), resumed)
    assert module("quality").validate(resumed)["ok"] is True
    assert len(resumed.read_table("transactions")) == 11
    assert all(path.read_bytes() == content for path, content in original.items())


@pytest.mark.parametrize("concurrency", [1, 3])
def test_known_unsupported_block_receipts_stops_future_probes(tmp_path, synthetic, concurrency):
    store = module("storage").Store(settings(tmp_path, concurrency=concurrency, blocks_per_partition=2), 100, 106)
    client = FixtureClient(synthetic, unsupported=True)
    module("collect").collect(client, store)
    assert 1 <= len(client.receipt_probes) <= concurrency
    assert all(number < 102 for number in client.receipt_probes)
    assert module("quality").validate(store)["ok"] is True


def test_failure_cancels_queued_work_and_waits_for_active_workers(tmp_path, synthetic, monkeypatch):
    collector = module("collect")
    running = threading.Event()
    release = threading.Event()
    finished = threading.Event()
    submitted = threading.Event()
    futures = []
    class TrackingExecutor(ThreadPoolExecutor):
        def submit(self, function, *args, **kwargs):
            future = super().submit(function, *args, **kwargs)
            futures.append(future)
            if len(futures) == 4:
                submitted.set()
            return future
        def shutdown(self, *args, **kwargs):
            release.set()
            return super().shutdown(*args, **kwargs)
    monkeypatch.setattr(collector, "ThreadPoolExecutor", TrackingExecutor, raising=False)
    client = FixtureClient(synthetic)
    original_call = client.call
    def call(method, params):
        if method == "eth_getBlockByNumber":
            number = int(params[0], 16)
            if number == 100:
                assert running.wait(5)
                assert submitted.wait(5)
                raise ValueError("historical full block unavailable")
            running.set()
            assert release.wait(5)
            finished.set()
        return original_call(method, params)
    client.call = call
    store = module("storage").Store(settings(tmp_path, concurrency=2), 100, 106)
    try:
        with pytest.raises(ValueError, match="historical full block unavailable"):
            collector.collect(client, store)
    finally:
        release.set()
    assert finished.is_set()
    assert all(future.done() for future in futures)
    assert len(futures) == 4
    assert any(future.cancelled() for future in futures)
    assert not store.completed(100)


def test_worker_failure_stops_refilling_window_before_ordered_consumption(tmp_path, synthetic, monkeypatch):
    collector = module("collect")
    submitted = []
    window_full = threading.Event()
    failed = threading.Event()
    class TrackingExecutor(ThreadPoolExecutor):
        def submit(self, function, *args, **kwargs):
            future = super().submit(function, *args, **kwargs)
            submitted.append(future)
            if len(submitted) == 4:
                window_full.set()
            return future
    monkeypatch.setattr(collector, "ThreadPoolExecutor", TrackingExecutor)
    client = FixtureClient(synthetic)
    original_call = client.call
    def call(method, params):
        if method == "eth_getBlockByNumber":
            number = int(params[0], 16)
            if number == 100:
                assert failed.wait(5)
            elif number == 101:
                assert window_full.wait(5)
                failed.set()
                raise ValueError("historical full block unavailable")
        return original_call(method, params)
    client.call = call
    store = module("storage").Store(settings(tmp_path, concurrency=2), 100, 106)
    with pytest.raises(ValueError, match="historical full block unavailable"):
        collector.collect(client, store)
    assert len(submitted) == 4


def test_later_worker_failure_cancels_queue_before_blocked_first_result(monkeypatch):
    collector = module("collect")
    first_started = threading.Event()
    release_first = threading.Event()
    first_finished = threading.Event()
    window_full = threading.Event()
    fatal_finished = threading.Event()
    queue_finished = threading.Event()
    submitted = []
    started = []
    observed = []
    fatal_error = ValueError("later block failed")
    class TrackingExecutor(ThreadPoolExecutor):
        def submit(self, function, *args, **kwargs):
            future = super().submit(function, *args, **kwargs)
            submitted.append(future)
            if args[0] == 1:
                future.add_done_callback(lambda completed: fatal_finished.set())
            if args[0] >= 2:
                def done(completed):
                    if len(submitted) == 4 and all(queued.done() for queued in submitted[2:]):
                        queue_finished.set()
                future.add_done_callback(done)
            if len(submitted) == 4:
                window_full.set()
            return future
    monkeypatch.setattr(collector, "ThreadPoolExecutor", TrackingExecutor)
    def fetch(number):
        started.append(number)
        if number == 0:
            first_started.set()
            assert release_first.wait(5)
            first_finished.set()
            return number
        if number == 1:
            assert first_started.wait(5)
            assert window_full.wait(5)
            raise fatal_error
        return number
    def release():
        try:
            observed.append(fatal_finished.wait(5) and queue_finished.wait(5))
            observed.append([future.cancelled() for future in submitted[2:]])
            observed.append(sorted(started))
            observed.append(first_finished.is_set())
        finally:
            release_first.set()
    monitor = threading.Thread(target=release)
    monitor.start()
    try:
        with pytest.raises(ValueError) as raised:
            list(collector._ordered_blocks(fetch, 0, 8, 2, deque()))
    finally:
        release_first.set()
        monitor.join(5)
    assert raised.value is fatal_error
    assert observed == [True, [True, True], [0, 1], False]
    assert first_finished.is_set()
    assert len(submitted) == 4
    assert all(future.done() for future in submitted)


@pytest.mark.parametrize("action", ["probe", "collect", "enrich"])
def test_cli_forwards_runtime_concurrency(tmp_path, monkeypatch, capsys, action):
    cli = module("cli")
    config_path = tmp_path / "runtime.toml"
    config_path.write_text("qps = 500\nconcurrency = 7\n")
    captured_settings = []
    class Client:
        def __init__(self, *args, **kwargs):
            captured_settings.append(kwargs)
        def close(self):
            pass
    capabilities = {"start_block": 100, "end_block": 100, "historical_coverage": {"status": "supported"},
                    "block_receipts": {"status": "supported"}, "batch": {"status": "supported"}}
    monkeypatch.setenv("OPBNB_RPC_URL", "https://node.invalid/SECRET")
    monkeypatch.setattr(cli, "RpcClient", Client)
    monkeypatch.setattr(cli, "probe", lambda *args: capabilities)
    monkeypatch.setattr(cli, "collect", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli, "validate", lambda store: {"ok": True})
    monkeypatch.setattr(cli, "enrich", lambda *args, **kwargs: {"status": "ok", "targets": {"recipients": [], "tokens": []}})
    if action == "enrich":
        module("storage").Store(settings(tmp_path), 100, 100)
    assert cli.main([action, "--config", str(config_path), "--dataset", str(tmp_path), "--run-id", "concurrent"]) == 0
    output = capsys.readouterr()
    assert captured_settings[0]["concurrency"] == 7
    assert captured_settings[0]["qps"] == 500
    assert "blocks_per_partition=500 concurrency=7" in output.err
    assert isinstance(json.loads(output.out), dict)
    assert "SECRET" not in output.err


def test_offline_benchmark_validates_equal_contents_and_counts():
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run([sys.executable, str(root / "tools" / "benchmark_collection.py"),
                             "--blocks", "12", "--latency-ms", "1", "--concurrency", "3", "--qps", "100000"],
                            cwd=root, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    output = json.loads(result.stdout)
    assert output["synthetic"] is True
    assert output["equal_outputs"] is True
    assert output["counts"] == {"blocks": 12, "transactions": 22, "receipts": 22, "logs": 4}
    assert output["serial"]["http_attempts"] == 25
    assert output["concurrent"]["http_attempts"] == 25
    assert output["serial"]["peak_in_flight"] == 1
    assert 1 < output["concurrent"]["peak_in_flight"] <= 3
    assert output["serial"]["elapsed_seconds"] > 0
    assert output["concurrent"]["elapsed_seconds"] > 0
