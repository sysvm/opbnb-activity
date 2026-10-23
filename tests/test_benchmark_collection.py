import importlib.util
import threading
from argparse import Namespace
from pathlib import Path

import pytest


@pytest.fixture
def benchmark():
    path = Path(__file__).resolve().parents[1] / "tools" / "benchmark_collection.py"
    spec = importlib.util.spec_from_file_location("benchmark_collection", path)
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded


def controlled_peaks(benchmark, monkeypatch, client_peak=2, transport_peak=1):
    original_client = benchmark.RpcClient
    original_transport = benchmark.SimulatedTransport
    class SnapshotClient(original_client):
        def snapshot(self):
            counters = super().snapshot()
            counters["peak_in_flight"] = client_peak
            return counters
    class SerializedTransport(original_transport):
        def __init__(self, *args):
            super().__init__(*args)
            self.serialization = threading.Lock()
        def __call__(self, request):
            with self.serialization:
                response = super().__call__(request)
                self.peak = transport_peak
                return response
    monkeypatch.setattr(benchmark, "RpcClient", SnapshotClient)
    monkeypatch.setattr(benchmark, "SimulatedTransport", SerializedTransport)


def test_valid_different_client_and_transport_peaks_are_reported(tmp_path, benchmark, monkeypatch):
    controlled_peaks(benchmark, monkeypatch)
    fixture, blocks, receipts = benchmark.synthetic_blocks(2)
    arguments = Namespace(qps=100000, blocks=2, latency_ms=0)
    result, contents, counts = benchmark.run(tmp_path, "different-peaks", 2, arguments, fixture, blocks, receipts)
    assert result["peak_in_flight"] == 2
    assert result["transport_peak_in_flight"] == 1
    assert result["http_attempts"] == 5
    assert counts == {"blocks": 2, "transactions": 6, "receipts": 6, "logs": 2}
    assert {name: len(rows) for name, rows in contents.items()} == counts


@pytest.mark.parametrize("client_peak,transport_peak", [(3, 1), (2, 3), (0, 1), (2, 0)])
def test_each_peak_is_independently_bounded(tmp_path, benchmark, monkeypatch, client_peak, transport_peak):
    controlled_peaks(benchmark, monkeypatch, client_peak, transport_peak)
    fixture, blocks, receipts = benchmark.synthetic_blocks(2)
    arguments = Namespace(qps=100000, blocks=2, latency_ms=0)
    with pytest.raises(AssertionError, match="unexpected synthetic request count or concurrency"):
        benchmark.run(tmp_path, "invalid-peak", 2, arguments, fixture, blocks, receipts)
