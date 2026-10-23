import importlib.util
import json
from pathlib import Path
import subprocess
import sys

import pytest


def benchmark_module():
    path = Path(__file__).resolve().parents[1] / "tools" / "benchmark_partitions.py"
    assert path.exists(), "large-partition benchmark is missing"
    spec = importlib.util.spec_from_file_location("benchmark_partitions", path)
    benchmark = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = benchmark
    spec.loader.exec_module(benchmark)
    return benchmark


@pytest.mark.parametrize("mode", ["representative", "empty"])
def test_lazy_fixture_chain_is_deterministic_valid_and_has_exact_counts(mode):
    benchmark = benchmark_module()
    chain = benchmark.SyntheticChain(mode)
    assert set(vars(chain)) == {"mode", "fixture", "timestamp"}
    first, receipts = chain.block(100)
    assert chain.block(100) == (first, receipts)
    later, _ = chain.block(100099)
    assert int(later["number"], 16) == 100099
    assert later["parentHash"] == f"0x{100098:064x}"
    from opbnb_activity.config import Config
    from opbnb_activity.models import normalize_block
    settings = Config(run_id="benchmark-test")
    observed = {name: 0 for name in ("blocks", "transactions", "receipts", "logs")}
    hashes = set()
    for number in range(100, 112):
        block, receipts = chain.block(number)
        normalized = normalize_block(block, {receipt["transactionHash"]: receipt for receipt in receipts}, settings.system_recipients)
        for table, rows in normalized.items():
            observed[table] += len(rows)
        for transaction in block["transactions"]:
            assert transaction["hash"] not in hashes
            hashes.add(transaction["hash"])
    assert observed == chain.expected_counts(12)
    if mode == "representative":
        assert observed["transactions"] > 12
        assert observed["logs"] > 0
    else:
        assert observed == {"blocks": 12, "transactions": 0, "receipts": 0, "logs": 0}


def test_peak_rss_platform_units_are_explicit():
    benchmark = benchmark_module()
    assert benchmark.rss_bytes(123, "darwin") == 123
    assert benchmark.rss_bytes(123, "linux") == 123 * 1024


def test_parser_accepts_acceptance_scale_and_rejects_unbounded_counts():
    benchmark = benchmark_module()
    arguments = benchmark.parser().parse_args(["--blocks", "100000"])
    assert arguments.checkpoint_blocks == 500
    assert arguments.interrupt_after == 1250
    for value in ("0", "100001"):
        with pytest.raises(SystemExit):
            benchmark.parser().parse_args(["--blocks", value])


def test_isolated_benchmark_compares_bounded_fingerprints_and_interrupted_resume(tmp_path):
    root = Path(__file__).resolve().parents[1]
    completed = subprocess.run([sys.executable, str(root / "tools" / "benchmark_partitions.py"),
                                "--blocks", "12", "--checkpoint-blocks", "2", "--interrupt-after", "5",
                                "--output-directory", str(tmp_path)], cwd=root, capture_output=True, text=True, timeout=60)
    assert completed.returncode == 0, completed.stderr
    output = json.loads(completed.stdout)
    assert output["isolated_processes"] is True
    process_ids = set()
    for mode, comparison in output["comparisons"].items():
        assert comparison["equal_outputs"] is True
        for key in ("partition_500", "partition_100000", "interrupted_resume"):
            run = comparison[key]
            process_ids.add(run["process_id"])
            assert run["counts"]["blocks"] == 12
            assert run["quality_ok"] is True
            assert run["final_directories"] == 1
            assert run["final_files"] == 5
            assert run["final_parquet_files"] == 4
            assert run["remaining_staging_bytes"] == 0
            assert run["peak_rss_bytes"] > 0
            assert run["finalization_seconds"] > 0
            assert run["finalization_events"] == 1
            assert run["fingerprints"] == comparison["partition_500"]["fingerprints"]
        resumed = comparison["interrupted_resume"]
        assert resumed["interruption"]["rpc_blocks_avoided"] == 4
        assert resumed["interruption"]["first_resumed_rpc_block"] == 104
        assert resumed["rpc_calls"]["eth_getBlockByNumber"] == 8
        assert resumed["peak_staging_disk_bytes"] > 0
        assert comparison["partition_500"]["counts"]["transactions"] == (22 if mode == "representative" else 0)
    assert len(process_ids) == 6
