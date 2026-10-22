import csv
import json
import subprocess
import sys

import pytest

from conftest import module


def test_end_to_end_exact_counts_precision_and_retention(tmp_path, synthetic):
    config = module("config").Config(dataset=tmp_path, run_id="fixture", start="2026-10-01")
    store = module("storage").Store(config, 100, 106, synthetic=True)
    normalize = module("models").normalize_block
    for block in synthetic["blocks"]:
        store.write_block(int(block["number"], 16), normalize(block, synthetic["receipts"], config.system_recipients))
    result = module("metrics").analyze(store, synthetic["labels"], synthetic["assets"])
    summary = result["summary"]
    assert summary["transactions"] == 11
    assert summary["system_transactions"] == 1
    assert summary["successful_non_system_transactions"] == 8
    assert summary["failed_non_system_transactions"] == 2
    assert summary["unique_successful_senders"] == 5
    assert summary["native_value_wei"] == str(10**45 + 81)
    assert summary["native_value_bnb"] == "1000000000000000000000000000.000000000000000081"
    assert summary["known_total_fee_wei"] == "336800"
    assert summary["unknown_total_fee_transactions"] == 2
    assert summary["verified_token_raw_amounts"] == {next(iter(synthetic["assets"])): str(10**40 + 1)}
    assert summary["ambiguous_transfer_logs"] == 1
    assert summary["project_top1_share"] == 0.5
    assert summary["project_top5_share"] == 0.9
    assert summary["project_top10_share"] == 0.9
    assert summary["unknown_project_share"] == 0.1
    assert summary["top1_percent_sender_successful_tx_share"] == 0.5
    alpha = next(row for row in result["projects"] if row["project"] == "Alpha")
    assert alpha["transactions"] == 5
    assert alpha["successful_senders"] == 2
    first_day = next(row for row in result["daily"] if row["day"] == "2026-10-01")
    assert first_day["active_senders"] == 2
    assert first_day["all_non_system_senders"] == 3
    assert first_day["native_value_wei"] == str(10**45 + 15)
    assert first_day["known_total_fee_wei"] == "84200"
    assert first_day["partial_l2_fee_wei"] == "126000"
    assert first_day["unknown_total_fee_transactions"] == 2
    assert first_day["full_fee_coverage"] == 0.6
    assert first_day["l2_fee_coverage"] == 1.0
    first_d1 = next(row for row in result["retention"] if row["cohort_day"] == "2026-10-01" and row["lag_days"] == 1)
    first_d7 = next(row for row in result["retention"] if row["cohort_day"] == "2026-10-01" and row["lag_days"] == 7)
    assert (first_d1["cohort_size"], first_d1["retained"], first_d1["rate"]) == (2, 1, 0.5)
    assert first_d7["retained"] == 1
    assert not any(row["cohort_day"] == "2026-10-21" for row in result["retention"])
    destination = module("report").generate(store, synthetic["labels"], synthetic["assets"])
    summary_text = (destination / "summary.md").read_text()
    assert "SYNTHETIC / TEST DATA" in summary_text
    assert "Asia/Shanghai" in summary_text
    assert "TVL" in summary_text
    assert (destination / "daily.png").stat().st_size > 1000
    assert (destination / "projects.png").stat().st_size > 1000
    assert (destination / "daily.csv").is_file()


def test_empty_valid_window_reports_zero(tmp_path):
    store = module("storage").Store(module("config").Config(dataset=tmp_path, run_id="empty"), 100, 100, synthetic=True)
    result = module("metrics").analyze(store, [], {})
    assert result["summary"]["transactions"] == 0
    assert result["summary"]["unique_successful_senders"] == 0
    assert module("report").generate(store, [], {}).joinpath("summary.md").is_file()


def test_cli_demo_is_offline_and_offline_commands_require_run_id(tmp_path):
    result = subprocess.run([sys.executable, "-m", "opbnb_activity", "demo", "--dataset", str(tmp_path), "--run-id", "cli-demo"], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert "SYNTHETIC" in result.stdout
    analyze = subprocess.run([sys.executable, "-m", "opbnb_activity", "analyze", "--dataset", str(tmp_path), "--run-id", "cli-demo"], capture_output=True, text=True)
    assert analyze.returncode == 0, analyze.stderr
    assert json.loads(analyze.stdout)["unknown_project_transactions"] == 1
    missing = subprocess.run([sys.executable, "-m", "opbnb_activity", "validate", "--dataset", str(tmp_path)], capture_output=True, text=True)
    assert missing.returncode != 0
    assert "--run-id" in missing.stderr


@pytest.mark.parametrize("label_state", ["missing", "invalid"])
def test_cli_validate_does_not_require_external_labels(tmp_path, label_state):
    labels = tmp_path / "labels.csv"
    labels.write_text("address,project,category,role,source_url,confidence,valid_from,valid_to\n")
    config = module("config").Config(dataset=tmp_path / "original", run_id="portable", labels=str(labels))
    store = module("storage").Store(config, 100, 100, synthetic=True)
    relocated = tmp_path / "relocated"
    config.dataset.rename(relocated)
    if label_state == "missing":
        labels.unlink()
    else:
        labels.write_text("invalid,csv\ninvalid,row\n")
    result = subprocess.run([sys.executable, "-m", "opbnb_activity", "validate", "--dataset", str(relocated),
                             "--run-id", "portable"], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["ok"] is True
    for command in ("analyze", "report"):
        result = subprocess.run([sys.executable, "-m", "opbnb_activity", command, "--dataset", str(relocated),
                                 "--run-id", "portable"], capture_output=True, text=True)
        assert result.returncode == 2
        assert "Error:" in result.stderr
