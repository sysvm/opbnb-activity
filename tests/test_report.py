import csv
import warnings

from conftest import address, module


def fixture_store(tmp_path, synthetic, *, one_day=False):
    config = module("config").Config(dataset=tmp_path, run_id="leadership", start="2026-10-01",
                                     end="2026-10-02" if one_day else "2026-10-22")
    blocks = synthetic["blocks"][:1] if one_day else synthetic["blocks"]
    store = module("storage").Store(config, 100, 100 + len(blocks), synthetic=True)
    for block in blocks:
        store.write_block(int(block["number"], 16), module("models").normalize_block(
            block, synthetic["receipts"], config.system_recipients))
    return store


def read_csv(destination, name):
    with (destination / (name + ".csv")).open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        return reader.fieldnames, list(reader)


def test_chinese_summary_reports_exact_counts_money_definitions_and_evidence(tmp_path, synthetic):
    store = fixture_store(tmp_path, synthetic)
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        destination = module("report").generate(store, synthetic["labels"], synthetic["assets"])
    summary = (destination / "summary.md").read_text()
    assert "# opBNB 活动分析摘要" in summary
    assert "SYNTHETIC / TEST DATA" in summary
    assert "Asia/Shanghai" in summary and "2026-10-22" in summary and "不含" in summary
    for label, value in (("全部交易", "11"), ("系统交易", "1"), ("非系统交易（含失败）", "10"), ("非系统成功交易", "8"),
                         ("非系统失败交易", "2"), ("非系统失败率", "20.00%"),
                         ("全窗口成功非系统发送地址去重", "5")):
        assert f"| {label} | {value}" in summary
    assert "1,000,000,000,000,000,000,000,000,000.000000000000000081" in summary
    assert "0.0000000000003368" in summary
    assert "项目/协议/资产归属覆盖率（公开资料匹配）" in summary
    assert "90.00%" in summary
    assert "接收地址" in summary
    assert "全部非系统交易（含失败）" in summary
    assert "成功非系统交易" in summary
    assert "project_published=项目方公开关联" in summary
    assert "bnb_official=BNB官方资料" in summary
    assert "未完整周/月" in summary and "observed_days" in summary
    assert "D1" in summary and "D7" in summary and "成熟" in summary
    assert "TVL" in summary
    assert "methods.csv" in summary and "events.csv" in summary
    assert "Dataset version" not in summary and "dataset_version" not in summary
    technical = (destination / "technical.md").read_text()
    for field in ("dataset_version", "labels_version", "assets_version", "registry_hash", "enrichment_hash", "identity"):
        assert field in technical
    assert str(tmp_path) not in technical
    assert "gasUsed" in technical and "l1Fee" in technical
    assert "empty_calldata" in summary and "不证明 EOA" in summary
    assert "selector_call" in summary and "业务意图" in summary
    assert "日志" in summary and "交易" in summary
    assert "verified_fungible" in technical and "observed_unverified" in technical
    assert (destination / "daily.png").stat().st_size > 1000
    assert (destination / "projects.png").stat().st_size > 1000


def test_all_report_tables_export_meaningful_empty_headers(tmp_path):
    store = module("storage").Store(module("config").Config(dataset=tmp_path, run_id="empty",
        start="2026-10-01", end="2026-10-02"), 100, 100, synthetic=True)
    destination = module("report").generate(store, [], {})
    expected = {"daily": "transactions", "contracts": "address", "projects": "project", "senders": "sender",
        "weekly": "observed_days", "monthly": "is_partial", "retention": "cohort_size",
        "transaction_types": "transaction_type", "methods": "signature_candidates", "hourly": "hour_utc",
        "fee_groups": "total_fee_coverage", "recipient_concentration": "top_n",
        "sender_concentration": "share", "sender_breakdown": "dimension",
        "recipient_details": "confidence", "token_metadata": "verification", "unresolved": "reason", "events": "topic0"}
    for name, column in expected.items():
        fields, rows = read_csv(destination, name)
        assert column in fields, name
    summary = (destination / "summary.md").read_text()
    assert "| 全部交易 | 0 |" in summary
    assert "D1：N/A" in summary and "D7：N/A" in summary
    assert "无交易" in summary


def test_one_day_has_composition_and_unknown_project_recipient_fallback(tmp_path, synthetic, monkeypatch):
    store = fixture_store(tmp_path, synthetic, one_day=True)
    import matplotlib.axes
    bar_labels = []
    original_bar = matplotlib.axes.Axes.bar

    def record_bar(axes, *arguments, **keywords):
        if len(arguments) >= 2:
            bar_labels.append((list(arguments[0]), list(arguments[1])))
        return original_bar(axes, *arguments, **keywords)

    monkeypatch.setattr(matplotlib.axes.Axes, "bar", record_bar)
    destination = module("report").generate(store, [], {})
    summary = (destination / "summary.md").read_text()
    assert "单日成功非系统发送地址（DAU）" in summary
    assert "已知归属 Top1/5/10：N/A / N/A / N/A" in summary
    assert "接收地址排名" in summary and "Unknown" in summary
    assert "D1：N/A" in summary and "D7：N/A" in summary
    assert any(categories == ["System", "Successful non-system", "Failed non-system"] and heights == [1, 3, 1]
               for categories, heights in bar_labels)
    _, periods = read_csv(destination, "weekly")
    assert periods[0]["observed_days"] == "1" and periods[0]["expected_days"] == "7"
    assert periods[0]["is_partial"] == "True"


def test_report_preserves_financial_csv_values_and_neutralizes_unsafe_text(tmp_path, synthetic):
    store = fixture_store(tmp_path, synthetic)
    for label in synthetic["labels"]:
        label.update(project="=SUM(1,2)|`<img>\n危险", source_url="javascript:alert(1)")
    next(iter(synthetic["assets"].values()))["symbol"] = "@SUM(1,2)|<b>"
    destination = module("report").generate(store, synthetic["labels"], synthetic["assets"])
    summary = (destination / "summary.md").read_text()
    assert "<img>" not in summary and "<b>" not in summary
    assert "javascript:" not in summary
    assert "&#124;" in summary and "&#96;" in summary and "&lt;img&gt;" in summary
    _, projects = read_csv(destination, "projects")
    assert projects[0]["project"].startswith("'=SUM")
    _, tokens = read_csv(destination, "token_metadata")
    assert tokens[0]["symbol"].startswith("'@SUM")
    assert tokens[0]["raw_amount"] == str(10**40 + 1)
    _, daily = read_csv(destination, "daily")
    assert daily[0]["native_value_wei"] == str(10**45 + 15)


def test_report_evidence_links_and_distinct_asset_amounts(tmp_path, synthetic):
    store = fixture_store(tmp_path, synthetic)
    for label in synthetic["labels"]:
        label.update(source_url="https://example.org/deployments?q=x(y)", confidence="project_published")
    destination = module("report").generate(store, synthetic["labels"], synthetic["assets"])
    summary = (destination / "summary.md").read_text()
    assert "[来源](https://example.org/deployments?q=x%28y%29)" in summary
    assert "TEST" in summary
    assert "10,000,000,000,000,000,000,000.000000000000000001" in summary
    _, details = read_csv(destination, "recipient_details")
    assert details[0]["address"].startswith("0x") and len(details[0]["address"]) == 42
    _, events = read_csv(destination, "events")
    assert events[0]["log_count"] == "2" and events[0]["eligible_log_count"] == "2"
    assert "signature_candidates" in events[0] and "signature_evidence" in events[0]


def test_partial_calendar_day_is_not_presented_as_full_dau(tmp_path, synthetic):
    config = module("config").Config(dataset=tmp_path, run_id="partial-day",
        start="2026-10-01T10:00:00", end="2026-10-01T18:00:00")
    store = module("storage").Store(config, 100, 101, synthetic=True)
    store.write_block(100, module("models").normalize_block(
        synthetic["blocks"][0], synthetic["receipts"], config.system_recipients))
    destination = module("report").generate(store, [], {})
    summary = (destination / "summary.md").read_text()
    assert "（DAU）" not in summary
    assert "不完整日" in summary
    assert "全窗口成功非系统发送地址去重" in summary


def test_verified_asset_amount_is_not_hidden_outside_metadata_top100(tmp_path, synthetic):
    receipt = synthetic["receipts"][synthetic["blocks"][0]["transactions"][0]["hash"]]
    template = receipt["logs"][0]
    receipt["logs"] = []
    for index in range(101):
        emitter = address(1000 + index)
        receipt["logs"].append(dict(template, address=emitter, logIndex=hex(index), data=hex(index + 1)))
        synthetic["assets"][emitter] = {"verified": True, "decimals": 0, "symbol": "ASSET" + str(index)}
    store = fixture_store(tmp_path, synthetic)
    destination = module("report").generate(store, synthetic["labels"], synthetic["assets"])
    _, metadata = read_csv(destination, "token_metadata")
    assert len(metadata) == 100
    summary = (destination / "summary.md").read_text()
    assert "0x000000…00044c" in summary
    assert "| 101 |" in summary


def test_project_label_is_plain_data_in_charts(tmp_path, synthetic):
    for label in synthetic["labels"]:
        label["project"] = "$\\badmacro$"
    store = fixture_store(tmp_path, synthetic, one_day=True)
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        destination = module("report").generate(store, synthetic["labels"], synthetic["assets"])
    assert (destination / "projects.png").stat().st_size > 1000
