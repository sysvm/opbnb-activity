import logging

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


class FixtureClient:
    def __init__(self, fixture):
        self.fixture = fixture
        self.requests = []

    def call(self, method, params):
        if method == "eth_chainId":
            return "0xcc"
        number = int(params[0], 16)
        self.requests.append((method, number))
        block = self.fixture["blocks"][number - 100]
        if method == "eth_getBlockByNumber":
            return block
        if method == "eth_getBlockReceipts":
            return [self.fixture["receipts"][transaction["hash"]] for transaction in block["transactions"]]
        raise AssertionError(method)


@pytest.mark.parametrize("concurrency", [1, 3])
def test_checkpoint_resume_avoids_durable_rpc_and_restores_exact_metrics(tmp_path, synthetic, monkeypatch, caplog, concurrency):
    collector = module("collect")
    config = module("config").Config(dataset=tmp_path, run_id="resume", blocks_per_partition=6,
                                      blocks_per_checkpoint=2, concurrency=concurrency)
    store = module("storage").Store(config, 100, 106, synthetic=True)
    normalize = collector.normalize_block

    def fail_fourth(block, *arguments):
        if int(block["number"], 16) == 103:
            raise ValueError("interrupted fixture")
        return normalize(block, *arguments)

    monkeypatch.setattr(collector, "normalize_block", fail_fourth)
    with pytest.raises(ValueError, match="interrupted fixture"):
        collector.collect(FixtureClient(synthetic), store)
    assert store.durable_end(100) == 102
    assert not store.completed(100)
    assert store.buffer_start is None
    store.checkpoint.close()

    monkeypatch.setattr(collector, "normalize_block", normalize)
    resumed = module("storage").Store.open(tmp_path, "resume")
    resumed.config.blocks_per_checkpoint = 3
    client = FixtureClient(synthetic)
    caplog.set_level(logging.INFO, logger="opbnb_activity")
    collector.collect(client, resumed)
    assert min(number for _, number in client.requests) == 102
    messages = [record.getMessage() for record in caplog.records]
    recovered = next(message for message in messages if "checkpoint recovered" in message)
    assert "processed_blocks=2/6 committed_blocks=0/6" in recovered
    assert "new_blocks=0 new_transactions=0" in recovered
    assert "durable_blocks=2/6 reused_blocks=2" in recovered
    assert "rate_blocks_per_second=0.000" in recovered
    assert "new_blocks=4 new_transactions=5" in messages[-1]
    assert "durable_blocks=6/6 reused_blocks=2" in messages[-1]
    assert "blocks_per_checkpoint=3" in messages[-1]
    direct_config = module("config").Config(dataset=tmp_path, run_id="direct", blocks_per_partition=6)
    direct = module("storage").Store(direct_config, 100, 106, synthetic=True)
    collector.collect(FixtureClient(synthetic), direct)
    for table in module("models").TABLES:
        assert resumed.read_table(table) == direct.read_table(table)
    assert module("metrics").analyze(resumed, synthetic["labels"], synthetic["assets"])["summary"] == module("metrics").analyze(direct, synthetic["labels"], synthetic["assets"])["summary"]
    resumed.checkpoint.close()
    direct.checkpoint.close()


def test_collector_uses_resume_end_and_abort_api(tmp_path, synthetic, monkeypatch):
    collector = module("collect")
    config = module("config").Config(dataset=tmp_path, run_id="contract", blocks_per_partition=6)
    store = module("storage").Store(config, 100, 106, synthetic=True)
    observed = []
    monkeypatch.setattr(store, "resume_partition", lambda number: observed.append(("resume", number)) or 102, raising=False)
    monkeypatch.setattr(store, "abort_partition", lambda: observed.append(("abort",)), raising=False)
    monkeypatch.setattr(store, "write_block", lambda *arguments: (_ for _ in ()).throw(ValueError("write failed")))
    client = FixtureClient(synthetic)
    with pytest.raises(ValueError, match="write failed"):
        collector.collect(client, store)
    assert observed == [("resume", 100), ("abort",)]
    assert client.requests[0] == ("eth_getBlockByNumber", 102)
    store.checkpoint.close()


def test_full_resumed_prefix_requires_no_block_or_receipt_rpc(tmp_path, synthetic, monkeypatch, caplog):
    collector = module("collect")
    store = module("storage").Store(module("config").Config(dataset=tmp_path, run_id="full"), 100, 106, synthetic=True)
    monkeypatch.setattr(store, "resume_partition", lambda number: 106, raising=False)
    caplog.set_level(logging.INFO, logger="opbnb_activity")
    client = FixtureClient(synthetic)
    collector.collect(client, store)
    assert client.requests == []
    assert "processed_blocks=6/6 committed_blocks=6/6" in caplog.records[-1].getMessage()
    assert "new_blocks=0 new_transactions=0" in caplog.records[-1].getMessage()
    store.checkpoint.close()


@pytest.mark.parametrize("message", [
    "blocks_per_partition must be an integer in [1,100000]",
    "blocks_per_checkpoint must be an integer in [1,5000]",
])
def test_cli_names_safe_partition_configuration_errors(tmp_path, monkeypatch, capsys, message):
    cli = module("cli")

    def fail_load(*arguments, **keywords):
        raise ValueError(message)

    monkeypatch.setattr(cli, "load", fail_load)
    assert cli.main(["collect", "--dataset", str(tmp_path), "--run-id", "invalid"]) == 2
    assert "Error: " + message in capsys.readouterr().err
