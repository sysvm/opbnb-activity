import importlib
import sys
import unittest
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))


class ConfigContract(unittest.TestCase):
    def implementation(self, name):
        try:
            return importlib.import_module("opbnb_activity." + name)
        except ModuleNotFoundError:
            self.fail("Missing offline implementation: " + name)

    def test_timestamp_lower_bounds(self):
        resolver = self.implementation("block_range").resolve_range
        timestamps = [10, 10, 14, 25, 25, 40]
        self.assertEqual(resolver(lambda number: {"timestamp": hex(timestamps[number])}, 5, 10, 25), (0, 3))
        with self.assertRaisesRegex(ValueError, "incomplete"):
            resolver(lambda number: {"timestamp": hex(timestamps[number])}, 5, 10, 41)

    def test_normalizer_keeps_huge_integer_and_checks_system_sender(self):
        normalize = self.implementation("models").normalize_block
        block = {"number": "0x1", "hash": "0xabc", "parentHash": "0xdef", "timestamp": "0x64", "transactions": [
            {"hash": "0xtx", "blockHash": "0xabc", "blockNumber": "0x1", "transactionIndex": "0x0", "from": "0xuser", "to": "0xsystem", "type": "0x7e", "input": "0x", "value": hex(10**50)}]}
        receipt = {"0xtx": {"transactionHash": "0xtx", "blockHash": "0xabc", "blockNumber": "0x1", "transactionIndex": "0x0", "status": "0x1", "gasUsed": "0x1", "effectiveGasPrice": "0x2", "logs": []}}
        result = normalize(block, receipt, ["0xsystem"])
        self.assertEqual(result["transactions"][0]["value_wei"], str(10**50))
        self.assertFalse(result["transactions"][0]["is_system"])
        self.assertIsNone(result["receipts"][0]["total_fee_wei"])

    def test_transfer_topic_ambiguity_and_label_validity(self):
        decoder = self.implementation("decode")
        labels = self.implementation("labels")
        event = {"address": "0xasset", "topics": [decoder.TRANSFER_TOPIC, "0x1", "0x2", "0x3"], "data": "0x"}
        self.assertEqual(decoder.decode_transfer(event, {})["kind"], "nft_or_ambiguous")
        index = labels.LabelIndex([{"address": "0xcontract", "project": "Test", "valid_from": "10", "valid_to": "20"}])
        self.assertEqual(index.lookup("0xcontract", 19)["project"], "Test")
        self.assertEqual(index.lookup("0xcontract", 20)["project"], "Unknown")

    def test_default_timezone_window_is_half_open_and_identity_changes(self):
        try:
            config_module = importlib.import_module("opbnb_activity.config")
        except ModuleNotFoundError:
            self.fail("Missing offline config implementation")
        config = config_module.Config(dataset=Path("data"), run_id="test")
        self.assertEqual(config.start_utc.isoformat(), "2026-07-21T16:00:00+00:00")
        self.assertEqual(config.end_utc.isoformat(), "2026-10-21T16:00:00+00:00")
        self.assertNotEqual(config.identity, config_module.Config(dataset=Path("data"), run_id="test", end="2026-10-21").identity)

    def test_rpc_error_taxonomy(self):
        rpc = self.implementation("rpc")
        self.assertEqual(rpc.error_kind(-32601, "method not found"), "unsupported")
        self.assertEqual(rpc.error_kind(-32000, "missing trie node"), "pruned")
        self.assertEqual(rpc.error_kind(-32005, "rate limit"), "transient")

    def test_checkpoint_records_atomic_partition_identity(self):
        checkpoint = self.implementation("checkpoint").Checkpoint
        with tempfile.TemporaryDirectory() as directory:
            database = checkpoint(Path(directory) / "tasks.sqlite", "identity-a")
            database.complete(100, 110, "date=2026-10-01/blocks=100-110", "checksum")
            self.assertEqual(database.get(100)["end_block"], 110)
            with self.assertRaisesRegex(ValueError, "identity"):
                checkpoint(Path(directory) / "tasks.sqlite", "identity-b")

    def test_probe_rejects_wrong_chain_before_history(self):
        probe = self.implementation("probe").probe
        calls = []
        class WrongChain:
            def call(self, method, params):
                calls.append(method)
                return "0x1"
        with self.assertRaisesRegex(ValueError, "204"):
            probe(WrongChain(), 0, 1)
        self.assertEqual(calls, ["eth_chainId"])


if __name__ == "__main__":
    unittest.main()
