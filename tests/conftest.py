import importlib
from datetime import datetime, timezone

import pytest


def module(name):
    try:
        return importlib.import_module(f"opbnb_activity.{name}")
    except ModuleNotFoundError as error:
        pytest.fail(f"Required offline implementation is missing: {error}")


def address(number):
    return f"0x{number:040x}"


def block_hash(number):
    return f"0x{number:064x}"


def timestamp(day):
    return int(datetime.fromisoformat(day + "T04:00:00+00:00").timestamp())


@pytest.fixture
def synthetic():
    sender_a, sender_b, sender_c, sender_d, sender_e = [address(number) for number in range(1, 6)]
    contract_a, contract_b, contract_c, unknown = [address(number) for number in range(101, 105)]
    system = "0x4200000000000000000000000000000000000015"
    transfer_topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    dates = ["2026-10-01", "2026-10-02", "2026-10-08", "2026-10-11", "2026-10-14", "2026-10-21"]
    descriptions = [
        [(sender_a, contract_a, 10**45 + 7, 1, 2), (sender_a, contract_b, 3, 1, 2),
         (sender_b, contract_a, 99, 0, 2), ("0xdeaddeaddeaddeaddeaddeaddeaddeaddead0001", system, 0, 1, 126),
         (sender_c, contract_c, 5, 1, 126)],
        [(sender_a, contract_b, 2, 1, 2)], [(sender_a, unknown, 11, 1, 2)],
        [(sender_b, contract_a, 13, 1, 2)], [(sender_d, contract_c, 17, 1, 2)],
        [(sender_d, contract_c, 19, 0, 2), (sender_e, contract_c, 23, 1, 2)],
    ]
    blocks, receipts, tx_number = [], {}, 0
    for offset, (day, descriptions_for_day) in enumerate(zip(dates, descriptions)):
        number = 100 + offset
        transactions = []
        for position, (sender, recipient, value, status, kind) in enumerate(descriptions_for_day):
            tx_number += 1
            tx_hash = block_hash(1000 + tx_number)
            transactions.append({"hash": tx_hash, "blockHash": block_hash(number), "blockNumber": hex(number),
                                 "transactionIndex": hex(position), "from": sender, "to": recipient,
                                 "value": hex(value), "input": "0x12345678", "type": hex(kind)})
            receipt = {"transactionHash": tx_hash, "blockHash": block_hash(number), "blockNumber": hex(number),
                       "transactionIndex": hex(position), "status": hex(status), "gasUsed": hex(21000),
                       "effectiveGasPrice": "0x2", "l1Fee": "0x64", "logs": []}
            if tx_number in (2, 5):
                receipt.pop("l1Fee")
            if kind == 126:
                receipt["gasUsed"] = "0x0"
            if recipient == system:
                receipt["l1Fee"] = "0x0"
            if tx_number in (1, 2):
                topics = [transfer_topic, "0x" + sender[2:].zfill(64), "0x" + recipient[2:].zfill(64)]
                if tx_number == 2:
                    topics.append(block_hash(77))
                receipt["logs"] = [{"address": address(500), "topics": topics,
                                    "data": hex(10**40 + 1) if tx_number == 1 else "0x",
                                    "logIndex": "0x0", "transactionHash": tx_hash,
                                    "blockHash": block_hash(number), "blockNumber": hex(number), "removed": False}]
            receipts[tx_hash] = receipt
        blocks.append({"number": hex(number), "hash": block_hash(number), "parentHash": block_hash(number - 1),
                       "timestamp": hex(timestamp(day)), "transactions": transactions})
    return {"blocks": blocks, "receipts": receipts, "labels": [
        {"address": contract_a, "project": "Alpha", "category": "test", "role": "contract", "source_url": "fixture://alpha", "confidence": "verified", "valid_from": "0", "valid_to": ""},
        {"address": contract_b, "project": "Alpha", "category": "test", "role": "contract", "source_url": "fixture://alpha", "confidence": "verified", "valid_from": "0", "valid_to": ""},
        {"address": contract_c, "project": "Beta", "category": "test", "role": "contract", "source_url": "fixture://beta", "confidence": "verified", "valid_from": "0", "valid_to": ""},
    ], "assets": {address(500): {"decimals": 18, "symbol": "TEST", "verified": True}}}
