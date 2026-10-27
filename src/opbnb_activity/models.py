from datetime import datetime, timezone

from .block_range import quantity
from .config import SYSTEM_DEPOSITOR
from .system_transactions import is_tx_dag

TABLES = ("blocks", "transactions", "receipts", "logs")


def optional_quantity(value):
    return None if value is None else str(quantity(value))


def normalize_block(block, receipts, system_recipients, system_depositor=SYSTEM_DEPOSITOR):
    number = quantity(block["number"])
    block_hash = block["hash"].lower()
    timestamp = quantity(block["timestamp"])
    timestamp_utc = datetime.fromtimestamp(timestamp, timezone.utc).isoformat()
    transactions = block["transactions"]
    result = {name: [] for name in TABLES}
    result["blocks"].append({"number": number, "hash": block_hash, "parent_hash": block["parentHash"].lower(),
                             "timestamp": timestamp, "timestamp_utc": timestamp_utc, "transaction_count": len(transactions)})
    seen_transactions = set()
    for position, transaction in enumerate(transactions):
        tx_hash = transaction["hash"].lower()
        receipt = receipts.get(tx_hash)
        if tx_hash in seen_transactions:
            raise ValueError("duplicate transaction hash")
        seen_transactions.add(tx_hash)
        if not receipt or receipt.get("transactionHash", "").lower() != tx_hash or receipt.get("blockHash", "").lower() != block_hash or quantity(receipt.get("blockNumber", -1)) != number or quantity(receipt.get("transactionIndex", -1)) != position:
            raise ValueError("missing or mismatched receipt")
        if transaction.get("blockHash", "").lower() != block_hash or quantity(transaction.get("blockNumber", -1)) != number or quantity(transaction.get("transactionIndex", -1)) != position:
            raise ValueError("transaction block hash/index mismatch")
        value = transaction.get("value")
        if isinstance(value, bool) or not isinstance(value, (str, int)):
            raise ValueError("transaction value is required and must be an integer quantity")
        try:
            value_wei = quantity(value)
        except ValueError as error:
            raise ValueError("transaction value must be an integer quantity") from error
        if value_wei < 0:
            raise ValueError("transaction value must be nonnegative")
        logs = receipt.get("logs")
        if not isinstance(logs, list) or any(not isinstance(log, dict) for log in logs):
            raise ValueError("receipt logs is required and must be a list of log objects")
        status = quantity(receipt["status"])
        if status not in (0, 1):
            raise ValueError("receipt status must be 0 or 1")
        sender = transaction["from"].lower()
        recipient = transaction.get("to")
        recipient = recipient.lower() if recipient else None
        kind = quantity(transaction.get("type", "0x0"))
        is_system = kind == 126 and sender == system_depositor.lower() and recipient in system_recipients
        classification = "system_l1_attributes" if is_system else "user_deposit" if kind == 126 else "contract_creation" if recipient is None else "unknown"
        input_data = transaction.get("input", "0x")
        result["transactions"].append({"hash": tx_hash, "block_number": number, "block_hash": block_hash,
                                        "transaction_index": position, "timestamp": timestamp, "timestamp_utc": timestamp_utc,
                                        "sender": sender, "recipient": recipient, "value_wei": str(value_wei),
                                        "input": input_data, "selector": input_data[:10] if len(input_data) >= 10 else None,
                                        "type": kind, "is_system": is_system, "classification": classification,
                                        "recipient_kind": "creation" if recipient is None else "unknown"})
        gas_used = optional_quantity(receipt.get("gasUsed"))
        gas_price = optional_quantity(receipt.get("effectiveGasPrice"))
        l2_fee = str(int(gas_used) * int(gas_price)) if gas_used is not None and gas_price is not None else None
        l1_fee = optional_quantity(receipt.get("l1Fee"))
        total_fee = str(int(l2_fee) + int(l1_fee)) if l2_fee is not None and l1_fee is not None else None
        result["receipts"].append({"transaction_hash": tx_hash, "block_number": number, "block_hash": block_hash,
                                    "transaction_index": position, "status": status, "gas_used": gas_used,
                                    "effective_gas_price_wei": gas_price, "l2_fee_wei": l2_fee,
                                    "l1_fee_wei": l1_fee, "total_fee_wei": total_fee,
                                    "contract_address": (receipt.get("contractAddress") or "").lower() or None})
        if is_tx_dag(result["transactions"][-1], result["receipts"][-1], len(transactions)):
            result["transactions"][-1].update(is_system=True, classification="system_tx_dag")
        seen_logs = set()
        for log in logs:
            log_index = quantity(log["logIndex"])
            if log.get("removed", False) or log.get("transactionHash", "").lower() != tx_hash or log.get("blockHash", "").lower() != block_hash or quantity(log.get("blockNumber", -1)) != number or log_index in seen_logs:
                raise ValueError("log receipt/hash/index mismatch")
            seen_logs.add(log_index)
            result["logs"].append({"transaction_hash": tx_hash, "block_number": number, "block_hash": block_hash,
                                   "log_index": log_index, "address": log["address"].lower(),
                                   "topics": [topic.lower() for topic in log["topics"]], "data": log["data"].lower()})
    return result
