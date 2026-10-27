import re


TX_DAG_SENDER = "0x8eece8cb98dc9aaddb37a8177e4615469d771631"
TX_DAG_RECIPIENT = "0xda90000000000000000000000000000000000000"
TX_DAG_SELECTOR = "0x5517ed8c"
SYSTEM_RULES_VERSION = "l1_attributes_and_known_txdag_v1"


def is_tx_dag(transaction, receipt, transaction_count):
    input_data = transaction.get("input")
    return (
        transaction.get("sender") == TX_DAG_SENDER
        and transaction.get("recipient") == TX_DAG_RECIPIENT
        and transaction.get("type") == 0
        and transaction.get("value_wei") == "0"
        and receipt.get("effective_gas_price_wei") == "0"
        and receipt.get("status") == 1
        and transaction.get("transaction_index") == transaction_count - 1
        and isinstance(input_data, str)
        and len(input_data) >= 138
        and input_data[:10].lower() == TX_DAG_SELECTOR
        and re.fullmatch(r"0x([0-9a-fA-F]{2})*", input_data) is not None
    )


def tx_dag_sql():
    return f"""coalesce(t.sender='{TX_DAG_SENDER}' AND t.recipient='{TX_DAG_RECIPIENT}'
        AND t.type=0 AND t.value_wei='0' AND r.effective_gas_price_wei='0' AND r.status=1
        AND t.transaction_index=b.transaction_count-1 AND length(t.input)>=138
        AND lower(substr(t.input,1,10))='{TX_DAG_SELECTOR}'
        AND regexp_full_match(t.input,'0x([0-9a-fA-F]{{2}})*'),FALSE)"""
