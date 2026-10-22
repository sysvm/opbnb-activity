from .models import normalize_block
from .probe import require_chain
from .rpc import RpcError


def collect(client, store, block_receipts=True, batch_supported=True):
    require_chain(client)
    for start, end in store.ranges():
        if store.completed(start) or store.recover(start):
            continue
        for number in range(start, end):
            block = client.call("eth_getBlockByNumber", [hex(number), True])
            if block is None:
                raise ValueError("historical full block unavailable")
            receipts = None
            if block_receipts:
                try:
                    receipts = client.call("eth_getBlockReceipts", [hex(number)])
                except RpcError as error:
                    if error.kind != "unsupported":
                        raise
                    block_receipts = False
            if receipts is None:
                calls = [("eth_getTransactionReceipt", [transaction["hash"]]) for transaction in block["transactions"]]
                receipts = client.batch(calls) if batch_supported else [client.call(method, params) for method, params in calls]
            if len(receipts) != len(block["transactions"]) or any(not receipt for receipt in receipts):
                raise ValueError("missing receipt prevents partition completion")
            by_hash = {receipt["transactionHash"].lower(): receipt for receipt in receipts}
            if len(by_hash) != len(receipts):
                raise ValueError("duplicate receipt prevents partition completion")
            normalized = normalize_block(block, by_hash, store.config.system_recipients, store.config.system_depositor)
            store.write_block(number, normalized)
    return store
