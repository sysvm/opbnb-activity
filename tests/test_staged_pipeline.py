import json
from datetime import datetime, timezone

from conftest import address, block_hash, module


def test_full_parquet_report_high_cardinality_with_128mb(tmp_path):
    config = module('config').Config(dataset=tmp_path, run_id='low-memory-parquet', start='2026-10-01',
        end='2026-10-11', blocks_per_partition=100, analysis_memory_limit_mb=128, analysis_threads=1)
    store = module('storage').Store(config, 100, 600, synthetic=True)
    token = address(9000000)
    topic = module('decode').TRANSFER_TOPIC
    days = ['2026-10-01', '2026-10-02', '2026-10-07', '2026-10-08', '2026-10-09']
    successful_senders = set()
    successful_days = {}
    expected_success = 0
    expected_logs = 0
    expected_tokens = 0
    for part, day in enumerate(days):
        tables = {name: [] for name in module('models').TABLES}
        base = int(datetime.fromisoformat(day + 'T04:00:00+00:00').timestamp())
        for offset in range(100):
            number = 100 + part * 100 + offset
            timestamp = base + offset * 60
            tables['blocks'].append(dict(number=number, hash=block_hash(number), parent_hash=block_hash(number - 1),
                timestamp=timestamp, timestamp_utc=datetime.fromtimestamp(timestamp, timezone.utc).isoformat(), transaction_count=500))
            for position in range(500):
                index = (number - 100) * 500 + position
                sender = address(index % 187500 + 1)
                recipient = address(index % 125000 + 1000000)
                tx_hash = block_hash(index + 10000000)
                status = int(index % 17 != 0)
                input_data = '0x' if index % 5 == 0 else '0x12345678'
                tables['transactions'].append(dict(hash=tx_hash, block_number=number, block_hash=block_hash(number),
                    transaction_index=position, timestamp=timestamp, timestamp_utc=datetime.fromtimestamp(timestamp, timezone.utc).isoformat(),
                    sender=sender, recipient=recipient, value_wei='1000000000000000000', input=input_data,
                    selector=input_data if input_data != '0x' else None, type=2, is_system=False, classification='unknown', recipient_kind='unknown'))
                tables['receipts'].append(dict(transaction_hash=tx_hash, block_number=number, block_hash=block_hash(number),
                    transaction_index=position, status=status, gas_used='21000', effective_gas_price_wei='1',
                    l2_fee_wei='21000', l1_fee_wei='100', total_fee_wei='21100', contract_address=None))
                if status:
                    expected_success += 1
                    successful_senders.add(sender)
                    successful_days.setdefault(sender, set()).add(day)
                if index % 3 == 0:
                    tables['logs'].append(dict(transaction_hash=tx_hash, block_number=number, block_hash=block_hash(number),
                        log_index=position, address=token, topics=[topic, '0x' + sender[2:].zfill(64), '0x' + recipient[2:].zfill(64)],
                        data='0x' + (index + 1).to_bytes(32).hex()))
                    if status:
                        expected_logs += 1
                        expected_tokens += index + 1
        store.commit_partition(100 + part * 100, 200 + part * 100, tables)
    before = store.manifest_path.read_bytes()
    destination = module('report').generate(store, [], {token: {'symbol': 'TEST', 'decimals': 18, 'verified': True}})
    result = json.loads((store.root / 'analysis.json').read_text())
    assert result['quality']['ok']
    assert result['summary']['transactions'] == 250000
    assert result['summary']['unique_all_non_system_senders'] == 187500
    assert result['summary']['unique_successful_senders'] == len(successful_senders)
    assert result['summary']['native_value_wei'] == str(expected_success * 10**18)
    assert result['summary']['known_total_fee_wei'] == str(250000 * 21100)
    assert result['summary']['verified_token_raw_amounts'][token] == str(expected_tokens)
    assert sum(row['log_count'] for row in result['events']) == expected_logs
    assert len(list(destination.glob('*.csv'))) == 26
    assert result['metadata']['synthetic'] is True
    assert store.manifest_path.read_bytes() == before
    assert not list((store.root / '.duckdb-tmp').glob('analysis-*'))
    for day in days:
        row = next(row for row in result['daily'] if row['day'] == day)
        assert row['active_senders'] == sum(day in activity_days for activity_days in successful_days.values())
    assert sum(row['active_senders'] for row in result['daily']) > result['summary']['unique_successful_senders']
