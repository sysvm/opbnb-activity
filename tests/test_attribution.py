from collections import defaultdict
from datetime import datetime, timezone

import duckdb
from conftest import address, module
from test_activity_metrics import analyze_fixture


def test_analyze_includes_offline_attribution(tmp_path, synthetic):
    result = analyze_fixture(tmp_path, synthetic)
    assert sum(row['transactions'] for row in result['daily_projects']) == 10
    assert sum(row['transactions'] for row in result['daily_recipients']) == 10
    assert len(result['failure_samples']) == 2


def attribution(rows, days=('2026-10-15', '2026-10-16', '2026-10-17'), inspect=None):
    import pyarrow as arrow
    schema = arrow.schema([(name, kind) for name, kind in [
        ('hash', arrow.string()), ('block_number', arrow.int64()), ('transaction_index', arrow.int64()),
        ('timestamp', arrow.int64()), ('sender', arrow.string()), ('recipient', arrow.string()),
        ('input', arrow.string()), ('method_selector', arrow.string()), ('value_wei', arrow.string()),
        ('is_system', arrow.bool_()), ('status', arrow.int64()), ('day', arrow.string()), ('project', arrow.string())]])
    connection = duckdb.connect()
    connection.register('activity', arrow.Table.from_pylist(rows, schema=schema))
    connection.execute('CREATE VIEW metric_activity AS SELECT * FROM activity')
    result = {'daily': [{'day': day} for day in days]}
    augment = getattr(module('metrics'), 'augment_attribution', None)
    assert callable(augment), 'offline attribution augmentation missing'
    try:
        augment(connection, result)
        if inspect:
            inspect(connection)
        return result
    finally:
        connection.close()


def transaction(number, day='2026-10-15', sender=1, recipient=101, **changes):
    row = dict(hash=f'0x{number:064x}', block_number=number, transaction_index=0,
        timestamp=int(datetime.fromisoformat(day + 'T00:00:00+00:00').timestamp()) + number,
        sender=address(sender), recipient=address(recipient) if recipient is not None else None,
        input='0x', method_selector='0x', value_wei='0', is_system=False, status=1,
        day=day, project='Unknown')
    row.update(changes)
    return row


def test_daily_panels_reconcile_zero_days_creation_and_label_switch():
    rows = [transaction(1, project='Old'), transaction(2, sender=2, recipient=None),
        transaction(3, day='2026-10-17', project='New'), transaction(4, day='2026-10-17', status=0),
        transaction(5, is_system=True)]
    result = attribution(rows)
    for name in ('daily_projects', 'daily_recipients'):
        totals = defaultdict(int)
        for row in result[name]:
            totals[row['day']] += row['transactions']
            assert row['transactions'] == row['successful_transactions'] + row['failed_transactions']
        assert dict(totals) == {'2026-10-15': 2, '2026-10-16': 0, '2026-10-17': 2}
    assert {'Old', 'New', 'Unknown'} == {row['project'] for row in result['daily_projects']}
    creations = [row for row in result['daily_recipients'] if row['recipient_group'] == 'creation']
    assert [row['transactions'] for row in creations] == [1, 0, 0]
    for dimension in ('project', 'recipient'):
        changes = [row for row in result['daily_changes'] if row['dimension'] == dimension]
        assert all(row['delta_transactions'] is None for row in changes if row['day'] == '2026-10-15')
        assert sum(row['delta_transactions'] for row in changes if row['day'] == '2026-10-16') == -2
        assert sum(row['delta_transactions'] for row in changes if row['day'] == '2026-10-17') == 2
        assert all(row['growth_rate'] is None for row in changes if row['day'] == '2026-10-17')


def test_stable_top20_other_includes_full_counts():
    rows = [transaction(number, recipient=100 + number) for number in range(1, 23)]
    rows += [transaction(99, day='2026-10-17', recipient=122)]
    result = attribution(rows)
    panel = result['daily_recipients']
    assert len(panel) == 3 * 22
    top = {row['recipient'] for row in panel if row['recipient_group'] == 'top20'}
    assert address(122) in top and len(top) == 20
    assert sum(row['transactions'] for row in panel if row['recipient_group'] == 'other') == 2
    assert sum(row['transactions'] for row in panel) == 23


def test_sender_profiles_exact_money_intervals_overlap_and_no_creation_target():
    huge = 10**50 + 3
    rows = [transaction(1, value_wei=str(huge), timestamp=100),
        transaction(2, status=0, value_wei='7', timestamp=100),
        transaction(3, recipient=None, timestamp=110), transaction(4, sender=2, timestamp=120),
        transaction(5, sender=2, recipient=102, timestamp=130), transaction(6, sender=3, recipient=None)]
    result = attribution(rows)
    profiles = {row['sender']: row for row in result['sender_profiles']}
    first = profiles[address(1)]
    assert first['transactions'] == 3 and first['unique_recipients'] == 1
    assert first['empty_calldata_transactions'] == 3
    assert first['zero_value_transactions'] == 1 and first['nonzero_value_transactions'] == 2
    assert first['attempted_value_wei'] == str(huge + 7)
    assert first['successful_value_wei'] == str(huge)
    assert first['min_gap_seconds'] == 0 and first['median_gap_seconds'] == 5
    assert profiles[address(3)]['median_gap_seconds'] is None
    pair = next(row for row in result['sender_overlap'] if row['sender_a'] == address(1) and row['sender_b'] == address(2))
    assert pair['shared_recipients'] == 1 and pair['union_recipients'] == 2
    assert pair['jaccard'] == 0.5
    assert next(row for row in result['sender_overlap'] if row['sender_b'] == address(3))['shared_recipients'] == 0
    assert len(result['sender_daily']) == 9


def test_failure_samples_bounded_deterministic_real_failures():
    rows = [transaction(number, status=0, input='0x12345678', method_selector='0x12345678') for number in range(1, 6)]
    rows += [transaction(8), transaction(9, is_system=True, status=0), transaction(10, status=0, recipient=None)]
    result = attribution(list(reversed(rows)))
    failures = result['failure_methods']
    assert sum(row['failed_transactions'] for row in failures) == 6
    assert failures[0]['failed_transactions'] == 5 and failures[0]['transactions'] == 5
    samples = [row for row in result['failure_samples'] if row['recipient'] == address(101)]
    assert [row['transaction_hash'] for row in samples] == [f'0x{number:064x}' for number in range(1, 4)]
    assert all(row['status'] == 0 and row['revert_evidence'] == 'not_collected' for row in result['failure_samples'])


def test_empty_window_and_sender_limits():
    result = attribution([])
    assert result['sender_profiles'] == [] and result['sender_overlap'] == []
    assert result['failure_methods'] == [] and result['failure_samples'] == []
    assert len(result['daily_projects']) == 3
    assert all(row['project'] == 'Unknown' and row['transactions'] == 0 for row in result['daily_projects'])
    result = attribution([transaction(number, sender=number) for number in range(1, 23)])
    assert len(result['sender_profiles']) == 20 and len(result['sender_overlap']) == 190
    assert {row['sender'] for row in result['sender_profiles']} == {address(number) for number in range(1, 21)}


def test_project_dau_not_additive_and_label_data_not_sql():
    result = attribution([transaction(1, project="' OR 1=1 --"), transaction(2, recipient=102, project='New')])
    first = [row for row in result['daily_projects'] if row['day'] == '2026-10-15']
    assert sum(row['active_senders'] for row in first) == 2
    assert sum(row['transactions'] for row in first) == 2
    assert any(row['project'] == "' OR 1=1 --" for row in first)


def test_reused_selections_cached_without_full_calldata():
    def inspect(connection):
        for name in ('attribution_senders', 'attribution_sender_activity', 'attribution_recipients',
                'attribution_failure_methods'):
            kind = connection.execute('SELECT table_type FROM information_schema.tables WHERE table_name=?', [name]).fetchone()[0]
            assert kind in {'BASE TABLE', 'LOCAL TEMPORARY'}, name + ' must not rescan source'
        columns = [row[0] for row in connection.execute('DESCRIBE attribution_sender_activity').fetchall()]
        assert 'input' not in columns and 'is_empty_calldata' in columns
        plan = connection.execute('EXPLAIN SELECT * FROM attribution_sender_activity').fetchone()[1]
        assert 'ARROW_SCAN' not in plan
    attribution([transaction(1)], inspect=inspect)
