from conftest import module
from test_attribution import attribution, transaction


def test_attribution_dedup_stages_persist_and_ignore_null_identities():
    def inspect(connection):
        for name in ('attribution_project_senders', 'attribution_recipient_senders',
                     'attribution_sender_targets', 'attribution_sender_days',
                     'attribution_sender_day_targets', 'attribution_sender_activity'):
            tables = connection.execute(
                'SELECT temporary FROM duckdb_tables() WHERE table_name=?', [name]).fetchall()
            assert tables == [(False,)], name

    null_sender = transaction(3)
    null_sender['sender'] = None
    result = attribution([
        transaction(1), transaction(2), null_sender,
        transaction(4, recipient=None, status=0),
        transaction(5, day='2026-10-17', recipient=102),
    ], inspect=inspect)
    project = next(row for row in result['daily_projects'] if row['day'] == '2026-10-15')
    assert project['transactions'] == 4
    assert project['active_senders'] == 1
    profile = result['sender_profiles'][0]
    assert profile['unique_recipients'] == 2
    assert profile['successful_active_days'] == 2


def test_high_cardinality_queries_have_no_distinct_aggregate():
    import inspect

    for name in ('activity_metrics', 'attribution'):
        source = inspect.getsource(module(name)).lower()
        assert 'count(distinct' not in source


def test_sender_gap_medians_use_persistent_sorted_intervals():
    def inspect(connection):
        assert connection.execute("""SELECT temporary FROM duckdb_tables()
            WHERE table_name='attribution_sender_ranked_gaps'""").fetchall() == [(False,)]

    result = attribution([transaction(number, timestamp=timestamp)
                          for number, timestamp in enumerate((100, 100, 101, 104, 113), 1)], inspect=inspect)
    profile = result['sender_profiles'][0]
    assert profile['min_gap_seconds'] == 0
    assert profile['max_gap_seconds'] == 9
    assert profile['mean_gap_seconds'] == 3.25
    assert profile['median_gap_seconds'] == 2


def test_hourly_persistent_dedup_combines_statuses_and_excludes_null_senders(tmp_path):
    from types import SimpleNamespace

    import duckdb

    with duckdb.connect(str(tmp_path / 'hourly.duckdb')) as connection:
        connection.execute("""CREATE TABLE activity AS SELECT sender,status,is_system,timestamp,
            'recipient' AS recipient,'0x' AS input,2 AS type,'2026-10-01' AS day,
            NULL::VARCHAR AS l1_fee_wei,NULL::VARCHAR AS l2_fee_wei,NULL::VARCHAR AS total_fee_wei
            FROM (VALUES ('first',1,false,1790812800),('first',0,false,1790812800),
                ('second',0,false,1790812800),(NULL,1,false,1790812800),
                ('system',1,true,1790816400)) entries(sender,status,is_system,timestamp)""")
        result = {'daily': [], 'weekly': [], 'monthly': [], 'summary': {}}
        store = SimpleNamespace(config=SimpleNamespace(timezone='UTC'), manifest={
            'start_utc': '2026-10-01T00:00:00+00:00', 'end_utc': '2026-10-02T00:00:00+00:00'})
        module('activity_metrics').augment(connection, result, store)
        assert [(row['active_senders'], row['all_non_system_senders'])
                for row in result['hourly']] == [(1, 2), (0, 0)]
        assert connection.execute('SELECT count(*) FROM metric_hour_senders').fetchone() == (2,)
        assert connection.execute("""SELECT temporary FROM duckdb_tables()
            WHERE table_name='metric_sender_activity'""").fetchall() == [(False,)]
        assert 'input' not in [row[0] for row in connection.execute('DESCRIBE metric_sender_activity').fetchall()]
    with duckdb.connect(str(tmp_path / 'hourly.duckdb')) as connection:
        assert connection.execute('SELECT count(*) FROM metric_hour_senders').fetchone() == (2,)
