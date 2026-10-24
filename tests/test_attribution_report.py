from conftest import address, module
from test_enrichment import FakeRpc, make_store
from test_report import fixture_store, read_csv


def test_report_exports_attribution_and_explains_boundaries(tmp_path, synthetic):
    store = fixture_store(tmp_path, synthetic)
    destination = module('report').generate(store, synthetic['labels'], synthetic['assets'])
    expected = {'daily_projects': 'active_senders', 'daily_recipients': 'recipient_group',
        'daily_changes': 'delta_transactions', 'sender_profiles': 'median_gap_seconds',
        'sender_daily': 'successful_value_wei', 'sender_overlap': 'jaccard',
        'failure_methods': 'share_of_failures', 'failure_samples': 'transaction_hash'}
    for name, column in expected.items():
        fields, rows = read_csv(destination, name)
        assert column in fields
        assert rows
    text = (destination / 'summary.md').read_text()
    assert '每日交易变化归因' in text
    assert '项目 DAU 不可相加' in text
    assert '窗口首日' in text and 'D1（剔除窗口首日 cohort）' in text
    assert 'failure_samples.csv' in text and '未采集 revert 原因' in text
    assert 'sender_profiles.csv' in text and '同一控制人' in text
    assert '不是普通用户费用缺失率' in text
    appendix = (destination / 'technical.md').read_text()
    for name in expected:
        assert name + '.csv' in appendix


def test_empty_attribution_tables_and_unsafe_fields(tmp_path, synthetic):
    for label in synthetic['labels']:
        label['project'] = '=BAD|<script>'
    store = fixture_store(tmp_path, synthetic)
    destination = module('report').generate(store, synthetic['labels'], synthetic['assets'])
    _, projects = read_csv(destination, 'daily_projects')
    assert any(row['project'].startswith("'=BAD") for row in projects)
    assert '<script>' not in (destination / 'summary.md').read_text()
    empty = module('storage').Store(module('config').Config(dataset=tmp_path, run_id='empty-new',
        start='2026-10-01', end='2026-10-02'), 100, 100, synthetic=True)
    destination = module('report').generate(empty, [], {})
    for name in ('sender_profiles', 'sender_daily', 'sender_overlap', 'failure_methods', 'failure_samples'):
        fields, rows = read_csv(destination, name)
        assert fields and rows == []


def test_code_observation_scope_not_historical_eoa_proof(tmp_path, synthetic):
    store = make_store(tmp_path, synthetic)
    module('enrichment').enrich(store, FakeRpc(), top=2, allow_synthetic=True)
    result = module('metrics').analyze(store, synthetic['labels'], synthetic['assets'])
    detail = next(row for row in result['recipient_details'] if row['address'] == address(103))
    assert detail['code_observation_scope'] == 'endpoints_mixed_or_incomplete'
    assert detail['historical_kind'] == 'unknown'
    unqueried = next(row for row in result['recipient_details'] if row['address'] == address(104))
    assert unqueried['code_observation_scope'] == 'unavailable'
