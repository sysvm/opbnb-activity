import json
import logging
from pathlib import Path

import duckdb
import pytest

from conftest import module
from test_streaming_quality import populated_store


def stages_module():
    return module('analysis_stages')


def test_partition_staged_tables_are_persistent_exact_and_cleanup(tmp_path, synthetic):
    store = populated_store(tmp_path, synthetic, partition_size=2)
    before = {str(path): path.read_bytes() for path in store.root.rglob('*.parquet')}
    manifest = store.manifest_path.read_bytes()
    with stages_module().AnalysisWorkspace(store) as connection:
        stages_module().prepare_activity(connection, store, synthetic['labels'])
        assert connection.execute('SELECT count(*) FROM activity').fetchone()[0] == 11
        assert connection.execute('SELECT count(*) FROM eligible_logs').fetchone()[0] == 2
        assert connection.execute('SELECT count(*) FROM activity WHERE project=\'Alpha\'').fetchone()[0] == 5
        tables = dict(connection.execute("SELECT table_name,table_type FROM information_schema.tables WHERE table_name IN ('activity','eligible_logs')").fetchall())
        assert set(tables.values()) == {'BASE TABLE'}
        assert connection.execute("SELECT database_name FROM duckdb_databases() WHERE database_name=current_database()").fetchone()[0] != 'memory'
        spill = Path(connection.execute("SELECT current_setting('temp_directory')").fetchall()[0][0])
        database = Path(connection.execute('SELECT path FROM duckdb_databases() WHERE database_name=current_database()').fetchall()[0][0])
        assert spill.is_relative_to(database.parent), 'report spill must be owned by workspace'
        plan = connection.execute('EXPLAIN SELECT count(*) FROM activity GROUP BY day').fetchone()[1]
        assert 'HASH_JOIN' not in plan and 'READ_PARQUET' not in plan
    assert store.manifest_path.read_bytes() == manifest
    assert before == {str(path): path.read_bytes() for path in store.root.rglob('*.parquet')}
    assert not list((store.root / '.duckdb-tmp').glob('analysis-*'))
    assert not spill.exists()
    status = json.loads((store.root / 'analysis-status.json').read_text())
    assert status['status'] == 'complete'


def test_staging_joins_only_one_physical_partition(tmp_path, synthetic, monkeypatch):
    store = populated_store(tmp_path, synthetic, partition_size=2)
    original = duckdb.DuckDBPyConnection.from_parquet
    bound = []

    def observed(connection, filenames, **options):
        if any(name in str(filenames) for name in ('transactions.parquet', 'receipts.parquet', 'logs.parquet')):
            bound.append(list(filenames))
        return original(connection, filenames, **options)

    with stages_module().AnalysisWorkspace(store) as connection:
        monkeypatch.setattr(duckdb.DuckDBPyConnection, 'from_parquet', observed)
        stages_module().prepare_activity(connection, store, synthetic['labels'])
    assert bound and all(len(paths) == 1 for paths in bound)


def test_stage_failure_is_safe_and_cleans_owned_workspace(tmp_path, synthetic, caplog):
    store = populated_store(tmp_path, synthetic)
    caplog.set_level(logging.INFO, logger='opbnb_activity')
    with pytest.raises(stages_module().AnalysisStageError, match='intentional_failure') as failure:
        with stages_module().AnalysisWorkspace(store) as connection:
            stages_module().run_query(connection, 'intentional_failure', "SELECT private_secret_does_not_exist")
    assert 'private_secret' not in str(failure.value)
    status = json.loads((store.root / 'analysis-status.json').read_text())
    assert status['status'] == 'failed' and status['stage'] == 'intentional_failure'
    assert 'private_secret' not in json.dumps(status) and 'private_secret' not in caplog.text
    assert not list((store.root / '.duckdb-tmp').glob('analysis-*'))


def test_report_uses_staged_logs_not_global_raw_joins(tmp_path, synthetic, monkeypatch):
    stages = stages_module()
    original = stages.prepare_activity
    calls = []

    def stage_and_remove_raw(*args):
        calls.append(True)
        original(*args)
        for name in ('transactions', 'receipts', 'logs'):
            args[0].execute('DROP VIEW ' + name)

    monkeypatch.setattr(stages, 'prepare_activity', stage_and_remove_raw)
    store = populated_store(tmp_path, synthetic)
    result = module('metrics').analyze(store, synthetic['labels'], synthetic['assets'])
    assert calls, 'report did not use staged preparation'
    assert result['summary']['transactions'] == 11
    assert result['summary']['native_value_wei'] == str(10**45 + 81)
    assert result['summary']['verified_token_raw_amounts']
    assert result['events']
