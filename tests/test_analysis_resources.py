import hashlib
import json
from pathlib import Path

import duckdb
import pytest

from conftest import module
from test_streaming_quality import populated_store


def test_resources_preserve_legacy_identity_and_manifest(tmp_path, synthetic):
    store = populated_store(tmp_path, synthetic)
    before = store.manifest_path.read_bytes()
    config = module('config').Config(run_id='resources', analysis_memory_limit_mb=8192,
        analysis_threads=2, analysis_temp_directory=str(tmp_path / 'scratch'))
    legacy = config.serializable()
    for name in ('dataset', 'run_id', 'qps', 'concurrency', 'batch_size', 'timeout_seconds', 'retries',
        'labels', 'assets', 'blocks_per_checkpoint', 'analysis_memory_limit_mb', 'analysis_threads', 'analysis_temp_directory'):
        legacy.pop(name)
    legacy.update(schema_version=1, chain_id=204)
    assert config.identity == hashlib.sha256(json.dumps(legacy, sort_keys=True).encode()).hexdigest()
    assert config.identity == module('config').Config(run_id='other', analysis_memory_limit_mb=512, analysis_threads=1).identity
    overrides = module('config').Config(run_id='ignored', start='2026-01-01', end='2026-01-02',
        analysis_memory_limit_mb=256, analysis_threads=3, analysis_temp_directory=str(tmp_path / 'spill'))
    opened = module('storage').Store.open(tmp_path, store.config.run_id, analysis_settings=overrides)
    assert opened.config.analysis_memory_limit_mb == 256
    assert opened.config.analysis_threads == 3
    assert opened.config.start == store.config.start
    assert opened.config.identity == store.config.identity
    assert store.manifest_path.read_bytes() == before


@pytest.mark.parametrize('field,value', [('analysis_memory_limit_mb', 0), ('analysis_memory_limit_mb', 63),
    ('analysis_memory_limit_mb', True), ('analysis_memory_limit_mb', '8GB'), ('analysis_memory_limit_mb', 256.0),
    ('analysis_threads', 0), ('analysis_threads', True), ('analysis_threads', 129),
    ('analysis_temp_directory', ''), ('analysis_temp_directory', False), ('analysis_temp_directory', 17)])
def test_invalid_analysis_settings_fail_early(field, value):
    with pytest.raises(ValueError, match='Invalid analysis resource settings'):
        module('config').Config(run_id='resources', **{field: value})


def test_connection_applies_runtime_limits_and_isolates_spill(tmp_path, synthetic):
    store = populated_store(tmp_path, synthetic)
    store.config.analysis_memory_limit_mb = 256
    store.config.analysis_threads = 3
    store.config.analysis_temp_directory = str(tmp_path / 'separate-disk')
    reference = duckdb.connect(config={'memory_limit': '256MB'})
    expected = reference.execute("SELECT current_setting('memory_limit')").fetchone()[0]
    reference.close()
    first = store.connection()
    second = store.connection()
    try:
        settings = first.execute("SELECT current_setting('memory_limit'),current_setting('threads'),current_setting('preserve_insertion_order'),current_setting('temp_directory')").fetchone()
        assert settings[:3] == (expected, 3, False)
        assert Path(settings[3]).is_relative_to(tmp_path / 'separate-disk')
        assert settings[3] != second.execute("SELECT current_setting('temp_directory')").fetchone()[0]
        assert first.execute('SELECT count(*) FROM transactions').fetchone()[0] == 11
    finally:
        first.close()
        second.close()


def test_relative_spill_path_resolves_against_config(tmp_path):
    config_file = tmp_path / 'config.toml'
    config_file.write_text('analysis_temp_directory = "./scratch"\nanalysis_memory_limit_mb = 256\nanalysis_threads = 3\n')
    config = module('config').load(config_file, run_id='resource-test')
    assert config.analysis_temp_directory == str(tmp_path / 'scratch')


def test_legacy_manifest_without_resource_fields_reopens_unchanged(tmp_path, synthetic):
    store = populated_store(tmp_path, synthetic)
    legacy = json.loads(store.manifest_path.read_text())
    for field in ('analysis_memory_limit_mb', 'analysis_threads', 'analysis_temp_directory'):
        legacy['config'].pop(field)
    store.manifest_path.write_text(json.dumps(legacy))
    before = store.manifest_path.read_bytes()
    config = module('config').Config(run_id=store.config.run_id, analysis_memory_limit_mb=256, analysis_threads=1)
    opened = module('storage').Store.open(tmp_path, store.config.run_id, analysis_settings=config)
    assert opened.config.identity == legacy['identity']
    assert opened.config.analysis_memory_limit_mb == 256
    assert store.manifest_path.read_bytes() == before


@pytest.mark.parametrize('value', ['17', 'true', 'false', '""'])
def test_bad_spill_type_in_toml_is_safe_validation_error(tmp_path, value):
    config_file = tmp_path / 'bad.toml'
    config_file.write_text('analysis_temp_directory = ' + value + '\n')
    with pytest.raises(ValueError, match='Invalid analysis resource settings'):
        module('config').load(config_file, run_id='bad-resource')


def test_setup_failure_closes_duckdb_connection(tmp_path, synthetic, monkeypatch):
    store = populated_store(tmp_path, synthetic)
    real_connect = duckdb.connect
    connections = []

    class FailSettings:
        def __init__(self):
            self.connection = real_connect()
            connections.append(self.connection)

        def execute(self, query, *args):
            raise RuntimeError('setup failure')

        def close(self):
            self.connection.close()

    monkeypatch.setattr(duckdb, 'connect', FailSettings)
    with pytest.raises(RuntimeError, match='setup failure'):
        store.connection()
    with pytest.raises(duckdb.ConnectionException):
        connections[0].execute('SELECT 1')


@pytest.mark.parametrize('action', ['validate', 'analyze', 'report', 'enrich'])
def test_cli_forwards_runtime_config_to_existing_run(tmp_path, synthetic, monkeypatch, action):
    store = populated_store(tmp_path, synthetic)
    config_file = tmp_path / 'config.toml'
    config_file.write_text(f'dataset = "{tmp_path}"\nanalysis_memory_limit_mb = 256\nanalysis_threads = 3\n')
    observed = []

    def capture(dataset, run_id, *, analysis_settings=None):
        observed.append(analysis_settings)
        raise ValueError('start must precede end')

    monkeypatch.setattr(module('cli').Store, 'open', capture)
    assert module('cli').main([action, '--config', str(config_file), '--run-id', store.config.run_id]) == 2
    assert observed and observed[0].analysis_memory_limit_mb == 256
    assert observed[0].analysis_threads == 3
