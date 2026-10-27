import logging
import re
import tempfile
from contextvars import ContextVar
from pathlib import Path

from .labels import LabelIndex
from .storage import atomic_json
from .system_transactions import tx_dag_sql

logger = logging.getLogger(__name__)
_workspace = ContextVar('analysis_workspace', default=None)


class AnalysisStageError(ValueError):
    pass


def begin_stage(name):
    if not re.fullmatch(r'[A-Za-z][A-Za-z0-9_. -]{0,95}', name):
        raise ValueError('Invalid analysis stage identifier')
    current = _workspace.get()
    if current is not None:
        current.stage = name
        atomic_json(current.store.root / 'analysis-status.json', {'status': 'running', 'stage': name})
    logger.info('Analysis stage start: %s', name)


def run_query(connection, name, query, parameters=None):
    begin_stage(name)
    try:
        return connection.execute(query, parameters) if parameters is not None else connection.execute(query)
    except Exception as error:
        logger.error('Analysis stage failed: stage=%s error_type=%s', name, type(error).__name__)
        raise


def materialize(connection, name, query, parameters=None):
    if not re.fullmatch(r'[A-Za-z][A-Za-z0-9_]*', name):
        raise ValueError('Invalid analysis table identifier')
    cursor = run_query(connection, name, f'CREATE TABLE {name} AS {query}', parameters)
    while cursor.fetchone() is not None:
        pass
    logger.info('Analysis stage complete: %s', name)


class AnalysisWorkspace:
    def __init__(self, store):
        self.store = store
        self.stage = 'workspace_open'
        self.connection = None
        self.directory = None
        self.token = None

    def __enter__(self):
        parent = (Path(self.store.config.analysis_temp_directory).expanduser().resolve()
            if self.store.config.analysis_temp_directory else self.store.root / '.duckdb-tmp')
        parent.mkdir(parents=True, exist_ok=True)
        self.directory = tempfile.TemporaryDirectory(prefix='analysis-', dir=parent)
        self.token = _workspace.set(self)
        try:
            begin_stage('workspace_open')
            self.connection = self.store.connection(database=Path(self.directory.name) / 'analysis.duckdb')
            return self.connection
        except Exception as error:
            self.__exit__(type(error), error, error.__traceback__)

    def __exit__(self, error_type, error, traceback):
        try:
            if self.connection is not None:
                self.connection.close()
        finally:
            try:
                if self.directory is not None:
                    self.directory.cleanup()
            finally:
                if self.token is not None:
                    _workspace.reset(self.token)
        status = {'status': 'failed' if error_type else 'complete', 'stage': self.stage}
        if error_type:
            status['error_type'] = error_type.__name__
        atomic_json(self.store.root / 'analysis-status.json', status)
        if error_type and issubclass(error_type, Exception):
            logger.error('Analysis failed: stage=%s error_type=%s', self.stage, error_type.__name__)
            raise AnalysisStageError(f'Analysis stage {self.stage} failed: {error_type.__name__}; see analysis-status.json') from None
        return False


def prepare_activity(connection, store, labels):
    import pyarrow as arrow
    label_schema = arrow.schema([('address', arrow.string()), ('project', arrow.string()), ('category', arrow.string()),
        ('role', arrow.string()), ('valid_from', arrow.int64()), ('valid_to', arrow.int64())])
    entries = [{key: row.get(key, 'unknown') for key in ('address', 'project', 'category', 'role', 'valid_from', 'valid_to')}
        for row in LabelIndex(labels).rows]
    connection.register('labels', arrow.Table.from_pylist(entries, schema=label_schema))
    materialize(connection, 'activity', """SELECT transactions.*,CAST(NULL AS BIGINT) AS status,
        CAST(NULL AS VARCHAR) AS l2_fee_wei,CAST(NULL AS VARCHAR) AS l1_fee_wei,CAST(NULL AS VARCHAR) AS total_fee_wei,
        CAST(NULL AS VARCHAR) AS day,CAST(NULL AS VARCHAR) AS project,CAST(NULL AS VARCHAR) AS category,
        CAST(NULL AS VARCHAR) AS role FROM transactions WHERE FALSE""")
    materialize(connection, 'eligible_logs', 'SELECT * FROM logs WHERE FALSE')
    zone = store.config.timezone.replace("'", "''")
    paths = store.partition_dirs()
    for position, path in enumerate(paths, 1):
        for table in ('transactions', 'receipts', 'logs', 'blocks'):
            connection.from_parquet([str(path / (table + '.parquet'))], hive_partitioning=False).create_view(
                'stage_' + table, replace=True)
        dag = tx_dag_sql()
        run_query(connection, f'classify_partition_{position}', f"""
            CREATE OR REPLACE TEMP VIEW stage_classified_transactions AS
            SELECT t.* REPLACE ((t.is_system OR {dag}) AS is_system,
                CASE WHEN {dag} THEN 'system_tx_dag' ELSE t.classification END AS classification)
            FROM stage_transactions t JOIN stage_receipts r ON t.hash=r.transaction_hash
            JOIN stage_blocks b ON t.block_number=b.number""")
        query = f"""INSERT INTO activity SELECT t.*,r.status,r.l2_fee_wei,r.l1_fee_wei,r.total_fee_wei,
            strftime(to_timestamp(t.timestamp) AT TIME ZONE '{zone}', '%Y-%m-%d') AS day,
            coalesce(labels.project,'Unknown'),coalesce(labels.category,'unknown'),coalesce(labels.role,'unknown')
            FROM stage_classified_transactions t JOIN stage_receipts r ON t.hash=r.transaction_hash
            LEFT JOIN labels ON t.recipient=labels.address AND t.block_number>=labels.valid_from AND t.block_number<labels.valid_to"""
        for name, statement in ((f'activity_partition_{position}', query), (f'logs_partition_{position}', """
            INSERT INTO eligible_logs SELECT logs.* FROM stage_logs logs
            JOIN stage_classified_transactions t ON logs.transaction_hash=t.hash
            JOIN stage_receipts r ON logs.transaction_hash=r.transaction_hash WHERE NOT t.is_system AND r.status=1""")):
            cursor = run_query(connection, name, statement)
            while cursor.fetchone() is not None:
                pass
        if position % 25 == 0 or position == len(paths):
            connection.execute('CHECKPOINT')
            logger.info('Analysis staging progress: partitions=%d/%d', position, len(paths))
    if paths:
        connection.execute('DROP VIEW stage_classified_transactions')
    for table in ('transactions', 'receipts', 'logs', 'blocks'):
        if paths:
            connection.execute('DROP VIEW stage_' + table)
    connection.execute('CHECKPOINT')
