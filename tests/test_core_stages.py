from pathlib import Path

import duckdb
import pytest

from opbnb_activity.core_metrics import (
    core_tables,
    period_tables,
    prepare_core,
    top_sender_transactions,
    unique_sender_counts,
)


def records(cursor):
    names = [column[0] for column in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


@pytest.fixture
def connection(tmp_path):
    connection = duckdb.connect(str(tmp_path / "report.duckdb"))
    connection.execute("""CREATE TABLE activity (
        day VARCHAR, sender VARCHAR, recipient VARCHAR, project VARCHAR,
        category VARCHAR, role VARCHAR, status INTEGER, is_system BOOLEAN)""")
    connection.executemany("INSERT INTO activity VALUES (?, ?, ?, ?, ?, ?, ?, ?)", [
        ("2026-09-30", "a", "x", "Old", "dex", "router", 1, False),
        ("2026-10-01", "a", "x", "New", "dex", "router", 1, False),
        ("2026-10-01", "a", "x", "New", "dex", "router", 1, False),
        ("2026-10-01", "a", "y", "New", "dex", "pool", 0, False),
        ("2026-10-01", "b", None, "Unknown", "unknown", "unknown", 0, False),
        ("2026-10-01", "system", "s", "System", "system", "system", 1, True),
        ("2026-10-02", "b", None, "Unknown", "unknown", "unknown", 1, False),
        ("2026-10-07", "a", "x", "New", "dex", "router", 1, False),
        ("2026-10-08", "b", "y", "New", "dex", "pool", 1, False),
        ("2026-10-08", "c", "y", "New", "dex", "pool", 0, False),
        ("2026-10-09", "system", "s", "System", "system", "system", 1, True),
    ])
    yield connection
    connection.close()


def test_core_tables_match_original_exact_queries(connection):
    sql_dir = Path(__file__).parents[1] / "src" / "opbnb_activity" / "sql"
    expected = {name: records(connection.execute((sql_dir / f"{name}.sql").read_text()))
                for name in ("daily", "contracts", "projects", "senders", "retention")}
    prepare_core(connection)
    assert core_tables(connection) == expected
    assert unique_sender_counts(connection) == {
        "unique_successful_senders": 2, "unique_all_non_system_senders": 3}
    assert top_sender_transactions(connection, 0) == 0
    assert top_sender_transactions(connection, 1) == 4
    assert top_sender_transactions(connection, 100) == 6
    assert connection.execute("SELECT count(*) FROM report_sender_day").fetchone()[0] == 7
    assert connection.execute("SELECT count(*) FROM report_senders").fetchone()[0] == 3
    assert not connection.execute("SELECT temporary FROM duckdb_tables() WHERE table_name='report_senders'").fetchone()[0]


def test_periods_count_window_users_once_and_include_system_only_periods(connection):
    connection.execute("INSERT INTO activity VALUES ('2026-11-01','system','s','System','system','system',1,true)")
    expected = {}
    for period in ("week", "month"):
        expected[period + "ly"] = records(connection.execute(f"""
            SELECT cast(date_trunc('{period}', day::DATE) AS VARCHAR) AS period_start,
                count(DISTINCT sender) FILTER (WHERE status=1 AND NOT is_system) AS active_senders,
                count(DISTINCT sender) FILTER (WHERE NOT is_system) AS all_non_system_senders
            FROM activity GROUP BY period_start ORDER BY period_start"""))
    prepare_core(connection)
    assert period_tables(connection) == expected


def test_empty_activity(connection):
    connection.execute("DELETE FROM activity")
    prepare_core(connection)
    assert all(not rows for rows in core_tables(connection).values())
    assert period_tables(connection) == {"weekly": [], "monthly": []}
    assert unique_sender_counts(connection) == {
        "unique_successful_senders": 0, "unique_all_non_system_senders": 0}
    assert top_sender_transactions(connection, 1) == 0


def test_contract_limit_uses_all_transactions_before_successful_senders(connection):
    connection.execute("DELETE FROM activity")
    connection.execute("""INSERT INTO activity
        SELECT '2026-10-01', 'user' || sender_number, 'contract' || contract_number,
            'Project', 'dex', 'router', 1, false
        FROM range(101) contracts(contract_number), range(2) senders(sender_number)""")
    connection.execute("""INSERT INTO activity
        SELECT '2026-10-01', 'failed', 'zz', 'Project', 'dex', 'router', 0, false
        FROM range(5)""")
    prepare_core(connection)
    contracts = core_tables(connection)["contracts"]
    assert len(contracts) == 100
    assert contracts[0] == {"address": "zz", "project": "Project", "role": "router",
                            "category": "dex", "transactions": 5,
                            "successful_transactions": 0, "successful_senders": 0}
    assert all(row["successful_senders"] == 2 for row in contracts[1:])
