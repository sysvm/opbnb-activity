from .analysis_stages import materialize, run_query


def _rows(connection, name, query, parameters=None):
    cursor = run_query(connection, name, query, parameters)
    names = [column[0] for column in cursor.description]
    result = []
    while batch := cursor.fetchmany(4096):
        result.extend(dict(zip(names, row)) for row in batch)
    return result


def prepare_core(connection):
    materialize(connection, "report_sender_day", """
        SELECT sender, day, count(*) AS transactions,
            count(*) FILTER (WHERE status=1) AS successful_transactions,
            count(*) FILTER (WHERE status=0) AS failed_transactions
        FROM activity WHERE NOT is_system GROUP BY sender, day""")
    materialize(connection, "report_senders", """
        SELECT sender, sum(transactions)::BIGINT AS transactions,
            sum(successful_transactions)::BIGINT AS successful_transactions,
            sum(failed_transactions)::BIGINT AS failed_transactions,
            count(*) FILTER (WHERE successful_transactions>0 AND day IS NOT NULL) AS active_days,
            min(day) FILTER (WHERE successful_transactions>0) AS cohort_day
        FROM report_sender_day GROUP BY sender""")


def core_tables(connection):
    materialize(connection, "report_daily_counts", """
        SELECT day, count(*) AS transactions,
            count(*) FILTER (WHERE status=1) AS successful_transactions,
            count(*) FILTER (WHERE status=0) AS failed_transactions,
            count(*) FILTER (WHERE is_system) AS system_transactions
        FROM activity GROUP BY day""")
    materialize(connection, "report_daily_senders", """
        SELECT day, count(*) FILTER (WHERE successful_transactions>0) AS active_senders,
            count(*) AS all_non_system_senders
        FROM report_sender_day WHERE sender IS NOT NULL GROUP BY day""")
    daily = _rows(connection, "core_daily", """
        SELECT counts.*, coalesce(senders.active_senders,0) AS active_senders,
            coalesce(senders.all_non_system_senders,0) AS all_non_system_senders
        FROM report_daily_counts counts LEFT JOIN report_daily_senders senders
        ON counts.day IS NOT DISTINCT FROM senders.day ORDER BY counts.day""")
    materialize(connection, "report_contract_counts", """
        SELECT recipient, project, role, category, count(*) AS transactions,
            count(*) FILTER (WHERE status=1) AS successful_transactions
        FROM activity WHERE NOT is_system GROUP BY recipient, project, role, category""")
    materialize(connection, "report_top_contracts", """
        SELECT * FROM report_contract_counts
        ORDER BY transactions DESC, coalesce(recipient,'(contract creation)') LIMIT 100""")
    materialize(connection, "report_contract_sender_keys", """
        SELECT activity.recipient, activity.project, activity.role, activity.category, activity.sender
        FROM activity JOIN report_top_contracts selected
        ON activity.recipient IS NOT DISTINCT FROM selected.recipient
        AND activity.project IS NOT DISTINCT FROM selected.project
        AND activity.role IS NOT DISTINCT FROM selected.role
        AND activity.category IS NOT DISTINCT FROM selected.category
        WHERE NOT activity.is_system AND activity.status=1 AND activity.sender IS NOT NULL
        GROUP BY activity.recipient, activity.project, activity.role, activity.category, activity.sender""")
    materialize(connection, "report_contract_senders", """
        SELECT recipient, project, role, category, count(*) AS successful_senders
        FROM report_contract_sender_keys GROUP BY recipient, project, role, category""")
    contracts = _rows(connection, "core_contracts", """
        SELECT coalesce(selected.recipient,'(contract creation)') AS address,
            selected.project, selected.role, selected.category, selected.transactions,
            selected.successful_transactions, coalesce(senders.successful_senders,0) AS successful_senders
        FROM report_top_contracts selected LEFT JOIN report_contract_senders senders
        ON selected.recipient IS NOT DISTINCT FROM senders.recipient
        AND selected.project IS NOT DISTINCT FROM senders.project
        AND selected.role IS NOT DISTINCT FROM senders.role
        AND selected.category IS NOT DISTINCT FROM senders.category
        ORDER BY transactions DESC, address""")
    materialize(connection, "report_project_counts", """
        SELECT project, count(*) AS transactions,
            count(*) FILTER (WHERE status=1) AS successful_transactions,
            count(*) FILTER (WHERE status=0) AS failed_transactions
        FROM activity WHERE NOT is_system GROUP BY project""")
    materialize(connection, "report_project_sender_keys", """
        SELECT project, sender FROM activity
        WHERE NOT is_system AND status=1 AND sender IS NOT NULL GROUP BY project, sender""")
    materialize(connection, "report_project_senders", """
        SELECT project, count(*) AS successful_senders FROM report_project_sender_keys GROUP BY project""")
    materialize(connection, "report_project_recipient_keys", """
        SELECT project, recipient FROM activity
        WHERE NOT is_system AND recipient IS NOT NULL GROUP BY project, recipient""")
    materialize(connection, "report_project_recipients", """
        SELECT project, count(*) AS direct_recipients FROM report_project_recipient_keys GROUP BY project""")
    projects = _rows(connection, "core_projects", """
        SELECT counts.*, coalesce(senders.successful_senders,0) AS successful_senders,
            coalesce(recipients.direct_recipients,0) AS direct_recipients
        FROM report_project_counts counts LEFT JOIN report_project_senders senders
        ON counts.project IS NOT DISTINCT FROM senders.project
        LEFT JOIN report_project_recipients recipients
        ON counts.project IS NOT DISTINCT FROM recipients.project
        ORDER BY transactions DESC, counts.project""")
    senders = _rows(connection, "core_senders", """
        SELECT sender, transactions, successful_transactions, failed_transactions, active_days
        FROM report_senders ORDER BY transactions DESC, sender LIMIT 100""")
    materialize(connection, "report_retention_counts", """
        SELECT first_seen.cohort_day, lags.lag_days, count(*) AS cohort_size,
            count(active.sender) AS retained
        FROM report_senders first_seen CROSS JOIN (SELECT unnest([1,7]) AS lag_days) lags
        LEFT JOIN report_sender_day active
        ON first_seen.sender=active.sender AND active.successful_transactions>0
        AND active.day::DATE=first_seen.cohort_day::DATE + lags.lag_days::INTEGER
        WHERE first_seen.successful_transactions>0
        GROUP BY first_seen.cohort_day, lags.lag_days""")
    retention = _rows(connection, "core_retention", """
        SELECT cohort_day, lag_days, cohort_size, retained, retained::DOUBLE/cohort_size AS rate
        FROM report_retention_counts ORDER BY cohort_day, lag_days""")
    return dict(daily=daily, contracts=contracts, projects=projects, senders=senders, retention=retention)


def period_tables(connection):
    result = {}
    for period in ("week", "month"):
        materialize(connection, f"report_{period}_sender_keys", f"""
            SELECT strftime(date_trunc('{period}',day::DATE), '%Y-%m-%d') AS period_start,
                sender, max(successful_transactions)>0 AS successful
            FROM report_sender_day WHERE sender IS NOT NULL GROUP BY period_start, sender""")
        materialize(connection, f"report_{period}_senders", f"""
            SELECT period_start, count(*) FILTER (WHERE successful) AS active_senders,
                count(*) AS all_non_system_senders
            FROM report_{period}_sender_keys GROUP BY period_start""")
        materialize(connection, f"report_{period}_periods", f"""
            SELECT strftime(date_trunc('{period}',day::DATE), '%Y-%m-%d') AS period_start
            FROM activity GROUP BY period_start""")
        result[period + "ly"] = _rows(connection, f"core_{period}", f"""
            SELECT periods.period_start, coalesce(senders.active_senders,0) AS active_senders,
                coalesce(senders.all_non_system_senders,0) AS all_non_system_senders
            FROM report_{period}_periods periods LEFT JOIN report_{period}_senders senders
            ON periods.period_start IS NOT DISTINCT FROM senders.period_start ORDER BY periods.period_start""")
    return result


def unique_sender_counts(connection):
    return _rows(connection, "core_unique_senders", """
        SELECT count(*) FILTER (WHERE successful_transactions>0) AS unique_successful_senders,
            count(*) AS unique_all_non_system_senders
        FROM report_senders WHERE sender IS NOT NULL""")[0]


def top_sender_transactions(connection, limit):
    cursor = run_query(connection, "core_top_sender_transactions", """
        SELECT coalesce(sum(successful_transactions),0)::BIGINT FROM
            (SELECT successful_transactions FROM report_senders WHERE successful_transactions>0
             ORDER BY successful_transactions DESC, sender LIMIT ?)""", [limit])
    return cursor.fetchone()[0]
