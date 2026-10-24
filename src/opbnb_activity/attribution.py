from .activity_metrics import rows


COUNTS = """count(*) AS transactions,
    count(*) FILTER (WHERE status=1) AS successful_transactions,
    count(*) FILTER (WHERE status=0) AS failed_transactions,
    count(DISTINCT sender) FILTER (WHERE status=1) AS active_senders"""
COUNT_FIELDS = ("transactions", "successful_transactions", "failed_transactions", "active_senders")


def daily_panels(connection, result):
    days = [row["day"] for row in result["daily"]]
    observed = {(row["day"], row["project"]): row for row in rows(connection,
        f"SELECT day,project,{COUNTS} FROM metric_activity WHERE NOT is_system GROUP BY day,project")}
    projects = sorted({"Unknown", *(project for day, project in observed)})
    result["daily_projects"] = [dict(day=day, project=project,
        **{field: observed.get((day, project), {}).get(field, 0) for field in COUNT_FIELDS})
        for day in days for project in projects]
    connection.execute("""CREATE TEMP TABLE attribution_recipients AS SELECT recipient,count(*) AS transactions
        FROM metric_activity WHERE NOT is_system AND recipient IS NOT NULL GROUP BY recipient
        ORDER BY transactions DESC,recipient LIMIT 20""")
    top = [row["recipient"] for row in rows(connection, "SELECT recipient FROM attribution_recipients")]
    connection.execute("""CREATE TEMP VIEW attribution_grouped AS SELECT activity.*,
        CASE WHEN activity.recipient IS NULL THEN 'creation'
             WHEN targets.recipient IS NOT NULL THEN 'top20' ELSE 'other' END AS recipient_group,
        targets.recipient AS panel_recipient
        FROM metric_activity activity LEFT JOIN attribution_recipients targets USING (recipient)
        WHERE NOT is_system""")
    observed = {(row["day"], row["recipient_group"], row["recipient"]): row for row in rows(connection,
        f"""SELECT day,recipient_group,panel_recipient AS recipient,{COUNTS}
            FROM attribution_grouped GROUP BY day,recipient_group,panel_recipient""")}
    groups = [("top20", recipient) for recipient in top] + [("other", None), ("creation", None)]
    result["daily_recipients"] = [dict(day=day, recipient_group=group, recipient=recipient,
        **{field: observed.get((day, group, recipient), {}).get(field, 0) for field in COUNT_FIELDS})
        for day in days for group, recipient in groups]
    result["daily_changes"] = []
    for dimension, name in (("project", "daily_projects"), ("recipient", "daily_recipients")):
        previous = {}
        changes_by_day = {}
        for row in result[name]:
            key = row["project"] if dimension == "project" else (row["recipient_group"], row["recipient"])
            prior = previous.get(key)
            count = row["transactions"]
            change = dict(day=row["day"], dimension=dimension, project=row.get("project"),
                recipient_group=row.get("recipient_group"), recipient=row.get("recipient"),
                transactions=count, previous_transactions=prior,
                delta_transactions=count - prior if prior is not None else None,
                growth_rate=(count - prior) / prior if prior else None)
            changes_by_day.setdefault(row["day"], []).append(change)
            previous[key] = count
        for changes in changes_by_day.values():
            ranked = sorted(changes, key=lambda row: (-abs(row["delta_transactions"] or 0),
                row["project"] or "", row["recipient_group"] or "", row["recipient"] or ""))
            for rank, row in enumerate(ranked, 1):
                row["contribution_rank"] = rank if row["delta_transactions"] is not None else None
            result["daily_changes"].extend(changes)


def sender_panels(connection, result):
    connection.execute("""CREATE TEMP TABLE attribution_senders AS
        SELECT sender,count(*) AS transactions FROM metric_activity WHERE NOT is_system
        GROUP BY sender ORDER BY transactions DESC,sender LIMIT 20""")
    connection.execute("""CREATE TEMP TABLE attribution_sender_activity AS
        SELECT activity.sender,day,recipient,timestamp,block_number,transaction_index,hash,status,value_wei,
            input='0x' AS is_empty_calldata
        FROM metric_activity activity JOIN attribution_senders USING (sender)
        WHERE NOT is_system""")
    profile_counts = """count(*) AS transactions,
        count(*) FILTER (WHERE status=1) AS successful_transactions,
        count(*) FILTER (WHERE status=0) AS failed_transactions,
        count(*) FILTER (WHERE is_empty_calldata) AS empty_calldata_transactions,
        count(DISTINCT recipient) AS unique_recipients,
        count(DISTINCT day) FILTER (WHERE status=1) AS successful_active_days"""
    profiles = list(rows(connection, f"""SELECT sender,{profile_counts},
        min(timestamp) AS first_timestamp,max(timestamp) AS last_timestamp
        FROM attribution_sender_activity GROUP BY sender ORDER BY transactions DESC,sender"""))
    gaps = {row["sender"]: row for row in rows(connection, """WITH intervals AS (
        SELECT sender,timestamp-lag(timestamp) OVER (PARTITION BY sender
            ORDER BY timestamp,block_number,transaction_index,hash) AS gap
        FROM attribution_sender_activity)
        SELECT sender,min(gap) AS min_gap_seconds,max(gap) AS max_gap_seconds,
            avg(gap) AS mean_gap_seconds,median(gap) AS median_gap_seconds
        FROM intervals GROUP BY sender""")}
    daily = {(row["day"], row["sender"]): row for row in rows(connection, f"""SELECT day,sender,{profile_counts}
        FROM attribution_sender_activity GROUP BY day,sender""")}
    result["sender_daily"] = []
    for day in (row["day"] for row in result["daily"]):
        for profile in profiles:
            row = daily.get((day, profile["sender"]), dict(day=day, sender=profile["sender"],
                **{field: 0 for field in ("transactions", "successful_transactions", "failed_transactions",
                    "empty_calldata_transactions", "unique_recipients", "successful_active_days")}))
            result["sender_daily"].append(row)
    profiles_by_sender = {row["sender"]: row for row in profiles}
    daily = {(row["day"], row["sender"]): row for row in result["sender_daily"]}
    for row in [*profiles, *result["sender_daily"]]:
        row.update(zero_value_transactions=0, nonzero_value_transactions=0,
            attempted_value_wei=0, successful_value_wei=0)
    for transaction in rows(connection, "SELECT sender,day,status,value_wei FROM attribution_sender_activity"):
        value = int(transaction["value_wei"])
        for row in (profiles_by_sender[transaction["sender"]], daily[(transaction["day"], transaction["sender"])]):
            row["zero_value_transactions" if value == 0 else "nonzero_value_transactions"] += 1
            row["attempted_value_wei"] += value
            if transaction["status"] == 1:
                row["successful_value_wei"] += value
    for row in [*profiles, *result["sender_daily"]]:
        row["attempted_value_wei"] = str(row["attempted_value_wei"])
        row["successful_value_wei"] = str(row["successful_value_wei"])
        row["failure_rate"] = row["failed_transactions"] / row["transactions"] if row["transactions"] else None
    for row in profiles:
        row.update({key: value for key, value in gaps[row["sender"]].items() if key != "sender"})
    result["sender_profiles"] = profiles
    result["sender_overlap"] = list(rows(connection, """WITH targets AS (
        SELECT DISTINCT sender,recipient FROM attribution_sender_activity WHERE recipient IS NOT NULL),
        sizes AS (SELECT sender,count(*) AS target_count FROM targets GROUP BY sender),
        shared AS (SELECT left_side.sender AS sender_a,right_side.sender AS sender_b,count(*) AS common
            FROM targets left_side JOIN targets right_side ON left_side.recipient=right_side.recipient
                AND left_side.sender<right_side.sender GROUP BY left_side.sender,right_side.sender)
        SELECT first.sender AS sender_a,second.sender AS sender_b,
            coalesce(common,0) AS shared_recipients,
            coalesce(first_size.target_count,0)+coalesce(second_size.target_count,0)-coalesce(common,0) AS union_recipients
        FROM attribution_senders first JOIN attribution_senders second ON first.sender<second.sender
        LEFT JOIN sizes first_size ON first.sender=first_size.sender
        LEFT JOIN sizes second_size ON second.sender=second_size.sender
        LEFT JOIN shared ON first.sender=shared.sender_a AND second.sender=shared.sender_b
        ORDER BY sender_a,sender_b"""))
    for row in result["sender_overlap"]:
        row["jaccard"] = row["shared_recipients"] / row["union_recipients"] if row["union_recipients"] else None


def failures(connection, result):
    connection.execute("""CREATE TEMP TABLE attribution_failure_methods AS
        SELECT recipient,method_selector AS selector,count(*) AS transactions,
            count(*) FILTER (WHERE status=0) AS failed_transactions
        FROM metric_activity WHERE NOT is_system GROUP BY recipient,method_selector
        HAVING count(*) FILTER (WHERE status=0)>0
        ORDER BY failed_transactions DESC,recipient NULLS LAST,selector NULLS LAST LIMIT 20""")
    result["failure_methods"] = list(rows(connection, "SELECT * FROM attribution_failure_methods"))
    total = connection.execute("SELECT count(*) FROM metric_activity WHERE NOT is_system AND status=0").fetchone()[0]
    for row in result["failure_methods"]:
        row["failure_rate"] = row["failed_transactions"] / row["transactions"]
        row["all_failed_transactions"] = total
        row["share_of_failures"] = row["failed_transactions"] / total
    result["failure_samples"] = list(rows(connection, """WITH ranked AS (
        SELECT activity.*,row_number() OVER (PARTITION BY activity.recipient,activity.method_selector
            ORDER BY block_number,transaction_index,hash) AS sample_rank
        FROM metric_activity activity JOIN attribution_failure_methods methods
            ON activity.recipient IS NOT DISTINCT FROM methods.recipient
            AND activity.method_selector IS NOT DISTINCT FROM methods.selector
        WHERE NOT activity.is_system AND status=0)
        SELECT recipient,method_selector AS selector,hash AS transaction_hash,block_number,transaction_index,
            timestamp,day,sender,status,value_wei,sample_rank,'not_collected' AS revert_evidence
        FROM ranked WHERE sample_rank<=3 ORDER BY recipient NULLS LAST,selector NULLS LAST,sample_rank"""))


def augment_attribution(connection, result):
    daily_panels(connection, result)
    sender_panels(connection, result)
    failures(connection, result)
