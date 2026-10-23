from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from .decode import decimal_units


COUNTS = """count(*) AS transactions,
    count(*) FILTER (WHERE is_system) AS system_transactions,
    count(*) FILTER (WHERE NOT is_system) AS non_system_transactions,
    count(*) FILTER (WHERE status=1 AND NOT is_system) AS successful_non_system_transactions,
    count(*) FILTER (WHERE status=0 AND NOT is_system) AS failed_non_system_transactions"""


def rows(connection, query):
    cursor = connection.execute(query)
    names = [column[0] for column in cursor.description]
    while batch := cursor.fetchmany(4096):
        for values in batch:
            yield dict(zip(names, values))


def rates(row):
    row["non_system_failure_rate"] = (row["failed_non_system_transactions"] / row["non_system_transactions"]
                                      if row["non_system_transactions"] else None)
    row["system_share"] = row["system_transactions"] / row["transactions"] if row["transactions"] else None


def fee_groups(connection):
    groups = {name: {"fee_group": name, "transactions": 0} for name in ("system", "user_deposit", "regular")}
    totals = {name: {component: 0 for component in ("l1", "l2", "total")} for name in groups}
    for group in groups.values():
        for component in ("l1", "l2", "total"):
            group[f"known_{component}_fee_transactions"] = 0
    for row in rows(connection, "SELECT is_system,type,l1_fee_wei,l2_fee_wei,total_fee_wei FROM activity"):
        name = "system" if row["is_system"] else "user_deposit" if row["type"] == 126 else "regular"
        group = groups[name]
        group["transactions"] += 1
        for component in ("l1", "l2", "total"):
            amount = row[f"{component}_fee_wei"]
            if amount is not None:
                totals[name][component] += int(amount)
                group[f"known_{component}_fee_transactions"] += 1
    for name, group in groups.items():
        for component in ("l1", "l2", "total"):
            known = group[f"known_{component}_fee_transactions"]
            group[f"missing_{component}_fee_transactions"] = group["transactions"] - known
            group[f"{component}_fee_coverage"] = known / group["transactions"] if group["transactions"] else None
            group[f"known_{component}_fee_wei"] = str(totals[name][component]) if known else None
            group[f"known_{component}_fee_bnb"] = decimal_units(totals[name][component], 18) if known else None
    return list(groups.values())


def concentration(connection, dimension, total):
    counts = [row["transactions"] for row in rows(connection, f"""SELECT count(*) AS transactions
        FROM activity WHERE NOT is_system AND {dimension} IS NOT NULL GROUP BY {dimension}
        ORDER BY transactions DESC, {dimension} LIMIT 20""")]
    return [{"top_n": top, "transactions": sum(counts[:top]),
             "share": sum(counts[:top]) / total if total else None} for top in (1, 5, 10, 20)]


def sender_breakdown(connection):
    breakdown = list(rows(connection, """WITH top_senders AS (
        SELECT sender,count(*) AS total_transactions FROM metric_activity WHERE NOT is_system
        GROUP BY sender ORDER BY total_transactions DESC,sender LIMIT 20), sender_activity AS (
        SELECT metric_activity.* FROM metric_activity JOIN top_senders USING (sender)
        WHERE NOT is_system), dimensions AS (
        SELECT sender,'recipient' AS dimension,recipient AS key,count(*) AS transactions
        FROM sender_activity GROUP BY sender,recipient
        UNION ALL
        SELECT sender,'selector',method_selector,count(*) FROM sender_activity
        GROUP BY sender,method_selector
        UNION ALL
        SELECT sender,'hour',cast(hour_epoch AS VARCHAR),count(*) FROM sender_activity
        GROUP BY sender,hour_epoch), ranked AS (
        SELECT dimensions.*,top_senders.total_transactions,
        row_number() OVER (PARTITION BY dimensions.sender,dimension ORDER BY transactions DESC,key NULLS LAST) AS rank
        FROM dimensions JOIN top_senders USING (sender))
        SELECT sender,dimension,key,transactions,total_transactions,
        transactions::DOUBLE / total_transactions AS share FROM ranked WHERE rank<=20
        ORDER BY sender,dimension,rank"""))
    for row in breakdown:
        if row["dimension"] == "hour":
            row["key"] = datetime.fromtimestamp(int(row["key"]), timezone.utc).isoformat()
    return breakdown


def periods(result, store):
    zone = ZoneInfo(store.config.timezone)
    start = datetime.fromisoformat(store.manifest["start_utc"]).astimezone(zone)
    end = datetime.fromisoformat(store.manifest["end_utc"]).astimezone(zone)
    for kind in ("week", "month"):
        name = kind + "ly"
        existing = {row["period_start"]: row for row in result[name]}
        first = start.date() - timedelta(days=start.weekday()) if kind == "week" else start.date().replace(day=1)
        period_start = datetime.combine(first, time.min, zone)
        result[name] = []
        while period_start < end:
            if kind == "week":
                period_end = period_start + timedelta(days=7)
            else:
                period_end = (period_start.replace(day=28) + timedelta(days=4)).replace(day=1)
            key = period_start.date().isoformat()
            row = existing.get(key, {"period_start": key, "active_senders": 0, "all_non_system_senders": 0})
            covered_start, covered_end = max(start, period_start), min(end, period_end)
            row.update(observed_days=((covered_end - timedelta(microseconds=1)).date() - covered_start.date()).days + 1,
                       expected_days=(period_end.date() - period_start.date()).days,
                       is_partial=start > period_start or end < period_end)
            result[name].append(row)
            period_start = period_end


def augment(connection, result, store):
    connection.execute("""CREATE TEMP VIEW metric_activity AS SELECT *,
        CASE WHEN is_system THEN 'system_l1_attributes' WHEN type=126 THEN 'user_deposit'
             WHEN recipient IS NULL THEN 'contract_creation'
             WHEN input='0x' THEN 'empty_calldata'
             WHEN NOT regexp_full_match(input,'0x([a-fA-F0-9]{2})*') OR input IS NULL THEN 'unknown'
             WHEN length(input)>=10 THEN 'selector_call' ELSE 'short_calldata' END AS transaction_type,
        CASE WHEN input='0x' THEN '0x'
             WHEN length(input)>=10 AND regexp_full_match(input,'0x([a-fA-F0-9]{2})*')
             THEN lower(substr(input,1,10)) ELSE NULL END AS method_selector,
        floor(timestamp/3600)::BIGINT*3600 AS hour_epoch FROM activity""")
    daily = {row["day"]: row for row in rows(connection, f"SELECT day,{COUNTS} FROM activity GROUP BY day")}
    empty = {"non_system_transactions": 0, "successful_non_system_transactions": 0, "failed_non_system_transactions": 0}
    for row in result["daily"]:
        row.update(daily.get(row["day"], empty))
        rates(row)
    summary = result["summary"]
    summary.update(next(rows(connection, f"SELECT {COUNTS} FROM activity")))
    rates(summary)
    total = summary["transactions"]
    non_system = summary["non_system_transactions"]
    result["transaction_types"] = list(rows(connection, """SELECT transaction_type,count(*) AS transactions
        FROM metric_activity GROUP BY transaction_type ORDER BY transactions DESC,transaction_type"""))
    for row in result["transaction_types"]:
        row.update(total_transactions=total, share=row["transactions"] / total if total else None)
    result["methods"] = list(rows(connection, """SELECT recipient,method_selector AS selector,count(*) AS transactions,
        count(*) FILTER (WHERE status=1) AS successful_transactions,
        count(*) FILTER (WHERE status=0) AS failed_transactions
        FROM metric_activity WHERE NOT is_system GROUP BY recipient,method_selector
        ORDER BY transactions DESC,recipient NULLS LAST,selector NULLS LAST LIMIT 100"""))
    for row in result["methods"]:
        row.update(total_transactions=non_system, share=row["transactions"] / non_system if non_system else None)
    result["hourly"] = list(rows(connection, f"""SELECT hour_epoch,{COUNTS},
        count(DISTINCT sender) FILTER (WHERE status=1 AND NOT is_system) AS active_senders,
        count(DISTINCT sender) FILTER (WHERE NOT is_system) AS all_non_system_senders
        FROM metric_activity GROUP BY hour_epoch ORDER BY hour_epoch"""))
    zone = ZoneInfo(store.config.timezone)
    for row in result["hourly"]:
        hour = datetime.fromtimestamp(row.pop("hour_epoch"), timezone.utc)
        row.update(hour_utc=hour.isoformat(), hour=hour.astimezone(zone).isoformat(), day=hour.astimezone(zone).date().isoformat())
        rates(row)
    result["fee_groups"] = fee_groups(connection)
    result["recipient_concentration"] = concentration(connection, "recipient", non_system)
    result["sender_concentration"] = concentration(connection, "sender", non_system)
    result["sender_breakdown"] = sender_breakdown(connection)
    periods(result, store)
