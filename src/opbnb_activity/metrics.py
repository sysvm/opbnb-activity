import hashlib
import json
from datetime import datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from .decode import decimal_units, decode_transfer
from .labels import LabelIndex
from .quality import validate
from .storage import atomic_json, digest


def records(cursor):
    names = [column[0] for column in cursor.description]
    while rows := cursor.fetchmany(4096):
        for row in rows:
            yield dict(zip(names, row))


def sql_table(connection, name):
    query = (Path(__file__).parent / "sql" / (name + ".sql")).read_text()
    return list(records(connection.execute(query)))


def analyze(store, labels, assets, save=True):
    import pyarrow as arrow
    quality = validate(store)
    if not quality["ok"]:
        raise ValueError("quality checks failed; incomplete data cannot produce final analytics")
    index = LabelIndex(labels)
    connection = store.connection()
    label_schema = arrow.schema([("address", arrow.string()), ("project", arrow.string()), ("category", arrow.string()),
                                 ("role", arrow.string()), ("valid_from", arrow.int64()), ("valid_to", arrow.int64())])
    label_rows = [{"address": row["address"], "project": row["project"], "category": row.get("category", "unknown"),
                   "role": row.get("role", "unknown"), "valid_from": row["valid_from"], "valid_to": row["valid_to"]} for row in index.rows]
    connection.register("labels", arrow.Table.from_pylist(label_rows, schema=label_schema))
    zone_name = store.config.timezone.replace("'", "''")
    connection.execute(f"""CREATE VIEW activity AS
        SELECT t.*, r.status, r.l2_fee_wei, r.l1_fee_wei, r.total_fee_wei,
               strftime(to_timestamp(t.timestamp) AT TIME ZONE '{zone_name}', '%Y-%m-%d') AS day,
               coalesce(labels.project,'Unknown') AS project,
               coalesce(labels.category,'unknown') AS category,
               coalesce(labels.role,'unknown') AS role
        FROM transactions t JOIN receipts r ON t.hash=r.transaction_hash
        LEFT JOIN labels ON t.recipient=labels.address AND t.block_number>=labels.valid_from AND t.block_number<labels.valid_to""")
    try:
        result = {name: sql_table(connection, name) for name in ("daily", "contracts", "projects", "senders", "retention")}
        zone = ZoneInfo(store.config.timezone)
        local_start = store.config.start_utc.astimezone(zone)
        local_end = store.config.end_utc.astimezone(zone)
        day_rows = {row["day"]: row for row in result["daily"]}
        day = local_start.date()
        result["daily"] = []
        while datetime.combine(day, time.min, zone) < local_end:
            result["daily"].append(day_rows.get(day.isoformat(), {"day": day.isoformat(), "transactions": 0,
                "successful_transactions": 0, "failed_transactions": 0, "system_transactions": 0,
                "active_senders": 0, "all_non_system_senders": 0}))
            day += timedelta(days=1)
        result["retention"] = [row for row in result["retention"]
                               if datetime.combine(datetime.fromisoformat(row["cohort_day"]).date(), time.min, zone) >= local_start
                               and datetime.combine(datetime.fromisoformat(row["cohort_day"]).date() + timedelta(days=row["lag_days"] + 1), time.min, zone) <= local_end]
        for period in ("week", "month"):
            result[period + "ly"] = list(records(connection.execute(f"""SELECT cast(date_trunc('{period}',day::DATE) AS VARCHAR) AS period_start,
                count(DISTINCT sender) FILTER (WHERE status=1 AND NOT is_system) AS active_senders,
                count(DISTINCT sender) FILTER (WHERE NOT is_system) AS all_non_system_senders
                FROM activity GROUP BY period_start ORDER BY period_start""")))
        summary = next(records(connection.execute("""SELECT count(*) AS transactions,
            count(*) FILTER (WHERE is_system) AS system_transactions,
            count(*) FILTER (WHERE status=1 AND NOT is_system) AS successful_non_system_transactions,
            count(*) FILTER (WHERE status=0 AND NOT is_system) AS failed_non_system_transactions,
            count(DISTINCT sender) FILTER (WHERE status=1 AND NOT is_system) AS unique_successful_senders,
            count(DISTINCT sender) FILTER (WHERE NOT is_system) AS unique_all_non_system_senders,
            count(*) FILTER (WHERE NOT is_system AND project='Unknown') AS unknown_project_transactions,
            count(*) FILTER (WHERE NOT is_system AND recipient_kind='unknown') AS unknown_recipient_kind_transactions,
            count(*) FILTER (WHERE total_fee_wei IS NULL) AS unknown_total_fee_transactions,
            count(*) FILTER (WHERE l2_fee_wei IS NULL) AS unknown_l2_fee_transactions
            FROM activity""")))
        native_value, full_fee, partial_l2 = 0, 0, 0
        money_by_day = {row["day"]: {"native": 0, "full_fee": 0, "l2_fee": 0, "missing_full": 0, "missing_l2": 0} for row in result["daily"]}
        for row in records(connection.execute("SELECT day,value_wei,status,is_system,total_fee_wei,l2_fee_wei FROM activity")):
            daily_money = money_by_day[row["day"]]
            if row["status"] == 1 and not row["is_system"]:
                native_value += int(row["value_wei"])
                daily_money["native"] += int(row["value_wei"])
            if row["total_fee_wei"] is not None:
                full_fee += int(row["total_fee_wei"])
                daily_money["full_fee"] += int(row["total_fee_wei"])
            else:
                daily_money["missing_full"] += 1
            if row["l2_fee_wei"] is not None:
                partial_l2 += int(row["l2_fee_wei"])
                daily_money["l2_fee"] += int(row["l2_fee_wei"])
            else:
                daily_money["missing_l2"] += 1
        for row in result["daily"]:
            daily_money = money_by_day[row["day"]]
            row.update(native_value_wei=str(daily_money["native"]), native_value_bnb=decimal_units(daily_money["native"], 18),
                       known_total_fee_wei=str(daily_money["full_fee"]), partial_l2_fee_wei=str(daily_money["l2_fee"]),
                       unknown_total_fee_transactions=daily_money["missing_full"], unknown_l2_fee_transactions=daily_money["missing_l2"],
                       full_fee_coverage=(row["transactions"] - daily_money["missing_full"]) / row["transactions"] if row["transactions"] else None,
                       l2_fee_coverage=(row["transactions"] - daily_money["missing_l2"]) / row["transactions"] if row["transactions"] else None)
        summary.update(native_value_wei=str(native_value), native_value_bnb=decimal_units(native_value, 18),
                       known_total_fee_wei=str(full_fee), partial_l2_fee_wei=str(partial_l2),
                       verified_token_raw_amounts={}, ambiguous_transfer_logs=0, unverified_transfer_logs=0)
        for row in records(connection.execute("""SELECT logs.address,logs.topics,logs.data FROM logs
             JOIN activity ON logs.transaction_hash=activity.hash WHERE activity.status=1 AND NOT activity.is_system""")):
            decoded = decode_transfer(row, assets)
            if decoded["kind"] == "verified_erc20":
                address = decoded["address"]
                summary["verified_token_raw_amounts"][address] = str(int(summary["verified_token_raw_amounts"].get(address, "0")) + int(decoded["raw_amount"]))
            elif decoded["kind"] == "nft_or_ambiguous":
                summary["ambiguous_transfer_logs"] += 1
            elif decoded["kind"] == "unverified_transfer":
                summary["unverified_transfer_logs"] += 1
        summary["verified_token_amounts"] = {address: decimal_units(raw, assets[address]["decimals"]) for address, raw in summary["verified_token_raw_amounts"].items()}
        non_system = summary["successful_non_system_transactions"] + summary["failed_non_system_transactions"]
        summary["known_project_coverage"] = (non_system - summary["unknown_project_transactions"]) / non_system if non_system else None
        summary["full_fee_coverage"] = (summary["transactions"] - summary["unknown_total_fee_transactions"]) / summary["transactions"] if summary["transactions"] else None
        summary["l2_fee_coverage"] = (summary["transactions"] - summary["unknown_l2_fee_transactions"]) / summary["transactions"] if summary["transactions"] else None
        project_counts = [row["transactions"] for row in result["projects"] if row["project"] != "Unknown"]
        summary["unknown_project_share"] = summary["unknown_project_transactions"] / non_system if non_system else None
        summary["project_top1_share"] = (project_counts[0] if project_counts else 0) / non_system if non_system else None
        summary["project_top5_share"] = sum(project_counts[:5]) / non_system if non_system else None
        summary["project_top10_share"] = sum(project_counts[:10]) / non_system if non_system else None
        summary["project_hhi"] = sum((count / non_system) ** 2 for count in project_counts) if non_system else None
        top_sender_count = (summary["unique_successful_senders"] + 99) // 100
        top_sender_transactions = connection.execute("""SELECT coalesce(sum(tx_count),0) FROM
            (SELECT count(*) AS tx_count FROM activity WHERE status=1 AND NOT is_system
             GROUP BY sender ORDER BY tx_count DESC, sender LIMIT ?)""", [top_sender_count]).fetchone()[0]
        summary["top1_percent_sender_count"] = top_sender_count
        summary["top1_percent_sender_successful_tx_share"] = top_sender_transactions / summary["successful_non_system_transactions"] if summary["successful_non_system_transactions"] else None
        result["summary"] = summary
        result["quality"] = quality
        result["metadata"] = {"run_id": store.config.run_id, "synthetic": store.manifest["synthetic"],
                              "timezone": store.config.timezone, "start_utc": store.manifest["start_utc"], "end_utc": store.manifest["end_utc"],
                              "dataset_version": hashlib.sha256("".join(digest(path / "_partition.json") for path in store.partition_dirs()).encode()).hexdigest(),
                              "labels_version": index.version, "assets_version": hashlib.sha256(json.dumps(assets, sort_keys=True).encode()).hexdigest(),
                              "dataset": ".", "identity": store.config.identity}
        if save:
            atomic_json(store.root / "analysis.json", result)
        return result
    finally:
        connection.close()
