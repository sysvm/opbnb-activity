import hashlib
import json
from datetime import datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from .activity_metrics import augment
from . import analysis_stages
from .analysis_stages import run_query
from .core_metrics import core_tables, period_tables, prepare_core, top_sender_transactions, unique_sender_counts
from .attribution import augment_attribution
from .decode import decimal_units, decode_transfer
from .enrichment import augment_result
from .labels import LabelIndex
from .registry import prepare_inputs
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
    quality = validate(store)
    if not quality["ok"]:
        raise ValueError("quality checks failed; incomplete data cannot produce final analytics")
    with analysis_stages.AnalysisWorkspace(store) as connection:
        return _analyze(connection, store, labels, assets, quality, save)


def _analyze(connection, store, labels, assets, quality, save):
    analysis_stages.begin_stage('prepare_inputs')
    labels, assets, provenance = prepare_inputs(store, labels, assets)
    index = LabelIndex(labels)
    analysis_stages.prepare_activity(connection, store, index.rows)
    prepare_core(connection)
    result = core_tables(connection)
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
    result.update(period_tables(connection))
    summary = next(records(run_query(connection, 'summary_counts', """SELECT count(*) AS transactions,
        count(*) FILTER (WHERE is_system) AS system_transactions,
        count(*) FILTER (WHERE status=1 AND NOT is_system) AS successful_non_system_transactions,
        count(*) FILTER (WHERE status=0 AND NOT is_system) AS failed_non_system_transactions,
        count(*) FILTER (WHERE NOT is_system AND project='Unknown') AS unknown_project_transactions,
        count(*) FILTER (WHERE NOT is_system AND recipient_kind='unknown') AS unknown_recipient_kind_transactions,
        count(*) FILTER (WHERE total_fee_wei IS NULL) AS unknown_total_fee_transactions,
        count(*) FILTER (WHERE l2_fee_wei IS NULL) AS unknown_l2_fee_transactions
        FROM activity""")))
    summary.update(unique_sender_counts(connection))
    native_value, full_fee, partial_l2 = 0, 0, 0
    money_by_day = {row["day"]: {"native": 0, "full_fee": 0, "l2_fee": 0, "missing_full": 0, "missing_l2": 0} for row in result["daily"]}
    for row in records(run_query(connection, 'native_and_fees', "SELECT day,value_wei,status,is_system,total_fee_wei,l2_fee_wei FROM activity")):
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
    for row in records(run_query(connection, 'token_transfers', "SELECT address,topics,data FROM eligible_logs")):
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
    summary["project_top1_share"] = project_counts[0] / non_system if non_system and project_counts else None
    summary["project_top5_share"] = sum(project_counts[:5]) / non_system if non_system and project_counts else None
    summary["project_top10_share"] = sum(project_counts[:10]) / non_system if non_system and project_counts else None
    summary["project_hhi"] = sum((count / non_system) ** 2 for count in project_counts) if non_system and project_counts else None
    top_sender_count = (summary["unique_successful_senders"] + 99) // 100
    top_transactions = top_sender_transactions(connection, top_sender_count)
    summary["top1_percent_sender_count"] = top_sender_count
    summary["top1_percent_sender_successful_tx_share"] = top_transactions / summary["successful_non_system_transactions"] if summary["successful_non_system_transactions"] else None
    result["summary"] = summary
    result["quality"] = quality
    result["metadata"] = {"run_id": store.config.run_id, "synthetic": store.manifest["synthetic"],
                          "timezone": store.config.timezone, "start_utc": store.manifest["start_utc"], "end_utc": store.manifest["end_utc"],
                          "dataset_version": hashlib.sha256("".join(digest(path / "_partition.json") for path in store.partition_dirs()).encode()).hexdigest(),
                          "labels_version": index.version, "assets_version": hashlib.sha256(json.dumps(assets, sort_keys=True).encode()).hexdigest(),
                          "dataset": ".", "identity": store.config.identity, "analysis_engine": "disk_staged_v1"}
    augment(connection, result, store)
    augment_attribution(connection, result)
    augment_result(connection, result, store, dict(provenance, labels=labels), assets)
    if save:
        analysis_stages.begin_stage('save_analysis')
        atomic_json(store.root / "analysis.json", result)
    return result
