import csv


COUNTS = ["transactions", "system_transactions", "non_system_transactions", "successful_non_system_transactions",
          "failed_non_system_transactions"]
RATES = ["non_system_failure_rate", "system_share"]
FEE_FIELDS = [field for component in ("l1", "l2", "total") for field in (
    f"known_{component}_fee_transactions", f"missing_{component}_fee_transactions", f"{component}_fee_coverage",
    f"known_{component}_fee_wei", f"known_{component}_fee_bnb")]
PERIOD_FIELDS = ["period_start", "active_senders", "all_non_system_senders", "observed_days", "expected_days", "is_partial"]
ACTIVITY_FIELDS = ["transactions", "successful_transactions", "failed_transactions", "active_senders"]
SENDER_FIELDS = ["transactions", "successful_transactions", "failed_transactions", "empty_calldata_transactions",
    "unique_recipients", "successful_active_days", "zero_value_transactions", "nonzero_value_transactions",
    "attempted_value_wei", "successful_value_wei", "failure_rate"]
TABLE_FIELDS = {
    "daily_projects": ["day", "project", *ACTIVITY_FIELDS],
    "daily_recipients": ["day", "recipient_group", "recipient", *ACTIVITY_FIELDS],
    "daily_changes": ["day", "dimension", "project", "recipient_group", "recipient", "transactions",
        "previous_transactions", "delta_transactions", "growth_rate", "contribution_rank"],
    "sender_profiles": ["sender", *SENDER_FIELDS, "first_timestamp", "last_timestamp", "min_gap_seconds",
        "max_gap_seconds", "mean_gap_seconds", "median_gap_seconds"],
    "sender_daily": ["day", "sender", *SENDER_FIELDS],
    "sender_overlap": ["sender_a", "sender_b", "shared_recipients", "union_recipients", "jaccard"],
    "failure_methods": ["recipient", "selector", "transactions", "failed_transactions", "failure_rate",
        "all_failed_transactions", "share_of_failures"],
    "failure_samples": ["recipient", "selector", "transaction_hash", "block_number", "transaction_index",
        "timestamp", "day", "sender", "status", "value_wei", "sample_rank", "revert_evidence"],
    "daily": ["day", "transactions", "successful_transactions", "failed_transactions", "system_transactions",
        "active_senders", "all_non_system_senders", "native_value_wei", "native_value_bnb", "known_total_fee_wei",
        "partial_l2_fee_wei", "unknown_total_fee_transactions", "unknown_l2_fee_transactions", "full_fee_coverage",
        "l2_fee_coverage", *COUNTS[2:], *RATES],
    "contracts": ["address", "project", "role", "category", "transactions", "successful_transactions", "successful_senders"],
    "projects": ["project", "transactions", "successful_transactions", "failed_transactions", "successful_senders", "direct_recipients"],
    "senders": ["sender", "transactions", "successful_transactions", "failed_transactions", "active_days"],
    "weekly": PERIOD_FIELDS,
    "monthly": PERIOD_FIELDS,
    "retention": ["cohort_day", "lag_days", "cohort_size", "retained", "rate"],
    "transaction_types": ["transaction_type", "transactions", "total_transactions", "share"],
    "methods": ["recipient", "selector", "transactions", "successful_transactions", "failed_transactions",
        "total_transactions", "share", "signature_candidates", "signature_evidence"],
    "hourly": ["hour_utc", "hour", "day", *COUNTS, "active_senders", "all_non_system_senders", *RATES],
    "fee_groups": ["fee_group", "transactions", *FEE_FIELDS],
    "recipient_concentration": ["top_n", "transactions", "share"],
    "sender_concentration": ["top_n", "transactions", "share"],
    "sender_breakdown": ["sender", "dimension", "key", "transactions", "total_transactions", "share"],
    "recipient_details": ["address", "transactions", "historical_kind", "code_observation_scope", "project", "category", "role", "source_url",
        "confidence", "temporal_scope", "label_scope", "sample_first_block", "sample_first_block_hash", "sample_first_kind",
        "sample_first_status", "sample_first_code_hash", "sample_last_block", "sample_last_block_hash", "sample_last_kind",
        "sample_last_status", "sample_last_code_hash", "current_contract_name", "abi_status"],
    "token_metadata": ["address", "log_count", "transactions", "symbol", "decimals", "verification", "source_url", "confidence",
        "acquired_date", "source_timestamp", "observed_block", "observed_block_hash", "observation_status", "raw_amount", "scaled_amount"],
    "unresolved": ["address", "transactions", "reason", "status", "explorer_url"],
    "events": ["address", "topic0", "log_count", "transactions", "eligible_log_count", "share", "signature_candidates", "signature_evidence"],
}
TEXT_FIELDS = {"day", "address", "project", "role", "category", "sender", "period_start", "cohort_day",
    "recipient_group", "sender_a", "sender_b", "transaction_hash", "revert_evidence", "code_observation_scope",
    "transaction_type", "recipient", "selector", "signature_candidates", "signature_evidence", "hour_utc", "hour",
    "fee_group", "dimension", "key", "historical_kind", "source_url", "confidence", "temporal_scope", "label_scope",
    "sample_first_block_hash", "sample_first_kind", "sample_first_status", "sample_first_code_hash", "sample_last_block_hash",
    "sample_last_kind", "sample_last_status", "sample_last_code_hash", "current_contract_name", "abi_status", "symbol",
    "verification", "acquired_date", "source_timestamp", "observed_block_hash", "observation_status", "reason", "status",
    "explorer_url", "topic0"}


def spreadsheet_value(field, value):
    if field in TEXT_FIELDS and isinstance(value, str) and value and (
            value[0] in "\t\r\n" or value.lstrip().startswith(("=", "+", "-", "@"))):
        return "'" + value
    return value


def export_tables(destination, result):
    for name, defaults in TABLE_FIELDS.items():
        rows = result.get(name, [])
        fields = list(dict.fromkeys([*defaults, *(field for row in rows for field in row)]))
        with (destination / (name + ".csv")).open("w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows({field: spreadsheet_value(field, value) for field, value in row.items()} for row in rows)
