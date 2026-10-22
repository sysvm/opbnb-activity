SELECT day,
       count(*) AS transactions,
       count(*) FILTER (WHERE status=1) AS successful_transactions,
       count(*) FILTER (WHERE status=0) AS failed_transactions,
       count(*) FILTER (WHERE is_system) AS system_transactions,
       count(DISTINCT sender) FILTER (WHERE status=1 AND NOT is_system) AS active_senders,
       count(DISTINCT sender) FILTER (WHERE NOT is_system) AS all_non_system_senders
FROM activity GROUP BY day ORDER BY day
