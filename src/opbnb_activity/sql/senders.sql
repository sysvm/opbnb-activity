SELECT sender, count(*) AS transactions,
       count(*) FILTER (WHERE status=1) AS successful_transactions,
       count(*) FILTER (WHERE status=0) AS failed_transactions,
       count(DISTINCT day) FILTER (WHERE status=1) AS active_days
FROM activity WHERE NOT is_system
GROUP BY sender ORDER BY transactions DESC, sender LIMIT 100
