SELECT project, count(*) AS transactions,
       count(*) FILTER (WHERE status=1) AS successful_transactions,
       count(*) FILTER (WHERE status=0) AS failed_transactions,
       count(DISTINCT sender) FILTER (WHERE status=1) AS successful_senders,
       count(DISTINCT recipient) AS direct_recipients
FROM activity WHERE NOT is_system
GROUP BY project ORDER BY transactions DESC, project
