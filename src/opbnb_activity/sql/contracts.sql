SELECT coalesce(recipient, '(contract creation)') AS address,
       project, role, category,
       count(*) AS transactions,
       count(*) FILTER (WHERE status=1) AS successful_transactions,
       count(DISTINCT sender) FILTER (WHERE status=1) AS successful_senders
FROM activity WHERE NOT is_system
GROUP BY recipient, project, role, category
ORDER BY transactions DESC, address LIMIT 100
