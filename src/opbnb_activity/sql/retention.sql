WITH first_seen AS (
    SELECT sender, min(day) AS cohort_day FROM activity
    WHERE status=1 AND NOT is_system GROUP BY sender
), active AS (
    SELECT DISTINCT sender, day FROM activity WHERE status=1 AND NOT is_system
), lags AS (SELECT unnest([1, 7]) AS lag_days)
SELECT first_seen.cohort_day, lags.lag_days, count(*) AS cohort_size,
       count(active.sender) AS retained,
       count(active.sender)::DOUBLE / count(*) AS rate
FROM first_seen CROSS JOIN lags LEFT JOIN active
ON first_seen.sender=active.sender
AND active.day::DATE=first_seen.cohort_day::DATE + lags.lag_days::INTEGER
GROUP BY first_seen.cohort_day, lags.lag_days
ORDER BY first_seen.cohort_day, lags.lag_days
