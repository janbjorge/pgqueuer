SELECT COUNT(*) AS unaggregated
FROM pgqueuer_log
WHERE NOT aggregated
