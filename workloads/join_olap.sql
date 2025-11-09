EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
SELECT 
    a.education,
    COUNT(*) AS match_count,
    AVG(a.age) AS avg_age
FROM {table_name} a
JOIN {table_name} b ON a.occupation = b.occupation
WHERE a.age > 30 AND b.age > 30
GROUP BY a.education
LIMIT 100;