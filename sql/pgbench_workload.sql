-- pgbench custom workload for concurrency testing
-- Tests read-heavy OLAP workload under concurrent load

\set id random(1, 100000)
\set age1 random(20, 40)
\set age2 random(40, 60)

-- 60% - Point lookups
SELECT * FROM adult_raw WHERE id = :id;

-- 30% - Aggregates
SELECT education, COUNT(*) FROM adult_raw WHERE age BETWEEN :age1 AND :age2 GROUP BY education LIMIT 5;

-- 10% - Range scans
SELECT AVG(hours_per_week) FROM adult_raw WHERE age > :age1;