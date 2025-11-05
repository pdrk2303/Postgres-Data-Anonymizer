-- workloads/pgbench_custom.sql

\setrandom id 1 1000000
-- read aggregate
SELECT state, COUNT(*) FROM {TABLE} WHERE age BETWEEN :id % 60 + 18 AND :id % 60 + 40 GROUP BY state;
-- point select
SELECT * FROM {TABLE} WHERE person_id = :id;
-- simulated update
SELECT 1 FROM {TABLE} WHERE person_id = :id;
