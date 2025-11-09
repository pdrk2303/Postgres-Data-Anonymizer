
\setrandom id 1 1000000
SELECT state, COUNT(*) FROM {TABLE} WHERE age BETWEEN :id % 60 + 18 AND :id % 60 + 40 GROUP BY state;
SELECT * FROM {TABLE} WHERE person_id = :id;
SELECT 1 FROM {TABLE} WHERE person_id = :id;
