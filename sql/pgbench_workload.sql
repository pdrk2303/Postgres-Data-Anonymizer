
\set id random(1, 100000)
\set age1 random(20, 40)
\set age2 random(40, 60)

SELECT * FROM adult_raw WHERE id = :id;

SELECT education, COUNT(*) FROM adult_raw WHERE age BETWEEN :age1 AND :age2 GROUP BY education LIMIT 5;

SELECT AVG(hours_per_week) FROM adult_raw WHERE age > :age1;