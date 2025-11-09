
SELECT occupation, 
       COUNT(*) as frequency,
       AVG(age) as avg_age,
       AVG(hours_per_week) as avg_hours
FROM adult_raw
WHERE occupation IS NOT NULL AND occupation != '?'
GROUP BY occupation
ORDER BY frequency DESC
LIMIT 10;

SELECT medication,
       COUNT(*) as prescription_count,
       AVG(billing_amount) as avg_cost,
       MIN(billing_amount) as min_cost,
       MAX(billing_amount) as max_cost
FROM healthcare_raw
WHERE medication IS NOT NULL
GROUP BY medication
ORDER BY prescription_count DESC
LIMIT 15;