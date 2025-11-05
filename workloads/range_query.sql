-- Workload 2: Range Query with Aggregation
-- Tests: Sequential scan, aggregation performance, masking impact on filtering

-- Adult: Count people in age range
SELECT COUNT(*) FROM adult_raw WHERE age BETWEEN 25 AND 45;

-- Healthcare: Average billing in age range
SELECT AVG(billing_amount) FROM healthcare_raw WHERE age BETWEEN 30 AND 50;