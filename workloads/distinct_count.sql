-- Workload 4: Distinct Count
-- Tests: Impact of masking on cardinality estimation, HashAggregate performance

-- Adult: Unique occupations by education level
SELECT education, COUNT(DISTINCT occupation) as unique_occupations
FROM adult_raw
GROUP BY education;

-- Healthcare: Unique doctors per hospital
SELECT hospital, COUNT(DISTINCT doctor) as doctor_count
FROM healthcare_raw
GROUP BY hospital
ORDER BY doctor_count DESC;