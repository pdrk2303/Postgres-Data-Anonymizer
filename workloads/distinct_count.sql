
SELECT education, COUNT(DISTINCT occupation) as unique_occupations
FROM adult_raw
GROUP BY education;

SELECT hospital, COUNT(DISTINCT doctor) as doctor_count
FROM healthcare_raw
GROUP BY hospital
ORDER BY doctor_count DESC;