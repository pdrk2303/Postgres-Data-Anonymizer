
SELECT education, income, COUNT(*) as cnt, AVG(age) as avg_age
FROM adult_raw 
GROUP BY education, income 
ORDER BY cnt DESC 
LIMIT 20;

SELECT medical_condition, gender, COUNT(*) as patient_count, AVG(billing_amount) as avg_billing
FROM healthcare_raw
GROUP BY medical_condition, gender
ORDER BY patient_count DESC
LIMIT 20;