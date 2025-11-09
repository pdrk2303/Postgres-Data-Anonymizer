-- View-based masking (Baseline B)
DROP VIEW IF EXISTS adult_masked_view CASCADE;
CREATE OR REPLACE VIEW adult_masked_view AS
SELECT 
    id,
    (age / 5) * 5 AS age,
    workclass,
    encode(digest(fnlwgt::text, 'sha256'), 'hex') AS fnlwgt,
    CASE 
        WHEN education IN ('Preschool', '1st-4th', '5th-6th', '7th-8th', '9th', '10th', '11th', '12th')
            THEN 'Some-School'
        WHEN education IN ('HS-grad', 'Some-college')
            THEN 'HS-Some-College'
        ELSE education
    END AS education,
    education_num,
    marital_status,
    CASE 
        WHEN occupation IS NULL THEN NULL
        ELSE substring(occupation, 1, 3) || '***'
    END AS occupation,
    relationship,
    race,
    sex,
    capital_gain,
    capital_loss,
    hours_per_week,
    CASE 
        WHEN native_country = 'United-States' THEN 'United-States'
        ELSE 'Other'
    END AS native_country,
    income
FROM adult_raw_100000;

DROP MATERIALIZED VIEW IF EXISTS adult_masked_matview CASCADE;
CREATE MATERIALIZED VIEW adult_masked_matview AS
SELECT * FROM adult_masked_view;

CREATE INDEX idx_matview_age ON adult_masked_matview(age);

DROP VIEW IF EXISTS healthcare_view_masked CASCADE;
CREATE VIEW healthcare_view_masked AS
SELECT 
    id,
    substring(name, 1, 2) || '***' AS name,
    (age / 5) * 5 AS age,
    gender,
    blood_type,
    medical_condition,
    date_of_admission,
    encode(digest(doctor, 'sha256'), 'hex') AS doctor,
    hospital,
    insurance_provider,
    round(billing_amount / 100) * 100 AS billing_amount,
    NULL AS room_number,
    admission_type,
    discharge_date,
    medication,
    test_results
FROM healthcare_raw_100000;

DROP MATERIALIZED VIEW IF EXISTS healthcare_masked_matview CASCADE;
CREATE MATERIALIZED VIEW healthcare_masked_matview AS
SELECT * FROM healthcare_view_masked;

GRANT SELECT ON adult_masked_view TO analyst, masked_user;
GRANT SELECT ON healthcare_view_masked TO analyst, masked_user;

SELECT COUNT(*) FROM adult_masked_view;
SELECT COUNT(*) FROM healthcare_view_masked;