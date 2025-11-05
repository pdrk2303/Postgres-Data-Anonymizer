-- Baseline A: Native Postgres tables (no masking)
-- These are your ground truth tables

-- Adult Census table
DROP TABLE IF EXISTS adult_raw CASCADE;
CREATE TABLE adult_raw (
    id SERIAL PRIMARY KEY,
    age INT,
    workclass TEXT,
    fnlwgt INT,
    education TEXT,
    education_num INT,
    marital_status TEXT,
    occupation TEXT,
    relationship TEXT,
    race TEXT,
    sex TEXT,
    capital_gain INT,
    capital_loss INT,
    hours_per_week INT,
    native_country TEXT,
    income TEXT
);

-- Healthcare table
DROP TABLE IF EXISTS healthcare_raw CASCADE;
CREATE TABLE healthcare_raw (
    id SERIAL PRIMARY KEY,
    name TEXT,
    age INT,
    gender TEXT,
    blood_type TEXT,
    medical_condition TEXT,
    date_of_admission DATE,
    doctor TEXT,
    hospital TEXT,
    insurance_provider TEXT,
    billing_amount NUMERIC(10,2),
    room_number TEXT,
    admission_type TEXT,
    discharge_date DATE,
    medication TEXT,
    test_results TEXT
);

-- Grant permissions
GRANT SELECT ON adult_raw TO analyst, masked_user;
GRANT SELECT ON healthcare_raw TO analyst, masked_user;

-- Create indexes for baseline performance
CREATE INDEX idx_adult_age ON adult_raw(age);
CREATE INDEX idx_adult_education ON adult_raw(education);
CREATE INDEX idx_healthcare_age ON healthcare_raw(age);
CREATE INDEX idx_healthcare_condition ON healthcare_raw(medical_condition);

VACUUM ANALYZE adult_raw;
VACUUM ANALYZE healthcare_raw;