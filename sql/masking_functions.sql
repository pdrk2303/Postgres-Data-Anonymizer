-- Create tables with different masking functions
-- This allows comparing performance and characteristics of different masking approaches

-- Test dataset (sample from adult_raw_100000)
DROP TABLE IF EXISTS mask_test_data;
CREATE TABLE mask_test_data AS 
SELECT * FROM adult_raw_100000 LIMIT 50000;

-- Function 1: Deterministic Hash (HMAC-SHA256)
DROP TABLE IF EXISTS adult_mask_hash;
CREATE TABLE adult_mask_hash AS
SELECT 
    id,
    age,
    workclass,
    fnlwgt,
    education,
    education_num,
    marital_status,
    -- Hash-based masking (deterministic)
    encode(hmac(occupation, 'secret_key_123', 'sha256'), 'hex') AS occupation,
    relationship,
    race,
    sex,
    capital_gain,
    capital_loss,
    hours_per_week,
    encode(hmac(native_country, 'secret_key_123', 'sha256'), 'hex') AS native_country,
    income
FROM mask_test_data;

-- Function 2: Partial Masking (substring + stars)
DROP TABLE IF EXISTS adult_mask_partial;
CREATE TABLE adult_mask_partial AS
SELECT 
    id,
    age,
    workclass,
    fnlwgt,
    education,
    education_num,
    marital_status,
    -- Partial masking: first 2 chars + ***
    CASE 
        WHEN occupation IS NOT NULL AND occupation != '?' 
        THEN substring(occupation, 1, 2) || '***'
        ELSE occupation
    END AS occupation,
    relationship,
    race,
    sex,
    capital_gain,
    capital_loss,
    hours_per_week,
    CASE 
        WHEN native_country IS NOT NULL 
        THEN substring(native_country, 1, 2) || '***'
        ELSE native_country
    END AS native_country,
    income
FROM mask_test_data;

-- Function 3: Shuffle (column permutation)
DROP TABLE IF EXISTS adult_mask_shuffle;
CREATE TABLE adult_mask_shuffle AS
WITH shuffled AS (
    SELECT 
        id,
        age,
        workclass,
        fnlwgt,
        education,
        education_num,
        marital_status,
        occupation,
        relationship,
        race,
        sex,
        capital_gain,
        capital_loss,
        hours_per_week,
        native_country,
        income,
        -- Generate random ordering
        ROW_NUMBER() OVER (ORDER BY random()) as rn_occupation,
        ROW_NUMBER() OVER (ORDER BY random()) as rn_country
    FROM mask_test_data
),
occupation_map AS (
    SELECT rn_occupation, occupation as shuffled_occupation 
    FROM shuffled
),
country_map AS (
    SELECT rn_country, native_country as shuffled_country
    FROM shuffled
)
SELECT 
    s.id,
    s.age,
    s.workclass,
    s.fnlwgt,
    s.education,
    s.education_num,
    s.marital_status,
    om.shuffled_occupation AS occupation,
    s.relationship,
    s.race,
    s.sex,
    s.capital_gain,
    s.capital_loss,
    s.hours_per_week,
    cm.shuffled_country AS native_country,
    s.income
FROM shuffled s
JOIN occupation_map om ON s.rn_occupation = om.rn_occupation
JOIN country_map cm ON s.rn_country = cm.rn_country;

-- Function 4: Anon extension fake_* functions
DROP TABLE IF EXISTS adult_mask_anon_fake;
CREATE TABLE adult_mask_anon_fake AS
SELECT * FROM mask_test_data;

-- Apply anon masking rules
SECURITY LABEL FOR anon ON COLUMN adult_mask_anon_fake.occupation
  IS 'MASKED WITH FUNCTION anon.fake_company()';

SECURITY LABEL FOR anon ON COLUMN adult_mask_anon_fake.native_country
  IS 'MASKED WITH FUNCTION anon.fake_country()';

-- Anonymize in place
SELECT anon.anonymize_table('adult_mask_anon_fake');

-- Function 5: Noise injection (Laplace-like)
DROP TABLE IF EXISTS adult_mask_noise;
CREATE TABLE adult_mask_noise AS
SELECT 
    id,
    age,
    workclass,
    fnlwgt,
    education,
    education_num,
    marital_status,
    occupation,
    relationship,
    race,
    sex,
    -- Add noise to numeric columns (±20% range)
    GREATEST(0, capital_gain + (random() - 0.5) * capital_gain * 0.4)::INT AS capital_gain,
    GREATEST(0, capital_loss + (random() - 0.5) * capital_loss * 0.4)::INT AS capital_loss,
    GREATEST(1, hours_per_week + (random() - 0.5) * 10)::INT AS hours_per_week,
    native_country,
    income
FROM mask_test_data;

-- Function 6: Generalization (buckets)
DROP TABLE IF EXISTS adult_mask_generalize;
CREATE TABLE adult_mask_generalize AS
SELECT 
    id,
    -- Age buckets
    (age / 10) * 10 AS age,
    workclass,
    fnlwgt,
    -- Education categories
    CASE 
        WHEN education IN ('Preschool', '1st-4th', '5th-6th', '7th-8th') THEN 'Primary'
        WHEN education IN ('9th', '10th', '11th', '12th', 'HS-grad') THEN 'High-School'
        WHEN education IN ('Some-college', 'Assoc-voc', 'Assoc-acdm', 'Bachelors') THEN 'College'
        WHEN education IN ('Masters', 'Prof-school', 'Doctorate') THEN 'Graduate'
        ELSE 'Other'
    END AS education,
    education_num,
    marital_status,
    -- Occupation categories
    CASE
        WHEN occupation IN ('Exec-managerial', 'Prof-specialty') THEN 'Professional'
        WHEN occupation IN ('Tech-support', 'Sales', 'Adm-clerical') THEN 'Office'
        WHEN occupation IN ('Craft-repair', 'Machine-op-inspct', 'Transport-moving') THEN 'Skilled-Labor'
        WHEN occupation IN ('Handlers-cleaners', 'Farming-fishing', 'Other-service') THEN 'Service'
        WHEN occupation IN ('Priv-house-serv', 'Protective-serv', 'Armed-Forces') THEN 'Other'
        ELSE 'Unknown'
    END AS occupation,
    relationship,
    race,
    sex,
    -- Round financial values
    ROUND(capital_gain / 1000) * 1000 AS capital_gain,
    ROUND(capital_loss / 1000) * 1000 AS capital_loss,
    hours_per_week,
    native_country,
    income
FROM mask_test_data;

-- Create indexes on all masked tables
CREATE INDEX idx_hash_occupation ON adult_mask_hash(occupation);
CREATE INDEX idx_partial_occupation ON adult_mask_partial(occupation);
CREATE INDEX idx_shuffle_occupation ON adult_mask_shuffle(occupation);
CREATE INDEX idx_fake_occupation ON adult_mask_anon_fake(occupation);
CREATE INDEX idx_noise_capital ON adult_mask_noise(capital_gain);
CREATE INDEX idx_generalize_age ON adult_mask_generalize(age);

-- Analyze all tables
VACUUM ANALYZE adult_mask_hash;
VACUUM ANALYZE adult_mask_partial;
VACUUM ANALYZE adult_mask_shuffle;
VACUUM ANALYZE adult_mask_anon_fake;
VACUUM ANALYZE adult_mask_noise;
VACUUM ANALYZE adult_mask_generalize;

-- Grant permissions
GRANT SELECT ON adult_mask_hash, adult_mask_partial, adult_mask_shuffle,
                adult_mask_anon_fake, adult_mask_noise, adult_mask_generalize 
TO analyst, masked_user;

-- Summary view
SELECT 'mask_test_data (original)' as table_name, COUNT(*) as rows FROM mask_test_data
UNION ALL
SELECT 'adult_mask_hash', COUNT(*) FROM adult_mask_hash
UNION ALL
SELECT 'adult_mask_partial', COUNT(*) FROM adult_mask_partial
UNION ALL
SELECT 'adult_mask_shuffle', COUNT(*) FROM adult_mask_shuffle
UNION ALL
SELECT 'adult_mask_anon_fake', COUNT(*) FROM adult_mask_anon_fake
UNION ALL
SELECT 'adult_mask_noise', COUNT(*) FROM adult_mask_noise
UNION ALL
SELECT 'adult_mask_generalize', COUNT(*) FROM adult_mask_generalize;