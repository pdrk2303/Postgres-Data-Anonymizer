-- System Under Test: postgresql_anonymizer extension
-- Configure dynamic masking rules
-- DROP OWNED BY masked_user;
-- DROP ROLE IF EXISTS masked_user;
-- DROP OWNED BY analyst;
-- DROP ROLE IF EXISTS analyst;

ALTER DATABASE benchmark SET anon.transparent_dynamic_masking TO true;

ALTER DATABASE benchmark SET session_preload_libraries = 'anon';

\connect benchmark postgres
SELECT anon.init();

SECURITY LABEL FOR anon ON ROLE masked_user IS 'MASKED';

-- Enable dynamic masking globally
ALTER DATABASE benchmark SET anon.transparent_dynamic_masking TO true;

-- Reconnect for setting to take effect (do this manually or in script)
\connect benchmark postgres

CREATE OR REPLACE FUNCTION anon.fake_education()
RETURNS text AS $$
  SELECT (ARRAY[
    'Bachelors', 'Masters', 'PhD', 'High School', 'Diploma', 'Associate Degree'
  ])[floor(random()*6 + 1)];
$$ LANGUAGE sql VOLATILE;

CREATE OR REPLACE FUNCTION anon.fake_name()
RETURNS text AS $$
  SELECT anon.fake_first_name() || ' ' || anon.fake_last_name();
$$ LANGUAGE sql VOLATILE;

-- Define masking rules for Adult Census
SECURITY LABEL FOR anon ON COLUMN adult_raw_100000.age
  IS 'MASKED WITH FUNCTION anon.random_int_between(18, 90)';

SECURITY LABEL FOR anon ON COLUMN adult_raw_100000.fnlwgt
  IS 'MASKED WITH VALUE NULL';

SECURITY LABEL FOR anon ON COLUMN adult_raw_100000.education
  IS 'MASKED WITH FUNCTION anon.fake_education()';

SECURITY LABEL FOR anon ON COLUMN adult_raw_100000.occupation
  IS 'MASKED WITH FUNCTION anon.fake_company()';

SECURITY LABEL FOR anon ON COLUMN adult_raw_100000.native_country
  IS 'MASKED WITH FUNCTION anon.fake_country()';

SECURITY LABEL FOR anon ON COLUMN adult_raw_100000.capital_gain
  IS 'MASKED WITH FUNCTION anon.noise(adult_raw_100000.capital_gain, 0.2)';

SECURITY LABEL FOR anon ON COLUMN adult_raw_100000.capital_loss
  IS 'MASKED WITH FUNCTION anon.noise(adult_raw_100000.capital_loss, 0.2)';

-- Define masking rules for Healthcare
SECURITY LABEL FOR anon ON COLUMN healthcare_raw_100000.name
  IS 'MASKED WITH FUNCTION anon.fake_name()';

SECURITY LABEL FOR anon ON COLUMN healthcare_raw_100000.age
  IS 'MASKED WITH FUNCTION anon.random_int_between(18, 90)';

SECURITY LABEL FOR anon ON COLUMN healthcare_raw_100000.doctor
  IS 'MASKED WITH FUNCTION anon.fake_name()';

SECURITY LABEL FOR anon ON COLUMN healthcare_raw_100000.room_number
  IS 'MASKED WITH VALUE NULL';

SECURITY LABEL FOR anon ON COLUMN healthcare_raw_100000.billing_amount
  IS 'MASKED WITH FUNCTION anon.noise(healthcare_raw_100000.billing_amount, 0.15)';

-- Create static masked tables for comparison
DROP TABLE IF EXISTS adult_static_masked;
SELECT * INTO adult_static_masked FROM adult_raw_100000;
SELECT anon.anonymize_table('adult_static_masked');

DROP TABLE IF EXISTS healthcare_static_masked;
SELECT * INTO healthcare_static_masked FROM healthcare_raw_100000;
SELECT anon.anonymize_table('healthcare_static_masked');

-- Create indexes on static masked tables
CREATE INDEX idx_adult_static_age ON adult_static_masked(age);
CREATE INDEX idx_adult_static_education ON adult_static_masked(education);
CREATE INDEX idx_healthcare_static_age ON healthcare_static_masked(age);

VACUUM ANALYZE adult_static_masked;
VACUUM ANALYZE healthcare_static_masked;

REVOKE ALL ON ALL TABLES IN SCHEMA public FROM masked_user;
REVOKE ALL ON ALL TABLES IN SCHEMA anon FROM masked_user;

GRANT USAGE ON SCHEMA anon TO masked_user;
GRANT SELECT ON ALL TABLES IN SCHEMA anon TO masked_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO masked_user;

-- GRANT pg_read_all_data TO masked_user;

SELECT rolname, 
       obj_description(oid, 'pg_authid') as security_label
FROM pg_authid 
WHERE rolname = 'masked_user';

-- Test: As postgres (unmasked), data should be raw
\connect benchmark postgres
SELECT 'Postgres sees (unmasked):', name, age, doctor 
FROM healthcare_raw_100000 LIMIT 2;

-- Test: As masked_user, data should be masked
\connect benchmark masked_user
SELECT 'Masked user sees (should be masked):', name, age, doctor 
FROM healthcare_raw_100000 LIMIT 2;

\connect benchmark postgres
SELECT 'Postgres sees (unmasked):', age, education, occupation 
FROM adult_raw_100000 LIMIT 2;

-- Test: As masked_user, data should be masked
\connect benchmark masked_user
SELECT 'Masked user sees (should be masked):', age, education, occupation 
FROM adult_raw_100000 LIMIT 2;

SELECT 'Masked user sees (should be masked):', * FROM adult_raw_100000 WHERE id = 5000;
