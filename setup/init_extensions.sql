-- Initialize extensions for benchmarking
-- Run this ONCE after database creation

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS anon CASCADE;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

SELECT anon.init();

CREATE ROLE analyst LOGIN PASSWORD 'analyst';
CREATE ROLE masked_user LOGIN PASSWORD 'masked';

SECURITY LABEL FOR anon ON ROLE masked_user IS 'MASKED';

GRANT CONNECT ON DATABASE benchmark TO analyst, masked_user;
GRANT USAGE ON SCHEMA public TO analyst, masked_user;

SELECT installed_version FROM pg_available_extensions WHERE name = 'anon';