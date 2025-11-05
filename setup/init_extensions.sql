-- Initialize extensions for benchmarking
-- Run this ONCE after database creation

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS anon CASCADE;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Initialize anon extension
SELECT anon.init();

-- Create roles for dynamic masking
CREATE ROLE analyst LOGIN PASSWORD 'analyst';
CREATE ROLE masked_user LOGIN PASSWORD 'masked';

-- Mark masked_user as masked
SECURITY LABEL FOR anon ON ROLE masked_user IS 'MASKED';

-- Grant read permissions
GRANT CONNECT ON DATABASE benchmark TO analyst, masked_user;
GRANT USAGE ON SCHEMA public TO analyst, masked_user;

-- Verify installation
SELECT installed_version FROM pg_available_extensions WHERE name = 'anon';