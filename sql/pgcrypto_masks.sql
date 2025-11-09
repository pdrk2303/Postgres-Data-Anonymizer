CREATE OR REPLACE FUNCTION fpe_hash(input TEXT, key TEXT DEFAULT 'fpe-key')
RETURNS TEXT AS $$
DECLARE
    hashed TEXT;
    result TEXT := '';
    i INTEGER;
BEGIN
    hashed := encode(hmac(input, key, 'sha256'), 'hex');
    FOR i IN 1..length(input) LOOP
        result := result || substring('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ' FROM 
            (get_byte(decode(substring(hashed FROM (i*2-1) FOR 2), 'hex'), 0) % 36) + 1 FOR 1);
    END LOOP;
    RETURN result;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE TABLE IF NOT EXISTS surrogate_mapping (
    original_value TEXT PRIMARY KEY,
    surrogate_value TEXT UNIQUE,
    column_name TEXT
);

CREATE OR REPLACE FUNCTION get_surrogate(input TEXT, col_name TEXT)
RETURNS TEXT AS $$
DECLARE
    surrogate TEXT;
BEGIN
    SELECT surrogate_value INTO surrogate 
    FROM surrogate_mapping 
    WHERE original_value = input AND column_name = col_name;
    
    IF surrogate IS NULL THEN
        surrogate := 'SRG-' || encode(gen_random_bytes(8), 'hex');
        INSERT INTO surrogate_mapping VALUES (input, surrogate, col_name)
        ON CONFLICT DO NOTHING;
    END IF;
    
    RETURN surrogate;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION add_laplace_noise(value NUMERIC, epsilon NUMERIC DEFAULT 1.0, sensitivity NUMERIC DEFAULT 1.0)
RETURNS NUMERIC AS $$
DECLARE
    u NUMERIC;
    scale NUMERIC;
BEGIN
    scale := sensitivity / epsilon;
    u := random() - 0.5;
    RETURN value + (scale * sign(u) * ln(1 - 2 * abs(u)));
END;
$$ LANGUAGE plpgsql VOLATILE;