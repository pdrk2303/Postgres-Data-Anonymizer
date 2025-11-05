-- Workload 1: Point Lookup
-- Tests: Index scan performance, masking overhead on single row

-- Adult table
SELECT * FROM adult_raw WHERE id = 5000;

-- Healthcare table  
SELECT * FROM healthcare_raw WHERE id = 5000;