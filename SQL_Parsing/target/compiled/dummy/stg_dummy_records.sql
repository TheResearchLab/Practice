-- FILE: stg_dummy_records.sql (Staging Layer)
-- Core dummy records from source system
-- Test with: table="stg_dummy_records", column="coefficient"

SELECT 
    record_id,
    source_system_id as source_system,
    version_flag,
    region,
    population_id,
    priority_score,
    variable_identifier,
    CAST(coefficient_raw AS NUMBER(8,3)) as coefficient,
    CASE WHEN validation_status = 'PASS' THEN true ELSE false END as is_valid
FROM dummy_data_lake.raw.dummy_records_v1
WHERE coefficient_raw IS NOT NULL