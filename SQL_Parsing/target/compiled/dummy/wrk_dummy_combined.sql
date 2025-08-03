-- FILE: wrk_dummy_combined.sql (Work Layer)
-- Simple work layer combining sources
-- Test with: table="wrk_dummy_combined", column="version_code"

SELECT 
    dr.record_id,
    dr.source_system,
    CASE 
        WHEN dr.version_flag = 'V2_ENHANCED' THEN 'V2'
        WHEN dr.version_flag = 'V1_STANDARD' THEN 'V1' 
        ELSE 'V0'
    END as version_code,
    dr.region as region_code,
    dp.segment_type as population_segment,
    dr.priority_score,
    dr.variable_identifier as variable_name,
    dr.coefficient as coefficient_value
FROM ph_dw_prod.staging.stg_dummy_records dr
LEFT JOIN ph_dw_prod.staging.stg_dummy_populations dp 
    ON dr.population_id = dp.population_id
WHERE dr.is_valid = true