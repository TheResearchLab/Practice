-- FILE: wrk_dummy_processed.sql (Staging Layer)
-- Simplified staging with CASE statement
-- Test with: table="wrk_dummy_processed", column="dummy_ver_name"

SELECT 
    record_id,
    CASE 
        WHEN source_system = 'SYSTEM_A' AND version_code = 'V2' THEN 'ADVANCED_V2_' || region_code
        WHEN source_system = 'SYSTEM_B' AND version_code = 'V1' THEN 'STANDARD_V1_' || region_code
        ELSE 'BASIC_' || COALESCE(version_code, 'UNKNOWN')
    END as dummy_ver_name,
    CASE 
        WHEN population_segment = 'PREMIUM' THEN 'PREMIUM_POP'
        WHEN population_segment = 'STANDARD' THEN 'STANDARD_POP'
        ELSE 'BASIC_POP'
    END as dummy_pop_name,
    CASE 
        WHEN priority_score >= 90 THEN 'PREMIUM'
        WHEN priority_score >= 70 THEN 'HIGH'
        ELSE 'STANDARD'
    END as dummy_level_cd,
    variable_name as dummy_var_name,
    coefficient_value as dummy_coef
FROM ph_dw_prod.work.wrk_dummy_combined
WHERE source_system IS NOT NULL