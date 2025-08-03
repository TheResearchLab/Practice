-- FILE: stg_dummy_raw.sql (Staging Layer)
-- Raw data transformation with CASE statement for dummy_ver_name
-- Test with: table="stg_dummy_raw", column="v" (the variant column)

SELECT 
    raw_id,
    source_system,
    CASE 
        WHEN source_system = 'SYSTEM_A' AND version_code = 'V2' THEN 
            object_construct(
                'DUMMY_VER_NAME', 'ADVANCED_V2_' || region_code,
                'DUMMY_POP_NAME', population_segment,
                'DUMMY_LEVEL_CD', service_level,
                'DUMMY_VAR_NAME', variable_name,
                'DUMMY_COEF', coefficient_value
            )
        WHEN source_system = 'SYSTEM_B' AND version_code = 'V1' THEN
            object_construct(
                'DUMMY_VER_NAME', 'STANDARD_V1_' || region_code,
                'DUMMY_POP_NAME', population_segment,
                'DUMMY_LEVEL_CD', service_level,
                'DUMMY_VAR_NAME', variable_name,
                'DUMMY_COEF', coefficient_value
            )
        ELSE
            object_construct(
                'DUMMY_VER_NAME', 'BASIC_' || COALESCE(version_code, 'UNKNOWN') || '_' || COALESCE(region_code, 'GLOBAL'),
                'DUMMY_POP_NAME', COALESCE(population_segment, 'GENERAL'),
                'DUMMY_LEVEL_CD', COALESCE(service_level, 'STANDARD'),
                'DUMMY_VAR_NAME', COALESCE(variable_name, 'DEFAULT'),
                'DUMMY_COEF', COALESCE(coefficient_value, '0.0')
            )
    END as v,
    version_code,
    region_code,
    population_segment,
    service_level,
    variable_name,
    coefficient_value,
    created_at
FROM ph_dw_prod.work.wrk_dummy_combined
WHERE source_system IS NOT NULL