-- FILE: stg_dummy_populations.sql (Staging Layer)  
-- Population segment data
-- Test with: table="stg_dummy_populations", column="segment_type"

SELECT 
    population_id,
    population_name,
    CASE 
        WHEN tier_level = 'T1' THEN 'PREMIUM'
        WHEN tier_level = 'T2' THEN 'STANDARD' 
        ELSE 'BASIC'
    END as segment_type,
    active_flag
FROM dummy_populations_db.segments.population_master
WHERE active_flag = 1