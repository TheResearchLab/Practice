-- FILE: dummy_aggregated.sql (Mart Layer)
-- Simplified CTE version
-- Test with: table="dummy_aggregated", column="dummy_id"

WITH dummy_metrics AS (
    SELECT
        dummy_ver_name,
        dummy_pop_name,
        dummy_level_cd,
        dummy_var_name,
        dummy_coef
    FROM ph_dw_prod.staging.stg_dummy_processed
)
SELECT
    COALESCE(dm.dummy_ver_name, '') || '~' || COALESCE(dm.dummy_pop_name, '') || '~' || COALESCE(dm.dummy_level_cd, '') AS dummy_id,
    dm.dummy_ver_name,
    dm.dummy_pop_name,
    dm.dummy_level_cd,
    dm.dummy_var_name,
    dm.dummy_coef
FROM dummy_metrics dm
WHERE dm.dummy_ver_name IS NOT NULL