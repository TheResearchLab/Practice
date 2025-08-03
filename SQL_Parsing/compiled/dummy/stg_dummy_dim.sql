-- FILE: stg_dummy_dim.sql (Staging Layer)
-- Dimension table for dummy data
-- Test with: table="stg_dummy_dim", column="dummy_ver_name"

SELECT 
    dim_id as dummy_key,
    UPPER(TRIM(version_name)) as dummy_ver_name,
    CASE 
        WHEN status_code = 'A' THEN 'ACTIVE'
        WHEN status_code = 'I' THEN 'INACTIVE'
        ELSE 'UNKNOWN'
    END as status,
    priority_level,
    created_date,
    updated_date
FROM external_dims_db.reference.dummy_dimension
WHERE status_code != 'D'  -- Exclude deleted record