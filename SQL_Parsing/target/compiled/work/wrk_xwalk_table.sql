-- Work layer for crosswalk/lookup table
WITH base_xwalk AS (
    SELECT 
        col_x,
        col_a,
        col_b,
        col_c,
        effective_date,
        expiration_date,
        is_active,
        ROW_NUMBER() OVER (PARTITION BY col_x ORDER BY effective_date DESC) as rn
    FROM ph_land_db.staging.stg_crosswalk_raw
    WHERE is_active = TRUE
),
current_xwalk AS (
    SELECT 
        col_x,
        col_a,
        col_b,
        col_c,
        effective_date,
        expiration_date
    FROM base_xwalk
    WHERE rn = 1
      AND (expiration_date IS NULL OR expiration_date > current_date())
)
SELECT 
    col_x,
    col_a,
    col_b,
    col_c,
    effective_date,
    expiration_date,
    current_timestamp() as processed_at
FROM current_xwalk