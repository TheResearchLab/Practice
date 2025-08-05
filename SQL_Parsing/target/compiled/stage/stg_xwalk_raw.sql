-- Staging for the crosswalk table (referenced by wrk_xwalk_table)
SELECT 
    col_x,
    col_a,
    col_b,
    col_c,
    effective_date,
    expiration_date,
    is_active,
    source_system,
    current_timestamp() as loaded_at
FROM raw_data_db.lookup_tables.crosswalk_master
WHERE source_system = 'MAIN'