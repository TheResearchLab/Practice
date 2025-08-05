-- Staging layer for source_table_3
SELECT 
    col_a,
    col_b,
    col_z,
    CASE 
        WHEN col_b IN ('ACTIVE', 'PENDING') THEN 'VALID'
        ELSE 'INVALID'
    END as status_flag,
    current_timestamp() as loaded_at
FROM raw_data_db.external_sources.raw_source_table_3
WHERE col_a IS NOT NULL