-- Staging layer for source_table_5
SELECT 
    col_q,
    col_r,
    col_s,
    col_t,
    col_ad,
    COALESCE(col_t, 4) as col_t_with_default,
    current_timestamp() as loaded_at
FROM raw_data_db.external_sources.raw_source_table_5
WHERE col_ad IS NOT NULL