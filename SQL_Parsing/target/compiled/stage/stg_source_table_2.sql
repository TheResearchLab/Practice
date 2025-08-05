-- Staging layer for source_table_2
SELECT 
    col_c,
    col_x,
    col_aa,
    UPPER(TRIM(col_c)) as cleaned_col_c,
    current_timestamp() as loaded_at
FROM raw_data_db.external_sources.raw_source_table_2
WHERE col_x > 0