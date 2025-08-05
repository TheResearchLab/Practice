-- Staging layer for source_table_4
SELECT 
    col_e,
    col_h,
    col_ab,
    col_v,
    col_w,
    TRY_TO_DATE(col_v::STRING, 'YYYYMMDD') as parsed_start_date,
    TRY_TO_DATE(col_w::STRING, 'YYYYMMDD') as parsed_end_date,
    current_timestamp() as loaded_at
FROM raw_data_db.external_sources.raw_source_table_4