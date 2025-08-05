-- Staging layer for source_table_1
SELECT 
    col_d,
    col_f,
    col_g,
    col_i,
    col_j,
    col_k,
    col_l,
    col_m,
    col_n,
    col_o,
    col_p,
    col_u,
    col_y,
    col_ac,
    col_ae,
    current_timestamp() as loaded_at
FROM raw_data_db.external_sources.raw_source_table_1
WHERE col_d IS NOT NULL