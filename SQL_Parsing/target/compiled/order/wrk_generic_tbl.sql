-- wrk_generic_tbl.sql
WITH  cte_source_table_1 AS (

SELECT *

  FROM ph_land_db.schema.source_table_1

),  cte_source_table_2 AS (

SELECT *

  FROM ph_land_db.schema.source_table_2

AT(timestamp => '{{ timestamp_placeholder }}'::TIMESTAMP_TZ)

),  cte_source_table_3 AS (

SELECT *

  FROM ph_land_db.schema.source_table_3

AT(timestamp => '{{ timestamp_placeholder }}'::TIMESTAMP_TZ)

),  cte_source_table_4 AS (

SELECT *

  FROM ph_land_db.schema.source_table_4

),  cte_source_table_5 AS (

SELECT *

  FROM ph_land_db.schema.source_table_5

AT(timestamp => '{{ timestamp_placeholder }}'::TIMESTAMP_TZ)

), cte_main AS (

 

-- Combining data from multiple sources

SELECT COALESCE(t3.col_a::VARCHAR, '') || '~' || COALESCE(t3.col_b::VARCHAR, '') || '~' || COALESCE(t2.col_c::VARCHAR, '') AS combined_key

      ,t3.col_a AS col_a

      ,t3.col_b AS col_b

      ,t2.col_c AS col_c

      ,TRY_TO_NUMBER(NULLIF(t1.col_d,''))::INTEGER AS col_d

      ,TRY_TO_NUMBER(NULLIF(t4.col_e,''))::INTEGER AS col_e

      ,TRY_TO_NUMBER(NULLIF(t1.col_f,''))::INTEGER AS col_f

      ,NVL(t1.col_g, t4.col_h) AS col_g

      ,NULLIF(t1.col_i,'') AS col_i

      ,NULLIF(t1.col_j,'') AS col_j

      ,NULLIF(t1.col_k,'') AS col_k

      ,NULLIF(t1.col_l,'') AS col_l

      ,NULLIF(t1.col_m,'') AS col_m

      ,NULLIF(t1.col_n,'') AS col_n

      ,NULLIF(t1.col_o,'') AS col_o

      ,NULLIF(t1.col_p,'')::BOOLEAN AS col_p

      ,t5.col_q AS col_q

      ,t5.col_r AS col_r

      ,t5.col_s AS col_s

      ,NVL(NULLIF(t5.col_t,-1),4) AS col_t

      ,NULLIF(t1.col_u,'') AS col_u

      ,TRY_TO_DATE(t4.col_v::STRING,'YYYYMMDD') AS col_v

      ,TRY_TO_DATE(t4.col_w::STRING,'YYYYMMDD') AS col_w

  FROM cte_source_table_1 t1

       INNER JOIN cte_source_table_2 t2 ON (t2.col_x = NULLIF(t1.col_y,'')::INTEGER)

       INNER JOIN cte_source_table_3 t3 ON (t3.col_z = t2.col_aa)

       LEFT OUTER JOIN cte_source_table_4 t4 ON (t4.col_ab = t1.col_ac)

       LEFT OUTER JOIN cte_source_table_5 t5 ON (t5.col_ad = NULLIF(t1.col_ae,'')::INTEGER)




UNION ALL


SELECT COALESCE(xwalk.col_a::VARCHAR, '') || '~' || COALESCE(xwalk.col_b::VARCHAR, '') || '~' || COALESCE(xwalk.col_c::VARCHAR, '') AS combined_key

      ,xwalk.col_a

      ,xwalk.col_b

      ,xwalk.col_c

      ,TRY_TO_NUMBER(NULLIF(t1.col_d,''))::INTEGER AS col_d

      ,TRY_TO_NUMBER(NULLIF(t4.col_e,''))::INTEGER AS col_e

      ,TRY_TO_NUMBER(NULLIF(t1.col_f,''))::INTEGER AS col_f

      ,NVL(t1.col_g, t4.col_h) AS col_g

      ,NULLIF(t1.col_i,'') AS col_i

      ,NULLIF(t1.col_j,'') AS col_j

      ,NULLIF(t1.col_k,'') AS col_k

      ,NULLIF(t1.col_l,'') AS col_l

      ,NULLIF(t1.col_m,'') AS col_m

      ,NULLIF(t1.col_n,'') AS col_n

      ,NULLIF(t1.col_o,'') AS col_o

      ,NULLIF(t1.col_p,'')::BOOLEAN AS col_p

      ,t5.col_q AS col_q

      ,t5.col_r AS col_r

      ,t5.col_s AS col_s

      ,NVL(NULLIF(t5.col_t,-1),4) AS col_t

      ,NULLIF(t1.col_u,'') AS col_u

      ,TRY_TO_DATE(t4.col_v::STRING,'YYYYMMDD') AS col_v

      ,TRY_TO_DATE(t4.col_w::STRING,'YYYYMMDD') AS col_w

  FROM cte_source_table_1 t1

       INNER JOIN ph_idea_db.schema.wrk_xwalk_table xwalk ON (xwalk.col_x = NULLIF(t1.col_y,'')::INTEGER)

       LEFT OUTER JOIN cte_source_table_4 t4 ON (t4.col_ab = t1.col_ac)

       LEFT OUTER JOIN cte_source_table_5 t5 ON (t5.col_ad = NULLIF(t1.col_ae,'')::INTEGER)

 

)

 

   ,cte_aggregated AS (

 

SELECT col_e

      ,MAX(IFNULL(col_d::STRING,'') || '~' || IFNULL(col_g::STRING,'') || '~' || IFNULL(col_v::STRING,'') || '~' || IFNULL(col_w::STRING,'')) AS aggregated_key

      ,listagg(DISTINCT col_a || ':' || col_c,'~') WITHIN GROUP (ORDER BY col_a || ':' || col_c) AS aggregated_list

      ,aggregated_key || '~' || sha2(aggregated_list) AS hash_key

  FROM cte_main

WHERE col_e IS NOT NULL

GROUP BY col_e

 

)

 

SELECT main.combined_key

      ,main.col_a

      ,main.col_b

      ,main.col_c

      ,main.col_g

      ,main.col_i

      ,main.col_j

      ,main.col_k

      ,main.col_l

      ,main.col_m

      ,main.col_n

      ,main.col_u

      ,agg.hash_key

      ,main.col_q

      ,main.col_r

      ,main.col_s

      ,main.col_t

      ,main.col_p

  FROM cte_main main

       LEFT OUTER JOIN cte_aggregated agg ON (agg.col_e = main.col_e)