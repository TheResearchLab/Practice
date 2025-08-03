-- FILE: fct_dummy_analysis.sql (Presentation Layer)
-- Simplified version for testing
-- Test with: table="fct_dummy_analysis", column="dummy_id"

SELECT 
    da.dummy_id,
    da.dummy_ver_name,
    da.dummy_coef,
    CASE 
        WHEN da.dummy_coef > 0.8 THEN 'HIGH'
        WHEN da.dummy_coef > 0.5 THEN 'MEDIUM'
        ELSE 'LOW'
    END as impact_level
FROM ph_dw_prod.marts.dummy_aggregated da
WHERE da.dummy_coef IS NOT NULL