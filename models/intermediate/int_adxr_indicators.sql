-- ADXR (Average Directional Movement Rating) indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates ADXR 14 to match Python script exactly

WITH dx_data AS (
    SELECT 
        ticker, date, adx_14
    FROM {{ ref('int_dx_indicators') }}
)
SELECT 
    p.*,
    -- ADXR calculation (matching Python script exactly)
    -- ADXR = (ADX + ADX_shifted_14) / 2
    (d.adx_14 + LAG(d.adx_14, 14) OVER (PARTITION BY p.ticker ORDER BY p.date)) / 2 as adxr_14
    
FROM {{ ref('stg_price_data') }} p
LEFT JOIN dx_data d ON p.ticker = d.ticker AND p.date = d.date
