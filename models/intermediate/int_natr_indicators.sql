-- NATR (Normalized Average True Range) indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates NATR 14 to match Python script exactly

WITH atr_data AS (
    SELECT 
        ticker, date, atr_14
    FROM {{ ref('int_atr_indicators') }}
)
SELECT 
    p.*,
    -- NATR calculation (matching Python script exactly)
    -- NATR = 100 * (ATR / close)
    CASE 
        WHEN p.adjusted_closing_price = 0 THEN 0
        ELSE 100 * (a.atr_14 / p.adjusted_closing_price)
    END as natr_14
    
FROM {{ ref('stg_price_data') }} p
LEFT JOIN atr_data a ON p.ticker = a.ticker AND p.date = a.date
