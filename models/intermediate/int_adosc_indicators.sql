-- ADOSC (Accumulation/Distribution Oscillator) indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates ADOSC using periods 3 and 10 to match Python script exactly

WITH ad_data AS (
    SELECT 
        ticker, date, ad
    FROM {{ ref('int_ad_indicators') }}
)
SELECT 
    p.*,
    -- ADOSC calculation (matching Python script exactly)
    -- ADOSC = short_ema_3 - long_ema_10 of AD line
    AVG(a.ad) OVER (
        PARTITION BY p.ticker 
        ORDER BY p.date 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) - 
    AVG(a.ad) OVER (
        PARTITION BY p.ticker 
        ORDER BY p.date 
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) as adosc
    
FROM {{ ref('stg_price_data') }} p
LEFT JOIN ad_data a ON p.ticker = a.ticker AND p.date = a.date
