-- HT Trendline indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates HT Trendline to match Python script exactly

WITH ema_data AS (
    SELECT 
        ticker, date, ema_20
    FROM {{ ref('int_ema_indicators') }}
)
SELECT 
    p.*,
    -- HT Trendline (reusing ema_20 calculation from int_ema_indicators.sql)
    e.ema_20 as ht_trendline
    
FROM {{ ref('stg_price_data') }} p
LEFT JOIN ema_data e ON p.ticker = e.ticker AND p.date = e.date
