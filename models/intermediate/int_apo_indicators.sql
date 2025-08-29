-- APO (Absolute Price Oscillator) indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates APO using periods 12 and 26 to match Python script exactly

WITH ema_data AS (
    SELECT 
        ticker, date, ema_12, ema_26
    FROM {{ ref('int_ema_indicators') }}
)
SELECT 
    p.*,
    -- APO calculation (matching Python script exactly)
    -- APO = fast_ema_12 - slow_ema_26
    e.ema_12 - e.ema_26 as apo
    
FROM {{ ref('stg_price_data') }} p
LEFT JOIN ema_data e ON p.ticker = e.ticker AND p.date = e.date
