-- ATR (Average True Range) indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates ATR 14 to match Python script exactly

WITH trange_data AS (
    SELECT 
        ticker, date, trange
    FROM {{ ref('int_trange_indicators') }}
)
SELECT 
    p.*,
    -- ATR calculation (matching Python script exactly)
    -- ATR = 14-period average of True Range
    AVG(t.trange) OVER (
        PARTITION BY p.ticker 
        ORDER BY p.date 
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    ) as atr_14
    
FROM {{ ref('stg_price_data') }} p
LEFT JOIN trange_data t ON p.ticker = t.ticker AND p.date = t.date