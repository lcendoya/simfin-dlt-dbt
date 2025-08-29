-- HT Trend Mode indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates HT Trend Mode to match Python script exactly

SELECT 
    *,
    -- HT Trend Mode calculation (matching Python script exactly)
    -- HT Trend Mode = sign of price difference
    CASE 
        WHEN price_change > 0 THEN 1
        WHEN price_change < 0 THEN -1
        ELSE 0
    END as ht_trendmode

FROM {{ ref('stg_price_data') }}
