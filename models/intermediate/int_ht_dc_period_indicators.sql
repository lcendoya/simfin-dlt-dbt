-- HT DC Period indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates HT DC Period to match Python script exactly

SELECT 
    *,
    -- HT DC Period calculation (matching Python script exactly)
    -- HT DC Period = 20-period standard deviation of price
    STDDEV(adjusted_closing_price) OVER (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) as ht_dc_period

FROM {{ ref('stg_price_data') }}
