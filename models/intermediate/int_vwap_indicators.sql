-- VWAP (Volume Weighted Average Price) indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates cumulative VWAP to match Python script exactly

SELECT 
    *,
    -- VWAP (cumulative) - matches Python script exactly
    SUM(adjusted_closing_price * trading_volume) OVER (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS UNBOUNDED PRECEDING
    ) / SUM(trading_volume) OVER (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS UNBOUNDED PRECEDING
    ) as vwap_cumulative

FROM {{ ref('stg_price_data') }}
