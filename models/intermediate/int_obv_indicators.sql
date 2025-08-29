-- OBV (On Balance Volume) indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates OBV to match Python script exactly

SELECT 
    company_id,
    company_name,
    ticker,
    currency,
    isin,
    date,
    dividend_paid,
    common_shares_outstanding,
    last_closing_price,
    adjusted_closing_price,
    highest_price,
    lowest_price,
    opening_price,
    trading_volume,
    daily_range,
    daily_return_pct,
    
    -- OBV calculation (matching Python script exactly)
    -- OBV = cumulative sum of volume where price_change > 0, -volume where price_change < 0
    SUM(
        CASE 
            WHEN price_change > 0 THEN trading_volume
            WHEN price_change < 0 THEN -trading_volume
            ELSE 0
        END
    ) OVER (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS UNBOUNDED PRECEDING
    ) as obv
    
FROM {{ ref('stg_price_data') }}
