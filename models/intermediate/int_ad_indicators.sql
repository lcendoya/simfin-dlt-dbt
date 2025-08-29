-- AD (Accumulation/Distribution Line) indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates AD to match Python script exactly

WITH price_data AS (
    SELECT 
        *,
        -- Close Location Value (CLV)
        CASE 
            WHEN (highest_price - lowest_price) = 0 THEN 0
            ELSE ((adjusted_closing_price - lowest_price) - (highest_price - adjusted_closing_price)) / (highest_price - lowest_price)
        END as clv
    FROM {{ ref('stg_price_data') }}
)
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
    
    -- AD calculation (matching Python script exactly)
    -- AD = cumulative sum of (CLV * volume)
    SUM(clv * trading_volume) OVER (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS UNBOUNDED PRECEDING
    ) as ad
    
FROM price_data
