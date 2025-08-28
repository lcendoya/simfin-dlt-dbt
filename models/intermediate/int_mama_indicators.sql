-- MAMA (MESA Adaptive Moving Average) indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates MAMA to match Python script exactly

WITH price_data AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date) as rn,
        -- Price change
        adjusted_closing_price - LAG(adjusted_closing_price, 1) OVER (PARTITION BY ticker ORDER BY date) as price_change
    FROM {{ ref('stg_price_data') }}
),
mama_recursive AS (
    SELECT 
        *,
        -- Initial fast_limit = 0.5, slow_limit = 0.05 (from Python defaults)
        0.5 as fast_limit,
        adjusted_closing_price as mama_value
    FROM price_data
    WHERE rn = 1
    
    UNION ALL
    
    SELECT 
        p.*,
        -- Update fast_limit: max(slow_limit, min(fast_limit * 1.5, 0.99))
        GREATEST(0.05, LEAST(m.fast_limit * 1.5, 0.99)) as fast_limit,
        -- MAMA calculation: mama[i] = mama[i-1] + fast_limit * diff
        m.mama_value + (GREATEST(0.05, LEAST(m.fast_limit * 1.5, 0.99)) * p.price_change) as mama_value
    FROM price_data p
    INNER JOIN mama_recursive m ON p.ticker = m.ticker AND p.rn = m.rn + 1
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
    
    -- MAMA indicator (matching Python script exactly)
    mama_value as mama
    
FROM mama_recursive
ORDER BY ticker, date
