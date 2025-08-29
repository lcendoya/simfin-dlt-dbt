-- FAMA (Following Adaptive Moving Average) indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates FAMA to match Python script exactly

WITH RECURSIVE price_data AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date) as rn
    FROM {{ ref('stg_price_data') }}
),
fama_recursive AS (
    SELECT 
        *,
        -- Initial slow_limit = 0.05 (from Python defaults)
        0.05 as slow_limit,
        adjusted_closing_price as fama_value
    FROM price_data
    WHERE rn = 1
    
    UNION ALL
    
    SELECT 
        p.*,
        -- Update slow_limit: max(slow_limit, min(slow_limit * 1.5, 0.99))
        GREATEST(0.05, LEAST(m.slow_limit * 1.5, 0.99)) as slow_limit,
        -- FAMA calculation: fama[i] = fama[i-1] + slow_limit * diff (using previous slow_limit)
        m.fama_value + (m.slow_limit * p.price_change) as fama_value
    FROM price_data p
    INNER JOIN fama_recursive m ON p.ticker = m.ticker AND p.rn = m.rn + 1
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
    
    -- FAMA indicator (matching Python script exactly)
    fama_value as fama
    
FROM fama_recursive
