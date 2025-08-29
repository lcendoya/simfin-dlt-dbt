-- KAMA (Kaufman Adaptive Moving Average) indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates KAMA 10 to match Python script exactly

WITH RECURSIVE price_data AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date) as rn,
        -- Calculate change over 10 periods (absolute) - not available in staging
        ABS(adjusted_closing_price - LAG(adjusted_closing_price, 10) OVER (PARTITION BY ticker ORDER BY date)) as change_10
    FROM {{ ref('stg_price_data') }}
),
kama_calc AS (
    SELECT 
        *,
        -- Efficiency Ratio (matching Python script exactly)
        CASE 
            WHEN SUM(ABS(price_change)) OVER (
                PARTITION BY ticker 
                ORDER BY date 
                ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
            ) = 0 THEN 0
            ELSE change_10 / SUM(ABS(price_change)) OVER (
                PARTITION BY ticker 
                ORDER BY date 
                ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
            )
        END as efficiency_ratio,
        
        -- Fast and slow constants (matching Python script exactly)
        2.0 / (2.0 + 1.0) as fast_ema,  -- 2/(2+1) = 0.6667
        2.0 / (30.0 + 1.0) as slow_ema   -- 2/(30+1) = 0.0645
    FROM price_data
),
kama_final AS (
    SELECT 
        *,
        -- Smoothing constant (matching Python script exactly)
        (efficiency_ratio * (fast_ema - slow_ema) + slow_ema) ^ 2 as smoothing_constant
    FROM kama_calc
),
kama_recursive AS (
    SELECT 
        *,
        -- Base case: 10th row gets the price value (matching Python script exactly)
        CASE WHEN rn = 10 THEN adjusted_closing_price ELSE NULL END as kama_10
    FROM kama_final
    WHERE rn = 10
    
    UNION ALL
    
    SELECT 
        p.*,
        -- Recursive case: KAMA[i] = KAMA[i-1] + smoothing_constant * (price[i] - KAMA[i-1])
        m.kama_10 + (p.smoothing_constant * (p.adjusted_closing_price - m.kama_10)) as kama_10
    FROM kama_final p
    INNER JOIN kama_recursive m ON p.ticker = m.ticker AND p.rn = m.rn + 1
    WHERE p.rn > 10
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
    
    -- KAMA indicator (matching Python script exactly)
    kama_10
    
FROM kama_recursive
