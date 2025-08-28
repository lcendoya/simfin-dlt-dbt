-- KAMA (Kaufman Adaptive Moving Average) indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates KAMA 10 to match Python script exactly

WITH price_data AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date) as rn,
        -- Calculate change over 10 periods (absolute)
        ABS(adjusted_closing_price - LAG(adjusted_closing_price, 10) OVER (PARTITION BY ticker ORDER BY date)) as change_10,
        -- Calculate volatility (sum of absolute changes over 10 periods)
        SUM(ABS(adjusted_closing_price - LAG(adjusted_closing_price, 1) OVER (PARTITION BY ticker ORDER BY date))) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) as volatility_10
    FROM {{ ref('stg_price_data') }}
),
kama_calc AS (
    SELECT 
        *,
        -- Efficiency Ratio (matching Python script exactly)
        CASE 
            WHEN volatility_10 = 0 THEN 0
            ELSE change_10 / volatility_10
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
    
    -- KAMA calculation (matching Python script exactly)
    CASE 
        WHEN rn <= 10 THEN adjusted_closing_price  -- Use price for first 10 periods
        ELSE LAG(adjusted_closing_price, 1) OVER (PARTITION BY ticker ORDER BY date) + 
             smoothing_constant * (adjusted_closing_price - LAG(adjusted_closing_price, 1) OVER (PARTITION BY ticker ORDER BY date))
    END as kama_10
    
FROM kama_final
ORDER BY ticker, date
