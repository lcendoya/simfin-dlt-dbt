-- T3 (Triple Exponential Moving Average T3) indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates T3 with period 5 and vfactor 0.7 to match Python script exactly

WITH ema_data AS (
    SELECT 
        ticker, date, ema_10
    FROM {{ ref('int_ema_indicators') }}
),
t3_calc AS (
    SELECT 
        *,
        -- First EMA of EMA-10 (period 5)
        AVG(ema_10) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) as ema1,
        
        -- Second EMA of EMA of EMA-10 (period 5)
        AVG(AVG(ema_10) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        )) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) as ema2,
        
        -- Third EMA of EMA of EMA of EMA-10 (period 5)
        AVG(AVG(AVG(ema_10) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        )) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        )) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) as ema3
    FROM ema_data
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
    
    -- T3 calculation (matching Python script exactly)
    -- vfactor = 0.7, period = 5
    -- c1 = -vfactor^3 = -0.343
    -- c2 = 3*vfactor^2 + 3*vfactor^3 = 1.029
    -- c3 = -6*vfactor^2 - 3*vfactor - 3*vfactor^3 = -1.029
    -- c4 = 1 + 3*vfactor + 3*vfactor^2 + vfactor^3 = 1.343
    (-0.343 * ema3) + (1.029 * ema2) + (-1.029 * ema1) + (1.343 * adjusted_closing_price) as t3_5
    
FROM t3_calc
ORDER BY ticker, date
