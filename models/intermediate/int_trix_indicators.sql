-- TRIX indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates TRIX 15 to match Python script exactly

WITH ema_data AS (
    SELECT 
        ticker, date, ema_10
    FROM {{ ref('int_ema_indicators') }}
),
ema1_calc AS (
    SELECT 
        *,
        -- First EMA of price (period 15)
        AVG(adjusted_closing_price) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
        ) as ema1
    FROM {{ ref('stg_price_data') }}
),
ema2_calc AS (
    SELECT 
        *,
        -- Second EMA of EMA1 (period 15)
        AVG(ema1) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
        ) as ema2
    FROM ema1_calc
),
ema3_calc AS (
    SELECT 
        *,
        -- Third EMA of EMA2 (period 15)
        AVG(ema2) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
        ) as ema3
    FROM ema2_calc
)
SELECT 
    p.company_id,
    p.company_name,
    p.ticker,
    p.currency,
    p.isin,
    p.date,
    p.dividend_paid,
    p.common_shares_outstanding,
    p.last_closing_price,
    p.adjusted_closing_price,
    p.highest_price,
    p.lowest_price,
    p.opening_price,
    p.trading_volume,
    p.daily_range,
    p.daily_return_pct,
    
    -- TRIX calculation (matching Python script exactly)
    -- TRIX = 100 * (ema3 - ema3_prev) / ema3_prev
    CASE 
        WHEN LAG(ema3, 1) OVER (PARTITION BY p.ticker ORDER BY p.date) = 0 THEN 0
        ELSE 100 * (ema3 - LAG(ema3, 1) OVER (PARTITION BY p.ticker ORDER BY p.date)) / 
             LAG(ema3, 1) OVER (PARTITION BY p.ticker ORDER BY p.date)
    END as trix_15
    
FROM {{ ref('stg_price_data') }} p
LEFT JOIN ema3_calc e ON p.ticker = e.ticker AND p.date = e.date
