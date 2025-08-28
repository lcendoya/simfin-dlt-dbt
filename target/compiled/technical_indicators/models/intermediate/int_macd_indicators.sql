-- MACD (Moving Average Convergence Divergence) indicators
-- Intermediate layer: Business logic and calculations

WITH ema_calc AS (
    SELECT 
        *,
        -- Fast EMA (12 periods)
        CASE 
            WHEN ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date) = 1 
            THEN adjusted_closing_price
            ELSE adjusted_closing_price * (2.0/13.0) + 
                 LAG(adjusted_closing_price) OVER (PARTITION BY ticker ORDER BY date) * (11.0/13.0)
        END as ema_12,
        -- Slow EMA (26 periods)
        CASE 
            WHEN ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date) = 1 
            THEN adjusted_closing_price
            ELSE adjusted_closing_price * (2.0/27.0) + 
                 LAG(adjusted_closing_price) OVER (PARTITION BY ticker ORDER BY date) * (25.0/27.0)
        END as ema_26
    FROM "postgres"."simfin_dbt"."stg_price_data"
),
macd_calc AS (
    SELECT 
        *,
        ema_12 - ema_26 as macd_line
    FROM ema_calc
),
macd_signal AS (
    SELECT 
        *,
        -- Signal line (9-period EMA of MACD)
        CASE 
            WHEN ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date) = 1 
            THEN macd_line
            ELSE macd_line * (2.0/10.0) + 
                 LAG(macd_line) OVER (PARTITION BY ticker ORDER BY date) * (8.0/10.0)
        END as macd_signal
    FROM macd_calc
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
    ema_12,
    ema_26,
    macd_line,
    macd_signal,
    macd_line - macd_signal as macd_histogram,
    -- MACD percentage
    CASE 
        WHEN LAG(macd_line) OVER (PARTITION BY ticker ORDER BY date) = 0 THEN 0
        ELSE ((macd_line - LAG(macd_line) OVER (PARTITION BY ticker ORDER BY date)) / 
               LAG(macd_line) OVER (PARTITION BY ticker ORDER BY date)) * 100
    END as macd_percentage
FROM macd_signal
ORDER BY ticker, date