-- Exponential Moving Average (EMA) indicators
-- Uses recursive CTEs to calculate EMAs properly

WITH RECURSIVE ema_calc AS (
    -- Base case: first row for each ticker
    SELECT 
        *,
        adjusted_closing_price as ema_5,
        adjusted_closing_price as ema_10,
        adjusted_closing_price as ema_12,
        adjusted_closing_price as ema_20,
        adjusted_closing_price as ema_26,
        adjusted_closing_price as ema_50,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date) as rn
    FROM "postgres"."simfin_dbt"."stg_price_data"
    
    UNION ALL
    
    -- Recursive case: calculate EMAs for subsequent rows
    SELECT 
        p.*,
        -- EMA 5 (smoothing factor = 2/(5+1) = 0.333)
        p.adjusted_closing_price * 0.333 + e.ema_5 * 0.667 as ema_5,
        -- EMA 10 (smoothing factor = 2/(10+1) = 0.182)
        p.adjusted_closing_price * 0.182 + e.ema_10 * 0.818 as ema_10,
        -- EMA 12 (smoothing factor = 2/(12+1) = 0.154)
        p.adjusted_closing_price * 0.154 + e.ema_12 * 0.846 as ema_12,
        -- EMA 20 (smoothing factor = 2/(20+1) = 0.095)
        p.adjusted_closing_price * 0.095 + e.ema_20 * 0.905 as ema_20,
        -- EMA 26 (smoothing factor = 2/(26+1) = 0.074)
        p.adjusted_closing_price * 0.074 + e.ema_26 * 0.926 as ema_26,
        -- EMA 50 (smoothing factor = 2/(50+1) = 0.039)
        p.adjusted_closing_price * 0.039 + e.ema_50 * 0.961 as ema_50,
        p.rn
    FROM "postgres"."simfin_dbt"."stg_price_data" p
    INNER JOIN ema_calc e ON p.ticker = e.ticker 
        AND p.rn = e.rn + 1
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
    ema_5,
    ema_10,
    ema_12,
    ema_20,
    ema_26,
    ema_50
FROM ema_calc
ORDER BY ticker, date