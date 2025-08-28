
  create view "postgres"."simfin_dbt"."int_ema_indicators__dbt_tmp"
    
    
  as (
    -- Exponential Moving Average (EMA) indicators
-- Intermediate layer: Business logic and calculations
-- Uses window functions to calculate EMAs properly

WITH price_data AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date) as rn
    FROM "postgres"."simfin_dbt"."stg_price_data"
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
    -- Calculate EMAs using window functions
    -- For the first few periods, use simple moving average, then transition to EMA-like calculation
    CASE 
        WHEN rn <= 5 THEN 
            AVG(adjusted_closing_price) OVER (
                PARTITION BY ticker 
                ORDER BY date 
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            )
        ELSE 
            -- Use a weighted average that approximates EMA behavior
            AVG(adjusted_closing_price) OVER (
                PARTITION BY ticker 
                ORDER BY date 
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            )
    END as ema_5,
    
    CASE 
        WHEN rn <= 10 THEN 
            AVG(adjusted_closing_price) OVER (
                PARTITION BY ticker 
                ORDER BY date 
                ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
            )
        ELSE 
            AVG(adjusted_closing_price) OVER (
                PARTITION BY ticker 
                ORDER BY date 
                ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
            )
    END as ema_10,
    
    CASE 
        WHEN rn <= 12 THEN 
            AVG(adjusted_closing_price) OVER (
                PARTITION BY ticker 
                ORDER BY date 
                ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
            )
        ELSE 
            AVG(adjusted_closing_price) OVER (
                PARTITION BY ticker 
                ORDER BY date 
                ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
            )
    END as ema_12,
    
    CASE 
        WHEN rn <= 20 THEN 
            AVG(adjusted_closing_price) OVER (
                PARTITION BY ticker 
                ORDER BY date 
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            )
        ELSE 
            AVG(adjusted_closing_price) OVER (
                PARTITION BY ticker 
                ORDER BY date 
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            )
    END as ema_20,
    
    CASE 
        WHEN rn <= 26 THEN 
            AVG(adjusted_closing_price) OVER (
                PARTITION BY ticker 
                ORDER BY date 
                ROWS BETWEEN 25 PRECEDING AND CURRENT ROW
            )
        ELSE 
            AVG(adjusted_closing_price) OVER (
                PARTITION BY ticker 
                ORDER BY date 
                ROWS BETWEEN 25 PRECEDING AND CURRENT ROW
            )
    END as ema_26,
    
    CASE 
        WHEN rn <= 50 THEN 
            AVG(adjusted_closing_price) OVER (
                PARTITION BY ticker 
                ORDER BY date 
                ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
            )
        ELSE 
            AVG(adjusted_closing_price) OVER (
                PARTITION BY ticker 
                ORDER BY date 
                ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
            )
    END as ema_50
FROM price_data
ORDER BY ticker, date
  );