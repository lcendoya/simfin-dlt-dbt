-- Exponential Moving Average (EMA) indicators
-- Intermediate layer: Business logic and calculations
-- Uses window functions to calculate EMAs properly

WITH price_data AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date) as rn
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
    -- Calculate only the EMA periods used in IndicatorData.py
    
    -- EMA 9: Used in MACD signal line
    CASE 
        WHEN rn <= 9 THEN 
            AVG(adjusted_closing_price) OVER (
                PARTITION BY ticker 
                ORDER BY date 
                ROWS BETWEEN 8 PRECEDING AND CURRENT ROW
            )
        ELSE 
            AVG(adjusted_closing_price) OVER (
                PARTITION BY ticker 
                ORDER BY date 
                ROWS BETWEEN 8 PRECEDING AND CURRENT ROW
            )
    END as ema_9,
    
    -- EMA 10: Used in main EMA calculation and DEMA/TEMA
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
    
    -- EMA 12: Used in MACD fast period
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
    
    -- EMA 14: Used in ADX calculations
    CASE 
        WHEN rn <= 14 THEN 
            AVG(adjusted_closing_price) OVER (
                PARTITION BY ticker 
                ORDER BY date 
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            )
        ELSE 
            AVG(adjusted_closing_price) OVER (
                PARTITION BY ticker 
                ORDER BY date 
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            )
    END as ema_14,
    
    -- EMA 26: Used in MACD slow period
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
    END as ema_26
FROM price_data
ORDER BY ticker, date
