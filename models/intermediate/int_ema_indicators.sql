{{ config(materialized='incremental', unique_key=['ticker', 'date']) }}

-- EMA (Exponential Moving Average) indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates EMAs for periods used in IndicatorData.py

WITH price_data AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date) as rn
    FROM {{ ref('stg_price_data') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
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
    AVG(adjusted_closing_price) OVER (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS BETWEEN 8 PRECEDING AND CURRENT ROW
    ) as ema_9,
    
    -- EMA 10: Used in main EMA calculation and DEMA/TEMA
    AVG(adjusted_closing_price) OVER (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) as ema_10,
    
    -- EMA 12: Used in MACD fast period
    AVG(adjusted_closing_price) OVER (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) as ema_12,
    
    -- EMA 14: Used in ADX calculations
    AVG(adjusted_closing_price) OVER (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    ) as ema_14,
    
    -- EMA 20: Used in HT Trendline (span=20)
    AVG(adjusted_closing_price) OVER (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) as ema_20,
    
    -- EMA 26: Used in MACD slow period
    AVG(adjusted_closing_price) OVER (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS BETWEEN 25 PRECEDING AND CURRENT ROW
    ) as ema_26
FROM price_data
