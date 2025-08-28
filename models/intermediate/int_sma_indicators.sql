-- Simple Moving Average (SMA) indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates SMA periods actually used in IndicatorData.py

SELECT 
    *,
    -- 9-day SMA: Used in MACDEXT signal line
    AVG(adjusted_closing_price) OVER (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS BETWEEN 8 PRECEDING AND CURRENT ROW
    ) as sma_9,
    
    -- 12-day SMA: Used in MACDEXT fast period
    AVG(adjusted_closing_price) OVER (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) as sma_12,
    
    -- 20-day SMA: Used in Bollinger Bands and main SMA calculation
    AVG(adjusted_closing_price) OVER (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) as sma_20,
    
    -- 26-day SMA: Used in MACDEXT slow period
    AVG(adjusted_closing_price) OVER (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS BETWEEN 25 PRECEDING AND CURRENT ROW
    ) as sma_26

FROM {{ ref('stg_price_data') }}
