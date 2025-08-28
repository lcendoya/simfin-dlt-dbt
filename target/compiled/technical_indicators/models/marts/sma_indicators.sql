-- Simple Moving Average (SMA) indicators
-- Calculates various period SMAs for price analysis

SELECT 
    *,
    -- 5-day SMA
    AVG(adjusted_closing_price) OVER (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) as sma_5,
    
    -- 10-day SMA
    AVG(adjusted_closing_price) OVER (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) as sma_10,
    
    -- 20-day SMA (commonly used)
    AVG(adjusted_closing_price) OVER (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) as sma_20,
    
    -- 50-day SMA
    AVG(adjusted_closing_price) OVER (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
    ) as sma_50,
    
    -- 200-day SMA (long-term trend)
    AVG(adjusted_closing_price) OVER (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
    ) as sma_200,
    
    -- Volume SMA for comparison
    AVG(trading_volume) OVER (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) as volume_sma_20

FROM "postgres"."simfin_dbt"."stg_price_data"