-- MACD (Moving Average Convergence Divergence) indicators
-- Intermediate layer: Business logic and calculations
-- References existing EMA calculations from int_ema_indicators

WITH ema_data AS (
    SELECT 
        ticker, date, ema_9, ema_12, ema_26
    FROM {{ ref('int_ema_indicators') }}
),
macd_calc AS (
    SELECT 
        *,
        -- Standard MACD (always uses EMAs)
        ema_12 - ema_26 as macd_line,
        
        -- MACDEXT (using default parameters: all EMAs, same as standard MACD)
        ema_12 - ema_26 as macdext_line
    FROM ema_data
),
macd_signal AS (
    SELECT 
        *,
        -- Standard MACD signal line (9-period EMA of MACD line)
        AVG(macd_line) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 8 PRECEDING AND CURRENT ROW
        ) as macd_signal,
        
        -- MACDEXT signal line (9-period EMA of MACDEXT line, same as standard MACD signal)
        AVG(macdext_line) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 8 PRECEDING AND CURRENT ROW
        ) as macdext_signal
    FROM macd_calc
)
SELECT 
    ticker,
    date,
    
    -- Standard MACD components
    ema_12,
    ema_26,
    ema_9,
    macd_line,
    macd_signal,
    macd_line - macd_signal as macd_histogram,
    
    -- MACD percentage
    CASE 
        WHEN LAG(macd_line) OVER (PARTITION BY ticker ORDER BY date) = 0 THEN 0
        ELSE ((macd_line - LAG(macd_line) OVER (PARTITION BY ticker ORDER BY date)) / 
               LAG(macd_line) OVER (PARTITION BY ticker ORDER BY date)) * 100
    END as macd_percentage,
    
    -- MACDEXT components (matching Python script exactly)
    macdext_line,
    macdext_signal
    
FROM macd_signal
ORDER BY ticker, date
