-- DEMA (Double Exponential Moving Average) and TEMA (Triple Exponential Moving Average) indicators
-- Intermediate layer: Business logic and calculations
-- References existing EMA-10 from int_ema_indicators and calculates derived indicators

WITH ema_data AS (
    SELECT 
        ticker, date, ema_10
    FROM {{ ref('int_ema_indicators') }}
),
dema_tema_calc AS (
    SELECT 
        *,
        -- DEMA = 2 * EMA - EMA(EMA)
        -- First calculate EMA of EMA-10
        AVG(ema_10) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) as ema_of_ema_10,
        
        -- Then calculate DEMA: 2 * EMA_10 - EMA(EMA_10)
        2 * ema_10 - AVG(ema_10) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) as dema_10
    FROM ema_data
),
tema_calc AS (
    SELECT 
        *,
        -- TEMA = 3 * EMA - 3 * EMA(EMA) + EMA(EMA(EMA))
        -- First calculate EMA of EMA of EMA-10
        AVG(ema_of_ema_10) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) as ema_of_ema_of_ema_10,
        
        -- Then calculate TEMA: 3 * EMA_10 - 3 * EMA(EMA_10) + EMA(EMA(EMA_10))
        3 * ema_10 - 3 * ema_of_ema_10 + ema_of_ema_of_ema_10 as tema_10
    FROM dema_tema_calc
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
    
    -- DEMA and TEMA indicators (matching Python script exactly)
    dema_10,
    tema_10
    
FROM tema_calc
ORDER BY ticker, date
