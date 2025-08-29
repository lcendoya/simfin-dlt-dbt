-- DEMA (Double Exponential Moving Average) and TEMA (Triple Exponential Moving Average) indicators
-- Intermediate layer: Business logic and calculations
-- References existing EMA-10 from int_ema_indicators and calculates derived indicators

WITH RECURSIVE ema_data AS (
    SELECT 
        ticker, date, ema_10,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date) as rn
    FROM {{ ref('int_ema_indicators') }}
),
ema_of_ema_recursive AS (
    SELECT 
        ticker, date, ema_10, rn,
        -- Base case: first row gets the EMA_10 value
        ema_10 as ema_of_ema_10
    FROM ema_data
    WHERE rn = 1
    
    UNION ALL
    
    SELECT 
        e.ticker, e.date, e.ema_10, e.rn,
        -- Recursive case: EMA[i] = alpha * current_value + (1-alpha) * previous_EMA
        -- alpha = 2/(10+1) = 0.1818
        0.1818 * e.ema_10 + 0.8182 * r.ema_of_ema_10 as ema_of_ema_10
    FROM ema_data e
    INNER JOIN ema_of_ema_recursive r ON e.ticker = r.ticker AND e.rn = r.rn + 1
),
ema_of_ema_of_ema_recursive AS (
    SELECT 
        ticker, date, ema_10, rn, ema_of_ema_10,
        -- Base case: first row gets the EMA of EMA value
        ema_of_ema_10 as ema_of_ema_of_ema_10
    FROM ema_of_ema_recursive
    WHERE rn = 1
    
    UNION ALL
    
    SELECT 
        e.ticker, e.date, e.ema_10, e.rn, e.ema_of_ema_10,
        -- Recursive case: EMA[i] = alpha * current_value + (1-alpha) * previous_EMA
        -- alpha = 2/(10+1) = 0.1818
        0.1818 * e.ema_of_ema_10 + 0.8182 * r.ema_of_ema_of_ema_10 as ema_of_ema_of_ema_10
    FROM ema_of_ema_recursive e
    INNER JOIN ema_of_ema_of_ema_recursive r ON e.ticker = r.ticker AND e.rn = r.rn + 1
),
dema_tema_final AS (
    SELECT 
        ticker, date,
        -- DEMA = 2 * EMA - EMA(EMA)
        2 * ema_10 - ema_of_ema_10 as dema_10,
        
        -- TEMA = 3 * EMA - 3 * EMA(EMA) + EMA(EMA(EMA))
        3 * ema_10 - 3 * ema_of_ema_10 + ema_of_ema_of_ema_10 as tema_10
    FROM ema_of_ema_of_ema_recursive
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
    
    -- DEMA and TEMA indicators (matching Python script exactly)
    d.dema_10,
    d.tema_10
    
FROM {{ ref('stg_price_data') }} p
LEFT JOIN dema_tema_final d ON p.ticker = d.ticker AND p.date = d.date
