{{ config(materialized='incremental', unique_key=['ticker', 'date']) }}

-- MACD (Moving Average Convergence Divergence) indicators
-- Intermediate layer: Business logic and calculations
-- References existing EMAs from int_ema_indicators and calculates MACD lines and signals

WITH ema_data AS (
    SELECT 
        ticker, date, ema_9, ema_12, ema_26
    FROM {{ ref('int_ema_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
macd_calc AS (
    SELECT 
        *,
        -- MACD line = EMA(12) - EMA(26)
        ema_12 - ema_26 as macd_line,
        
        -- MACDEXT line (default EMA-based, matching Python script exactly)
        ema_12 - ema_26 as macdext_line
    FROM ema_data
),
macd_signal AS (
    SELECT 
        *,
        -- MACD signal line = EMA(9) of MACD line
        AVG(macd_line) OVER (
            PARTITION BY ticker
            ORDER BY date
            ROWS BETWEEN 8 PRECEDING AND CURRENT ROW
        ) as macd_signal,
        
        -- MACDEXT signal line = EMA(9) of MACDEXT line
        AVG(macdext_line) OVER (
            PARTITION BY ticker
            ORDER BY date
            ROWS BETWEEN 8 PRECEDING AND CURRENT ROW
        ) as macdext_signal
    FROM macd_calc
)
SELECT 
    -- Base price data
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
    p.price_change,
    
    -- Standard MACD components
    m.macd_line,
    m.macd_signal,
    
    -- MACDEXT components (default EMA-based, matching Python script exactly)
    m.macdext_line,
    m.macdext_signal

FROM {{ ref('stg_price_data') }} p
LEFT JOIN macd_signal m ON p.ticker = m.ticker AND p.date = m.date
