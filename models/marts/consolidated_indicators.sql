{{ config(materialized='incremental', unique_key=['ticker', 'date']) }}

-- Consolidated Technical Indicators
-- Combines all calculated indicators into one comprehensive table
-- Uses incremental materialization for efficient processing of new data

WITH sma_data AS (
    SELECT * FROM {{ ref('int_sma_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
ema_data AS (
    SELECT * FROM {{ ref('int_ema_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
rsi_data AS (
    SELECT * FROM {{ ref('int_rsi_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
macd_data AS (
    SELECT * FROM {{ ref('int_macd_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
bb_data AS (
    SELECT * FROM {{ ref('int_bollinger_bands') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
vwap_data AS (
    SELECT * FROM {{ ref('int_vwap_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
)
SELECT 
    -- Base price data
    s.company_id,
    s.company_name,
    s.ticker,
    s.currency,
    s.isin,
    s.date,
    s.dividend_paid,
    s.common_shares_outstanding,
    s.last_closing_price,
    s.adjusted_closing_price,
    s.highest_price,
    s.lowest_price,
    s.opening_price,
    s.trading_volume,
    s.daily_range,
    s.daily_return_pct,
    
    -- SMA indicators
    s.sma_5,
    s.sma_10,
    s.sma_20,
    s.sma_50,
    s.sma_200,
    s.volume_sma_20,
    
    -- EMA indicators
    e.ema_5,
    e.ema_10,
    e.ema_12,
    e.ema_20,
    e.ema_26,
    e.ema_50,
    
    -- RSI indicators
    r.rsi_7,
    r.rsi_14,
    r.rsi_21,
    r.price_change,
    
    -- MACD indicators
    m.macd_line,
    m.macd_signal,
    m.macd_histogram,
    m.macd_percentage,
    
    -- Bollinger Bands
    bb.bb_upper_20,
    bb.bb_middle_20,
    bb.bb_lower_20,
    bb.bb_width_20,
    bb.bb_percent_b_20,
    
    -- VWAP indicators
    v.vwap_cumulative,
    v.vwap_20,
    v.volume_ratio_20,
    v.price_vs_vwap_pct,
    
    -- Additional derived indicators
    -- Price vs moving averages
    CASE 
        WHEN s.sma_20 = 0 THEN 0
        ELSE ((s.adjusted_closing_price - s.sma_20) / s.sma_20) * 100
    END as price_vs_sma_20_pct,
    
    CASE 
        WHEN e.ema_20 = 0 THEN 0
        ELSE ((s.adjusted_closing_price - e.ema_20) / e.ema_20) * 100
    END as price_vs_ema_20_pct,
    
    -- Moving average crossovers
    CASE 
        WHEN s.sma_20 > s.sma_50 THEN 1
        WHEN s.sma_20 < s.sma_50 THEN -1
        ELSE 0
    END as sma_crossover_signal,
    
    CASE 
        WHEN e.ema_12 > e.ema_26 THEN 1
        WHEN e.ema_12 < e.ema_26 THEN -1
        ELSE 0
    END as ema_crossover_signal,
    
    -- RSI signals
    CASE 
        WHEN r.rsi_14 > 70 THEN 'Overbought'
        WHEN r.rsi_14 < 30 THEN 'Oversold'
        ELSE 'Neutral'
    END as rsi_signal,
    
    -- MACD signals
    CASE 
        WHEN m.macd_line > m.macd_signal THEN 'Bullish'
        WHEN m.macd_line < m.macd_signal THEN 'Bearish'
        ELSE 'Neutral'
    END as macd_signal,
    
    -- Bollinger Band signals
    CASE 
        WHEN s.adjusted_closing_price > bb.bb_upper_20 THEN 'Above Upper Band'
        WHEN s.adjusted_closing_price < bb.bb_lower_20 THEN 'Below Lower Band'
        ELSE 'Within Bands'
    END as bb_signal,
    
    -- Volume analysis
    CASE 
        WHEN v.volume_ratio_20 > 1.5 THEN 'High Volume'
        WHEN v.volume_ratio_20 < 0.5 THEN 'Low Volume'
        ELSE 'Normal Volume'
    END as volume_signal

FROM sma_data s
LEFT JOIN ema_data e ON s.ticker = e.ticker AND s.date = e.date
LEFT JOIN rsi_data r ON s.ticker = r.ticker AND s.date = r.date
LEFT JOIN macd_data m ON s.ticker = m.ticker AND s.date = m.date
LEFT JOIN bb_data bb ON s.ticker = bb.ticker AND s.date = bb.date
LEFT JOIN vwap_data v ON s.ticker = v.ticker AND s.date = v.date

ORDER BY s.ticker, s.date

-- Note: This model uses incremental materialization
-- - First run: Processes all historical data
-- - Subsequent runs: Only processes new dates since last run
-- - Intermediate models are filtered to only include new data for efficiency
