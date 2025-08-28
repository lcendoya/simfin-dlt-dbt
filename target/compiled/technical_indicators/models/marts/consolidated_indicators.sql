

-- Consolidated Technical Indicators
-- Combines all calculated indicators into one comprehensive table
-- Uses incremental materialization for efficient processing of new data

WITH sma_data AS (
    SELECT 
        company_id, company_name, ticker, currency, isin, date,
        dividend_paid, common_shares_outstanding, last_closing_price,
        adjusted_closing_price, highest_price, lowest_price, opening_price,
        trading_volume, daily_range, daily_return_pct,
        sma_5, sma_10, sma_20, sma_50, sma_200, volume_sma_20
    FROM "postgres"."simfin_dbt"."int_sma_indicators"
    
      WHERE date > (SELECT MAX(date) FROM "postgres"."simfin_dbt"."consolidated_indicators")
    
),
ema_data AS (
    SELECT 
        ticker, date,
        ema_5, ema_10, ema_12, ema_20, ema_26, ema_50
    FROM "postgres"."simfin_dbt"."int_ema_indicators"
    
      WHERE date > (SELECT MAX(date) FROM "postgres"."simfin_dbt"."consolidated_indicators")
    
),
rsi_data AS (
    SELECT 
        ticker, date,
        rsi_7, rsi_14, rsi_21, price_change
    FROM "postgres"."simfin_dbt"."int_rsi_indicators"
    
      WHERE date > (SELECT MAX(date) FROM "postgres"."simfin_dbt"."consolidated_indicators")
    
),
macd_data AS (
    SELECT 
        ticker, date,
        macd_line, macd_signal, macd_histogram, macd_percentage
    FROM "postgres"."simfin_dbt"."int_macd_indicators"
    
      WHERE date > (SELECT MAX(date) FROM "postgres"."simfin_dbt"."consolidated_indicators")
    
),
bb_data AS (
    SELECT 
        ticker, date,
        bb_upper_20, bb_middle_20, bb_lower_20, bb_width_20, bb_percent_b_20
    FROM "postgres"."simfin_dbt"."int_bollinger_bands"
    
      WHERE date > (SELECT MAX(date) FROM "postgres"."simfin_dbt"."consolidated_indicators")
    
),
vwap_data AS (
    SELECT 
        ticker, date,
        vwap_cumulative, vwap_20, volume_ratio_20, price_vs_vwap_pct
    FROM "postgres"."simfin_dbt"."int_vwap_indicators"
    
      WHERE date > (SELECT MAX(date) FROM "postgres"."simfin_dbt"."consolidated_indicators")
    
)
SELECT 
    -- Base price data (from sma_data since it contains all base columns)
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
    END as macd_trading_signal,
    
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