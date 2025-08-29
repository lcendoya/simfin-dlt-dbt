{{ config(materialized='incremental', unique_key=['ticker', 'date']) }}

-- RSI (Relative Strength Index) indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates RSI for period 14 as used in IndicatorData.py

WITH price_changes AS (
    SELECT 
        *,
        -- Use existing price_change, handle NULL for first row
        COALESCE(price_change, 0) as price_change_for_rsi
    FROM {{ ref('stg_price_data') }}
    -- Note: RSI needs historical data for 14-day window calculations
    -- Incremental filtering is handled at the final SELECT level
),
rsi_calc AS (
    SELECT 
        *,
        CASE WHEN price_change_for_rsi > 0 THEN price_change_for_rsi ELSE 0 END as gain,
        CASE WHEN price_change_for_rsi < 0 THEN ABS(price_change_for_rsi) ELSE 0 END as loss
    FROM price_changes
),
rsi_final AS (
    SELECT 
        *,
        -- RSI 14 (most common) - matches Python script exactly
        AVG(gain) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) as avg_gain_14,
        AVG(loss) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) as avg_loss_14
    FROM rsi_calc
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
    price_change,
    -- RSI calculation (matching Python script exactly)
    CASE 
        WHEN avg_loss_14 = 0 THEN 100
        ELSE 100 - (100 / (1 + (avg_gain_14 / avg_loss_14)))
    END as rsi_14
FROM rsi_final
{% if is_incremental() %}
  WHERE date > (SELECT MAX(date) FROM {{ this }})
{% endif %}
