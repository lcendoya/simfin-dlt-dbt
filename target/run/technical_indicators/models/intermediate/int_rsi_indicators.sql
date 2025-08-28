
  create view "postgres"."simfin_dbt"."int_rsi_indicators__dbt_tmp"
    
    
  as (
    -- RSI (Relative Strength Index) indicators
-- Intermediate layer: Business logic and calculations

WITH price_changes AS (
    SELECT 
        *,
        adjusted_closing_price - LAG(adjusted_closing_price) OVER (
            PARTITION BY ticker ORDER BY date
        ) as price_change
    FROM "postgres"."simfin_dbt"."stg_price_data"
),
rsi_calc AS (
    SELECT 
        *,
        CASE WHEN price_change > 0 THEN price_change ELSE 0 END as gain,
        CASE WHEN price_change < 0 THEN ABS(price_change) ELSE 0 END as loss
    FROM price_changes
),
rsi_final AS (
    SELECT 
        *,
        -- RSI 14 (most common)
        AVG(gain) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) as avg_gain_14,
        AVG(loss) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) as avg_loss_14,
        -- RSI 7 (shorter period)
        AVG(gain) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as avg_gain_7,
        AVG(loss) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as avg_loss_7,
        -- RSI 21 (longer period)
        AVG(gain) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
        ) as avg_gain_21,
        AVG(loss) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
        ) as avg_loss_21
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
    -- RSI calculations
    CASE 
        WHEN avg_loss_7 = 0 THEN 100
        ELSE 100 - (100 / (1 + (avg_gain_7 / avg_loss_7)))
    END as rsi_7,
    CASE 
        WHEN avg_loss_14 = 0 THEN 100
        ELSE 100 - (100 / (1 + (avg_gain_14 / avg_loss_14)))
    END as rsi_14,
    CASE 
        WHEN avg_loss_21 = 0 THEN 100
        ELSE 100 - (100 / (1 + (avg_gain_21 / avg_loss_21)))
    END as rsi_21
FROM rsi_final
ORDER BY ticker, date
  );