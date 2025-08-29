-- CMO (Chande Momentum Oscillator) indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates CMO 14 to match Python script exactly

WITH cmo_calc AS (
    SELECT 
        *,
        -- Gain: only positive changes
        CASE WHEN price_change > 0 THEN price_change ELSE 0 END as gain,
        -- Loss: only negative changes (absolute)
        CASE WHEN price_change < 0 THEN ABS(price_change) ELSE 0 END as loss
    FROM {{ ref('stg_price_data') }}
),
cmo_final AS (
    SELECT 
        *,
        -- Sum of gains over 14 periods
        SUM(gain) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) as gain_sum_14,
        
        -- Sum of losses over 14 periods
        SUM(loss) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) as loss_sum_14
    FROM cmo_calc
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
    
    -- CMO calculation (matching Python script exactly)
    -- CMO = 100 * (gain - loss) / (gain + loss)
    CASE 
        WHEN (gain_sum_14 + loss_sum_14) = 0 THEN 0
        ELSE 100 * (gain_sum_14 - loss_sum_14) / (gain_sum_14 + loss_sum_14)
    END as cmo_14
    
FROM cmo_final
