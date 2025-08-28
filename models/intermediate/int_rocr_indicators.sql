-- Rate of Change Ratio (ROCR) indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates ROCR 10 to match Python script exactly

SELECT 
    *,
    -- Rate of Change Ratio 10 (matching Python script exactly)
    -- ROCR = current_price / price_10_periods_ago
    CASE 
        WHEN LAG(adjusted_closing_price, 10) OVER (PARTITION BY ticker ORDER BY date) = 0 THEN 0
        ELSE adjusted_closing_price / LAG(adjusted_closing_price, 10) OVER (PARTITION BY ticker ORDER BY date)
    END as rocr_10

FROM {{ ref('stg_price_data') }}
ORDER BY ticker, date
