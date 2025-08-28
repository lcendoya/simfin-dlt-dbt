-- Rate of Change (ROC) indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates ROC 10 to match Python script exactly

SELECT 
    *,
    -- Rate of Change 10 (matching Python script exactly)
    -- ROC = ((current_price - price_10_periods_ago) / price_10_periods_ago) * 100
    CASE 
        WHEN LAG(adjusted_closing_price, 10) OVER (PARTITION BY ticker ORDER BY date) = 0 THEN 0
        ELSE ((adjusted_closing_price - LAG(adjusted_closing_price, 10) OVER (PARTITION BY ticker ORDER BY date)) / 
               LAG(adjusted_closing_price, 10) OVER (PARTITION BY ticker ORDER BY date)) * 100
    END as roc_10

FROM {{ ref('stg_price_data') }}
ORDER BY ticker, date
