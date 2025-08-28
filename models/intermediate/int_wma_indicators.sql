-- WMA (Weighted Moving Average) indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates WMA 10 to match Python script exactly

SELECT 
    *,
    -- WMA 10: Used in main WMA calculation
    -- Weighted average where recent prices have higher weight
    (10 * adjusted_closing_price + 
     9 * LAG(adjusted_closing_price, 1) OVER (PARTITION BY ticker ORDER BY date) +
     8 * LAG(adjusted_closing_price, 2) OVER (PARTITION BY ticker ORDER BY date) +
     7 * LAG(adjusted_closing_price, 3) OVER (PARTITION BY ticker ORDER BY date) +
     6 * LAG(adjusted_closing_price, 4) OVER (PARTITION BY ticker ORDER BY date) +
     5 * LAG(adjusted_closing_price, 5) OVER (PARTITION BY ticker ORDER BY date) +
     4 * LAG(adjusted_closing_price, 6) OVER (PARTITION BY ticker ORDER BY date) +
     3 * LAG(adjusted_closing_price, 7) OVER (PARTITION BY ticker ORDER BY date) +
     2 * LAG(adjusted_closing_price, 8) OVER (PARTITION BY ticker ORDER BY date) +
     1 * LAG(adjusted_closing_price, 9) OVER (PARTITION BY ticker ORDER BY date)) / 55 as wma_10

FROM {{ ref('stg_price_data') }}
ORDER BY ticker, date
