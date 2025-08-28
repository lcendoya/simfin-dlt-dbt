-- Williams %R (WILLR) indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates WILLR 14 to match Python script exactly

SELECT 
    *,
    -- Williams %R 14 (matching Python script exactly)
    -- WILLR = -100 * (highest_high_14 - close) / (highest_high_14 - lowest_low_14)
    -100 * (
        (adjusted_closing_price - MIN(highest_price) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        )) / 
        (MAX(highest_price) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) - MIN(lowest_price) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ))
    ) as willr_14

FROM {{ ref('stg_price_data') }}
ORDER BY ticker, date
