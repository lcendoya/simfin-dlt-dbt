-- Midprice indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates Midprice 14 to match Python script exactly

SELECT 
    *,
    -- Midprice calculation (matching Python script exactly)
    -- Midprice = (max(high_14) + min(low_14)) / 2
    (
        MAX(highest_price) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) + 
        MIN(lowest_price) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        )
    ) / 2 as midprice_14

FROM {{ ref('stg_price_data') }}
