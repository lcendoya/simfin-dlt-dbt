-- Midpoint indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates Midpoint 14 to match Python script exactly

SELECT 
    *,
    -- Midpoint calculation (matching Python script exactly)
    -- Midpoint = (max(close_14) + min(close_14)) / 2
    (
        MAX(adjusted_closing_price) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) + 
        MIN(adjusted_closing_price) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        )
    ) / 2 as midpoint_14

FROM {{ ref('stg_price_data') }}
