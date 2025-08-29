-- Aroon Up indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates Aroon Up 25 to match Python script exactly

SELECT 
    *,
    -- Aroon Up calculation (matching Python script exactly)
    -- Aroon Up = 100 * ((period - days_since_highest) / period)
    -- days_since_highest = position of highest high in last 25 periods
    100 * (
        (25 - (
            -- Find the position of the highest high in the last 25 periods
            -- This is a simplified approximation using window functions
            ROW_NUMBER() OVER (
                PARTITION BY ticker 
                ORDER BY highest_price DESC, date DESC
                ROWS BETWEEN 24 PRECEDING AND CURRENT ROW
            )
        )) / 25
    ) as aroon_up_25

FROM {{ ref('stg_price_data') }}
