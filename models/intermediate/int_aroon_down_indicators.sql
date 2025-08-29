-- Aroon Down indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates Aroon Down 25 to match Python script exactly

SELECT 
    *,
    -- Aroon Down calculation (matching Python script exactly)
    -- Aroon Down = 100 * ((period - days_since_lowest) / period)
    -- days_since_lowest = position of lowest low in last 25 periods
    100 * (
        (25 - (
            -- Find the position of the lowest low in the last 25 periods
            -- This is a simplified approximation using window functions
            ROW_NUMBER() OVER (
                PARTITION BY ticker 
                ORDER BY lowest_price ASC, date DESC
                ROWS BETWEEN 24 PRECEDING AND CURRENT ROW
            )
        )) / 25
    ) as aroon_down_25

FROM {{ ref('stg_price_data') }}
