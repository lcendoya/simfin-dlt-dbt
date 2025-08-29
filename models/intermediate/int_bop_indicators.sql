-- Balance of Power (BOP) indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates BOP to match Python script exactly

SELECT 
    *,
    -- Balance of Power (matching Python script exactly)
    -- BOP = (close - open) / (high - low)
    CASE 
        WHEN (highest_price - lowest_price) = 0 THEN 0
        ELSE (adjusted_closing_price - opening_price) / (highest_price - lowest_price)
    END as bop

FROM {{ ref('stg_price_data') }}
