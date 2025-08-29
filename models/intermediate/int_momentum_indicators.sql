-- Momentum (MOM) indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates MOM 10 to match Python script exactly

SELECT 
    *,
    -- Momentum 10 (matching Python script exactly)
    -- MOM = current_price - price_10_periods_ago
    adjusted_closing_price - LAG(adjusted_closing_price, 10) OVER (
        PARTITION BY ticker ORDER BY date
    ) as mom_10

FROM {{ ref('stg_price_data') }}
