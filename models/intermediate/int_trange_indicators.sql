-- True Range (TRANGE) indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates TRANGE to match Python script exactly

SELECT 
    *,
    -- True Range calculation (matching Python script exactly)
    -- TR = max(high-low, |high-close_prev|, |low-close_prev|)
    GREATEST(
        highest_price - lowest_price,
        ABS(highest_price - LAG(adjusted_closing_price, 1) OVER (PARTITION BY ticker ORDER BY date)),
        ABS(lowest_price - LAG(adjusted_closing_price, 1) OVER (PARTITION BY ticker ORDER BY date))
    ) as trange

FROM {{ ref('stg_price_data') }}
