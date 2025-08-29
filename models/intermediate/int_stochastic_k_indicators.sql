-- Stochastic %K indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates Stochastic %K 14 to match Python script exactly

SELECT 
    *,
    -- Stochastic %K calculation (matching Python script exactly)
    -- %K = 100 * (close - lowest_low_14) / (highest_high_14 - lowest_low_14)
    CASE 
        WHEN (MAX(highest_price) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) - MIN(lowest_price) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        )) = 0 THEN 0
        ELSE 100 * (
            adjusted_closing_price - MIN(lowest_price) OVER (
                PARTITION BY ticker 
                ORDER BY date 
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            )
        ) / (
            MAX(highest_price) OVER (
                PARTITION BY ticker 
                ORDER BY date 
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            ) - MIN(lowest_price) OVER (
                PARTITION BY ticker 
                ORDER BY date 
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            )
        )
    END as stoch_k_14

FROM {{ ref('stg_price_data') }}
