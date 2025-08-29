-- Stochastic %D indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates Stochastic %D 3 to match Python script exactly

WITH stoch_k_data AS (
    SELECT 
        ticker, date, stoch_k_14
    FROM {{ ref('int_stochastic_k_indicators') }}
)
SELECT 
    p.*,
    -- Stochastic %D calculation (matching Python script exactly)
    -- %D = 3-period average of %K
    AVG(s.stoch_k_14) OVER (
        PARTITION BY p.ticker 
        ORDER BY p.date 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) as stoch_d_3
    
FROM {{ ref('stg_price_data') }} p
LEFT JOIN stoch_k_data s ON p.ticker = s.ticker AND p.date = s.date
