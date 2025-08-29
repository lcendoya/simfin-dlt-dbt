-- Aroon Oscillator indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates Aroon Oscillator 25 to match Python script exactly

WITH aroon_data AS (
    SELECT 
        a.ticker, a.date, a.aroon_up_25, b.aroon_down_25
    FROM {{ ref('int_aroon_up_indicators') }} a
    JOIN {{ ref('int_aroon_down_indicators') }} b ON a.ticker = b.ticker AND a.date = b.date
)
SELECT 
    p.*,
    -- Aroon Oscillator calculation (matching Python script exactly)
    -- Aroon Oscillator = Aroon Up - Aroon Down
    a.aroon_up_25 - a.aroon_down_25 as aroon_oscillator_25
    
FROM {{ ref('stg_price_data') }} p
LEFT JOIN aroon_data a ON p.ticker = a.ticker AND p.date = a.date
