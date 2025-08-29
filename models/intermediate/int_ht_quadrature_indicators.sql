-- HT Quadrature indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates HT Quadrature to match Python script exactly

SELECT 
    *,
    -- HT Quadrature calculation (matching Python script exactly)
    -- HT Quadrature = sin(price)
    SIN(adjusted_closing_price) as ht_quadrature

FROM {{ ref('stg_price_data') }}
