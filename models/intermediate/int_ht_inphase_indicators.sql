-- HT Inphase indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates HT Inphase to match Python script exactly

SELECT 
    *,
    -- HT Inphase calculation (matching Python script exactly)
    -- HT Inphase = cos(price)
    COS(adjusted_closing_price) as ht_inphase

FROM {{ ref('stg_price_data') }}
