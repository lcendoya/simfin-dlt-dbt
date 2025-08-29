-- HT DC Phase indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates HT DC Phase to match Python script exactly

SELECT 
    *,
    -- HT DC Phase calculation (matching Python script exactly)
    -- HT DC Phase = np.angle(series) where series is real numbers
    -- For real numbers, np.angle() returns 0 (no imaginary component)
    0 as ht_dc_phase

FROM {{ ref('stg_price_data') }}
