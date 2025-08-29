-- HT Lead Sine indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates HT Lead Sine to match Python script exactly

SELECT 
    *,
    -- HT Lead Sine calculation (matching Python script exactly)
    -- HT Lead Sine = sin(price) shifted forward by 1 period (np.roll(sine, -1))
    LEAD(SIN(adjusted_closing_price), 1) OVER (
        PARTITION BY ticker 
        ORDER BY date
    ) as ht_lead_sine

FROM {{ ref('stg_price_data') }}
