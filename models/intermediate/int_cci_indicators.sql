-- CCI (Commodity Channel Index) indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates CCI 20 to match Python script exactly

WITH price_data AS (
    SELECT 
        *,
        -- Typical Price: (high + low + close) / 3
        (highest_price + lowest_price + adjusted_closing_price) / 3 as typical_price
    FROM {{ ref('stg_price_data') }}
),
sma_calc AS (
    SELECT 
        *,
        -- SMA of Typical Price over 20 periods
        AVG(typical_price) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) as sma_tp_20
    FROM price_data
),
mad_calc AS (
    SELECT 
        *,
        -- Calculate absolute deviation from SMA
        ABS(typical_price - sma_tp_20) as abs_deviation
    FROM sma_calc
),
cci_calc AS (
    SELECT 
        *,
        -- Mean Absolute Deviation (MAD) over 20 periods
        AVG(abs_deviation) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) as mad_20
    FROM mad_calc
)
SELECT 
    company_id,
    company_name,
    ticker,
    currency,
    isin,
    date,
    dividend_paid,
    common_shares_outstanding,
    last_closing_price,
    adjusted_closing_price,
    highest_price,
    lowest_price,
    opening_price,
    trading_volume,
    daily_range,
    daily_return_pct,
    
    -- CCI calculation (matching Python script exactly)
    -- CCI = (typical_price - sma_tp) / (0.015 * mad)
    CASE 
        WHEN mad_20 = 0 THEN 0
        ELSE (typical_price - sma_tp_20) / (0.015 * mad_20)
    END as cci_20
    
FROM cci_calc
