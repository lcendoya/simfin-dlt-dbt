-- Ultimate Oscillator (ULTOSC) indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates ULTOSC using periods 7, 14, 28 to match Python script exactly

WITH price_data AS (
    SELECT 
        *,
        -- Buying Pressure: close - min(low, close_prev)
        adjusted_closing_price - LEAST(lowest_price, LAG(adjusted_closing_price, 1) OVER (PARTITION BY ticker ORDER BY date)) as bp,
        
        -- True Range: max(high, close_prev) - min(low, close_prev)
        GREATEST(highest_price, LAG(adjusted_closing_price, 1) OVER (PARTITION BY ticker ORDER BY date)) - 
        LEAST(lowest_price, LAG(adjusted_closing_price, 1) OVER (PARTITION BY ticker ORDER BY date)) as tr
    FROM {{ ref('stg_price_data') }}
),
ultosc_calc AS (
    SELECT 
        *,
        -- Average BP/TR over 7 periods
        CASE 
            WHEN SUM(tr) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) = 0 THEN 0
            ELSE SUM(bp) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) / 
                 SUM(tr) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
        END as avg7,
        
        -- Average BP/TR over 14 periods
        CASE 
            WHEN SUM(tr) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) = 0 THEN 0
            ELSE SUM(bp) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) / 
                 SUM(tr) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW)
        END as avg14,
        
        -- Average BP/TR over 28 periods
        CASE 
            WHEN SUM(tr) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 27 PRECEDING AND CURRENT ROW) = 0 THEN 0
            ELSE SUM(bp) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 27 PRECEDING AND CURRENT ROW) / 
                 SUM(tr) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)
        END as avg28
    FROM price_data
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
    
    -- Ultimate Oscillator calculation (matching Python script exactly)
    -- ULTOSC = 100 * (4*avg7 + 2*avg14 + avg28) / (4 + 2 + 1)
    100 * (4 * avg7 + 2 * avg14 + avg28) / 7 as ultosc
    
FROM ultosc_calc
