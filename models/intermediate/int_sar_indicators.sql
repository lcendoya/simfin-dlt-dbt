-- SAR (Parabolic SAR) indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates SAR to match Python script exactly

WITH RECURSIVE price_data AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date) as rn
    FROM {{ ref('stg_price_data') }}
),
sar_recursive AS (
    SELECT 
        *,
        -- Initial SAR = first high
        highest_price as sar_value,
        -- Initial trend = uptrend (True)
        1 as trend,
        -- Initial extreme point = first high
        highest_price as ep,
        -- Initial acceleration factor = 0.02
        0.02 as af
    FROM price_data
    WHERE rn = 1
    
    UNION ALL
    
    SELECT 
        p.*,
        -- SAR calculation based on trend
        CASE 
            WHEN s.trend = 1 THEN  -- Uptrend
                CASE 
                    WHEN p.highest_price > s.ep THEN  -- New high
                        s.sar_value + s.af * (p.highest_price - s.sar_value)
                    ELSE  -- No new high
                        s.sar_value + s.af * (s.ep - s.sar_value)
                END
            ELSE  -- Downtrend
                CASE 
                    WHEN p.lowest_price < s.ep THEN  -- New low
                        s.sar_value + s.af * (p.lowest_price - s.sar_value)
                    ELSE  -- No new low
                        s.sar_value + s.af * (s.ep - s.sar_value)
                END
        END as sar_value,
        
        -- Update trend
        CASE 
            WHEN s.trend = 1 AND p.lowest_price < s.sar_value THEN 0  -- Switch to downtrend
            WHEN s.trend = 0 AND p.highest_price > s.sar_value THEN 1  -- Switch to uptrend
            ELSE s.trend
        END as trend,
        
        -- Update extreme point
        CASE 
            WHEN s.trend = 1 AND p.highest_price > s.ep THEN p.highest_price
            WHEN s.trend = 0 AND p.lowest_price < s.ep THEN p.lowest_price
            ELSE s.ep
        END as ep,
        
        -- Update acceleration factor
        CASE 
            WHEN s.trend = 1 AND p.highest_price > s.ep THEN LEAST(s.af + 0.02, 0.2)
            WHEN s.trend = 0 AND p.lowest_price < s.ep THEN LEAST(s.af + 0.02, 0.2)
            ELSE s.af
        END as af
    FROM price_data p
    INNER JOIN sar_recursive s ON p.ticker = s.ticker AND p.rn = s.rn + 1
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
    
    -- SAR indicator (matching Python script exactly)
    sar_value as sar
    
FROM sar_recursive
