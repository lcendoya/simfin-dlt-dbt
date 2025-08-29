-- MFI (Money Flow Index) indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates MFI 14 to match Python script exactly

WITH price_data AS (
    SELECT 
        *,
        -- Typical Price: (high + low + close) / 3
        (highest_price + lowest_price + adjusted_closing_price) / 3 as typical_price
    FROM {{ ref('stg_price_data') }}
),
mfi_calc AS (
    SELECT 
        *,
        -- Money Flow: typical_price * volume
        typical_price * trading_volume as money_flow,
        
        -- Positive Flow: money_flow when typical_price > previous_typical_price
        CASE 
            WHEN typical_price > LAG(typical_price, 1) OVER (PARTITION BY ticker ORDER BY date) 
            THEN typical_price * trading_volume 
            ELSE 0 
        END as positive_flow,
        
        -- Negative Flow: money_flow when typical_price < previous_typical_price
        CASE 
            WHEN typical_price < LAG(typical_price, 1) OVER (PARTITION BY ticker ORDER BY date) 
            THEN typical_price * trading_volume 
            ELSE 0 
        END as negative_flow
    FROM price_data
),
mfi_final AS (
    SELECT 
        *,
        -- Sum of positive flow over 14 periods
        SUM(positive_flow) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) as positive_flow_sum_14,
        
        -- Sum of negative flow over 14 periods
        SUM(negative_flow) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) as negative_flow_sum_14
    FROM mfi_calc
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
    
    -- MFI calculation (matching Python script exactly)
    -- MFI = 100 - (100 / (1 + (positive_flow / negative_flow)))
    CASE 
        WHEN negative_flow_sum_14 = 0 THEN 100
        ELSE 100 - (100 / (1 + (positive_flow_sum_14 / negative_flow_sum_14)))
    END as mfi_14
    
FROM mfi_final
