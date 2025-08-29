-- DX (Directional Movement Index) indicators
-- Intermediate layer: Business logic and calculations
-- Only calculates DX 14 to match Python script exactly

WITH price_data AS (
    SELECT 
        *,
        -- Plus DM: high difference, only positive values
        CASE 
            WHEN highest_price - LAG(highest_price, 1) OVER (PARTITION BY ticker ORDER BY date) > 0 
            THEN highest_price - LAG(highest_price, 1) OVER (PARTITION BY ticker ORDER BY date)
            ELSE 0 
        END as plus_dm,
        
        -- Minus DM: low difference, only negative values (absolute)
        CASE 
            WHEN lowest_price - LAG(lowest_price, 1) OVER (PARTITION BY ticker ORDER BY date) < 0 
            THEN ABS(lowest_price - LAG(lowest_price, 1) OVER (PARTITION BY ticker ORDER BY date))
            ELSE 0 
        END as minus_dm,
        
        -- True Range: max of (high-low), |high-close_prev|, |low-close_prev|
        GREATEST(
            highest_price - lowest_price,
            ABS(highest_price - LAG(adjusted_closing_price, 1) OVER (PARTITION BY ticker ORDER BY date)),
            ABS(lowest_price - LAG(adjusted_closing_price, 1) OVER (PARTITION BY ticker ORDER BY date))
        ) as tr
    FROM {{ ref('stg_price_data') }}
),
dx_calc AS (
    SELECT 
        *,
        -- ATR: 14-period average of True Range
        AVG(tr) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) as atr_14,
        
        -- Plus DI: 100 * (Plus DM EMA / ATR)
        100 * (
            AVG(plus_dm) OVER (
                PARTITION BY ticker 
                ORDER BY date 
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            ) / NULLIF(AVG(tr) OVER (
                PARTITION BY ticker 
                ORDER BY date 
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            ), 0)
        ) as plus_di_14,
        
        -- Minus DI: 100 * (Minus DM EMA / ATR)
        100 * (
            AVG(minus_dm) OVER (
                PARTITION BY ticker 
                ORDER BY date 
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            ) / NULLIF(AVG(tr) OVER (
                PARTITION BY ticker 
                ORDER BY date 
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            ), 0)
        ) as minus_di_14
    FROM price_data
),
final_calc AS (
    SELECT 
        *,
        -- DX calculation (matching Python script exactly)
        -- DX = 100 * |Plus DI - Minus DI| / (Plus DI + Minus DI)
        CASE 
            WHEN (plus_di_14 + minus_di_14) = 0 THEN 0
            ELSE 100 * ABS(plus_di_14 - minus_di_14) / (plus_di_14 + minus_di_14)
        END as dx_14
    FROM dx_calc
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
    
    -- Plus DI and Minus DI (matching Python script exactly)
    plus_di_14,
    minus_di_14,
    
    -- ADX calculation (matching Python script exactly)
    -- ADX = EMA of DX over 14 periods
    AVG(dx_14) OVER (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    ) as adx_14
    
FROM final_calc
