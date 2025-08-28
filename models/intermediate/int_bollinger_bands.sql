-- Bollinger Bands indicators
-- Intermediate layer: Business logic and calculations
-- References existing SMA-20 from int_sma_indicators instead of recalculating

WITH sma_data AS (
    SELECT 
        ticker, date, sma_20
    FROM {{ ref('int_sma_indicators') }}
),
bb_calc AS (
    SELECT 
        *,
        -- Use existing SMA-20 from int_sma_indicators
        s.sma_20 as bb_middle_20,
        
        -- Upper band (SMA-20 + 2 standard deviations)
        s.sma_20 + (2 * STDDEV(adjusted_closing_price) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        )) as bb_upper_20,
        
        -- Lower band (SMA-20 - 2 standard deviations)
        s.sma_20 - (2 * STDDEV(adjusted_closing_price) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        )) as bb_lower_20
    FROM {{ ref('stg_price_data') }} p
    LEFT JOIN sma_data s ON p.ticker = s.ticker AND p.date = s.date
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
    bb_middle_20,
    bb_upper_20,
    bb_lower_20
FROM bb_calc
ORDER BY ticker, date
