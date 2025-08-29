-- Staging model for price data
-- This model cleans and prepares the raw data from the API

SELECT 
    company_id,
    company_name,
    ticker,
    currency,
    isin,
    CAST(date AS DATE) as date,
    dividend_paid,
    common_shares_outstanding,
    last_closing_price,
    adjusted_closing_price,
    highest_price,
    lowest_price,
    opening_price,
    trading_volume,
    -- Add some basic calculated fields
    (highest_price - lowest_price) as daily_range,
    ((adjusted_closing_price - opening_price) / opening_price * 100) as daily_return_pct,
    -- Price change for technical indicators
    adjusted_closing_price - LAG(adjusted_closing_price, 1) OVER (PARTITION BY ticker ORDER BY date) as price_change
FROM {{ source('simfin_raw', 'price') }}
WHERE date IS NOT NULL 
  AND adjusted_closing_price IS NOT NULL
  AND trading_volume IS NOT NULL
