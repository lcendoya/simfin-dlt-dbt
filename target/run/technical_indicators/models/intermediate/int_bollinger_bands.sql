
  create view "postgres"."simfin_dbt"."int_bollinger_bands__dbt_tmp"
    
    
  as (
    -- Bollinger Bands indicators
-- Intermediate layer: Business logic and calculations

SELECT 
    *,
    -- Middle band (20-period SMA)
    AVG(adjusted_closing_price) OVER (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) as bb_middle_20,
    
    -- Upper band (middle + 2 standard deviations)
    AVG(adjusted_closing_price) OVER (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) + (2 * STDDEV(adjusted_closing_price) OVER (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    )) as bb_upper_20,
    
    -- Lower band (middle - 2 standard deviations)
    AVG(adjusted_closing_price) OVER (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) - (2 * STDDEV(adjusted_closing_price) OVER (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    )) as bb_lower_20,
    
    -- Bollinger Band Width (upper - lower)
    (AVG(adjusted_closing_price) OVER (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) + (2 * STDDEV(adjusted_closing_price) OVER (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ))) - (AVG(adjusted_closing_price) OVER (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) - (2 * STDDEV(adjusted_closing_price) OVER (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ))) as bb_width_20,
    
    -- %B (position within bands)
    CASE 
        WHEN (AVG(adjusted_closing_price) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) + (2 * STDDEV(adjusted_closing_price) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ))) - (AVG(adjusted_closing_price) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) - (2 * STDDEV(adjusted_closing_price) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ))) = 0 THEN 0
        ELSE (adjusted_closing_price - (AVG(adjusted_closing_price) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) - (2 * STDDEV(adjusted_closing_price) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        )))) / ((AVG(adjusted_closing_price) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) + (2 * STDDEV(adjusted_closing_price) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ))) - (AVG(adjusted_closing_price) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) - (2 * STDDEV(adjusted_closing_price) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ))))
    END as bb_percent_b_20

FROM "postgres"."simfin_dbt"."stg_price_data"
  );