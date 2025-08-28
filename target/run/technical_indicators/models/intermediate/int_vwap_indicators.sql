
  create view "postgres"."simfin_dbt"."int_vwap_indicators__dbt_tmp"
    
    
  as (
    -- VWAP (Volume Weighted Average Price) indicators
-- Intermediate layer: Business logic and calculations

SELECT 
    *,
    -- VWAP (cumulative)
    SUM(adjusted_closing_price * trading_volume) OVER (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS UNBOUNDED PRECEDING
    ) / SUM(trading_volume) OVER (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS UNBOUNDED PRECEDING
    ) as vwap_cumulative,
    
    -- 20-period VWAP
    SUM(adjusted_closing_price * trading_volume) OVER (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) / SUM(trading_volume) OVER (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) as vwap_20,
    
    -- Volume SMA for comparison
    AVG(trading_volume) OVER (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) as volume_sma_20,
    
    -- Volume ratio (current volume vs 20-period average)
    CASE 
        WHEN AVG(trading_volume) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) = 0 THEN 0
        ELSE trading_volume / AVG(trading_volume) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        )
    END as volume_ratio_20,
    
    -- Price vs VWAP
    CASE 
        WHEN SUM(adjusted_closing_price * trading_volume) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) / SUM(trading_volume) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) = 0 THEN 0
        ELSE ((adjusted_closing_price - (SUM(adjusted_closing_price * trading_volume) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) / SUM(trading_volume) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ))) / (SUM(adjusted_closing_price * trading_volume) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) / SUM(trading_volume) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ))) * 100
    END as price_vs_vwap_pct

FROM "postgres"."simfin_dbt"."stg_price_data"
  );