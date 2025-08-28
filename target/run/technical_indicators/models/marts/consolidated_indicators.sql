
      
        delete from "postgres"."simfin_dbt"."consolidated_indicators" as DBT_INTERNAL_DEST
        where (ticker, date) in (
            select distinct ticker, date
            from "consolidated_indicators__dbt_tmp112203833947" as DBT_INTERNAL_SOURCE
        );

    

    insert into "postgres"."simfin_dbt"."consolidated_indicators" ("company_id", "company_name", "ticker", "currency", "isin", "date", "dividend_paid", "common_shares_outstanding", "last_closing_price", "adjusted_closing_price", "highest_price", "lowest_price", "opening_price", "trading_volume", "daily_range", "daily_return_pct", "sma_5", "sma_10", "sma_20", "sma_50", "sma_200", "volume_sma_20", "ema_5", "ema_10", "ema_12", "ema_20", "ema_26", "ema_50", "rsi_7", "rsi_14", "rsi_21", "price_change", "macd_line", "macd_signal", "macd_histogram", "macd_percentage", "bb_upper_20", "bb_middle_20", "bb_lower_20", "bb_width_20", "bb_percent_b_20", "vwap_cumulative", "vwap_20", "volume_ratio_20", "price_vs_vwap_pct", "price_vs_sma_20_pct", "price_vs_ema_20_pct", "sma_crossover_signal", "ema_crossover_signal", "rsi_signal", "macd_trading_signal", "bb_signal", "volume_signal")
    (
        select "company_id", "company_name", "ticker", "currency", "isin", "date", "dividend_paid", "common_shares_outstanding", "last_closing_price", "adjusted_closing_price", "highest_price", "lowest_price", "opening_price", "trading_volume", "daily_range", "daily_return_pct", "sma_5", "sma_10", "sma_20", "sma_50", "sma_200", "volume_sma_20", "ema_5", "ema_10", "ema_12", "ema_20", "ema_26", "ema_50", "rsi_7", "rsi_14", "rsi_21", "price_change", "macd_line", "macd_signal", "macd_histogram", "macd_percentage", "bb_upper_20", "bb_middle_20", "bb_lower_20", "bb_width_20", "bb_percent_b_20", "vwap_cumulative", "vwap_20", "volume_ratio_20", "price_vs_vwap_pct", "price_vs_sma_20_pct", "price_vs_ema_20_pct", "sma_crossover_signal", "ema_crossover_signal", "rsi_signal", "macd_trading_signal", "bb_signal", "volume_signal"
        from "consolidated_indicators__dbt_tmp112203833947"
    )
  