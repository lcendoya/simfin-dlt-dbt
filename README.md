# SimFin Technical Indicators Pipeline

This project implements a complete data pipeline using **dlt (data load tool)** to extract financial data from SimFin API and **dbt (data build tool)** to transform it into comprehensive technical analysis indicators. The main execution is handled by the Python pipeline script that orchestrates the entire data flow.

## Project Structure

```
technical_indicators/
├── simfin_pipeline.py        # Main Python pipeline script
├── dbt_project.yml          # dbt project configuration
├── models/
│   ├── staging/
│   │   └── stg_price_data.sql      # Cleaned and prepared price data
│   ├── intermediate/
│   │   ├── int_sma_indicators.sql      # Simple Moving Averages
│   │   ├── int_ema_indicators.sql      # Exponential Moving Averages
│   │   ├── int_rsi_indicators.sql      # Relative Strength Index
│   │   ├── int_macd_indicators.sql     # MACD indicators
│   │   ├── int_bollinger_bands.sql     # Bollinger Bands
│   │   ├── int_vwap_indicators.sql     # Volume Weighted Average Price
│   │   ├── int_wma_indicators.sql      # Weighted Moving Averages
│   │   ├── int_dema_tema_indicators.sql # Double/Triple Exponential
│   │   ├── int_kama_indicators.sql     # Kaufman Adaptive
│   │   ├── int_williams_r_indicators.sql # Williams %R
│   │   ├── int_dx_indicators.sql       # Directional Movement
│   │   ├── int_adxr_indicators.sql     # ADXR
│   │   ├── int_apo_indicators.sql      # Absolute Price Oscillator
│   │   ├── int_ppo_indicators.sql      # Percentage Price Oscillator
│   │   ├── int_momentum_indicators.sql # Momentum
│   │   ├── int_bop_indicators.sql      # Balance of Power
│   │   ├── int_cci_indicators.sql      # Commodity Channel Index
│   │   ├── int_cmo_indicators.sql      # Chande Momentum Oscillator
│   │   ├── int_t3_indicators.sql       # T3 Moving Average
│   │   ├── int_stochastic_k_indicators.sql # Stochastic %K
│   │   ├── int_stochastic_d_indicators.sql # Stochastic %D
│   │   ├── int_roc_indicators.sql      # Rate of Change
│   │   ├── int_rocr_indicators.sql     # Rate of Change Ratio
│   │   ├── int_aroon_up_indicators.sql # Aroon Up
│   │   ├── int_aroon_down_indicators.sql # Aroon Down
│   │   ├── int_aroon_oscillator_indicators.sql # Aroon Oscillator
│   │   ├── int_mfi_indicators.sql      # Money Flow Index
│   │   ├── int_trix_indicators.sql     # TRIX
│   │   ├── int_ultosc_indicators.sql   # Ultimate Oscillator
│   │   ├── int_midpoint_indicators.sql # Midpoint
│   │   ├── int_midprice_indicators.sql # Midprice
│   │   ├── int_sar_indicators.sql      # Parabolic SAR
│   │   ├── int_trange_indicators.sql   # True Range
│   │   ├── int_atr_indicators.sql      # Average True Range
│   │   ├── int_natr_indicators.sql     # Normalized ATR
│   │   ├── int_ad_indicators.sql       # Accumulation/Distribution
│   │   ├── int_adosc_indicators.sql    # AD Oscillator
│   │   ├── int_obv_indicators.sql      # On-Balance Volume
│   │   ├── int_ht_trendline_indicators.sql # HT Trendline
│   │   ├── int_ht_lead_sine_indicators.sql # HT Lead Sine
│   │   ├── int_ht_trendmode_indicators.sql # HT Trend Mode
│   │   ├── int_ht_dc_period_indicators.sql # HT DC Period
│   │   ├── int_ht_dc_phase_indicators.sql # HT DC Phase
│   │   ├── int_ht_inphase_indicators.sql # HT Inphase
│   │   ├── int_ht_quadrature_indicators.sql # HT Quadrature
│   │   └── [and more...]               # 40+ total intermediate models
│   ├── marts/
│   │   └── consolidated_indicators.sql  # All indicators combined
│   └── schema.yml           # Model documentation and tests
├── macros/
│   └── positive_values.sql  # Custom test for positive values
└── README.md                # This file
```

## Technical Indicators Implemented

### Final Output Indicators (Available in consolidated_indicators)

1. **Moving Averages**
   - Simple Moving Average (SMA): 20-period
   - Exponential Moving Average (EMA): 10-period
   - Weighted Moving Average (WMA): 10-period
   - Double Exponential (DEMA): 10-period
   - Triple Exponential (TEMA): 10-period
   - Kaufman Adaptive (KAMA): 10-period

2. **Momentum Indicators**
   - Relative Strength Index (RSI): 14-period
   - MACD: Line and Signal (12/26/9 periods)
   - MACDEXT: Extended MACD line and signal
   - Stochastic: %K (14-period) and %D (3-period)
   - Williams %R: 14-period
   - Rate of Change (ROC): 10-period
   - Rate of Change Ratio (ROCR): 10-period

3. **Volatility Indicators**
   - Bollinger Bands: Upper, Middle, Lower (20-period, 2 std dev)
   - Average True Range (ATR): 14-period
   - Normalized ATR (NATR): 14-period
   - True Range (TRANGE)

4. **Volume Indicators**
   - Volume Weighted Average Price (VWAP): Cumulative
   - On-Balance Volume (OBV)
   - Money Flow Index (MFI): 14-period
   - Accumulation/Distribution (AD)
   - Accumulation/Distribution Oscillator (ADOSC)

5. **Trend Indicators**
   - Directional Movement: Plus DI, Minus DI, ADX (14-period)
   - ADXR: Average Directional Index Rating (14-period)
   - APO: Absolute Price Oscillator
   - PPO: Percentage Price Oscillator
   - Momentum: 10-period
   - Balance of Power (BOP)
   - CCI: Commodity Channel Index (20-period)
   - CMO: Chande Momentum Oscillator (14-period)
   - T3: Triple Exponential Moving Average (5-period)

6. **Advanced Oscillators**
   - TRIX: Triple Exponential Average (15-period)
   - Ultimate Oscillator (ULTOSC)
   - Midpoint: 14-period
   - Midprice: 14-period
   - Parabolic SAR (SAR)

7. **Hilbert Transform Indicators**
   - HT Trendline
   - HT Lead Sine
   - HT Trend Mode
   - HT DC Period
   - HT DC Phase
   - HT Inphase
   - HT Quadrature

8. **Aroon Indicators**
   - Aroon Up: 25-period
   - Aroon Down: 25-period
   - Aroon Oscillator: 25-period

**Note**: All indicators are calculated for each ticker and date, providing a comprehensive technical analysis dataset ready for trading strategies and analysis.

## Setup Instructions

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Environment Variables

Set these two environment variables:

```bash
# SimFin API access token
SOURCES__SIMFIN__ACCESS_TOKEN

# Database connection (if not using dlt secrets)
DESTINATION__POSTGRES__CREDENTIALS
```

**Note**: You can also use the `.dlt/secrets.toml` file instead of environment variables for better security.

### 3. Run the Complete Pipeline

```bash
python simfin_pipeline.py
```

That's it! The script will:
- Extract data from SimFin API
- Load it into PostgreSQL (credentials managed by dlt)
- Run all dbt transformations
- Produce the final consolidated indicators table

## Data Flow

1. **Raw Data**: Your API data loaded into `raw_price_data` table
2. **Staging**: `stg_price_data` cleans and prepares the data
3. **Individual Indicators**: Each indicator type calculated separately
4. **Consolidated**: All indicators combined into final analysis table

## Key Benefits

- **Performance**: SQL window functions are highly optimized
- **Maintainability**: Easy to modify periods or add new indicators
- **Testing**: Built-in data quality tests
- **Documentation**: Auto-generated documentation
- **Lineage**: Clear data transformation lineage

## Customization

### Adding New Indicators

1. Create a new model in `models/intermediate/`
2. Follow the naming convention: `int_indicator_name.sql`
3. Add documentation to `models/schema.yml`
4. Include in `consolidated_indicators.sql`

## Troubleshooting

### Common Issues

1. **Recursive CTE Errors**: Ensure your PostgreSQL version supports recursive CTEs (9.1+)
2. **Window Function Performance**: Large datasets may need indexing on (ticker, date)
3. **Memory Issues**: Consider materializing more intermediate models as tables

### Performance Optimization

- Add indexes on `(ticker, date)` for window functions
- Use `+materialized: table` for frequently queried models
- Consider partitioning by ticker for very large datasets

## Support

For issues or questions:
1. Check the dbt logs: `dbt run --log-level debug`
2. Verify database connectivity
3. Ensure sufficient data exists for indicator calculations

## License

This project is open source and available under the MIT License.
