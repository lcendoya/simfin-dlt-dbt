# Technical Indicators dbt Project

This dbt project transforms raw financial price data into comprehensive technical analysis indicators, converting the Python calculations from `IndicatorData.py` into optimized SQL transformations.

## Project Structure

```
technical_indicators/
├── dbt_project.yml          # Project configuration
├── profiles.yml             # Database connection settings
├── models/
│   ├── staging/
│   │   └── stg_price_data.sql      # Cleaned and prepared price data
│   ├── marts/
│   │   ├── sma_indicators.sql      # Simple Moving Averages
│   │   ├── ema_indicators.sql      # Exponential Moving Averages
│   │   ├── rsi_indicators.sql      # Relative Strength Index
│   │   ├── macd_indicators.sql     # MACD indicators
│   │   ├── bollinger_bands.sql     # Bollinger Bands
│   │   ├── vwap_indicators.sql     # Volume Weighted Average Price
│   │   └── consolidated_indicators.sql  # All indicators combined
│   └── schema.yml           # Model documentation and tests
├── macros/
│   └── positive_values.sql  # Custom test for positive values
└── README.md                # This file
```

## Features

### Technical Indicators Implemented

1. **Simple Moving Averages (SMA)**
   - 5, 10, 20, 50, and 200-period SMAs
   - Volume SMA for comparison

2. **Exponential Moving Averages (EMA)**
   - 5, 10, 12, 20, 26, and 50-period EMAs
   - Uses recursive CTEs for accurate calculations

3. **Relative Strength Index (RSI)**
   - 7, 14, and 21-period RSI
   - Includes price change calculations

4. **MACD (Moving Average Convergence Divergence)**
   - MACD line (12-period EMA - 26-period EMA)
   - Signal line (9-period EMA of MACD)
   - MACD histogram and percentage

5. **Bollinger Bands**
   - Upper, middle, and lower bands (20-period)
   - Band width and %B calculations

6. **VWAP (Volume Weighted Average Price)**
   - Cumulative and 20-period VWAP
   - Volume analysis and price vs VWAP metrics

7. **Consolidated Indicators**
   - All indicators combined in one table
   - Derived signals and crossovers
   - Trading signals (Overbought/Oversold, Bullish/Bearish)

## Setup Instructions

### 1. Install dbt

```bash
pip install dbt-postgres
```

### 2. Configure Database Connection

Update `profiles.yml` with your PostgreSQL credentials:

```yaml
technical_indicators:
  target: dev
  outputs:
    dev:
      type: postgres
      host: your_host
      user: your_username
      password: your_password
      port: 5432
      dbname: your_database
      schema: public
```

### 3. Install Dependencies

```bash
dbt deps
```

### 4. Run the Models

```bash
# Run all models
dbt run

# Run specific models
dbt run --select stg_price_data
dbt run --select marts
dbt run --select consolidated_indicators

# Run with full refresh
dbt run --full-refresh
```

### 5. Test the Models

```bash
# Run all tests
dbt test

# Test specific models
dbt test --select stg_price_data
```

### 6. Generate Documentation

```bash
dbt docs generate
dbt docs serve
```

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

1. Create a new model in `models/marts/`
2. Follow the naming convention: `indicator_name.sql`
3. Add documentation to `models/schema.yml`
4. Include in `consolidated_indicators.sql`

### Modifying Periods

Update the `ROWS BETWEEN` clauses in the window functions:

```sql
-- Change from 20-period to 30-period
ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
```

### Adding New Signals

Extend the consolidated model with additional CASE statements:

```sql
CASE 
    WHEN indicator > threshold THEN 'Signal A'
    WHEN indicator < threshold THEN 'Signal B'
    ELSE 'Neutral'
END as new_signal
```

## Troubleshooting

### Common Issues

1. **Recursive CTE Errors**: Ensure your PostgreSQL version supports recursive CTEs (9.1+)
2. **Window Function Performance**: Large datasets may need indexing on (ticker, date)
3. **Memory Issues**: Consider materializing intermediate models as tables

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
