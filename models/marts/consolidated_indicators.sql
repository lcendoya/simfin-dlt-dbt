{{ config(materialized='incremental', unique_key=['ticker', 'date']) }}

-- Consolidated Technical Indicators
-- Combines all calculated indicators into one comprehensive table
-- Uses incremental materialization for efficient processing of new data

WITH sma_data AS (
    SELECT 
        company_id, company_name, ticker, currency, isin, date,
        dividend_paid, common_shares_outstanding, last_closing_price,
        adjusted_closing_price, highest_price, lowest_price, opening_price,
        trading_volume, daily_range, daily_return_pct, price_change,
        sma_20
    FROM {{ ref('int_sma_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
ema_data AS (
    SELECT 
        ticker, date,
        ema_10
    FROM {{ ref('int_ema_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
wma_data AS (
    SELECT 
        ticker, date,
        wma_10
    FROM {{ ref('int_wma_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
dema_tema_data AS (
    SELECT 
        ticker, date,
        dema_10, tema_10
    FROM {{ ref('int_dema_tema_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
kama_data AS (
    SELECT 
        ticker, date,
        kama_10
    FROM {{ ref('int_kama_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
vwap_data AS (
    SELECT 
        ticker, date,
        vwap_cumulative
    FROM {{ ref('int_vwap_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
rsi_data AS (
    SELECT 
        ticker, date,
        rsi_14
    FROM {{ ref('int_rsi_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
macd_data AS (
    SELECT 
        ticker, date,
        -- Standard MACD (only what's used in final output)
        macd_line, macd_signal,
        -- MACDEXT (only what's used in final output)
        macdext_line, macdext_signal
    FROM {{ ref('int_macd_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
bb_data AS (
    SELECT 
        ticker, date,
        bb_upper_20, bb_middle_20, bb_lower_20
    FROM {{ ref('int_bollinger_bands') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
mama_data AS (
    SELECT 
        ticker, date,
        mama
    FROM {{ ref('int_mama_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
fama_data AS (
    SELECT 
        ticker, date,
        fama
    FROM {{ ref('int_fama_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
willr_data AS (
    SELECT 
        ticker, date,
        willr_14
    FROM {{ ref('int_williams_r_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
dx_data AS (
    SELECT 
        ticker, date,
        plus_di_14, minus_di_14, adx_14
    FROM {{ ref('int_dx_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
adxr_data AS (
    SELECT 
        ticker, date,
        adxr_14
    FROM {{ ref('int_adxr_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
apo_data AS (
    SELECT 
        ticker, date,
        apo
    FROM {{ ref('int_apo_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
ppo_data AS (
    SELECT 
        ticker, date,
        ppo
    FROM {{ ref('int_ppo_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
momentum_data AS (
    SELECT 
        ticker, date,
        mom_10
    FROM {{ ref('int_momentum_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
bop_data AS (
    SELECT 
        ticker, date,
        bop
    FROM {{ ref('int_bop_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
cci_data AS (
    SELECT 
        ticker, date,
        cci_20
    FROM {{ ref('int_cci_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
cmo_data AS (
    SELECT 
        ticker, date,
        cmo_14
    FROM {{ ref('int_cmo_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
t3_data AS (
    SELECT 
        ticker, date,
        t3_5
    FROM {{ ref('int_t3_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
stoch_k_data AS (
    SELECT 
        ticker, date,
        stoch_k_14
    FROM {{ ref('int_stochastic_k_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
stoch_d_data AS (
    SELECT 
        ticker, date,
        stoch_d_3
    FROM {{ ref('int_stochastic_d_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
roc_data AS (
    SELECT 
        ticker, date,
        roc_10
    FROM {{ ref('int_roc_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
rocr_data AS (
    SELECT 
        ticker, date,
        rocr_10
    FROM {{ ref('int_rocr_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
aroon_up_data AS (
    SELECT 
        ticker, date,
        aroon_up_25
    FROM {{ ref('int_aroon_up_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
aroon_down_data AS (
    SELECT 
        ticker, date,
        aroon_down_25
    FROM {{ ref('int_aroon_down_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
aroon_osc_data AS (
    SELECT 
        ticker, date,
        aroon_oscillator_25
    FROM {{ ref('int_aroon_oscillator_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
mfi_data AS (
    SELECT 
        ticker, date,
        mfi_14
    FROM {{ ref('int_mfi_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
trix_data AS (
    SELECT 
        ticker, date,
        trix_15
    FROM {{ ref('int_trix_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
ultosc_data AS (
    SELECT 
        ticker, date,
        ultosc
    FROM {{ ref('int_ultosc_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
midpoint_data AS (
    SELECT 
        ticker, date,
        midpoint_14
    FROM {{ ref('int_midpoint_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
midprice_data AS (
    SELECT 
        ticker, date,
        midprice_14
    FROM {{ ref('int_midprice_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
sar_data AS (
    SELECT 
        ticker, date,
        sar
    FROM {{ ref('int_sar_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
trange_data AS (
    SELECT 
        ticker, date,
        trange
    FROM {{ ref('int_trange_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
atr_data AS (
    SELECT 
        ticker, date,
        atr_14
    FROM {{ ref('int_atr_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
natr_data AS (
    SELECT 
        ticker, date,
        natr_14
    FROM {{ ref('int_natr_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
ad_data AS (
    SELECT 
        ticker, date,
        ad
    FROM {{ ref('int_ad_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
adosc_data AS (
    SELECT 
        ticker, date,
        adosc
    FROM {{ ref('int_adosc_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
obv_data AS (
    SELECT 
        ticker, date,
        obv
    FROM {{ ref('int_obv_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
ht_trendline_data AS (
    SELECT 
        ticker, date,
        ht_trendline
    FROM {{ ref('int_ht_trendline_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
ht_lead_sine_data AS (
    SELECT 
        ticker, date,
        ht_lead_sine
    FROM {{ ref('int_ht_lead_sine_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
ht_trendmode_data AS (
    SELECT 
        ticker, date,
        ht_trendmode
    FROM {{ ref('int_ht_trendmode_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
ht_dc_period_data AS (
    SELECT 
        ticker, date,
        ht_dc_period
    FROM {{ ref('int_ht_dc_period_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
ht_dc_phase_data AS (
    SELECT 
        ticker, date,
        ht_dc_phase
    FROM {{ ref('int_ht_dc_phase_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
ht_inphase_data AS (
    SELECT 
        ticker, date,
        ht_inphase
    FROM {{ ref('int_ht_inphase_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
ht_quadrature_data AS (
    SELECT 
        ticker, date,
        ht_quadrature
    FROM {{ ref('int_ht_quadrature_indicators') }}
    {% if is_incremental() %}
      WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
)
SELECT 
    -- Base price data (from sma_data since it contains all base columns)
    s.company_id,
    s.company_name,
    s.ticker,
    s.currency,
    s.isin,
    s.date,
    s.dividend_paid,
    s.common_shares_outstanding,
    s.last_closing_price,
    s.adjusted_closing_price,
    s.highest_price,
    s.lowest_price,
    s.opening_price,
    s.trading_volume,
    s.daily_range,
    s.daily_return_pct,
    s.price_change,
    
    -- SMA indicators (only SMA 20 as per Python script)
    s.sma_20 as sma,
    
    -- EMA indicators (only EMA 10 as per Python script)
    e.ema_10 as ema,
    
    -- WMA indicators (only WMA 10 as per Python script)
    w.wma_10 as wma,
    
    -- DEMA and TEMA indicators (only period 10 as per Python script)
    dt.dema_10 as dema,
    dt.tema_10 as tema,
    
    -- KAMA indicators (only KAMA 10 as per Python script)
    k.kama_10 as kama,
    
    -- VWAP indicators (only cumulative VWAP as per Python script)
    v.vwap_cumulative as vwap,
    
    -- RSI indicators (only RSI 14 as per Python script)
    r.rsi_14 as rsi,
    
    -- MACD indicators (only main MACD as per Python script)
    m.macd_line as macd,
    m.macd_signal as signal,
    
    -- MACDEXT indicators (only MACDEXT as per Python script)
    m.macdext_line as macdext,
    m.macdext_signal as macdext_signal,
    
    -- Bollinger Bands (only period 20 as per Python script)
    bb.bb_upper_20 as upper_bb,
    bb.bb_middle_20 as middle_bb,
    bb.bb_lower_20 as lower_bb,
    
    -- MAMA and FAMA indicators (as per Python script)
    mama.mama,
    fama.fama,
    
    -- Williams %R (only period 14 as per Python script)
    willr.willr_14 as willr,
    
    -- Directional Movement indicators (only period 14 as per Python script)
    dx.plus_di_14 as plus_di,
    dx.minus_di_14 as minus_di,
    dx.adx_14 as adx,
    
    -- ADXR (only period 14 as per Python script)
    adxr.adxr_14 as adxr,
    
    -- APO and PPO (as per Python script)
    apo.apo,
    ppo.ppo,
    
    -- Momentum and BOP (only period 10 for MOM as per Python script)
    mom.mom_10 as mom,
    bop.bop,
    
    -- CCI and CMO (only period 20 for CCI, 14 for CMO as per Python script)
    cci.cci_20 as cci,
    cmo.cmo_14 as cmo,
    
    -- T3 (only period 5 as per Python script)
    t3.t3_5 as t3,
    
    -- Stochastic indicators (only period 14 for K, period 3 for D as per Python script)
    stoch_k.stoch_k_14 as stochastic_k,
    stoch_d.stoch_d_3 as stochastic_d,
    
    -- ROC indicators (only period 10 as per Python script)
    roc.roc_10 as roc,
    rocr.rocr_10 as rocr,
    
    -- Aroon indicators (only period 25 as per Python script)
    aroon_up.aroon_up_25 as aroon_up,
    aroon_down.aroon_down_25 as aroon_down,
    aroon_osc.aroon_oscillator_25 as aroon_oscillator,
    
    -- MFI (only period 14 as per Python script)
    mfi.mfi_14 as mfi,
    
    -- TRIX (only period 15 as per Python script)
    trix.trix_15 as trix,
    
    -- ULTOSC (as per Python script)
    ultosc.ultosc,
    
    -- Midpoint and Midprice (only period 14 as per Python script)
    midpoint.midpoint_14 as midpoint,
    midprice.midprice_14 as midprice,
    
    -- SAR (as per Python script)
    sar.sar,
    
    -- TRANGE, ATR, NATR (only period 14 for ATR/NATR as per Python script)
    trange.trange,
    atr.atr_14 as atr,
    natr.natr_14 as natr,
    
    -- AD and ADOSC (as per Python script)
    ad.ad,
    adosc.adosc,
    
    -- OBV (as per Python script)
    obv.obv,
    
    -- Hilbert Transform indicators (as per Python script)
    ht_trendline.ht_trendline as ht_trendline,
    ht_lead_sine.ht_lead_sine as ht_lead_sine,
    ht_trendmode.ht_trendmode as ht_trendmode,
    ht_dc_period.ht_dc_period as ht_dc_period,
    ht_dc_phase.ht_dc_phase as ht_dc_phase,
    ht_inphase.ht_inphase as ht_inphase,
    ht_quadrature.ht_quadrature as ht_quadrature

FROM sma_data s
LEFT JOIN ema_data e ON s.ticker = e.ticker AND s.date = e.date
LEFT JOIN wma_data w ON s.ticker = w.ticker AND s.date = w.date
LEFT JOIN dema_tema_data dt ON s.ticker = dt.ticker AND s.date = dt.date
LEFT JOIN kama_data k ON s.ticker = k.ticker AND s.date = k.date
LEFT JOIN vwap_data v ON s.ticker = v.ticker AND s.date = v.date
LEFT JOIN rsi_data r ON s.ticker = r.ticker AND s.date = r.date
LEFT JOIN macd_data m ON s.ticker = m.ticker AND s.date = m.date
LEFT JOIN bb_data bb ON s.ticker = bb.ticker AND s.date = bb.date
LEFT JOIN mama_data mama ON s.ticker = mama.ticker AND s.date = mama.date
LEFT JOIN fama_data fama ON s.ticker = fama.ticker AND s.date = fama.date
LEFT JOIN willr_data willr ON s.ticker = willr.ticker AND s.date = willr.date
LEFT JOIN dx_data dx ON s.ticker = dx.ticker AND s.date = dx.date
LEFT JOIN adxr_data adxr ON s.ticker = adxr.ticker AND s.date = adxr.date
LEFT JOIN apo_data apo ON s.ticker = apo.ticker AND s.date = apo.date
LEFT JOIN ppo_data ppo ON s.ticker = ppo.ticker AND s.date = ppo.date
LEFT JOIN momentum_data mom ON s.ticker = mom.ticker AND s.date = mom.date
LEFT JOIN bop_data bop ON s.ticker = bop.ticker AND s.date = bop.date
LEFT JOIN cci_data cci ON s.ticker = cci.ticker AND s.date = cci.date
LEFT JOIN cmo_data cmo ON s.ticker = cmo.ticker AND s.date = cmo.date
LEFT JOIN t3_data t3 ON s.ticker = t3.ticker AND s.date = t3.date
LEFT JOIN stoch_k_data stoch_k ON s.ticker = stoch_k.ticker AND s.date = stoch_k.date
LEFT JOIN stoch_d_data stoch_d ON s.ticker = stoch_d.ticker AND s.date = stoch_d.date
LEFT JOIN roc_data roc ON s.ticker = roc.ticker AND s.date = roc.date
LEFT JOIN rocr_data rocr ON s.ticker = rocr.ticker AND s.date = rocr.date
LEFT JOIN aroon_up_data aroon_up ON s.ticker = aroon_up.ticker AND s.date = aroon_up.date
LEFT JOIN aroon_down_data aroon_down ON s.ticker = aroon_down.ticker AND s.date = aroon_down.date
LEFT JOIN aroon_osc_data aroon_osc ON s.ticker = aroon_osc.ticker AND s.date = aroon_osc.date
LEFT JOIN mfi_data mfi ON s.ticker = mfi.ticker AND s.date = mfi.date
LEFT JOIN trix_data trix ON s.ticker = trix.ticker AND s.date = trix.date
LEFT JOIN ultosc_data ultosc ON s.ticker = ultosc.ticker AND s.date = ultosc.date
LEFT JOIN midpoint_data midpoint ON s.ticker = midpoint.ticker AND s.date = midpoint.date
LEFT JOIN midprice_data midprice ON s.ticker = midprice.ticker AND s.date = midprice.date
LEFT JOIN sar_data sar ON s.ticker = sar.ticker AND s.date = sar.date
LEFT JOIN trange_data trange ON s.ticker = trange.ticker AND s.date = trange.date
LEFT JOIN atr_data atr ON s.ticker = atr.ticker AND s.date = atr.date
LEFT JOIN natr_data natr ON s.ticker = natr.ticker AND s.date = natr.date
LEFT JOIN ad_data ad ON s.ticker = ad.ticker AND s.date = ad.date
LEFT JOIN adosc_data adosc ON s.ticker = adosc.ticker AND s.date = adosc.date
LEFT JOIN obv_data obv ON s.ticker = obv.ticker AND s.date = obv.date
LEFT JOIN ht_trendline_data ht_trendline ON s.ticker = ht_trendline.ticker AND s.date = ht_trendline.date
LEFT JOIN ht_lead_sine_data ht_lead_sine ON s.ticker = ht_lead_sine.ticker AND s.date = ht_lead_sine.date
LEFT JOIN ht_trendmode_data ht_trendmode ON s.ticker = ht_trendmode.ticker AND s.date = ht_trendmode.date
LEFT JOIN ht_dc_period_data ht_dc_period ON s.ticker = ht_dc_period.ticker AND s.date = ht_dc_period.date
LEFT JOIN ht_dc_phase_data ht_dc_phase ON s.ticker = ht_dc_phase.ticker AND s.date = ht_dc_phase.date
LEFT JOIN ht_inphase_data ht_inphase ON s.ticker = ht_inphase.ticker AND s.date = ht_inphase.date
LEFT JOIN ht_quadrature_data ht_quadrature ON s.ticker = ht_quadrature.ticker AND s.date = ht_quadrature.date

-- Note: This model uses incremental materialization
-- - First run: Processes all historical data
-- - Subsequent runs: Only processes new dates since last run
-- - Intermediate models are filtered to only include new data for efficiency
