import pandas as pd
from datetime import datetime, timedelta
from cryptodatapy.data_requests.datarequest import DataRequest
from cryptodatapy.util.convertparams import ConvertParams
import pytest


@pytest.fixture
def datarequest():
    return DataRequest()


# convert cryptodatapy tickers to source format
@pytest.mark.parametrize(
    "dr_tickers, cv_tickers",
    [
        ('btc', ['BTC']),
        ('eth', ['ETH']),
        (['spy', 'tlt'], ['SPY', 'TLT']),
        (['btc', 'eth', 'sol'], ['BTC', 'ETH', 'SOL'])
    ]
)
def test_convert_tickers(dr_tickers, cv_tickers) -> None:
    """
    Test tickers parameter conversion to CryptoCompare, CCXT and Alpha Vantage-daily formats.
    """
    data_req = DataRequest(tickers=dr_tickers)
    cc_params = ConvertParams(data_source='cryptocompare').convert_to_source(data_req)
    assert cc_params['tickers'] == cv_tickers, "Tickers parameter conversion failed."
    cc_params = ConvertParams(data_source='ccxt').convert_to_source(data_req)
    assert cc_params['tickers'] == cv_tickers, "Tickers parameter conversion failed."
    cc_params = ConvertParams(data_source='av-daily').convert_to_source(data_req)
    assert cc_params['tickers'] == cv_tickers, "Tickers parameter conversion failed."


@pytest.mark.parametrize(
    "dr_tickers, tg_tickers",
    [
        ('SPY', ['spy']),
        (['SPY', 'TLT', 'VXX'], ['spy', 'tlt', 'vxx'])
    ]
)
def test_convert_tg_tickers(dr_tickers, tg_tickers) -> None:
    """
    Test tickers parameter conversion to Tiingo format.
    """
    data_req = DataRequest(tickers=dr_tickers)
    tg_params = ConvertParams(data_source='tiingo').convert_to_source(data_req)
    assert tg_params['tickers'] == tg_tickers, "Tickers parameter conversion failed."


@pytest.mark.parametrize(
    "dr_tickers, tg_tickers",
    [
        ('EUR', ['eurusd']),
        (['BTC', 'ETH'], ['btcusd', 'ethusd'])
    ]
)
def test_convert_tg_fx_tickers(dr_tickers, tg_tickers) -> None:
    """
    Test fx tickers parameter conversion to Tiingo format.
    """
    data_req = DataRequest(tickers=dr_tickers, cat='fx')
    tg_params = ConvertParams(data_source='tiingo').convert_to_source(data_req)
    assert tg_params['tickers'] == tg_tickers, "Tickers parameter conversion failed."


@pytest.mark.parametrize(
    "dr_tickers, ip_tickers",
    [
        ('eur', ['EUR/USD']),
        (['gbp', 'cad'], ['GBP/USD', 'CAD/USD'])
    ]
)
def test_convert_ip_fx_tickers(dr_tickers, ip_tickers) -> None:
    """
    Test fx tickers parameter conversion to InvestPy format.
    """
    data_req = DataRequest(tickers=dr_tickers, cat='fx')
    ip_params = ConvertParams(data_source='investpy').convert_to_source(data_req)
    assert ip_params['tickers'] == ip_tickers, "Tickers parameter conversion failed."


@pytest.mark.parametrize(
    "dr_tickers, ip_tickers",
    [
        (['spy', 'tlt'], ['SPY', 'TLT']),
        (['US_Eqty_Idx', 'JP_Eqty_MSCI'], ['S&P 500', 'MSCI Japan JPY'])
    ]
)
def test_convert_ip_eqty_tickers(dr_tickers, ip_tickers) -> None:
    """
    Test eqty tickers parameter conversion to InvestPy format.
    """
    data_req = DataRequest(tickers=dr_tickers, cat='eqty')
    ip_params = ConvertParams(data_source='investpy').convert_to_source(data_req)
    assert ip_params['tickers'] == ip_tickers, "Tickers parameter conversion failed."


@pytest.mark.parametrize(
    "dr_tickers, ip_tickers",
    [
        ('GB_Rates_IRS_30Y', ['GBP 30 Years IRS Interest Rate Swap']),
        (['Crude_Oil_Brent', 'Gold_Spot'], ['Brent Oil', 'Gold Spot US Dollar']),
        (['BR_Manuf_PMI', 'US_Infl_CPI_MoM'], ['S&P Global Manufacturing PMI', 'CPI (MoM)'])
    ]
)
def test_convert_ip_macro_tickers(dr_tickers, ip_tickers) -> None:
    """
    Test macro tickers parameter conversion to InvestPy format.
    """
    data_req = DataRequest(tickers=dr_tickers, cat='macro')
    ip_params = ConvertParams(data_source='investpy').convert_to_source(data_req)
    assert ip_params['tickers'] == ip_tickers, "Tickers parameter conversion failed."


@pytest.mark.parametrize(
    "dr_tickers, db_tickers",
    [
        ('US_GDP_Sh_PPP', ['IMF/WEO:2021-04/USA.PPPSH.pcent']),
        (['EZ_Infl_Exp_1Y', 'US_CB_MB'], ['ECB/SPF/M.U2.HICP.POINT.P12M.Q.AVG', 'FED/H6_H6_MBASE/RESMO14A_N.M']),
    ]
)
def test_convert_db_tickers(dr_tickers, db_tickers) -> None:
    """
    Test tickers parameter conversion to DBnomics format.
    """
    data_req = DataRequest(tickers=dr_tickers)
    db_params = ConvertParams(data_source='dbnomics').convert_to_source(data_req)
    assert db_params['tickers'] == db_tickers, "Tickers parameter conversion failed."


@pytest.mark.parametrize(
    "dr_tickers, fred_tickers",
    [
        ('US_UE_Rate', ['UNRATE']),
        (['US_Credit_IG_Yield', 'US_Credit_HY_Yield'], ['BAMLC0A0CMEY', 'BAMLH0A0HYM2EY']),
    ]
)
def test_convert_fred_tickers(dr_tickers, fred_tickers) -> None:
    """
    Test tickers parameter conversion to Fred format.
    """
    data_req = DataRequest(tickers=dr_tickers)
    fred_params = ConvertParams(data_source='fred').convert_to_source(data_req)
    assert fred_params['tickers'] == fred_tickers, "Tickers parameter conversion failed."


@pytest.mark.parametrize(
    "dr_tickers, av_tickers",
    [
        ('eur', ['EUR/USD']),
        (['gbp', 'cad'], ['GBP/USD', 'CAD/USD'])
    ]
)
def test_convert_av_fx_tickers(dr_tickers, av_tickers) -> None:
    """
    Test fx tickers parameter conversion to Alpha Vantage fx format.
    """
    data_req = DataRequest(tickers=dr_tickers)
    av_params = ConvertParams(data_source='av-forex-daily').convert_to_source(data_req)
    assert av_params['tickers'] == av_tickers, "Tickers parameter conversion failed."


# convert cryptodatapy params to markets in source format
@pytest.mark.parametrize(
    "dr_tickers, cm_mkts",
    [
        ('btc', ['binance-btc-usdt-spot']),
        ('eth', ['binance-eth-usdt-spot'])
    ]
)
def test_convert_tickers_to_cm_mkts(dr_tickers, cm_mkts) -> None:
    """
    Test tickers to markets parameter conversion for Coin Metrics format.
    """
    data_req = DataRequest(tickers=dr_tickers)
    cm_params = ConvertParams(data_source='coinmetrics').convert_to_source(data_req)
    assert cm_params['mkts'] == cm_mkts, "Tickers to markets parameter conversion failed."


@pytest.mark.parametrize(
    "dr_mkt_types, cm_mkts",
    [
        ('spot', ['binance-btc-usdt-spot']),
        ('perpetual_future', ['binance-BTCUSDT-future'])
    ]
)
def test_convert_mkt_types_to_cm_mkts(dr_mkt_types, cm_mkts) -> None:
    """
    Test market type to markets parameter conversion for Coin Metrics format.
    """
    data_req = DataRequest(mkt_type=dr_mkt_types)
    cm_params = ConvertParams(data_source='coinmetrics').convert_to_source(data_req)
    assert cm_params['mkts'] == cm_mkts, "Market type to markets parameter conversion failed."


@pytest.mark.parametrize(
    "dr_exch, cm_mkts",
    [
        ('bitmex', ['bitmex-BTCUSDT-future']),
        ('ftx', ['ftx-BTC-PERP-future']),
        ('okex', ['okex-BTC-USDT-SWAP-future']),
        ('huobi', ['huobi-BTC-USDT_SWAP-future']),
    ]
)
def test_convert_exchange_to_cm_mkts(dr_exch, cm_mkts) -> None:
    """
    Test exchange to markets parameter conversion for Coin Metrics format.
    """
    data_req = DataRequest(exch=dr_exch, mkt_type='perpetual_future')
    cm_params = ConvertParams(data_source='coinmetrics').convert_to_source(data_req)
    assert cm_params['mkts'] == cm_mkts, "Exchange to markets parameter conversion failed."


@pytest.mark.parametrize(
    "dr_exch, cx_mkts",
    [
        ('binance', ['BTC/USDT']),
        ('kucoin', ['BTC/USDT:USDT']),
        ('huobi', ['BTC/USDT:USDT']),
        ('okx', ['BTC/USDT:USDT']),
    ]
)
def test_convert_exch_to_cx_mkts(dr_exch, cx_mkts) -> None:
    """
    Test exchange to markets parameter conversion for CCXT format.
    """
    data_req = DataRequest(exch=dr_exch, mkt_type='perpetual_future')
    cx_params = ConvertParams(data_source='ccxt').convert_to_source(data_req)
    assert cx_params['mkts'] == cx_mkts, "Exchange to markets parameter conversion failed."


@pytest.mark.parametrize(
    "dr_mkt_types, cx_mkts",
    [
        ('spot', ['BTC/USDT']),
        ('perpetual_future', ['BTC/USDT:USDT'])
    ]
)
def test_convert_mkt_types_to_cx_mkts(dr_mkt_types, cx_mkts) -> None:
    """
    Test market type to markets parameter conversion for CCXT format.
    """
    data_req = DataRequest(mkt_type=dr_mkt_types, exch='ftx')
    cx_params = ConvertParams(data_source='ccxt').convert_to_source(data_req)
    assert cx_params['mkts'] == cx_mkts, "Market type to markets parameter conversion failed."


# convert cryptodatapy freq to source format
@pytest.mark.parametrize(
    "dr_freq, cc_freq",
    [
        ('5min', 'histominute'),
        ('30min', 'histominute'),
        ('1h', 'histohour'),
        ('d', 'histoday'),
        ('w', 'histoday')
    ]
)
def test_convert_cc_freq(dr_freq, cc_freq) -> None:
    """
    Test frequency parameter conversion to CryptoCompare format.
    """
    data_req = DataRequest(freq=dr_freq)
    cc_params = ConvertParams(data_source='cryptocompare').convert_to_source(data_req)
    assert cc_params['freq'] == cc_freq, "Frequency parameter conversion failed."


@pytest.mark.parametrize(
    "dr_freq, cm_freq",
    [
        ('block', '1b'),
        ('tick', 'tick'),
        ('1s', '1s'),
        ('30min', '1m'),
        ('4h', '1h'),
        ('d', '1d'),
        ('w', '1d')
    ]
)
def test_convert_cm_freq(dr_freq, cm_freq) -> None:
    """
    Test frequency parameter conversion to Coin Metrics format.
    """
    data_req = DataRequest(freq=dr_freq)
    cm_params = ConvertParams(data_source='coinmetrics').convert_to_source(data_req)
    assert cm_params['freq'] == cm_freq, "Frequency parameter conversion failed."


@pytest.mark.parametrize(
    "dr_freq, cx_freq",
    [
        ('tick', 'tick'),
        ('5min', '5m'),
        ('d', '1d'),
        ('w', '1w'),
        ('m', '1M'),
        ('3m', '3M'),
        ('q', '1q'),
        ('y', '1y')
    ]
)
def test_convert_cx_freq(dr_freq, cx_freq) -> None:
    """
    Test frequency parameter conversion to CCXT format.
    """
    data_req = DataRequest(freq=dr_freq)
    cx_params = ConvertParams(data_source='ccxt').convert_to_source(data_req)
    assert cx_params['freq'] == cx_freq, "Frequency parameter conversion failed."


@pytest.mark.parametrize(
    "dr_freq, gn_freq",
    [
        ('5min', '10m'),
        ('2h', '1h'),
        ('d', '24h'),
        ('w', '1w'),
        ('m', '1month'),
    ]
)
def test_convert_gn_freq(dr_freq, gn_freq) -> None:
    """
    Test frequency parameter conversion to Glassnode format.
    """
    data_req = DataRequest(freq=dr_freq)
    gn_params = ConvertParams(data_source='glassnode').convert_to_source(data_req)
    assert gn_params['freq'] == gn_freq, "Frequency parameter conversion failed."


@pytest.mark.parametrize(
    "dr_freq, tg_freq",
    [
        ('5min', '5min'),
        ('4h', '1hour'),
        ('d', '1day'),
        ('w', '1week'),
    ]
)
def test_convert_tg_freq(dr_freq, tg_freq) -> None:
    """
    Test frequency parameter conversion to Tiingo format.
    """
    data_req = DataRequest(freq=dr_freq)
    tg_params = ConvertParams(data_source='tiingo').convert_to_source(data_req)
    assert tg_params['freq'] == tg_freq, "Frequency parameter conversion failed."


@pytest.mark.parametrize(
    "dr_freq, ip_freq",
    [
        ('5min', 'Daily'),
        ('d', 'Daily'),
    ]
)
def test_convert_ip_freq(dr_freq, ip_freq) -> None:
    """
    Test frequency parameter conversion to InvestPy format (non-macro categories).
    """
    data_req = DataRequest(freq=dr_freq, cat='rates')
    ip_params = ConvertParams(data_source='investpy').convert_to_source(data_req)
    assert ip_params['freq'] == ip_freq, "Frequency parameter conversion failed."


# convert cryptodatapy quote ccy to source format
@pytest.mark.parametrize(
    "dr_ccy, cv_ccy",
    [
        (None, 'USD'),
        ('usdt', 'USDT'),
        ('btc', 'BTC'),
    ]
)
def test_convert_quote_ccy(dr_ccy, cv_ccy) -> None:
    """
    Test quote currency parameter conversion to CryptoCompare, Glassnode, Alpha Vantage-fx and InvestPy formats.
    """
    data_req = DataRequest(quote_ccy=dr_ccy)
    cv_params = ConvertParams(data_source='cryptocompare').convert_to_source(data_req)
    assert cv_params['quote_ccy'] == cv_ccy, "Quote currency parameter conversion failed."
    cv_params = ConvertParams(data_source='glassnode').convert_to_source(data_req)
    assert cv_params['quote_ccy'] == cv_ccy, "Quote currency parameter conversion failed."
    cv_params = ConvertParams(data_source='av-forex-daily').convert_to_source(data_req)
    assert cv_params['quote_ccy'] == cv_ccy, "Quote currency parameter conversion failed."
    cv_params = ConvertParams(data_source='investpy').convert_to_source(data_req)
    assert cv_params['quote_ccy'] == cv_ccy, "Quote currency parameter conversion failed."


@pytest.mark.parametrize(
    "dr_ccy, cm_ccy",
    [
        (None, 'usdt'),
        ('usdt', 'usdt'),
        ('btc', 'btc'),
    ]
)
def test_convert_cm_quote_ccy(dr_ccy, cm_ccy) -> None:
    """
    Test quote currency parameter conversion to Coin Metrics format.
    """
    data_req = DataRequest(quote_ccy=dr_ccy)
    cm_params = ConvertParams(data_source='coinmetrics').convert_to_source(data_req)
    assert cm_params['quote_ccy'] == cm_ccy, "Quote currency parameter conversion failed."


@pytest.mark.parametrize(
    "dr_ccy, cx_ccy",
    [
        (None, 'USDT'),
        ('usdt', 'USDT'),
        ('btc', 'BTC'),
    ]
)
def test_convert_cx_quote_ccy(dr_ccy, cx_ccy) -> None:
    """
    Test quote currency parameter conversion to CCXT format.
    """
    data_req = DataRequest(quote_ccy=dr_ccy)
    cx_params = ConvertParams(data_source='ccxt').convert_to_source(data_req)
    assert cx_params['quote_ccy'] == cx_ccy, "Quote currency parameter conversion failed."


@pytest.mark.parametrize(
    "dr_ccy, tg_ccy",
    [
        (None, 'usd'),
        ('USDT', 'usdt'),
        ('btc', 'btc'),
    ]
)
def test_convert_tg_quote_ccy(dr_ccy, tg_ccy) -> None:
    """
    Test quote currency parameter conversion to Tiingo format.
    """
    data_req = DataRequest(quote_ccy=dr_ccy)
    tg_params = ConvertParams(data_source='tiingo').convert_to_source(data_req)
    assert tg_params['quote_ccy'] == tg_ccy, "Quote currency parameter conversion failed."


# convert cryptodatapy exch to source format
@pytest.mark.parametrize(
    "dr_exch, cc_exch",
    [
        (None, 'CCCAGG'),
        ('binance', 'binance'),
    ]
)
def test_convert_cc_exchange(dr_exch, cc_exch) -> None:
    """
    Test exchange parameter conversion to CryptoCompare format.
    """
    data_req = DataRequest(exch=dr_exch)
    cc_params = ConvertParams(data_source='cryptocompare').convert_to_source(data_req)
    assert cc_params['exch'] == cc_exch, "Exchange parameter conversion failed."


@pytest.mark.parametrize(
    "dr_exch, cm_exch",
    [
        (None, 'binance'),
        ('coinbase', 'coinbase'),
    ]
)
def test_convert_cm_exchange(dr_exch, cm_exch) -> None:
    """
    Test exchange parameter conversion to Coin Metrics format.
    """
    data_req = DataRequest(exch=dr_exch)
    cm_params = ConvertParams(data_source='coinmetrics').convert_to_source(data_req)
    assert cm_params['exch'] == cm_exch, "Exchange parameter conversion failed."


@pytest.mark.parametrize(
    "dr_exch, cx_exch",
    [
        (None, 'binance'),
        ('binance', 'binanceusdm'),
        ('kucoin', 'kucoinfutures'),
        ('huobi', 'huobipro'),
        ('bitfinex', 'bitfinex2'),
        ('mexc', 'mexc3'),
    ]
)
def test_convert_cx_exchange(dr_exch, cx_exch) -> None:
    """
    Test exchange parameter conversion to CCXT format.
    """
    data_req = DataRequest(exch=dr_exch, mkt_type='perpetual_future')
    cx_params = ConvertParams(data_source='ccxt').convert_to_source(data_req)
    assert cx_params['exch'] == cx_exch, "Exchange parameter conversion failed."


@pytest.mark.parametrize(
    "dr_exch, tg_exch",
    [
        (None, 'iex'),
    ]
)
def test_convert_tg_exchange(dr_exch, tg_exch) -> None:
    """
    Test exchange parameter conversion to Tiingo format.
    """
    data_req = DataRequest(exch=dr_exch, cat='eqty', freq='5min')
    tg_params = ConvertParams(data_source='tiingo').convert_to_source(data_req)
    assert tg_params['exch'] == tg_exch, "Exchange parameter conversion failed."


# convert cryptodatapy tickers to countries for investpy
@pytest.mark.parametrize(
    "dr_tickers, ip_ctys",
    [
        (['US_Rates_10Y', 'CA_Rates_2Y'], ['united states', 'canada']),
        (['CN_Manuf_PMI', 'IN_Infl_CPI_YoY'], ['china', 'india']),
    ]
)
def test_convert_tickers_to_ctys(dr_tickers, ip_ctys) -> None:
    """
    Test tickers to countries parameter conversion in InvestPy format.
    """
    data_req = DataRequest(tickers=dr_tickers, cat='macro')
    ip_params = ConvertParams(data_source='investpy').convert_to_source(data_req)
    assert ip_params['ctys'] == ip_ctys, "Tickers to countries parameter conversion failed."


# convert cryptodatapy start date to source format
@pytest.mark.parametrize(
    "dr_sd, cc_sd",
    [
        (None, 1230940800),
        ('2015-01-01', 1420070400),
        (datetime(2015, 1, 1), 1420070400),
        (pd.Timestamp('2015-01-01 00:00:00'), 1420070400),
    ]
)
def test_convert_cc_start_date(dr_sd, cc_sd) -> None:
    """
    Test start date parameter conversion to CryptoCompare format.
    """
    data_req = DataRequest(start_date=dr_sd)
    cc_params = ConvertParams(data_source='cryptocompare').convert_to_source(data_req)
    assert cc_params['start_date'] == cc_sd, "Start date parameter conversion failed."


@pytest.mark.parametrize(
    "dr_sd, cx_sd",
    [
        (None, 1262304000000),
        ('2015-01-01', 1420070400000),
        (datetime(2015, 1, 1), 1420070400000),
        (pd.Timestamp('2015-01-01 00:00:00'), 1420070400000),
    ]
)
def test_convert_cx_start_date(dr_sd, cx_sd) -> None:
    """
    Test start date parameter conversion to CCXT format.
    """
    data_req = DataRequest(start_date=dr_sd)
    cx_params = ConvertParams(data_source='ccxt').convert_to_source(data_req)
    assert cx_params['start_date'] == cx_sd, "Start date parameter conversion failed."


@pytest.mark.parametrize(
    "dr_sd, gn_sd",
    [
        (None, 1230940800),
        ('2015-01-01', 1420070400),
        (datetime(2015, 1, 1), 1420070400),
        (pd.Timestamp('2015-01-01 00:00:00'), 1420070400),
    ]
)
def test_convert_gn_start_date(dr_sd, gn_sd) -> None:
    """
    Test start date parameter conversion to Glassnode format.
    """
    data_req = DataRequest(start_date=dr_sd)
    gn_params = ConvertParams(data_source='glassnode').convert_to_source(data_req)
    assert gn_params['start_date'] == gn_sd, "Start date parameter conversion failed."


@pytest.mark.parametrize(
    "dr_sd, ip_sd",
    [
        (None, '01/01/1960'),
        ('2015-01-01', '01/01/2015'),
        (datetime(2015, 1, 1), '01/01/2015'),
        (pd.Timestamp('2015-01-01 00:00:00'), '01/01/2015'),
    ]
)
def test_convert_ip_start_date(dr_sd, ip_sd) -> None:
    """
    Test start date parameter conversion to InvestPy format.
    """
    data_req = DataRequest(start_date=dr_sd)
    ip_params = ConvertParams(data_source='investpy').convert_to_source(data_req)
    assert ip_params['start_date'] == ip_sd, "Start date parameter conversion failed."


@pytest.mark.parametrize(
    "dr_sd, cv_sd",
    [
        (None, datetime(1920, 1, 1)),
        ('2015-01-01', datetime.strptime('2015-01-01', '%Y-%m-%d')),
        (datetime(2015, 1, 1), datetime(2015, 1, 1)),
        (pd.Timestamp('2015-01-01 00:00:00'), pd.Timestamp('2015-01-01 00:00:00')),
    ]
)
def test_convert_start_date(dr_sd, cv_sd) -> None:
    """
    Test start date parameter conversion to Fred, Alpha Vantage-daily, Alpha Vantage-forex and Yahoo formats.
    """
    data_req = DataRequest(start_date=dr_sd)
    cv_params = ConvertParams(data_source='fred').convert_to_source(data_req)
    assert cv_params['start_date'] == cv_sd, "Start date parameter conversion failed."
    cv_params = ConvertParams(data_source='av-daily').convert_to_source(data_req)
    assert cv_params['start_date'] == cv_sd, "Start date parameter conversion failed."
    cv_params = ConvertParams(data_source='av-forex-daily').convert_to_source(data_req)
    assert cv_params['start_date'] == cv_sd, "Start date parameter conversion failed."
    cv_params = ConvertParams(data_source='yahoo').convert_to_source(data_req)
    assert cv_params['start_date'] == cv_sd, "Start date parameter conversion failed."


# convert cryptodatapy end date to source format
@pytest.mark.parametrize(
    "dr_ed, cc_ed",
    [
        (None, round(pd.Timestamp(datetime.utcnow()).timestamp())),
        ('2020-12-31', 1609372800),
        (datetime(2020, 12, 31), 1609372800),
        (pd.Timestamp('2020-12-31 00:00:00'), 1609372800),
    ]
)
def test_convert_cc_end_date(dr_ed, cc_ed) -> None:
    """
    Test end date parameter conversion to CryptoCompare format.
    """
    data_req = DataRequest(end_date=dr_ed)
    cc_params = ConvertParams(data_source='cryptocompare').convert_to_source(data_req)
    if dr_ed is None:
        assert pd.to_datetime(cc_params['end_date'], unit='s').date() == datetime.utcnow().date()
    else:
        assert cc_params['end_date'] == cc_ed, "End date parameter conversion failed."


@pytest.mark.parametrize(
    "dr_ed, cx_ed",
    [
        (None, round(pd.Timestamp(datetime.utcnow()).timestamp() * 1e3)),
        ('2020-12-31', 1609372800000),
        (datetime(2020, 12, 31), 1609372800000),
        (pd.Timestamp('2020-12-31 00:00:00'), 1609372800000),
    ]
)
def test_convert_cx_end_date(dr_ed, cx_ed) -> None:
    """
    Test end date parameter conversion to CCXT format.
    """
    data_req = DataRequest(end_date=dr_ed)
    cx_params = ConvertParams(data_source='ccxt').convert_to_source(data_req)
    if dr_ed is None:
        assert pd.to_datetime(cx_params['end_date'], unit='ms').date() == datetime.utcnow().date()
    else:
        assert cx_params['end_date'] == cx_ed, "End date parameter conversion failed."


@pytest.mark.parametrize(
    "dr_ed, gn_ed",
    [
        (None, None),
        ('2020-12-31', 1609372800),
        (datetime(2020, 12, 31), 1609372800),
        (pd.Timestamp('2020-12-31 00:00:00'), 1609372800),
    ]
)
def test_convert_gn_end_date(dr_ed, gn_ed) -> None:
    """
    Test end date parameter conversion to Glassnode format.
    """
    data_req = DataRequest(end_date=dr_ed)
    gn_params = ConvertParams(data_source='glassnode').convert_to_source(data_req)
    assert gn_params['end_date'] == gn_ed, "End date parameter conversion failed."


@pytest.mark.parametrize(
    "dr_ed, ip_ed",
    [
        (None, datetime.utcnow().strftime("%d/%m/%Y")),
        ('2020-12-31', '31/12/2020'),
        (datetime(2020, 12, 31), '31/12/2020'),
        (pd.Timestamp('2020-12-31 00:00:00'), '31/12/2020'),
    ]
)
def test_convert_ip_end_date(dr_ed, ip_ed) -> None:
    """
    Test end date parameter conversion to InvestPy format.
    """
    data_req = DataRequest(end_date=dr_ed)
    ip_params = ConvertParams(data_source='investpy').convert_to_source(data_req)
    assert ip_params['end_date'] == ip_ed, "End date parameter conversion failed."


@pytest.mark.parametrize(
    "dr_ed, cv_ed",
    [
        (None, datetime.utcnow()),
        ('2020-12-31', '2020-12-31'),
        (datetime(2020, 12, 31), datetime(2020, 12, 31)),
        (pd.Timestamp('2020-12-31 00:00:00'), pd.Timestamp('2020-12-31 00:00:00')),
    ]
)
def test_convert_end_date(dr_ed, cv_ed) -> None:
    """
    Test end date parameter conversion to Tiingo, Fred, Alpha Vantage-daily and Yahoo formats.
    """
    data_req = DataRequest(end_date=dr_ed)
    if dr_ed is None:
        cv_params = ConvertParams(data_source='tiingo').convert_to_source(data_req)
        assert cv_params['end_date'].date() == datetime.utcnow().date()
    else:
        cv_params = ConvertParams(data_source='tiingo').convert_to_source(data_req)
        assert cv_params['end_date'] == cv_ed, "End date parameter conversion failed."
        cv_params = ConvertParams(data_source='fred').convert_to_source(data_req)
        assert cv_params['end_date'] == cv_ed, "End date parameter conversion failed."
        cv_params = ConvertParams(data_source='av-daily').convert_to_source(data_req)
        assert cv_params['end_date'] == cv_ed, "End date parameter conversion failed."
        cv_params = ConvertParams(data_source='yahoo').convert_to_source(data_req)
        assert cv_params['end_date'] == cv_ed, "End date parameter conversion failed."


# convert cryptodatapy fields to source format
@pytest.mark.parametrize(
    "dr_fields, cv_fields",
    [
        ('date', ['Date']),
        (['open', 'high', 'low', 'close', 'volume'], ['Open', 'High', 'Low', 'Close', 'Volume']),
    ]
)
def test_convert_fields(dr_fields, cv_fields) -> None:
    """
    Test fields parameter conversion to InvestPy and Yahoo formats.
    """
    data_req = DataRequest(fields=dr_fields)
    cv_params = ConvertParams(data_source='investpy').convert_to_source(data_req)
    assert cv_params['fields'] == cv_fields, "Fields parameter conversion failed."
    cv_params = ConvertParams(data_source='yahoo').convert_to_source(data_req)
    assert cv_params['fields'] == cv_fields, "Fields parameter conversion failed."


@pytest.mark.parametrize(
    "dr_fields, cc_fields",
    [
        ('date', ['time']),
        ('ticker', ['symbol']),
        ('add_act', ['active_addresses']),
        (['open', 'high', 'low', 'close', 'volume'], ['open', 'high', 'low', 'close', 'volumefrom']),
    ]
)
def test_convert_cc_fields(dr_fields, cc_fields) -> None:
    """
    Test fields parameter conversion to CryptoCompare format.
    """
    data_req = DataRequest(fields=dr_fields)
    cc_params = ConvertParams(data_source='cryptocompare').convert_to_source(data_req)
    assert cc_params['fields'] == cc_fields, "Fields parameter conversion failed."


@pytest.mark.parametrize(
    "dr_fields, cm_fields",
    [
        ('date', ['time']),
        ('ticker', ['market']),
        ('add_act', ['AdrActCnt']),
        (['open', 'high', 'low', 'close', 'volume'],
         ['price_open', 'price_high', 'price_low', 'price_close', 'volume']),
    ]
)
def test_convert_cm_fields(dr_fields, cm_fields) -> None:
    """
    Test fields parameter conversion to Coin Metrics format.
    """
    data_req = DataRequest(fields=dr_fields)
    cm_params = ConvertParams(data_source='coinmetrics').convert_to_source(data_req)
    assert cm_params['fields'] == cm_fields, "Fields parameter conversion failed."


# convert cryptodatapy timezone to source format
@pytest.mark.parametrize(
    "dr_tz, cv_tz",
    [
        (None, 'UTC'),
    ]
)
def test_convert_utc_tz(dr_tz, cv_tz) -> None:
    """
    Test timezone parameter conversion to Coin Metrics, CryptoCompare adn Glassnode formats.
    """
    data_req = DataRequest(tz=dr_tz)
    cv_params = ConvertParams(data_source='coinmetrics').convert_to_source(data_req)
    assert cv_params['tz'] == cv_tz, "Timezone parameter conversion failed."
    cv_params = ConvertParams(data_source='cryptocompare').convert_to_source(data_req)
    assert cv_params['tz'] == cv_tz, "Timezone parameter conversion failed."
    cv_params = ConvertParams(data_source='glassnode').convert_to_source(data_req)
    assert cv_params['tz'] == cv_tz, "Timezone parameter conversion failed."


@pytest.mark.parametrize(
    "dr_tz, am_tz",
    [
        (None, 'America/New_York'),
    ]
)
def test_convert_am_tz(dr_tz, am_tz) -> None:
    """
    Test timezone parameter conversion to Fred, Alpha Vantage-daily and Alpha Vantage-forex formats.
    """
    data_req = DataRequest(tz=dr_tz)
    am_params = ConvertParams(data_source='fred').convert_to_source(data_req)
    assert am_params['tz'] == am_tz, "Timezone parameter conversion failed."
    data_req = DataRequest(tz=dr_tz)
    am_params = ConvertParams(data_source='av-daily').convert_to_source(data_req)
    assert am_params['tz'] == am_tz, "Timezone parameter conversion failed."
    data_req = DataRequest(tz=dr_tz)
    am_params = ConvertParams(data_source='av-forex-daily').convert_to_source(data_req)
    assert am_params['tz'] == am_tz, "Timezone parameter conversion failed."


# convert cryptodatapy institution to source format
@pytest.mark.parametrize(
    "dr_inst, cm_inst",
    [
        (None, 'grayscale'),
        ('Purpose', 'purpose'),
    ]
)
def test_convert_cm_inst(dr_inst, cm_inst) -> None:
    """
    Test institution parameter conversion to Coin Metrics format.
    """
    data_req = DataRequest(inst=dr_inst)
    cm_params = ConvertParams(data_source='coinmetrics').convert_to_source(data_req)
    assert cm_params['inst'] == cm_inst, "Institution parameter conversion failed."


@pytest.mark.parametrize(
    "dr_inst, gn_inst",
    [
        (None, 'purpose'),
        ('grayscale', 'grayscale'),
    ]
)
def test_convert_gn_inst(dr_inst, gn_inst) -> None:
    """
    Test institution parameter conversion to Glassnode format.
    """
    data_req = DataRequest(inst=dr_inst)
    gn_params = ConvertParams(data_source='glassnode').convert_to_source(data_req)
    assert gn_params['inst'] == gn_inst, "Institution parameter conversion failed."


if __name__ == "__main__":
    pytest.main()
