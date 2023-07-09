from datetime import datetime

import pandas as pd
import pytest

from cryptodatapy.extract.datarequest import DataRequest
from cryptodatapy.transform.convertparams import ConvertParams


@pytest.fixture
def datarequest():
    return DataRequest()


# Tickers
# convert cryptodatapy tickers to source format
@pytest.mark.parametrize(
    "dr_tickers, cv_tickers",
    [
        ("btc", ["BTC"]),
        ("eth", ["ETH"]),
        (["spy", "tlt"], ["SPY", "TLT"]),
        (["btc", "eth", "sol"], ["BTC", "ETH", "SOL"]),
    ],
)
def test_convert_tickers(dr_tickers, cv_tickers) -> None:
    """
    Test tickers parameter conversion to CryptoCompare and CCXT formats.
    """
    data_req = DataRequest(tickers=dr_tickers)
    cc_params = ConvertParams(data_req=data_req).to_cryptocompare()
    assert cc_params["tickers"] == cv_tickers, "Tickers parameter conversion failed."
    cc_params = ConvertParams(data_req=data_req).to_ccxt()
    assert cc_params["tickers"] == cv_tickers, "Tickers parameter conversion failed."


@pytest.mark.parametrize(
    "dr_tickers, tg_tickers",
    [("SPY", ["spy"]), (["SPY", "TLT", "VXX"], ["spy", "tlt", "vxx"]), ("EUR", ["eur"]),
     (["BTC", "ETH"], ["btc", "eth"])],
)
def test_convert_tg_tickers(dr_tickers, tg_tickers) -> None:
    """
    Test tickers parameter conversion to Tiingo format.
    """
    data_req = DataRequest(tickers=dr_tickers)
    tg_params = ConvertParams(data_req=data_req).to_tiingo()
    assert tg_params["tickers"] == tg_tickers, "Tickers parameter conversion failed."


# fx mkts
@pytest.mark.parametrize(
    "dr_tickers, fx_mkts",
    [(["cad", "eur", "jpy"], ["usdcad", "eurusd", "usdjpy"]), ("mxn", ["usdmxn"])],
)
def test_convert_tickers_to_tg_fx_mkts(dr_tickers, fx_mkts) -> None:
    """
    Test tickers to markets parameter conversion for Tiingo format.
    """
    data_req = DataRequest(tickers=dr_tickers, cat="fx")
    tg_params = ConvertParams(data_req=data_req).to_tiingo()
    assert (
        tg_params["mkts"] == fx_mkts
    ), "Tickers to markets parameter conversion failed."


@pytest.mark.parametrize(
    "dr_tickers, db_tickers",
    [
        ("US_GDP_Sh_PPP", ["IMF/WEO:2021-04/USA.PPPSH.pcent"]),
        (
            ["EZ_Infl_Exp_1Y", "US_MB"],
            ["ECB/SPF/M.U2.HICP.POINT.P12M.Q.AVG", "FED/H6_H6_MBASE/RESMO14A_N.M"],
        ),
    ],
)
def test_convert_db_tickers(dr_tickers, db_tickers) -> None:
    """
    Test tickers parameter conversion to DBnomics format.
    """
    data_req = DataRequest(tickers=dr_tickers)
    db_params = ConvertParams(data_req=data_req).to_dbnomics()
    assert db_params["tickers"] == db_tickers, "Tickers parameter conversion failed."


@pytest.mark.parametrize(
    "dr_tickers, fred_tickers",
    [
        ("US_UE_Rate", ["UNRATE"]),
        (
            ["US_Credit_IG_Yield", "US_Credit_HY_Yield"],
            ["BAMLC0A0CMEY", "BAMLH0A0HYM2EY"],
        ),
    ],
)
def test_convert_fred_tickers(dr_tickers, fred_tickers) -> None:
    """
    Test tickers parameter conversion to Fred format.
    """
    data_req = DataRequest(tickers=dr_tickers)
    fred_params = ConvertParams(data_req=data_req).to_fred()
    assert fred_params["tickers"] == fred_tickers, "Tickers parameter conversion failed."


@pytest.mark.parametrize(
    "dr_tickers, yahoo_tickers",
    [
        ("Corn", ['ZC=F']),
        (
            ['US_Rates_10Y', 'US_Eqty_Idx'],
            ['^TNX', '^GSPC'],
        ),
    ],
)
def test_convert_yahoo_tickers(dr_tickers, yahoo_tickers) -> None:
    """
    Test tickers parameter conversion to Yahoo format.
    """
    data_req = DataRequest(tickers=dr_tickers)
    yahoo_params = ConvertParams(data_req=data_req).to_yahoo()
    assert yahoo_params["tickers"] == yahoo_tickers, "Tickers parameter conversion failed."


@pytest.mark.parametrize(
    "dr_tickers, wb_tickers",
    [
        ("WL_Internet_Users_Sh_Pop", ['IT.NET.USER.ZS']),
        (
            ['US_GDP_Real_USD', 'WL_GDP_Real_USD'],
            ['NY.GDP.MKTP.KD'],
        ),
    ],
)
def test_convert_wb_tickers(dr_tickers, wb_tickers) -> None:
    """
    Test tickers parameter conversion to World Bank format.
    """
    data_req = DataRequest(tickers=dr_tickers, cat='macro')
    wb_params = ConvertParams(data_req=data_req).to_wb()
    assert wb_params["tickers"] == wb_tickers, "World Bank tickers parameter conversion failed."


@pytest.mark.parametrize(
    "dr_tickers, ff_tickers",
    [
        ("US_Eqty_Val", ["F-F_Research_Data_Factors_daily"]),
        (
            ['US_Eqty_Val', 'US_Eqty_Mom'],
            ['F-F_Research_Data_Factors_daily', 'F-F_Momentum_Factor_daily'],
        ),
    ],
)
def test_convert_ff_tickers(dr_tickers, ff_tickers) -> None:
    """
    Test tickers parameter conversion to Fama French format.
    """
    data_req = DataRequest(tickers=dr_tickers)
    ff_params = ConvertParams(data_req=data_req).to_famafrench()
    assert ff_params["tickers"] == ff_tickers, "Fama French tickers parameter conversion failed."


@pytest.mark.parametrize(
    "dr_tickers, aqr_tickers",
    [
        ("Cmdty_ER", {'Cmdty_ER': ('Commodities-for-the-Long-Run-Index-Level-Data-', 'Commodities for the Long Run')}),
        (
            ['WL_Eqty_Mom', 'WL_FX_Carry'],
            {'WL_Eqty_Mom': ('The-Devil-in-HMLs-Details-Factors-', 'UMD'),
             'WL_FX_Carry': ('Century-of-Factor-Premia-', 'Century of Factor Premia')},
        ),
    ],
)
def test_convert_aqr_tickers(dr_tickers, aqr_tickers) -> None:
    """
    Test tickers parameter conversion to AQR format.
    """
    data_req = DataRequest(tickers=dr_tickers)
    aqr_params = ConvertParams(data_req=data_req).to_aqr()
    assert aqr_params["tickers"] == aqr_tickers, "AQR tickers parameter conversion failed."


# convert cryptodatapy params to markets in source format
@pytest.mark.parametrize(
    "dr_tickers, cm_mkts",
    [("btc", ["binance-btc-usdt-spot"]), ("eth", ["binance-eth-usdt-spot"])],
)
def test_convert_tickers_to_cm_mkts(dr_tickers, cm_mkts) -> None:
    """
    Test tickers to markets parameter conversion for Coin Metrics format.
    """
    data_req = DataRequest(tickers=dr_tickers)
    cm_params = ConvertParams(data_req=data_req).to_coinmetrics()
    assert (
        cm_params["mkts"] == cm_mkts
    ), "Tickers to markets parameter conversion failed."


@pytest.mark.parametrize(
    "dr_mkt_types, cm_mkts",
    [
        ("spot", ["binance-btc-usdt-spot"]),
        ("perpetual_future", ["binance-BTCUSDT-future"]),
    ],
)
def test_convert_mkt_types_to_cm_mkts(dr_mkt_types, cm_mkts) -> None:
    """
    Test market type to markets parameter conversion for Coin Metrics format.
    """
    data_req = DataRequest(mkt_type=dr_mkt_types)
    cm_params = ConvertParams(data_req=data_req).to_coinmetrics()
    assert (
        cm_params["mkts"] == cm_mkts
    ), "Market type to markets parameter conversion failed."


@pytest.mark.parametrize(
    "dr_exch, cm_mkts",
    [
        ("bitmex", ["bitmex-BTCUSDT-future"]),
        ("ftx", ["ftx-BTC-PERP-future"]),
        ("okex", ["okex-BTC-USDT-SWAP-future"]),
        ("huobi", ["huobi-BTC-USDT_SWAP-future"]),
    ],
)
def test_convert_exchange_to_cm_mkts(dr_exch, cm_mkts) -> None:
    """
    Test exchange to markets parameter conversion for Coin Metrics format.
    """
    data_req = DataRequest(exch=dr_exch, mkt_type="perpetual_future")
    cm_params = ConvertParams(data_req=data_req).to_coinmetrics()
    assert (
        cm_params["mkts"] == cm_mkts
    ), "Exchange to markets parameter conversion failed."


@pytest.mark.parametrize(
    "dr_exch, cx_mkts",
    [
        ("binance", ["BTC/USDT"]),
        ("kucoin", ["BTC/USDT:USDT"]),
        ("huobi", ["BTC/USDT:USDT"]),
        ("okx", ["BTC/USDT:USDT"]),
    ],
)
def test_convert_exch_to_cx_mkts(dr_exch, cx_mkts) -> None:
    """
    Test exchange to markets parameter conversion for CCXT format.
    """
    data_req = DataRequest(exch=dr_exch, mkt_type="perpetual_future")
    cx_params = ConvertParams(data_req=data_req).to_ccxt()
    assert (
        cx_params["mkts"] == cx_mkts
    ), "Exchange to markets parameter conversion failed."


# convert cryptodatapy freq to source format
@pytest.mark.parametrize(
    "dr_freq, cc_freq",
    [
        ("5min", "histominute"),
        ("30min", "histominute"),
        ("1h", "histohour"),
        ("d", "histoday"),
        ("w", "histoday"),
    ],
)
def test_convert_cc_freq(dr_freq, cc_freq) -> None:
    """
    Test frequency parameter conversion to CryptoCompare format.
    """
    data_req = DataRequest(freq=dr_freq)
    cc_params = ConvertParams(data_req=data_req).to_cryptocompare()
    assert cc_params["freq"] == cc_freq, "Frequency parameter conversion failed."


@pytest.mark.parametrize(
    "dr_freq, cm_freq",
    [
        ("block", "1b"),
        ("tick", "tick"),
        ("1s", "1s"),
        ("30min", "1m"),
        ("4h", "1h"),
        ("d", "1d"),
        ("w", "1d"),
    ],
)
def test_convert_cm_freq(dr_freq, cm_freq) -> None:
    """
    Test frequency parameter conversion to Coin Metrics format.
    """
    data_req = DataRequest(freq=dr_freq)
    cm_params = ConvertParams(data_req=data_req).to_coinmetrics()
    assert cm_params["freq"] == cm_freq, "Frequency parameter conversion failed."


@pytest.mark.parametrize(
    "dr_freq, cx_freq",
    [
        ("tick", "tick"),
        ("5min", "1m"),
        ("d", "1d"),
        ("w", "1d"),
        ("m", "1d"),
        ("3m", "1d"),
        ("q", "1d"),
        ("y", "1d"),
    ],
)
def test_convert_cx_freq(dr_freq, cx_freq) -> None:
    """
    Test frequency parameter conversion to CCXT format.
    """
    data_req = DataRequest(freq=dr_freq)
    cx_params = ConvertParams(data_req=data_req).to_coinmetrics()
    assert cx_params["freq"] == cx_freq, "Frequency parameter conversion failed."


@pytest.mark.parametrize(
    "dr_freq, gn_freq",
    [
        ("5min", "10m"),
        ("2h", "1h"),
        ("d", "24h"),
        ("w", "1w"),
        ("m", "1month"),
    ],
)
def test_convert_gn_freq(dr_freq, gn_freq) -> None:
    """
    Test frequency parameter conversion to Glassnode format.
    """
    data_req = DataRequest(freq=dr_freq)
    gn_params = ConvertParams(data_req=data_req).to_glassnode()
    assert gn_params["freq"] == gn_freq, "Frequency parameter conversion failed."


@pytest.mark.parametrize(
    "dr_freq, tg_freq",
    [
        ("5min", "5min"),
        ("4h", "1hour"),
        ("d", "1day"),
        ("w", "1week"),
    ],
)
def test_convert_tg_freq(dr_freq, tg_freq) -> None:
    """
    Test frequency parameter conversion to Tiingo format.
    """
    data_req = DataRequest(freq=dr_freq)
    tg_params = ConvertParams(data_req=data_req).to_tiingo()
    assert tg_params["freq"] == tg_freq, "Tiingo frequency parameter conversion failed."


@pytest.mark.parametrize(
    "dr_freq, aqr_freq",
    [
        ("d", "Daily"),
        ("w", "Daily"),
        ("m", "Monthly"),
    ],
)
def test_convert_aqr_freq(dr_freq, aqr_freq) -> None:
    """
    Test frequency parameter conversion to AQR format.
    """
    data_req = DataRequest(tickers="US_Eqty_Val", freq=dr_freq)
    aqr_params = ConvertParams(data_req=data_req).to_aqr()
    assert aqr_params["freq"] == aqr_freq, "AQR frequency parameter conversion failed."


# convert cryptodatapy quote ccy to source format
@pytest.mark.parametrize(
    "dr_ccy, cv_ccy",
    [
        (None, "USD"),
        ("usdt", "USDT"),
        ("btc", "BTC"),
    ],
)
def test_convert_quote_ccy(dr_ccy, cv_ccy) -> None:
    """
    Test quote currency parameter conversion to CryptoCompare, and Glassnode formats.
    """
    data_req = DataRequest(quote_ccy=dr_ccy)
    cv_params = ConvertParams(data_req=data_req).to_cryptocompare()
    assert (
        cv_params["quote_ccy"] == cv_ccy
    ), "Quote currency parameter conversion failed."
    cv_params = ConvertParams(data_req=data_req).to_glassnode()
    assert (
        cv_params["quote_ccy"] == cv_ccy
    ), "Quote currency parameter conversion failed."


@pytest.mark.parametrize(
    "dr_ccy, cm_ccy",
    [
        (None, "usdt"),
        ("usdt", "usdt"),
        ("btc", "btc"),
    ],
)
def test_convert_cm_quote_ccy(dr_ccy, cm_ccy) -> None:
    """
    Test quote currency parameter conversion to Coin Metrics format.
    """
    data_req = DataRequest(quote_ccy=dr_ccy)
    cm_params = ConvertParams(data_req=data_req).to_coinmetrics()
    assert (
        cm_params["quote_ccy"] == cm_ccy
    ), "Quote currency parameter conversion failed."


@pytest.mark.parametrize(
    "dr_ccy, cx_ccy",
    [
        (None, "USDT"),
        ("usdt", "USDT"),
        ("btc", "BTC"),
    ],
)
def test_convert_cx_quote_ccy(dr_ccy, cx_ccy) -> None:
    """
    Test quote currency parameter conversion to CCXT format.
    """
    data_req = DataRequest(quote_ccy=dr_ccy)
    cx_params = ConvertParams(data_req=data_req).to_ccxt()
    assert (
        cx_params["quote_ccy"] == cx_ccy
    ), "Quote currency parameter conversion failed."


@pytest.mark.parametrize(
    "dr_ccy, tg_ccy",
    [
        (None, "usd"),
        ("USDT", "usdt"),
        ("btc", "btc"),
    ],
)
def test_convert_tg_quote_ccy(dr_ccy, tg_ccy) -> None:
    """
    Test quote currency parameter conversion to Tiingo format.
    """
    data_req = DataRequest(quote_ccy=dr_ccy)
    tg_params = ConvertParams(data_req=data_req).to_tiingo()
    assert (
        tg_params["quote_ccy"] == tg_ccy
    ), "Quote currency parameter conversion failed."


@pytest.mark.parametrize(
    "dr_ccy, wb_ccy",
    [
        (None, "USD"),
        ("usd", "USD"),
        ("eur", "EUR"),
    ],
)
def test_convert_wb_quote_ccy(dr_ccy, wb_ccy) -> None:
    """
    Test quote currency parameter conversion to World Bank format.
    """
    data_req = DataRequest(quote_ccy=dr_ccy)
    wb_params = ConvertParams(data_req=data_req).to_wb()
    assert (
        wb_params["quote_ccy"] == wb_ccy
    ), "World Bank quote currency parameter conversion failed."


# convert cryptodatapy exch to source format
@pytest.mark.parametrize(
    "dr_exch, cc_exch",
    [
        (None, "CCCAGG"),
        ("binance", "binance"),
    ],
)
def test_convert_cc_exchange(dr_exch, cc_exch) -> None:
    """
    Test exchange parameter conversion to CryptoCompare format.
    """
    data_req = DataRequest(exch=dr_exch)
    cc_params = ConvertParams(data_req=data_req).to_cryptocompare()
    assert cc_params["exch"] == cc_exch, "Exchange parameter conversion failed."


@pytest.mark.parametrize(
    "dr_exch, cm_exch",
    [
        (None, "binance"),
        ("coinbase", "coinbase"),
    ],
)
def test_convert_cm_exchange(dr_exch, cm_exch) -> None:
    """
    Test exchange parameter conversion to Coin Metrics format.
    """
    data_req = DataRequest(exch=dr_exch)
    cm_params = ConvertParams(data_req=data_req).to_coinmetrics()
    assert cm_params["exch"] == cm_exch, "Exchange parameter conversion failed."


@pytest.mark.parametrize(
    "dr_exch, cx_exch",
    [
        (None, "binanceusdm"),
        ("binance", "binanceusdm"),
        ("kucoin", "kucoinfutures"),
        ("huobi", "huobipro"),
        ("bitfinex", "bitfinex2"),
        ("mexc", "mexc3"),
    ],
)
def test_convert_cx_exchange(dr_exch, cx_exch) -> None:
    """
    Test exchange parameter conversion to CCXT format.
    """
    data_req = DataRequest(exch=dr_exch, mkt_type="perpetual_future")
    cx_params = ConvertParams(data_req=data_req).to_ccxt()
    assert cx_params["exch"] == cx_exch, "Exchange parameter conversion failed."


@pytest.mark.parametrize(
    "dr_exch, tg_exch",
    [
        (None, "iex"),
    ],
)
def test_convert_tg_exchange(dr_exch, tg_exch) -> None:
    """
    Test exchange parameter conversion to Tiingo format.
    """
    data_req = DataRequest(exch=dr_exch, cat="eqty", freq="5min")
    tg_params = ConvertParams(data_req=data_req).to_tiingo()
    assert tg_params["exch"] == tg_exch, "Exchange parameter conversion failed."


# convert cryptodatapy start date to source format
@pytest.mark.parametrize(
    "dr_sd, cc_sd",
    [
        (None, 1230940800),
        ("2015-01-01", 1420070400),
        (datetime(2015, 1, 1), 1420070400),
        (pd.Timestamp("2015-01-01 00:00:00"), 1420070400),
    ],
)
def test_convert_cc_start_date(dr_sd, cc_sd) -> None:
    """
    Test start date parameter conversion to CryptoCompare format.
    """
    data_req = DataRequest(start_date=dr_sd)
    cc_params = ConvertParams(data_req=data_req).to_cryptocompare()
    assert cc_params["start_date"] == cc_sd, "Start date parameter conversion failed."


@pytest.mark.parametrize(
    "dr_sd, cx_sd",
    [
        (None, 1262304000000),
        ("2015-01-01", 1420070400000),
        (datetime(2015, 1, 1), 1420070400000),
        (pd.Timestamp("2015-01-01 00:00:00"), 1420070400000),
    ],
)
def test_convert_cx_start_date(dr_sd, cx_sd) -> None:
    """
    Test start date parameter conversion to CCXT format.
    """
    data_req = DataRequest(start_date=dr_sd)
    cx_params = ConvertParams(data_req=data_req).to_ccxt()
    assert cx_params["start_date"] == cx_sd, "Start date parameter conversion failed."


@pytest.mark.parametrize(
    "dr_sd, gn_sd",
    [
        (None, 1230940800),
        ("2015-01-01", 1420070400),
        (datetime(2015, 1, 1), 1420070400),
        (pd.Timestamp("2015-01-01 00:00:00"), 1420070400),
    ],
)
def test_convert_gn_start_date(dr_sd, gn_sd) -> None:
    """
    Test start date parameter conversion to Glassnode format.
    """
    data_req = DataRequest(start_date=dr_sd)
    gn_params = ConvertParams(data_req=data_req).to_glassnode()
    assert gn_params["start_date"] == gn_sd, "Start date parameter conversion failed."


@pytest.mark.parametrize(
    "dr_sd, f_sd",
    [
        (None, datetime(1920, 1, 1)),
        ("2015-01-01", datetime(2015, 1, 1, 0, 0)),
        (datetime(2015, 1, 1), datetime(2015, 1, 1)),
        (pd.Timestamp("2015-01-01 00:00:00"), pd.Timestamp("2015-01-01 00:00:00")),
    ],
)
def test_convert_fred_start_date(dr_sd, f_sd) -> None:
    """
    Test start date parameter conversion to Fred format.
    """
    data_req = DataRequest(start_date=dr_sd, tickers='US_UE_Rate')
    cv_params = ConvertParams(data_req=data_req).to_fred()
    assert cv_params["start_date"] == f_sd, "Start date parameter conversion failed."


@pytest.mark.parametrize(
    "dr_sd, wb_sd",
    [
        (None, 1920),
        ("2015-01-01", 2015),
    ],
)
def test_convert_wb_start_date(dr_sd, wb_sd) -> None:
    """
    Test start date parameter conversion to Yahoo format.
    """
    data_req = DataRequest(tickers='US_GDP_Real_USD', start_date=dr_sd)
    wb_params = ConvertParams(data_req=data_req).to_wb()
    assert wb_params["start_date"] == wb_sd, "World Bank start date parameter conversion failed."


@pytest.mark.parametrize(
    "dr_sd, y_sd",
    [
        (None, '1920-01-01'),
        ("2015-01-01", datetime(2015, 1, 1, 0, 0)),
    ],
)
def test_convert_yahoo_start_date(dr_sd, y_sd) -> None:
    """
    Test start date parameter conversion to Yahoo format.
    """
    data_req = DataRequest(tickers='US_Eqty_Idx', start_date=dr_sd)
    y_params = ConvertParams(data_req=data_req).to_yahoo()
    assert y_params["start_date"] == y_sd, "Yahoo start date parameter conversion failed."


@pytest.mark.parametrize(
    "dr_sd, ff_sd",
    [
        (None, datetime(1920, 1, 1, 0, 0)),
        ("2015-01-01", datetime(2015, 1, 1, 0, 0)),
    ],
)
def test_convert_famafrench_start_date(dr_sd, ff_sd) -> None:
    """
    Test start date parameter conversion to Fama French format.
    """
    data_req = DataRequest(tickers='US_Eqty_Val', start_date=dr_sd)
    ff_params = ConvertParams(data_req=data_req).to_famafrench()
    assert ff_params["start_date"] == ff_sd, "Fama French start date parameter conversion failed."


# convert cryptodatapy end date to source format
@pytest.mark.parametrize(
    "dr_ed, cc_ed",
    [
        (None, round(pd.Timestamp(datetime.utcnow()).timestamp())),
        ("2020-12-31", 1609372800),
        (datetime(2020, 12, 31), 1609372800),
        (pd.Timestamp("2020-12-31 00:00:00"), 1609372800),
    ],
)
def test_convert_cc_end_date(dr_ed, cc_ed) -> None:
    """
    Test end date parameter conversion to CryptoCompare format.
    """
    data_req = DataRequest(end_date=dr_ed)
    cc_params = ConvertParams(data_req=data_req).to_cryptocompare()
    if dr_ed is None:
        assert (
            pd.to_datetime(cc_params["end_date"], unit="s").date()
            == datetime.utcnow().date()
        )
    else:
        assert cc_params["end_date"] == cc_ed, "End date parameter conversion failed."


@pytest.mark.parametrize(
    "dr_ed, cx_ed",
    [
        (None, round(pd.Timestamp(datetime.utcnow()).timestamp() * 1e3)),
        ("2020-12-31", 1609372800000),
        (datetime(2020, 12, 31), 1609372800000),
        (pd.Timestamp("2020-12-31 00:00:00"), 1609372800000),
    ],
)
def test_convert_cx_end_date(dr_ed, cx_ed) -> None:
    """
    Test end date parameter conversion to CCXT format.
    """
    data_req = DataRequest(end_date=dr_ed)
    cx_params = ConvertParams(data_req=data_req).to_ccxt()
    if dr_ed is None:
        assert (
            pd.to_datetime(cx_params["end_date"], unit="ms").date()
            == datetime.utcnow().date()
        )
    else:
        assert cx_params["end_date"] == cx_ed, "End date parameter conversion failed."


@pytest.mark.parametrize(
    "dr_ed, gn_ed",
    [
        (None, None),
        ("2020-12-31", 1609372800),
        (datetime(2020, 12, 31), 1609372800),
        (pd.Timestamp("2020-12-31 00:00:00"), 1609372800),
    ],
)
def test_convert_gn_end_date(dr_ed, gn_ed) -> None:
    """
    Test end date parameter conversion to Glassnode format.
    """
    data_req = DataRequest(end_date=dr_ed)
    gn_params = ConvertParams(data_req=data_req).to_glassnode()
    assert gn_params["end_date"] == gn_ed, "End date parameter conversion failed."


@pytest.mark.parametrize(
    "dr_ed, cv_ed",
    [
        (None, datetime.utcnow().date()),
        ("2020-12-31", datetime(2020, 12, 31, 0, 0)),
        # (datetime(2020, 12, 31), datetime(2020, 12, 31)),
        (pd.Timestamp("2020-12-31 00:00:00"), pd.Timestamp("2020-12-31 00:00:00")),
    ],
)
def test_convert_end_date(dr_ed, cv_ed) -> None:
    """
    Test end date parameter conversion to Tiingo, Fred and Yahoo formats.
    """
    data_req = DataRequest(end_date=dr_ed)
    if dr_ed is None:
        cv_params = ConvertParams(data_req=data_req).to_tiingo()
        assert cv_params["end_date"].date() == cv_ed
        cv_params = ConvertParams(data_req=data_req).to_fred()
        assert cv_params["end_date"].date() == cv_ed
        cv_params = ConvertParams(data_req=data_req).to_yahoo()
        assert cv_params["end_date"] == datetime.utcnow().strftime('%Y-%m-%d')
    else:
        cv_params = ConvertParams(data_req=data_req).to_tiingo()
        assert cv_params["end_date"] == cv_ed, "Tiingo end date parameter conversion failed."
        cv_params = ConvertParams(data_req=data_req).to_fred()
        assert cv_params["end_date"] == cv_ed, "Fred end date parameter conversion failed."
        cv_params = ConvertParams(data_req=data_req).to_yahoo()
        assert cv_params["end_date"] == cv_ed, "Yahoo end date parameter conversion failed."
        cv_params = ConvertParams(data_req=data_req).to_famafrench()
        assert cv_params["end_date"] == cv_ed, "Fama-French end date parameter conversion failed."


@pytest.mark.parametrize(
    "dr_ed, wb_ed",
    [
        (None, datetime.utcnow().year),
        ("2015-01-01", 2015),
    ],
)
def test_convert_wb_end_date(dr_ed, wb_ed) -> None:
    """
    Test start date parameter conversion to World Bank format.
    """
    data_req = DataRequest(tickers='US_GDP_Real_USD', end_date=dr_ed)
    wb_params = ConvertParams(data_req=data_req).to_wb()
    assert wb_params["end_date"] == wb_ed, "World Bank end date parameter conversion failed."


# convert cryptodatapy fields to source format
@pytest.mark.parametrize(
    "dr_fields, cv_fields",
    [
        ("date", ["Date"]),
        (
            ["open", "high", "low", "close", 'close_adj', "volume"],
            ["Open", "High", "Low", "Close", "Adj Close", "Volume"],
        ),
    ],
)
def test_convert_fields(dr_fields, cv_fields) -> None:
    """
    Test fields parameter conversion to InvestPy and Yahoo formats.
    """
    data_req = DataRequest(fields=dr_fields)
    cv_params = ConvertParams(data_req=data_req).to_yahoo()
    assert cv_params["fields"] == cv_fields, "Fields parameter conversion failed."


@pytest.mark.parametrize(
    "dr_fields, cc_fields",
    [
        ("date", ["time"]),
        ("ticker", ["symbol"]),
        ("add_act", ["active_addresses"]),
        (
            ["open", "high", "low", "close", "volume"],
            ["open", "high", "low", "close", "volumefrom"],
        ),
    ],
)
def test_convert_cc_fields(dr_fields, cc_fields) -> None:
    """
    Test fields parameter conversion to CryptoCompare format.
    """
    data_req = DataRequest(fields=dr_fields)
    cc_params = ConvertParams(data_req=data_req).to_cryptocompare()
    assert cc_params["fields"] == cc_fields, "Fields parameter conversion failed."


@pytest.mark.parametrize(
    "dr_fields, cm_fields",
    [
        ("date", ["time"]),
        ("ticker", ["market"]),
        ("add_act", ["AdrActCnt"]),
        (
            ["open", "high", "low", "close", "volume"],
            ["price_open", "price_high", "price_low", "price_close", "volume"],
        ),
    ],
)
def test_convert_cm_fields(dr_fields, cm_fields) -> None:
    """
    Test fields parameter conversion to Coin Metrics format.
    """
    data_req = DataRequest(fields=dr_fields)
    cm_params = ConvertParams(data_req=data_req).to_coinmetrics()
    assert cm_params["fields"] == cm_fields, "Fields parameter conversion failed."


# convert cryptodatapy timezone to source format
@pytest.mark.parametrize(
    "dr_tz, cv_tz",
    [
        (None, "UTC"),
    ],
)
def test_convert_utc_tz(dr_tz, cv_tz) -> None:
    """
    Test timezone parameter conversion to Coin Metrics, CryptoCompare adn Glassnode formats.
    """
    data_req = DataRequest(tz=dr_tz)
    cv_params = ConvertParams(data_req=data_req).to_coinmetrics()
    assert cv_params["tz"] == cv_tz, "Timezone parameter conversion failed."
    cv_params = ConvertParams(data_req=data_req).to_cryptocompare()
    assert cv_params["tz"] == cv_tz, "Timezone parameter conversion failed."
    cv_params = ConvertParams(data_req=data_req).to_glassnode()
    assert cv_params["tz"] == cv_tz, "Timezone parameter conversion failed."


@pytest.mark.parametrize(
    "dr_tz, am_tz",
    [
        (None, "America/New_York"),
    ],
)
def test_convert_am_tz(dr_tz, am_tz) -> None:
    """
    Test timezone parameter conversion to Fred.
    """
    data_req = DataRequest(tz=dr_tz)
    am_params = ConvertParams(data_req=data_req).to_fred()
    assert am_params["tz"] == am_tz, "Fred timezone parameter conversion failed."
    am_params = ConvertParams(data_req=data_req).to_yahoo()
    assert am_params["tz"] == am_tz, "Yahoo timezone parameter conversion failed."


# convert cryptodatapy institution to source format
@pytest.mark.parametrize(
    "dr_inst, cm_inst",
    [
        (None, "grayscale"),
        ("Purpose", "purpose"),
    ],
)
def test_convert_cm_inst(dr_inst, cm_inst) -> None:
    """
    Test institution parameter conversion to Coin Metrics format.
    """
    data_req = DataRequest(inst=dr_inst)
    cm_params = ConvertParams(data_req=data_req).to_coinmetrics()
    assert cm_params["inst"] == cm_inst, "Institution parameter conversion failed."


@pytest.mark.parametrize(
    "dr_inst, gn_inst",
    [
        (None, "purpose"),
        ("grayscale", "grayscale"),
    ],
)
def test_convert_gn_inst(dr_inst, gn_inst) -> None:
    """
    Test institution parameter conversion to Glassnode format.
    """
    data_req = DataRequest(inst=dr_inst)
    gn_params = ConvertParams(data_req=data_req).to_glassnode()
    assert gn_params["inst"] == gn_inst, "Institution parameter conversion failed."


if __name__ == "__main__":
    pytest.main()
