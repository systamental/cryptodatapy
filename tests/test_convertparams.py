import pandas as pd
from datetime import datetime, timedelta
from cryptodatapy.data_requests.datarequest import DataRequest
from cryptodatapy.util.convertparams import ConvertParams
import pytest


@pytest.fixture
def datarequest():
    return DataRequest()


@pytest.mark.parametrize(
    "dr_tickers, cc_tickers",
    [
        ('btc', ['BTC']),
        ('eth', ['ETH']),
        (['btc', 'eth', 'sol'], ['BTC', 'ETH', 'SOL'])
    ]
)
def test_convert_params_cc_tickers(dr_tickers, cc_tickers) -> None:
    """
    Test tickers parameter conversion to CryptoCompare format.
    """
    data_req = DataRequest(tickers=dr_tickers)
    cc_params = ConvertParams(data_source='cryptocompare').convert_to_source(data_req)
    assert cc_params['tickers'] == cc_tickers, "Tickers parameter conversion failed."


@pytest.mark.parametrize(
    "dr_tickers, cm_mkts",
    [
        ('btc', ['binance-btc-usdt-spot']),
        ('eth', ['binance-eth-usdt-spot'])
    ]
)
def test_convert_params_tickers_to_cm_mkts(dr_tickers, cm_mkts) -> None:
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
def test_convert_params_mkt_types_to_cm_mkts(dr_mkt_types, cm_mkts) -> None:
    """
    Test market type to markets parameter conversion for CryptoCompare format.
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
def test_convert_params_exchange_to_cm_mkts(dr_exch, cm_mkts) -> None:
    """
    Test exchange to markets parameter conversion for CryptoCompare format.
    """
    data_req = DataRequest(exch=dr_exch, mkt_type='perpetual_future')
    cm_params = ConvertParams(data_source='coinmetrics').convert_to_source(data_req)
    assert cm_params['mkts'] == cm_mkts, "Exchange to markets parameter conversion failed."


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
def test_convert_params_cc_freq(dr_freq, cc_freq) -> None:
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
def test_convert_data_req_params_cm_freq(dr_freq, cm_freq) -> None:
    """
    Test frequency parameter conversion to Coin Metrics format.
    """
    data_req = DataRequest(freq=dr_freq)
    cm_params = ConvertParams(data_source='coinmetrics').convert_to_source(data_req)
    assert cm_params['freq'] == cm_freq, "Frequency parameter conversion failed."


@pytest.mark.parametrize(
    "dr_ccy, cc_ccy",
    [
        (None, 'USD'),
        ('usdt', 'USDT'),
        ('btc', 'BTC'),
    ]
)
def test_convert_data_req_params_cc_quote_ccy(dr_ccy, cc_ccy) -> None:
    """
    Test quote currency parameter conversion to CryptoCompare format.
    """
    data_req = DataRequest(quote_ccy=dr_ccy)
    cc_params = ConvertParams(data_source='cryptocompare').convert_to_source(data_req)
    assert cc_params['quote_ccy'] == cc_ccy, "Quote currency parameter conversion failed."


@pytest.mark.parametrize(
    "dr_ccy, cm_ccy",
    [
        (None, 'usdt'),
        ('usdt', 'usdt'),
        ('btc', 'btc'),
    ]
)
def test_convert_params_cm_quote_ccy(dr_ccy, cm_ccy) -> None:
    """
    Test quote currency parameter conversion to Coin Metrics format.
    """
    data_req = DataRequest(quote_ccy=dr_ccy)
    cm_params = ConvertParams(data_source='coinmetrics').convert_to_source(data_req)
    assert cm_params['quote_ccy'] == cm_ccy, "Quote currency parameter conversion failed."


@pytest.mark.parametrize(
    "dr_exch, cc_exch",
    [
        (None, 'CCCAGG'),
        ('binance', 'binance'),
    ]
)
def test_convert_params_cc_exchange(dr_exch, cc_exch) -> None:
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
def test_convert_params_cm_exchange(dr_exch, cm_exch) -> None:
    """
    Test exchange parameter conversion to Coin Metrics format.
    """
    data_req = DataRequest(exch=dr_exch)
    cm_params = ConvertParams(data_source='coinmetrics').convert_to_source(data_req)
    assert cm_params['exch'] == cm_exch, "Exchange parameter conversion failed."


@pytest.mark.parametrize(
    "dr_sd, cc_sd",
    [
        (None, 1230940800),
        ('2015-01-01', 1420070400),
        (datetime(2015, 1, 1), 1420070400),
        (pd.Timestamp('2015-01-01 00:00:00'), 1420070400),
    ]
)
def test_convert_params_start_date(dr_sd, cc_sd) -> None:
    """
    Test start date parameter conversion to CryptoCompare format.
    """
    data_req = DataRequest(start_date=dr_sd)
    cc_params = ConvertParams(data_source='cryptocompare').convert_to_source(data_req)
    assert cc_params['start_date'] == cc_sd, "Start date parameter conversion failed."


@pytest.mark.parametrize(
    "dr_ed, cc_ed",
    [
        (None, round(pd.Timestamp(datetime.utcnow()).timestamp())),
        ('2020-12-31', 1609372800),
        (datetime(2020, 12, 31), 1609372800),
        (pd.Timestamp('2020-12-31 00:00:00'), 1609372800),
    ]
)
def test_convert_data_req_params_end_date(dr_ed, cc_ed) -> None:
    """
    Test end date parameter conversion to CryptoCompare format.
    """
    data_req = DataRequest(end_date=dr_ed)
    cc_params = ConvertParams(data_source='cryptocompare').convert_to_source(data_req)
    if dr_ed is None:
        assert pd.to_datetime(cc_params['end_date'], unit='s').date() == datetime.utcnow().date()
    else:
        assert cc_params['end_date'] == cc_ed, "End date parameter conversion failed."
