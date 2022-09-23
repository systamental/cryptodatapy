import json


from cryptodatapy.extract.data_vendors import CoinMetrics
from cryptodatapy.extract.data_vendors import CryptoCompare
from cryptodatapy.extract.datarequest import DataRequest
from cryptodatapy.extract.libraries import CCXT
from cryptodatapy.extract.libraries import DBnomics
from cryptodatapy.extract.data_vendors import Glassnode
from cryptodatapy.extract.libraries import InvestPy
from cryptodatapy.extract.libraries import PandasDataReader
from cryptodatapy.extract.data_vendors import Tiingo


def cc_req_meta(info_type: str, filename: str) -> None:
    """
    Submits get request for meta data to CryptoCompare and returns data response, in JSON format.

    Parameters
    ----------
    info_type: str,  {'exchanges_info', 'indexes_info', 'assets_info', 'markets_info', 'news', 'top_mkt_cap_info'}
        Type of metadata to request.
    filename: str
        Name to save file under.
    """
    data_resp = CryptoCompare().req_meta(info_type)
    filepath = 'data/' + 'cc_' + filename + '_req.json'
    with open(filepath, 'w') as json_file:
        json.dump(data_resp, json_file)


def cc_req_top_mkt_cap() -> None:
    """
    Get request for CryptoCompare top mkt cap coins info.
    """
    data_resp = CryptoCompare().req_top_mkt_cap(n=100)
    filepath = 'data/' + 'cc_top_mkt_cap' + '_req.json'
    with open(filepath, 'w') as json_file:
        json.dump(data_resp, json_file)


def cc_set_urls_params(data_type: str) -> None:
    """
    Sets url and params for CryptoCompare get request.

    Parameters
    ----------
    data_type: str, {'indexes', 'ohlcv', 'on-chain', 'social'}
        Data type to retrieve.
    """
    cc = CryptoCompare()
    data_req = DataRequest()
    url_params = cc.set_urls_params(data_req, data_type=data_type, ticker='BTC')
    filepath = 'data/' + 'cc_' + data_type + '_url_params.json'
    with open(filepath, 'w') as json_file:
        json.dump(url_params, json_file)


def cc_req_data(data_type: str) -> None:
    """
    Submits get request to CryptoCompare API.

    Parameters
    ----------
    data_type: str, {'indexes', 'ohlcv', 'on-chain', 'social'}
        Data type to retrieve.
    """
    cc = CryptoCompare()
    data_req = DataRequest(end_date='2022-09-15')
    data_resp = cc.req_data(data_req, data_type=data_type, ticker='btc')
    filepath = 'data/' + 'cc_' + data_type + '_data_req.json'
    with open(filepath, 'w') as json_file:
        json.dump(data_resp, json_file)


def cc_all_data_hist(data_type: str) -> None:
    """
    Submits get requests to API until entire data history has been collected. Only necessary when
    number of observations is larger than the maximum number of observations per call.

    Parameters
    ----------
    data_type: str, {'indexes', 'ohlcv', 'on-chain', 'social'}
        Data type to retrieve.
    """
    cc = CryptoCompare()
    data_req = DataRequest()
    if data_type == 'indexes':
        ticker = 'MVDA'
    else:
        ticker = 'BTC'
    df = cc.get_all_data_hist(data_req, data_type=data_type, ticker=ticker)
    filepath = 'data/' + 'cc_' + data_type + '_df.csv'
    df.to_csv(filepath)


def cc_get_test_data() -> None:
    """
    Get raw data fromn CryptoCompare for data cleaning tests.
    """
    cc = CryptoCompare()
    data_req = DataRequest(tickers=['btc', 'eth', 'ada'], fields=['close', 'add_act', 'tx_count'])
    df = cc.get_data(data_req)
    df.to_csv('data/cc_raw_oc_df.csv')
    data_req = DataRequest(tickers=['btc', 'eth', 'ada'], fields=['open', 'high', 'low', 'close', 'volume'])
    df = cc.get_data(data_req)
    df.to_csv('data/cc_raw_ohlcv_df.csv')


def ccxt_get_all_ohlcv_hist() -> None:
    """
    Submits get requests to API until entire OHLCV history has been collected.
    """
    data_req = DataRequest()
    cx = CCXT()
    df = cx.get_all_ohlcv_hist(data_req, ticker='BTC/USDT')
    df.to_csv('data/ccxt_ohlcv_df.csv')


def cm_req_data(data_type: str) -> None:
    """
    Sends data request to Python client.

    Parameters
    ----------
    data_type: str, {'get_index_levels', 'get_institution_metrics', 'get_market_candles', 'get_asset_metrics',
                     'get_market_open_interest', 'get_market_funding_rates', 'get_market_trades',
                     'get_market_quotes'}
        Data type to retrieve.
    """
    cm = CoinMetrics()
    df = cm.req_data(data_type=data_type, markets=['binance-btc-usdt-spot', 'binance-eth-usdt-spot'])
    df.to_csv('data/cm_ohlcv_df.csv')


def cm_get_test_data() -> None:
    """
    Get raw data from CoinMetrics for data cleaning tests.
    """
    cm = CoinMetrics()
    data_req = DataRequest(tickers=['btc', 'eth', 'ada'], fields=['close', 'add_act', 'tx_count'])
    df = cm.get_data(data_req)
    df.to_csv('data/cm_raw_oc_df.csv')
    data_req = DataRequest(tickers=['btc', 'eth', 'ada'], fields=['open', 'high', 'low', 'close', 'volume'])
    df = cm.get_data(data_req)
    df.to_csv('data/cm_raw_ohlcv_df.csv')


def db_get_series() -> None:
    """
    Get get series from DBnomics.
    """
    db = DBnomics()
    df = db.get_series('BIS/total_credit/Q.US.C.A.M.770.A')
    df.to_csv('data/db_series_df.csv')


def fred_data() -> None:
    """
    Get FRED data from Pandas-datareader.
    """
    pdr = PandasDataReader()
    data_req = DataRequest(tickers=['US_CB_MB', 'US_UE_Rate'])
    df = pdr.fred(data_req)
    df.to_csv('data/fred_df.csv')


def yahoo_data() -> None:
    """
    Get Yahhoo data from Pandas-datareader.
    """
    pdr = PandasDataReader()
    data_req = DataRequest(tickers=['SPY', 'TLT', 'GLD'])
    df = pdr.yahoo(data_req)
    df.to_csv('data/yahoo_df.csv')


def gn_req_assets() -> None:
    """
    Submits get request for assets info to Glassnode and returns data response, in JSON format.
    """
    gn = Glassnode()
    data_resp = gn.req_assets()
    filepath = 'data/' + 'gn_assets_req.json'
    with open(filepath, 'w') as json_file:
        json.dump(data_resp, json_file)


def gn_req_fields() -> None:
    """
    Submits get request for fields info to Glassnode and returns data response, in JSON format.
    """
    gn = Glassnode()
    data_resp = gn.req_fields()
    filepath = 'data/' + 'gn_fields_req.json'
    with open(filepath, 'w') as json_file:
        json.dump(data_resp, json_file)


def gn_req_data() -> None:
    """
    Submits get request for data to Glassnode and returns data response, in JSON format.
    """
    gn = Glassnode()
    data_req = DataRequest()
    data_resp = gn.req_data(data_req, ticker='btc', field='addresses/count')
    filepath = 'data/' + 'gn_data_req.json'
    with open(filepath, 'w') as json_file:
        json.dump(data_resp, json_file)


def ip_econ_cal() -> None:
    """
    Retrieves econ calendar from InvestPy
    """
    ip = InvestPy()
    data_req = DataRequest(tickers=['US_Manuf_PMI', 'EZ_Infl_CPI_YoY'], cat='macro',
                           fields=['actual', 'expected', 'surprise'])
    econ_cal = ip.get_all_ctys_eco_cals(data_req)
    econ_cal.to_csv('data/ip_econ_cal.csv')


def tg_req_crypto() -> None:
    """
    Submits get request for crypto info to Tiingo and returns data response, in JSON format.
    """
    tg = Tiingo()
    data_resp = tg.req_crypto()
    filepath = 'data/' + 'tg_crypto_req.json'
    with open(filepath, 'w') as json_file:
        json.dump(data_resp, json_file)


def tg_req_iex() -> None:
    """
    Submits get request for IEX eqty data to Tiingo and returns data response, in JSON format.
    """
    tg = Tiingo()
    data_req = DataRequest(freq='8h', start_date='2022-01-01', end_date='2022-09-15')
    data_resp = tg.req_data(data_req, data_type='iex', ticker='spy')
    filepath = 'data/' + 'tg_iex_data_req.json'
    with open(filepath, 'w') as json_file:
        json.dump(data_resp, json_file)


def tg_req_eqty() -> None:
    """
    Submits get request for daily eqty data to Tiingo and returns data response, in JSON format.
    """
    tg = Tiingo()
    data_req = DataRequest(start_date='2000-01-01', end_date='2021-12-31')
    data_resp = tg.req_data(data_req, data_type='eqty', ticker='spy')
    filepath = 'data/' + 'tg_eqty_data_req.json'
    with open(filepath, 'w') as json_file:
        json.dump(data_resp, json_file)


def tg_req_crypto_data() -> None:
    """
    Submits get request for crypto data to Tiingo and returns data response, in JSON format.
    """
    tg = Tiingo()
    data_req = DataRequest(start_date='2015-01-01', end_date='2021-12-31')
    data_resp = tg.req_data(data_req, data_type='crypto', ticker='btcusd')
    filepath = 'data/' + 'tg_crypto_data_req.json'
    with open(filepath, 'w') as json_file:
        json.dump(data_resp, json_file)


def tg_req_fx_data() -> None:
    """
    Submits get request for FX data to Tiingo and returns data response, in JSON format.
    """
    tg = Tiingo()
    data_req = DataRequest(start_date='2015-01-01', end_date='2021-12-31')
    data_resp = tg.req_data(data_req, data_type='fx', ticker='eurusd')
    filepath = 'data/' + 'tg_fx_data_req.json'
    with open(filepath, 'w') as json_file:
        json.dump(data_resp, json_file)
