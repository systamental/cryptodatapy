import pandas as pd
import numpy as np
from cryptodatapy.data_requests.datarequest import DataRequest
from cryptodatapy.data_requests.getdata import GetData
import pytest


@pytest.fixture
def data_req_ccxt():
    data_req = DataRequest(data_source='ccxt', start_date='2018-01-01', tickers=['btc', 'eth', 'ada'])
    return data_req


@pytest.fixture
def data_req_cm():
    data_req = DataRequest(data_source='coinmetrics', start_date='2000-01-01', tickers=['btc', 'eth', 'ada'],
                           fields=['close', 'add_act', 'issuance'])
    return data_req


@pytest.fixture
def data_req_cc():
    data_req = DataRequest(data_source='cryptocompare', tickers=['btc', 'eth', 'ada'],
                           fields=['add_act', 'sm_followers', 'tx_count'])
    return data_req


@pytest.fixture
def data_req_db():
    data_req = DataRequest(data_source='dbnomics', tickers=['US_GDP_Sh_PPP', 'EZ_GDP_Sh_PPP', 'CN_GDP_Sh_PPP',
                           'US_Credit/GDP_HH', 'WL_Credit_Banks'], fields='actual', cat='macro')
    return data_req


@pytest.fixture
def data_req_gn():
    data_req = DataRequest(data_source='glassnode', tickers=['btc', 'eth'], freq='d',
                           fields=['close', 'add_act', 'tx_count'])
    return data_req


@pytest.fixture
def data_req_ip():
    data_req = DataRequest(data_source='investpy', tickers=['US_Eqty_Idx', 'US_Eqty_MSCI', 'SPY', 'US_Rates_Long_ETF',
                                                            'TLT'], cat='eqty')
    return data_req


@pytest.fixture
def data_req_pdr():
    data_req = DataRequest(data_source='fred', tickers=['US_Credit_BAA_Spread', 'US_BE_Infl_10Y', 'US_Eqty_Vol_Idx',
                                                        'EM_Eqty_Vol_Idx'], fields='close', cat='eqty')
    return data_req


@pytest.fixture
def data_req_tg():
    data_req = DataRequest(data_source='tiingo', tickers=['meta', 'aapl', 'amzn', 'nflx', 'goog'], cat='eqty')
    return data_req


def test_get_meta_integration_ccxt(data_req_ccxt) -> None:
    """
    Test get metadata for CCXT
    """
    meta = GetData(data_req_ccxt).get_meta(method='get_exchanges_info', exch='ftx')
    assert not meta.empty, "Dataframe was returned empty."  # non empty
    assert meta.loc['ftx', 'name'] == 'FTX', "Exchange info is incorrect."  # exch name
    assert meta.loc['ftx', 'rateLimit'] == '28.57', "Rate limit info is incorrect"  # rate limit info
    assert isinstance(meta, pd.DataFrame), "Metadata should be a dataframe."  # type


def test_get_data_integration_ccxt(data_req_ccxt) -> None:
    """
    Test get data method for CCXT
    """

    df = GetData(data_req_ccxt).get_series()
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert list(df.index.droplevel(0).unique()) == ['BTC', 'ETH', 'ADA'], \
        "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == ['close'], "Fields are missing from dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp('2018-01-01 00:00:00'), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < pd.Timedelta(days=3), \
        "End date is more than 72h ago."  # end date
    assert isinstance(df.close.dropna().iloc[-1], np.float64), "Close is not a numpy float."  # dtypes


def test_get_meta_integration_cm(data_req_cm) -> None:
    """
    Test get metadata for CoinMetrics
    """
    meta = GetData(data_req_cm).get_meta(method='get_assets_info')
    assert not meta.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(meta, pd.DataFrame), "Metadata is not a dataframe"  # type
    assert meta.loc['btc', 'full_name'] == 'Bitcoin', "Bitcoin name is incorrect."  # assets
    assert meta.loc['eth', 'metrics'][0]['frequencies'][0]['frequency'] == '1d', "ETH metrics are incorrect."  # metrics
    meta = GetData(data_req_cm).get_meta(method='get_avail_assets_info', data_req=data_req_cm)
    assert len(meta) != 0, "List was returned empty."  # non empty list
    assert isinstance(meta, list), "Metadata is wrong type"  # type
    assert 'btc' in meta, "Bitcoin is not in asset list."  # asset ist


def test_get_data_integration_cm(data_req_cm) -> None:
    """
    Test get data method for CoinMetrics
    """
    df = GetData(data_req_cm).get_series()
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert list(df.index.droplevel(0).unique()) == ['BTC', 'ETH', 'ADA'], \
        "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == ['close', 'add_act', 'issuance'], "Fields are missing from dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp('2009-01-09 00:00:00'), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < pd.Timedelta(days=3), \
        "End date is more than 72h ago."  # end date
    assert isinstance(df.add_act.dropna().iloc[-1], np.int64), "Active addresses are not a numpy int."  # dtypes
    assert isinstance(df.close.dropna().iloc[-1], np.float64), "Close is not a numpy float."  # dtypes
    assert isinstance(df.issuance.dropna().iloc[-1], np.float64), "Transaction counts are not a numpy int."  # dtypes


def test_get_meta_integration_cc(data_req_cc) -> None:
    """
    Test get metadata for CryptoCompare
    """
    meta = GetData(data_req_cc).get_meta(method='get_news_sources')
    assert not meta.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(meta, pd.DataFrame), "Metadata is not a dataframe"
    assert meta.loc['coindesk', 'lang'] == 'EN', "Coindesk language is incorrect."  # news sources lang
    assert 'cryptocompare' in meta.index, "Cryptocompare is missing from sources."  # sources


def test_get_data_integration_cc(data_req_cc) -> None:
    """
    Test get data method for CryptoCompare
    """
    df = GetData(data_req_cc).get_series()
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert list(df.index.droplevel(0).unique()) == ['BTC', 'ETH', 'ADA'], \
        "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == ['add_act', 'sm_followers', 'tx_count'], "Fields are missing from dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp('2009-01-03 00:00:00'), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < pd.Timedelta(days=3), \
        "End date is more than 72h ago."  # end date
    assert isinstance(df.add_act.dropna().iloc[-1], np.int64), "Active addresses are not a numpy int."  # dtypes
    assert isinstance(df.sm_followers.dropna().iloc[-1], np.int64), "Followers are not a numpy int."  # dtypes
    assert isinstance(df.tx_count.dropna().iloc[-1], np.int64), "Transaction counts are not a numpy int."  # dtypes


def test_get_meta_integration_db(data_req_db) -> None:
    """
    Test get metadata for DBnomics
    """
    meta = GetData(data_req_db).get_meta(method='get_assets_info')
    assert meta is None, "Metadata is not None"
    meta = GetData(data_req_db).get_meta(attr='fields')
    assert isinstance(meta, dict), "Metadata is not a dictionary."
    assert meta['macro'] == ['actual'], "Fields for macro cat are incorrect."


def test_get_data_integration_db(data_req_db) -> None:
    """
    Test get data method for DBnomics
    """
    df = GetData(data_req_db).get_series()
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert list(df.index.droplevel(0).unique()) == ['US_Credit/GDP_HH', 'WL_Credit_Banks', 'CN_GDP_Sh_PPP',
                                                    'EZ_GDP_Sh_PPP', 'US_GDP_Sh_PPP'], \
        "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == ['actual'], "Fields are missing from dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp('1947-10-01 00:00:00'), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < pd.Timedelta(days=365), \
        "End date is more than 1 year ago."  # end date
    assert isinstance(df.actual.dropna().iloc[-1], np.float64), "Actual is not a numpy float."  # dtypes


def test_get_meta_integration_gn(data_req_gn) -> None:
    """
    Test get metadata for Glassnode
    """
    meta = GetData(data_req_gn).get_meta(method='get_assets_info')
    assert meta.loc['BTC', 'name'] == 'Bitcoin', "Bitcoin is not in assets info."
    meta = GetData(data_req_gn).get_meta(attr='fields')
    assert isinstance(meta, list), "Metadata is not a dictionary."
    assert 'addresses/count' in meta, "Addresses missing from metadata."


def test_get_data_integration_gn(data_req_gn) -> None:
    """
    Test get data method for Glassnode
    """
    df = GetData(data_req_gn).get_series()
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert list(df.index.droplevel(0).unique()) == ['BTC', 'ETH'], "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == ['close', 'add_act', 'tx_count'], "Fields are missing from dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp('2009-01-12'), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < pd.Timedelta(days=2), \
        "End date is more than 48h ago."  # end date
    assert isinstance(df.close.dropna().iloc[-1], np.float64), "Close is not a numpy float."  # dtypes
    assert isinstance(df.add_act.dropna().iloc[-1], np.int64), "Active addresses is not a numpy int."  # dtypes
    assert isinstance(df.tx_count.dropna().iloc[-1], np.int64), "Transactions count is not a numpy int."  # dtypes


def test_get_meta_integration_ip(data_req_ip) -> None:
    """
    Test get metadata for InvestPy
    """
    meta = GetData(data_req_ip).get_meta(method='get_assets_info', cat='cmdty')
    assert meta.loc['Gold', 'full_name'] == 'Gold Futures', "Gold is not in asset info."
    meta = GetData(data_req_ip).get_meta(attr='fields')
    assert isinstance(meta, dict), "Metadata is not a dictionary."
    assert meta['cmdty'] == ['date', 'open', 'high', 'low', 'close', 'volume'], "Fields are incorrect."


def test_get_data_integration_ip(data_req_ip) -> None:
    """
    Test get data method for InvestPy
    """
    df = GetData(data_req_ip).get_series()
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert set(df.index.droplevel(0).unique()) == {'SPY', 'TLT', 'US_Eqty_Idx', 'US_Eqty_MSCI', 'US_Rates_Long_ETF'}, \
        "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == ['close'], "Fields are missing from dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp('1979-12-26 00:00:00'), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < pd.Timedelta(days=4), \
        "End date is more than 4 days ago."  # end date
    assert isinstance(df.close.dropna().iloc[-1], np.float64), "Close is not a numpy float."  # dtypes


def test_get_meta_integration_pdr(data_req_pdr) -> None:
    """
    Test get metadata for Pandas-datareader
    """
    meta = GetData(data_req_pdr).get_meta(method='get_assets_info')
    assert meta is None, "Metadata is not None."
    meta = GetData(data_req_pdr).get_meta(attr='fields')
    assert isinstance(meta, dict), "Metadata is not a dictionary."
    assert meta['rates'] == ['open', 'high', 'low', 'close', 'volume'], "Fields are incorrect."


def test_get_data_integration_pdr(data_req_pdr) -> None:
    """
    Test get data method for Pandas-datareader
    """
    df = GetData(data_req_pdr).get_series()
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert set(df.index.droplevel(0).unique()) == {'EM_Eqty_Vol_Idx', 'US_BE_Infl_10Y', 'US_Credit_BAA_Spread',
                                                   'US_Eqty_Vol_Idx'}, \
        "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == ['close'], "Fields are missing from dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp('1986-01-02 00:00:00'), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < pd.Timedelta(days=4), \
        "End date is more than 4 days ago."  # end date
    assert isinstance(df.close.dropna().iloc[-1], np.float64), "Close is not a numpy float."  # dtypes


def test_get_meta_integration_tg(data_req_tg) -> None:
    """
    Test get metadata for Tiingo
    """
    meta = GetData(data_req_tg).get_meta(method='get_assets_info')
    assert meta['eqty'].loc['SPY', 'exchange'] == 'NYSE ARCA', "SPY not in asset info."
    meta = GetData(data_req_tg).get_meta(attr='fields')
    assert isinstance(meta, dict), "Metadata is not a dictionary."
    assert meta['eqty'] == ['open', 'high', 'low', 'close', 'volume', 'open_adj', 'high_adj', 'close_adj', 'dividend',
                            'split'], "Fields are incorrect."


def test_get_data_integration_tg(data_req_tg) -> None:
    """
    Test get data method for Tiingo
    """
    df = GetData(data_req_tg).get_series()
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert set(df.index.droplevel(0).unique()) == {'AAPL', 'AMZN', 'GOOG', 'META', 'NFLX'}, \
        "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == ['close'], "Fields are missing from dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp('1980-12-12 00:00:00'), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < pd.Timedelta(days=4), \
        "End date is more than 4 days ago."  # end date
    assert isinstance(df.close.dropna().iloc[-1], np.float64), "Close is not a numpy float."  # dtypes


if __name__ == "__main__":
    pytest.main()
