import numpy as np
import pandas as pd
import pytest
import responses
import json

from cryptodatapy.extract.data_vendors.cryptocompare_api import CryptoCompare
from cryptodatapy.extract.datarequest import DataRequest
from cryptodatapy.util.datacredentials import DataCredentials
from cryptodatapy.transform.wrangle import WrangleInfo


# url endpoints
base_url = DataCredentials().cryptocompare_base_url
urls = {'exchanges_info': 'exchanges/general', 'indexes_info': 'index/list', 'assets_info': 'all/coinlist',
        'markets_info': 'v2/cccagg/pairs', 'on-chain_tickers_info': 'blockchain/list',
        'on-chain_info': 'blockchain/latest?fsym=BTC', 'social_info': 'social/coin/histo/day',
        'news': 'v2/news/?lang=EN', 'news_sources': 'news/feeds', 'rate_limit_info': 'rate/limit',
        'top_mkt_cap_info': 'top/mktcapfull?', 'indexes': 'index/'}


@pytest.fixture
def data_req():
    return DataRequest()


@pytest.fixture
def cc():
    return CryptoCompare()


@pytest.fixture
def exch_req():
    with open('data/cc_exchanges_req.json') as f:
        return json.load(f)


@responses.activate
def test_exch_req(exch_req, cc):
    """
    Test get request for exchanges info.
    """
    url = base_url + urls['exchanges_info'] + '?api_key=' + cc.api_key
    responses.add(responses.GET, url, json=exch_req, status=200)

    exch_resp = cc.req_meta(info_type='exchanges_info')
    assert exch_resp == exch_req


def test_exch_format(exch_req):
    """
    Test formatting exchanges info.
    """
    # wrangle data resp
    df = WrangleInfo(exch_req).cc_exch_info()
    assert df.loc['Binance', 'InternalName'] == 'Binance'


@pytest.fixture
def idx_req():
    with open('data/cc_indexes_req.json') as f:
        return json.load(f)


@responses.activate
def test_idx_req(idx_req, cc):
    """
    Test get request for indexes info.
    """
    url = base_url + urls['indexes_info'] + '?api_key=' + cc.api_key
    responses.add(responses.GET, url, json=idx_req, status=200)

    idx_resp = cc.req_meta(info_type='indexes_info')
    assert idx_resp == idx_req


def test_idx_format(idx_req):
    """
    Test formatting indexes info.
    """
    # wrangle data resp
    df = WrangleInfo(idx_req).cc_indexes_info()
    assert df.loc['MVDA', 'index_market_name'] == 'CCMVDA_MVIS'


@pytest.fixture
def assets_req():
    with open('data/cc_assets_req.json') as f:
        return json.load(f)


@responses.activate
def test_assets_req(assets_req, cc):
    """
    Test get request for assets info.
    """
    url = base_url + urls['assets_info'] + '?api_key=' + cc.api_key
    responses.add(responses.GET, url, json=assets_req, status=200)

    assets_resp = cc.req_meta(info_type='assets_info')
    assert assets_resp == assets_req


def test_assets_format(assets_req):
    """
    Tes
    t formatting assets info.
    """
    # wrangle data resp
    df = WrangleInfo(assets_req).cc_assets_info()
    assert df.loc['BTC', 'CoinName'] == 'Bitcoin'


@pytest.fixture
def news_req():
    with open('data/cc_news_req.json') as f:
        return json.load(f)


@responses.activate
def test_news_req(news_req, cc):
    """
    Test get request for news.
    """
    url = base_url + urls['news'] + '&api_key=' + cc.api_key
    responses.add(responses.GET, url, json=news_req, status=200)

    news_resp = cc.req_meta(info_type='news')
    assert news_resp == news_req


def test_news_format(news_req):
    """
    Test formatting news info.
    """
    # wrangle data resp
    df = WrangleInfo(news_req).cc_news()
    assert 'source' in df.columns


@pytest.fixture
def top_mkt_cap_req():
    with open('data/cc_top_mkt_cap_req.json') as f:
        return json.load(f)


@responses.activate
def test_top_mkt_cap_req(top_mkt_cap_req, cc):
    """
    Test get request for top market cap coins.
    """
    url = base_url + urls['top_mkt_cap_info'] + '&limit=100' + '&tsym=USD' + '&api_key=' + cc.api_key
    responses.add(responses.GET, url, json=top_mkt_cap_req, status=200)

    top_mkt_cap_resp = cc.req_top_mkt_cap()
    assert top_mkt_cap_resp == top_mkt_cap_req


def test_top_mkt_cap_format(top_mkt_cap_req):
    """
    Test formatting news info.
    """
    # wrangle data resp
    lst = WrangleInfo(top_mkt_cap_req).cc_top_mkt_cap_info()
    assert 'BTC' in lst


@pytest.fixture
def ohlcv_url_params():
    with open('data/cc_ohlcv_url_params.json') as f:
        return json.load(f)


def test_ohlcv_url_params(ohlcv_url_params, cc) -> None:
    """
    Test OHLCV url and params formatting.
    """
    urls_params = ohlcv_url_params
    assert urls_params['url'] == 'https://min-api.cryptocompare.com/data/v2/histoday'
    assert urls_params['params']['fsym'] == 'BTC'
    assert urls_params['params']['tsym'] == 'USD'
    assert urls_params['params']['limit'] == 2000
    assert urls_params['params']['e'] == 'CCCAGG'
    assert urls_params['params']['api_key'] == cc.api_key


@pytest.fixture
def onchain_url_params():
    with open('data/cc_on-chain_url_params.json') as f:
        return json.load(f)


def test_onchain_url_params(onchain_url_params, cc) -> None:
    """
    Test on-chain url and params formatting.
    """
    urls_params = onchain_url_params
    assert urls_params['url'] == 'https://min-api.cryptocompare.com/data/blockchain/histo/day?'
    assert urls_params['params']['fsym'] == 'BTC'
    assert urls_params['params']['limit'] == 2000
    assert urls_params['params']['api_key'] == cc.api_key


@pytest.fixture
def social_url_params():
    with open('data/cc_social_url_params.json') as f:
        return json.load(f)


def test_social_url_params(social_url_params, cc) -> None:
    """
    Test social url and params formatting.
    """
    urls_params = social_url_params
    assert urls_params['url'] == 'https://min-api.cryptocompare.com/data/social/coin/histo/day'
    assert urls_params['params']['coinId'] == 1182
    assert urls_params['params']['limit'] == 2000
    assert urls_params['params']['api_key'] == cc.api_key


@pytest.fixture
def ohlcv_data_req():
    with open('data/cc_ohlcv_data_req.json') as f:
        return json.load(f)


@responses.activate
def test_req_data(ohlcv_data_req, cc):
    """
    Test get request for data.
    """
    url = base_url + 'v2/histoday?fsym=btc&tsym=USD&limit=2000&e=CCCAGG&toTs=1663200000&api_key=' + cc.api_key
    responses.add(responses.GET, url, json=ohlcv_data_req, status=200)

    data_req = DataRequest(end_date='2022-09-15')
    data_resp = cc.req_data(data_req, data_type='ohlcv', ticker='btc')
    assert data_resp == ohlcv_data_req



@pytest.fixture
def ohlcv_data_req():
    with open('data/cc_ohlcv_data_req.json') as f:
        return json.load(f)


@responses.activate
def test_req_data(ohlcv_data_req, cc):
    """
    Test get request for data.
    """
    url = base_url + 'v2/histoday?fsym=btc&tsym=USD&limit=2000&e=CCCAGG&toTs=1663200000&api_key=' + cc.api_key
    responses.add(responses.GET, url, json=ohlcv_data_req, status=200)

    data_req = DataRequest(end_date='2022-09-15')
    data_resp = cc.req_data(data_req, data_type='ohlcv', ticker='btc')
    assert data_resp == ohlcv_data_req


def test_integration_get_all_data_hist(cc, data_req) -> None:
    """
    Test integration of set_urls_params and req_data to request data in a loop
    until the full data history has been retrieved.
    """
    idx_df = cc.get_all_data_hist(data_req, 'indexes', 'MVDA')
    assert not idx_df.empty
    assert pd.to_datetime(idx_df.time.min(), unit='s') <= pd.Timestamp('2017-07-20'), \
        "Wrong index start date."  # start date
    ohlcv_df = cc.get_all_data_hist(data_req, 'ohlcv', 'BTC')
    assert not ohlcv_df.empty
    assert pd.to_datetime(ohlcv_df.time.min(), unit='s') <= pd.Timestamp('2010-07-17'), \
        "Wrong OHLCV start date."  # start date
    onchain_df = cc.get_all_data_hist(data_req, 'on-chain', 'BTC')
    assert not onchain_df.empty
    assert pd.to_datetime(onchain_df.time.min(), unit='s') <= pd.Timestamp("2009-01-03"), \
        "Wrong on-chain start date."  # start date
    social_df = cc.get_all_data_hist(data_req, 'social', 'BTC')
    assert not social_df.empty
    assert pd.to_datetime(social_df.time.min(), unit='s') <= pd.Timestamp("2017-05-26"), \
        "Wrong social stats start date."  # start date


@pytest.fixture
def indexes_df():
    return pd.read_csv('data/cc_indexes_df.csv', index_col=0)


def test_wrangle_idx_data_resp(data_req, cc, indexes_df) -> None:
    """
    Test wrangling of indexes data response from get_all_data_hist into tidy data format.
    """
    df = cc.wrangle_data_resp(data_req, indexes_df)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert (df == 0).sum().sum() == 0, "Found zero values."  # 0s
    assert isinstance(df.index, pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert list(df.columns) == ["open", "high", "low", "close"], "Fields are missing from dataframe."  # fields
    assert df.index[0] == pd.Timestamp(
        "2017-07-20"
    ), "Wrong start date."  # start date
    assert isinstance(
        df.close.dropna().iloc[-1], np.float64
    ), "Close is not a numpy float."  # dtypes


@pytest.fixture
def ohlcv_df():
    return pd.read_csv('data/cc_ohlcv_df.csv', index_col=0)


def test_wrangle_ohlcv_data_resp(data_req, cc, ohlcv_df) -> None:
    """
    Test wrangling of OHLCV data response from get_all_data_hist into tidy data format.
    """
    df = cc.wrangle_data_resp(data_req, ohlcv_df)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert (df == 0).sum().sum() == 0, "Found zero values."  # 0s
    assert isinstance(df.index, pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert list(df.columns) == ['open', 'high', 'low', 'close', 'volume'], \
        "Fields are missing from dataframe."  # fields
    assert df.index[0] == pd.Timestamp(
        "2010-07-17"
    ), "Wrong start date."  # start date
    assert isinstance(
        df.close.dropna().iloc[-1], np.float64
    ), "Close is not a numpy float."  # dtypes


@pytest.fixture
def onchain_df():
    return pd.read_csv('data/cc_on-chain_df.csv', index_col=0)


def test_wrangle_onchain_data_resp(data_req, cc, onchain_df) -> None:
    """
    Test wrangling of on-chain data response from get_all_data_hist into tidy data format.
    """
    df = cc.wrangle_data_resp(data_req, onchain_df)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert (df == 0).sum().sum() == 0, "Found zero values."  # 0s
    assert isinstance(df.index, pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert all([field in df.columns for field in ['add_zero_bal', 'add_tot', 'add_new', 'add_act', 'tx_count',
                                                  'hashrate']]), "Fields are missing from dataframe."  # fields
    assert df.index[0] == pd.Timestamp('2009-01-03 00:00:00'), "Wrong start date."  # start date
    assert isinstance(
        df.add_act.dropna().iloc[-1], np.int64
    ), "Active addresses is not a numpy int."  # dtypes


@pytest.fixture
def social_df():
    return pd.read_csv('data/cc_social_df.csv', index_col=0)


def test_wrangle_social_data_resp(data_req, cc, social_df) -> None:
    """
    Test wrangling of social media data response from get_all_data_hist into tidy data format.
    """
    df = cc.wrangle_data_resp(data_req, social_df)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert (df == 0).sum().sum() == 0, "Found zero values."  # 0s
    assert isinstance(df.index, pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert 'sm_followers' in df.columns, "Fields are missing from dataframe."
    assert df.index[0] == pd.Timestamp("2017-05-26"), "Wrong start date."  # start date
    assert isinstance(
        df.fb_likes.dropna().iloc[-1], np.int64
    ), "FB likes is not a numpy int."  # dtypes


def test_integration_get_all_tickers(cc) -> None:
    """
    Test integration of get_all_data_hist, wrangle_data_resp and get_tidy_data to retrieve data for all tickers
    by looping through tickers list and adding them to a multinindex dataframe.
    """
    data_req = DataRequest(tickers=['btc', 'eth', 'ada'])
    df = cc.get_all_tickers(data_req, data_type='ohlcv')
    assert set(df.index.droplevel(0).unique()) == {'BTC', 'ETH', 'ADA'}


def test_check_params(cc) -> None:
    """
    Test parameter values before calling API.
    """
    data_req = DataRequest(tickers=['notaticker'])
    with pytest.raises(ValueError):
        cc.check_params(data_req)
    data_req = DataRequest(tickers=["btc"], fields=["notafield"])
    with pytest.raises(ValueError):
        cc.check_params(data_req)


def test_integration_get_data(cc) -> None:
    """
    Test integration of get data method.
    """
    data_req = DataRequest(
        tickers=["btc", "eth", "ada"], fields=["close", "add_act", "sm_followers"]
    )
    df = cc.get_data(data_req)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(
        df.index, pd.MultiIndex
    ), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(
        df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert set(df.index.droplevel(0).unique()) == {"BTC", "ETH", "ADA"}  # tickers
    assert list(df.columns) == [
        "close",
        "add_act",
        "sm_followers",
    ], "Fields are missing from dataframe."  # fields
    assert df.index[0] == (
        pd.Timestamp("2009-01-03 00:00:00"),
        "BTC",
    ), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < pd.Timedelta(
        days=3
    ), "End date is more than 72h ago."  # end date
    assert isinstance(
        df.close.dropna().iloc[-1], np.float64
    ), "Close is not a numpy float."  # dtypes
    assert isinstance(
        df.add_act.dropna().iloc[-1], np.int64
    ), "Active addresses is not a numpy int."  # dtypes
    assert isinstance(
        df.sm_followers.dropna().iloc[-1], np.int64
    ), "Followers is not a numpy int."  # dtypes

if __name__ == "__main__":
    pytest.main()

