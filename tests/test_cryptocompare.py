import numpy as np
import pandas as pd
import pytest
import responses
import json

from cryptodatapy.extract.data_vendors.cryptocompare_api import CryptoCompare
from cryptodatapy.extract.datarequest import DataRequest
from cryptodatapy.util.datacredentials import DataCredentials
from cryptodatapy.transform.wrangle import WrangleInfo

# data credentials
data_cred = DataCredentials()
base_url = data_cred.cryptocompare_base_url,
api_endpoints = data_cred.cryptomcompare_endpoints,
api_key = data_cred.cryptocompare_api_key,


class TestCryptoCompare:
    """
    Test class for CryptoCompare.
    """
    @pytest.fixture(autouse=True)
    def data_req(self):
        self.data_req = DataRequest(tickers=['btc', 'eth', 'sol'], fields=["add_act", "tx_count", "close"])

    @pytest.fixture(autouse=True)
    def cc(self):
        self.cc = CryptoCompare()

    @pytest.fixture(autouse=True)
    def exch_req(self):
        with open('data/cc_exchanges_req.json') as f:
            self.exch = json.load(f)

    @pytest.fixture(autouse=True)
    def idx_req(self):
        with open('data/cc_indexes_req.json') as f:
            self.idx = json.load(f)

    @pytest.fixture(autouse=True)
    def assets_req(self):
        with open('data/cc_assets_req.json') as f:
            self.assets = json.load(f)

    @pytest.fixture(autouse=True)
    def news_req(self):
        with open('data/cc_news_req.json') as f:
            self.news = json.load(f)

    @pytest.fixture(autouse=True)
    def top_mkt_cap_req(self):
        with open('data/cc_top_mkt_cap_req.json') as f:
            self.mkt_cap = json.load(f)

    @pytest.fixture(autouse=True)
    def ohlcv_url_params(self):
        with open('data/cc_ohlcv_url_params.json') as f:
            self.ohlcv_params = json.load(f)

    @pytest.fixture(autouse=True)
    def onchain_url_params(self):
        with open('data/cc_on-chain_url_params.json') as f:
            self.onchain_params = json.load(f)

    @pytest.fixture(autouse=True)
    def social_url_params(self):
        with open('data/cc_social_url_params.json') as f:
            self.social_params = json.load(f)

    @pytest.fixture(autouse=True)
    def ohlcv_data_req(self):
        with open('data/cc_ohlcv_data_req.json') as f:
            self.ohlcv_req = json.load(f)

    @pytest.fixture(autouse=True)
    def indexes_df(self):
        self.indexes_df = pd.read_csv('data/cc_indexes_df.csv', index_col=0)

    @pytest.fixture(autouse=True)
    def ohlcv_df(self):
        self.ohlcv_df = pd.read_csv('data/cc_ohlcv_df.csv', index_col=0)

    @pytest.fixture(autouse=True)
    def onchain_df(self):
        self.onchain_df = pd.read_csv('data/cc_on-chain_df.csv', index_col=0)

    @pytest.fixture(autouse=True)
    def social_df(self):
        self.social_df = pd.read_csv('data/cc_social_df.csv', index_col=0)

    @responses.activate
    def test_exch_req(self):
        """
        Test get request for exchanges info.
        """
        url = self.cc.base_url + self.cc.api_endpoints['exchanges_info'] + '?api_key=' + self.cc.api_key
        responses.add(responses.GET, url, json=self.exch, status=200)

        exch_resp = self.cc.req_meta(info_type='exchanges_info')
        assert exch_resp == self.exch

    def test_exch_format(self):
        """
        Test formatting exchanges info.
        """
        # wrangle data resp
        df = WrangleInfo(self.exch).cc_exch_info()
        assert df.loc['Binance', 'InternalName'] == 'Binance'

    @responses.activate
    def test_idx_req(self):
        """
        Test get request for indexes info.
        """
        url = self.cc.base_url + self.cc.api_endpoints['indexes_info'] + '?api_key=' + self.cc.api_key
        responses.add(responses.GET, url, json=self.idx, status=200)

        idx_resp = self.cc.req_meta(info_type='indexes_info')
        assert idx_resp == self.idx

    def test_idx_format(self):
        """
        Test formatting indexes info.
        """
        # wrangle data resp
        df = WrangleInfo(self.idx).cc_indexes_info()
        assert df.loc['MVDA', 'index_market_name'] == 'CCMVDA_MVIS'

    @responses.activate
    def test_assets_req(self):
        """
        Test get request for assets info.
        """
        url = self.cc.base_url + self.cc.api_endpoints['assets_info'] + '?api_key=' + self.cc.api_key
        responses.add(responses.GET, url, json=self.assets, status=200)

        assets_resp = self.cc.req_meta(info_type='assets_info')
        assert assets_resp == self.assets

    def test_assets_format(self):
        """
        Tes
        t formatting assets info.
        """
        # wrangle data resp
        df = WrangleInfo(self.assets).cc_assets_info()
        assert df.loc['BTC', 'CoinName'] == 'Bitcoin'

    @responses.activate
    def test_news_req(self):
        """
        Test get request for news.
        """
        url = self.cc.base_url + self.cc.api_endpoints['news'] + '&api_key=' + self.cc.api_key
        responses.add(responses.GET, url, json=self.news, status=200)

        news_resp = self.cc.req_meta(info_type='news')
        assert news_resp == self.news

    def test_news_format(self):
        """
        Test formatting news info.
        """
        # wrangle data resp
        df = WrangleInfo(self.news).cc_news()
        assert 'source' in df.columns

    @responses.activate
    def test_top_mkt_cap_req(self):
        """
        Test get request for top market cap coins.
        """
        url = (self.cc.base_url + self.cc.api_endpoints['top_mkt_cap_info'] + '&limit=100' + '&tsym=USD' + '&api_key='
               + self.cc.api_key)
        responses.add(responses.GET, url, json=self.mkt_cap, status=200)

        top_mkt_cap_resp = self.cc.req_top_mkt_cap()
        assert top_mkt_cap_resp == self.mkt_cap

    def test_top_mkt_cap_format(self):
        """
        Test formatting news info.
        """
        # wrangle data resp
        lst = WrangleInfo(self.mkt_cap).cc_top_mkt_cap_info()
        assert 'BTC' in lst

    def test_ohlcv_url_params(self) -> None:
        """
        Test OHLCV url and params formatting.
        """
        # urls_params = ohlcv_url_params
        assert self.ohlcv_params['url'] == 'https://min-api.cryptocompare.com/data/v2/histoday'
        assert self.ohlcv_params['params']['fsym'] == 'BTC'
        assert self.ohlcv_params['params']['tsym'] == 'USD'
        assert self.ohlcv_params['params']['limit'] == 2000
        assert self.ohlcv_params['params']['e'] == 'CCCAGG'
        assert self.ohlcv_params['params']['api_key'] == self.cc.api_key

    def test_onchain_url_params(self) -> None:
        """
        Test on-chain url and params formatting.
        """
        # urls_params = onchain_url_params
        assert self.onchain_params['url'] == 'https://min-api.cryptocompare.com/data/blockchain/histo/day?'
        assert self.onchain_params['params']['fsym'] == 'BTC'
        assert self.onchain_params['params']['limit'] == 2000
        assert self.onchain_params['params']['api_key'] == self.cc.api_key

    def test_social_url_params(self) -> None:
        """
        Test social url and params formatting.
        """
        assert self.social_params['url'] == 'https://min-api.cryptocompare.com/data/social/coin/histo/day'
        assert self.social_params['params']['coinId'] == 1182
        assert self.social_params['params']['limit'] == 2000
        assert self.social_params['params']['api_key'] == self.cc.api_key

    @responses.activate
    def test_req_data(self):
        """
        Test get request for data.
        """
        url = (self.cc.base_url + 'v2/histoday?fsym=btc&tsym=USD&limit=2000&e=CCCAGG&toTs=1663200000&api_key='
               + self.cc.api_key)
        responses.add(responses.GET, url, json=self.ohlcv_req, status=200)

        data_req = DataRequest(end_date='2022-09-15')
        data_resp = self.cc.req_data(data_req, data_type='ohlcv', ticker='btc')
        assert data_resp == self.ohlcv_req

    def test_integration_get_all_data_hist(self) -> None:
        """
        Test integration of set_urls_params and req_data to request data in a loop
        until the full data history has been retrieved.
        """
        idx_df = self.cc.get_all_data_hist(self.data_req, 'indexes', 'MVDA')
        assert not idx_df.empty
        assert pd.to_datetime(idx_df.time.min(), unit='s') <= pd.Timestamp('2017-07-20'), \
            "Wrong index start date."  # start date
        ohlcv_df = self.cc.get_all_data_hist(self.data_req, 'ohlcv', 'BTC')
        assert not ohlcv_df.empty
        assert pd.to_datetime(ohlcv_df.time.min(), unit='s') <= pd.Timestamp('2010-07-17'), \
            "Wrong OHLCV start date."  # start date
        onchain_df = self.cc.get_all_data_hist(self.data_req, 'on-chain', 'BTC')
        assert not onchain_df.empty
        assert pd.to_datetime(onchain_df.time.min(), unit='s') <= pd.Timestamp("2009-01-03"), \
            "Wrong on-chain start date."  # start date
        social_df = self.cc.get_all_data_hist(self.data_req, 'social', 'BTC')
        assert not social_df.empty
        assert pd.to_datetime(social_df.time.min(), unit='s') <= pd.Timestamp("2017-05-26"), \
            "Wrong social stats start date."  # start date

    def test_wrangle_idx_data_resp(self) -> None:
        """
        Test wrangling of indexes data response from get_all_data_hist into tidy data format.
        """
        df = self.cc.wrangle_data_resp(self.data_req, self.indexes_df)
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

    def test_wrangle_ohlcv_data_resp(self) -> None:
        """
        Test wrangling of OHLCV data response from get_all_data_hist into tidy data format.
        """
        df = self.cc.wrangle_data_resp(self.data_req, self.ohlcv_df)
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

    def test_wrangle_onchain_data_resp(self) -> None:
        """
        Test wrangling of on-chain data response from get_all_data_hist into tidy data format.
        """
        df = self.cc.wrangle_data_resp(self.data_req, self.onchain_df)
        assert not df.empty, "Dataframe was returned empty."  # non empty
        assert (df == 0).sum().sum() == 0, "Found zero values."  # 0s
        assert isinstance(df.index, pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
        assert all([field in df.columns for field in ['add_zero_bal', 'add_tot', 'add_new', 'add_act', 'tx_count',
                                                      'hashrate']]), "Fields are missing from dataframe."  # fields
        assert df.index[0] == pd.Timestamp('2009-01-03 00:00:00'), "Wrong start date."  # start date
        assert isinstance(
            df.add_act.dropna().iloc[-1], np.int64
        ), "Active addresses is not a numpy int."  # dtypes

    def test_wrangle_social_data_resp(self) -> None:
        """
        Test wrangling of social media data response from get_all_data_hist into tidy data format.
        """
        df = self.cc.wrangle_data_resp(self.data_req, self.social_df)
        assert not df.empty, "Dataframe was returned empty."  # non empty
        assert (df == 0).sum().sum() == 0, "Found zero values."  # 0s
        assert isinstance(df.index, pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
        assert 'sm_followers' in df.columns, "Fields are missing from dataframe."
        assert df.index[0] == pd.Timestamp("2017-05-26"), "Wrong start date."  # start date
        assert isinstance(
            df.fb_likes.dropna().iloc[-1], np.int64
        ), "FB likes is not a numpy int."  # dtypes

    def test_integration_get_all_tickers(self) -> None:
        """
        Test integration of get_all_data_hist, wrangle_data_resp and get_tidy_data to retrieve data for all tickers
        by looping through tickers list and adding them to a multi-index dataframe.
        """
        data_req = DataRequest(tickers=['btc', 'eth', 'ada'])
        df = self.cc.get_all_tickers(data_req, data_type='ohlcv')
        assert set(df.index.droplevel(0).unique()) == {'BTC', 'ETH', 'ADA'}

    def test_check_params(self) -> None:
        """
        Test parameter values before calling API.
        """
        data_req = DataRequest(tickers=['notaticker'])
        with pytest.raises(ValueError):
            self.cc.check_params(data_req)

    def test_integration_get_data(self) -> None:
        """
        Test integration of get data method.
        """
        data_req = DataRequest(
            tickers=["btc", "eth", "ada"], fields=["close", "add_act", "twitter_followers"]
        )
        df = self.cc.get_data(data_req)
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
            "twitter_followers",
        ], "Fields are missing from dataframe."  # fields
        assert df.index[0] == (
            pd.Timestamp("2008-09-02 00:00:00"),
            "ADA",
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
            df.twitter_followers.dropna().iloc[-1], np.int64
        ), "Followers is not a numpy int."  # dtypes


if __name__ == "__main__":
    pytest.main()
