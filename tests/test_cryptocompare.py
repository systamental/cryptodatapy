import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from cryptodatapy.util.datacredentials import DataCredentials
from cryptodatapy.data_requests.datarequest import DataRequest
from cryptodatapy.data_vendors.cryptocompare_api import CryptoCompare
import pytest


@pytest.fixture
def datarequest():
    return DataRequest()


@pytest.fixture
def cryptocompare():
    return CryptoCompare()


def test_source_type(cryptocompare) -> None:
    """
    Test source type property.
    """
    cc = cryptocompare
    assert cc.source_type == 'data_vendor', "Source type should be 'data_vendor'."


def test_source_type_error(cryptocompare) -> None:
    """
    Test source type errors.
    """
    cc = cryptocompare
    with pytest.raises(ValueError):
        cc.source_type = 'anecdotal'


def test_categories(cryptocompare) -> None:
    """
    Test categories property.
    """
    cc = cryptocompare
    assert cc.categories == ['crypto'], "Category should be 'crypto'."


def test_categories_error(cryptocompare) -> None:
    """
    Test categories errors.
    """
    cc = cryptocompare
    with pytest.raises(ValueError):
        cc.categories = ['real_estate', 'art']


def test_assets(cryptocompare) -> None:
    """
    Test assets property.
    """
    cc = cryptocompare
    assert 'BTC' in cc.assets, "Assets list is missing 'BTC'."


def test_get_assets_info(cryptocompare) -> None:
    """
    Test get assets info method.
    """
    cc = cryptocompare
    assert cc.get_assets_info().loc['BTC', 'CoinName'] == 'Bitcoin', "Asset info is missing 'Bitcoin'."


def test_get_top_market_cap_assets(cryptocompare) -> None:
    """
    Test get top market cap assets method.
    """
    cc = cryptocompare
    assert 'BTC' in cc.get_top_market_cap_assets(), "'BTC' is not in top market cap list."


def test_top_market_cap_error(cryptocompare) -> None:
    """
    Test get top market cap assets method errors.
    """
    cc = cryptocompare
    with pytest.raises(ValueError):
        cc.get_top_market_cap_assets(n=101)


def test_indexes(cryptocompare) -> None:
    """
    Test indexes property.
    """
    cc = cryptocompare
    assert 'MVDA' in cc.indexes, "Index list is missing 'MVDA'."


def test_get_indexes_info(cryptocompare) -> None:
    """
    Test get indexes info method.
    """
    cc = cryptocompare
    assert cc.get_indexes_info().loc['MVDA', 'name'] == 'MVIS CryptoCompare Digital Assets 100', \
        "Index info is missing 'MVDA'."


def test_markets(cryptocompare) -> None:
    """
    Test markets property.
    """
    cc = cryptocompare
    assert 'BTCUSDT' in cc.markets, "Assets list is missing 'BTC'."


def test_get_markets_info(cryptocompare) -> None:
    """
    Test get markets info method.
    """
    cc = cryptocompare
    assert cc.get_markets_info()['BTC']['USD']['exchanges']['Coinbase'] == {'isActive': True, 'isTopTier': True}, \
        "Markets info is incorrect."


def test_market_types(cryptocompare) -> None:
    """
    Test market types property.
    """
    cc = cryptocompare
    assert cc.market_types == ['spot'], "Market types should be 'spot'."


def test_market_types_error(cryptocompare) -> None:
    """
    Test market types errors.
    """
    cc = cryptocompare
    with pytest.raises(ValueError):
        cc.market_types = ['swaps']


def test_fields_close(cryptocompare) -> None:
    """
    Test fields property.
    """
    cc = cryptocompare
    assert 'close' in cc.fields, "Fields list is missing 'close'."


def test_fields_active_addresses(cryptocompare) -> None:
    """
    Test fields property.
    """
    cc = cryptocompare
    assert 'active_addresses' in cc.fields, "Fields list is missing 'active_addresses'."


def test_fields_followers(cryptocompare) -> None:
    """
    Test fields property.
    """
    cc = cryptocompare
    assert 'followers' in cc.fields, "Fields list is missing 'followers'."


def test_frequencies(cryptocompare) -> None:
    """
    Test frequencies property.
    """
    cc = cryptocompare
    assert 'd' in cc.frequencies, "Frequencies list is missing 'd'."


def test_frequencies_error(cryptocompare) -> None:
    """
    Test frequencies error.
    """
    cc = cryptocompare
    with pytest.raises(TypeError):
        cc.frequencies = 5


def test_get_news(cryptocompare) -> None:
    """
    Test get news method.
    """
    cc = cryptocompare
    print(cc.get_news())
    assert 'title' in cc.get_news().columns, "News is missing 'title'."


def test_get_news_sources(cryptocompare) -> None:
    """
    Test get news sources method.
    """
    cc = cryptocompare
    assert cc.get_news_sources().loc['coindesk', 'name'] == 'CoinDesk', "News sources is missing 'CoinDesk'."


def test_base_url(cryptocompare) -> None:
    """
    Test base url property.
    """
    cc = cryptocompare
    assert cc.base_url == 'https://min-api.cryptocompare.com/data/', "Base url is incorrect."


def test_base_url_error(cryptocompare) -> None:
    """
    Test base url errors.
    """
    cc = cryptocompare
    with pytest.raises(TypeError):
        cc.base_url = 2225


def test_api_key(cryptocompare) -> None:
    """
    Test api key property.
    """
    cc = cryptocompare
    assert cc.api_key == DataCredentials().cryptocompare_api_key, "Api key is incorrect."


def test_api_key_error(cryptocompare) -> None:
    """
    Test api key errors.
    """
    cc = cryptocompare
    with pytest.raises(TypeError):
        cc.api_key = float(0.5)


def test_max_obs_per_call(cryptocompare) -> None:
    """
    Test max obs per call property.
    """
    cc = cryptocompare
    assert cc.max_obs_per_call == int(2000), "Max observations per call should be int(2000)."


def test_get_rate_limit_info(cryptocompare) -> None:
    """
    Test get rate limit info method.
    """
    cc = cryptocompare
    assert cc.get_rate_limit_info().loc['month', 'calls_left'] > 0, "Monthly rate limit has been reached."


def test_get_indexes(cryptocompare) -> None:
    """
    Test get indexes data method.
    """
    cc = cryptocompare
    data_req = DataRequest(tickers=['mvda', 'bvin'])
    df = cc.get_indexes(data_req)
    assert not df.empty, "Indexes dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert df.index.droplevel(0).unique().to_list() == ['MVDA', 'BVIN'], "Tickers are missing from dataframe"  # tickers
    assert list(df.columns) == ['open', 'high', 'low', 'close'], "Fields are missing from indexes dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp('2017-07-20'), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < timedelta(days=1), \
        "End date is more than 24h ago."  # end date
    assert isinstance(df.close.dropna().iloc[-1], np.float64), "Close is not a numpy float."  # dtypes


def test_get_ohlcv(cryptocompare, datarequest) -> None:
    """
    Test get OHLCV data method.
    """
    cc = cryptocompare
    data_req = datarequest
    df = cc.get_ohlcv(data_req)
    assert not df.empty, "OHLCV dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert df.index.droplevel(0).unique().to_list() == ['BTC']  # tickers
    assert list(df.columns) == ['open', 'high', 'low', 'close', 'volume'], "Fields are missing from OHLCV dataframe."
    assert df.index[0] == (pd.Timestamp('2010-07-17 00:00:00'), 'BTC'), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < timedelta(days=1), \
        "End date is more than 24h ago."  # end date
    assert isinstance(df.close.dropna().iloc[-1], np.float64), "Close is not a numpy float."  # dtypes


def test_get_onchain(cryptocompare, datarequest) -> None:
    """
    Test get on-chain data method.
    """
    cc = cryptocompare
    data_req = datarequest
    df = cc.get_onchain(data_req)
    assert not df.empty, "On-chain dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert df.index.droplevel(0).unique().to_list() == ['BTC']  # tickers
    assert 'add_act' in list(df.columns), "Fields are missing from on-chain dataframe."  # fields
    assert df.index[0] == (pd.Timestamp('2009-01-03 00:00:00'), 'BTC'), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < timedelta(days=3), \
        "End date is more than 48h ago."  # end date
    assert isinstance(df.add_act.dropna().iloc[-1], np.int64), "Close is not a numpy float."  # dtypes


def test_get_social(cryptocompare, datarequest) -> None:
    """
    Test get social media data method.
    """
    cc = cryptocompare
    data_req = datarequest
    df = cc.get_social(data_req)
    assert not df.empty, "Social media dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert df.index.droplevel(0).unique().to_list() == ['BTC']  # tickers
    assert 'sm_followers' in list(df.columns), "Fields are missing from social stats dataframe."  # fields
    assert df.index[0] == (pd.Timestamp('2017-05-26 00:00:00'), 'BTC'), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < timedelta(days=3), \
        "End date is more than 72h ago."  # end date
    assert isinstance(df.sm_followers.dropna().iloc[-1], np.int64), "Close is not a numpy float."  # dtypes


def test_get_data_integration(cryptocompare) -> None:
    """
    Test get data methods integration.
    """
    cc = cryptocompare
    data_req = DataRequest(tickers=['btc', 'eth', 'sol'], fields=['close', 'add_act', 'sm_followers'])
    df = cc.get_data(data_req)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert df.index.droplevel(0).unique().to_list() == ['BTC', 'ETH', 'SOL']  # tickers
    assert list(df.columns) == ['close', 'add_act', 'sm_followers'], "Fields are missing from dataframe."  # fields
    assert df.index[0] == (pd.Timestamp('2009-01-03 00:00:00'), 'BTC'), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < timedelta(days=3), \
        "End date is more than 72h ago."  # end date
    assert isinstance(df.close.dropna().iloc[-1], np.float64), "Close is not a numpy float."  # dtypes
    assert isinstance(df.add_act.dropna().iloc[-1], np.int64), "Active addresses is not a numpy int."  # dtypes
    assert isinstance(df.sm_followers.dropna().iloc[-1], np.int64), "Followers is not a numpy int."  # dtypes


if __name__ == "__main__":
    pytest.main()
