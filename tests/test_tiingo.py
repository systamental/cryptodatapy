import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from cryptodatapy.util.datacredentials import DataCredentials
from cryptodatapy.data_requests.datarequest import DataRequest
from cryptodatapy.data_vendors.tiingo_api import Tiingo
import pytest


@pytest.fixture
def tiingo():
    return Tiingo()


def test_source_type(tiingo) -> None:
    """
    Test source type property.
    """
    tg = tiingo
    assert tg.source_type == 'data_vendor', "Source type should be 'library'."


def test_source_type_error(tiingo) -> None:
    """
    Test source type errors.
    """
    tg = tiingo
    with pytest.raises(ValueError):
        tg.source_type = 'anecdotal'


def test_categories(tiingo) -> None:
    """
    Test categories property.
    """
    tg = tiingo
    assert tg.categories == ['crypto', 'fx', 'eqty'], "Incorrect categories."


def test_categories_error(tiingo) -> None:
    """
    Test categories errors.
    """
    tg = tiingo
    with pytest.raises(ValueError):
        tg.categories = ['real_estate', 'art']


def test_exchanges(tiingo) -> None:
    """
    Test exchanges property.
    """
    tg = tiingo
    assert 'NYSE' in tg.exchanges['eqty'], "Exchanges dictionary is missing 'NYSE'."


def test_assets(tiingo) -> None:
    """
    Test assets property.
    """
    tg = tiingo
    assert 'btcusd' in tg.assets['crypto'], "Assets dictionary is missing 'btcusd'."


def test_get_assets_info(tiingo) -> None:
    """
    Test get assets info method.
    """
    tg = tiingo
    assert tg.get_assets_info(cat='eqty').loc['SPY', 'exchange'] == 'NYSE ARCA', \
        "Assets info is missing 'SPY'."


def test_market_types(tiingo) -> None:
    """
    Test market types property.
    """
    tg = tiingo
    assert tg.market_types == ['spot'], "Market types should be 'spot'."


def test_market_types_error(tiingo) -> None:
    """
    Test market types errors.
    """
    tg = tiingo
    with pytest.raises(ValueError):
        tg.market_types = ['swaps']


def test_fields(tiingo) -> None:
    """
    Test fields property.
    """
    tg = tiingo
    assert tg.fields['eqty'] == ['open', 'high', 'low', 'close', 'volume', 'open_adj', 'high_adj', 'close_adj',
                                 'dividend', 'split'], "Fields are missing."


def test_frequencies(tiingo) -> None:
    """
    Test frequencies property.
    """
    tg = tiingo
    assert 'd' in tg.frequencies, "Frequencies list is missing 'd'."


def test_frequencies_error(tiingo) -> None:
    """
    Test frequencies error.
    """
    tg = tiingo
    with pytest.raises(TypeError):
        tg.frequencies = 5


def test_base_url(tiingo) -> None:
    """
    Test base url property.
    """
    tg = tiingo
    assert tg.base_url == 'https://api.tiingo.com/tiingo/', "Base url is incorrect."


def test_base_url_error(tiingo) -> None:
    """
    Test base url errors.
    """
    tg = tiingo
    with pytest.raises(TypeError):
        tg.base_url = 2225


def test_api_key(tiingo) -> None:
    """
    Test api key property.
    """
    tg = tiingo
    assert tg.api_key == DataCredentials().tiingo_api_key, "Api key is incorrect."


def test_api_key_error(tiingo) -> None:
    """
    Test api key errors.
    """
    tg = tiingo
    with pytest.raises(TypeError):
        tg.api_key = float(0.5)


def test_get_eqty_daily(tiingo) -> None:
    """
    Test get equity daily data method.
    """
    tg = tiingo
    data_req = DataRequest(tickers=['spy', 'tlt', 'gsp'], cat='eqty', start_date='2020-01-01')
    df = tg.get_eqty_daily(data_req)
    assert not df.empty, "Indexes dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert df.index.droplevel(0).unique().to_list() == ['SPY', 'TLT', 'GSP'], \
        "Tickers are missing from dataframe"  # tickers
    assert list(df.columns) == ['close', 'high', 'low', 'open', 'volume', 'close_adj', 'high_adj', 'low_adj',
                                'open_adj', 'volume_adj', 'dividend', 'split'], \
        "Fields are missing from indexes dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp('2020-01-02 00:00:00'), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < timedelta(days=4), \
        "End date is more than 4 days ago."  # end date
    assert isinstance(df.close.dropna().iloc[-1], np.float64), "Close is not a numpy float."  # dtypes


def test_get_eqty_iex(tiingo) -> None:
    """
    Test get equity iex data method.
    """
    tg = tiingo
    data_req = DataRequest(tickers=['meta', 'aapl', 'amzn', 'nflx', 'goog'], cat='eqty', freq='5min')
    df = tg.get_eqty_iex(data_req)
    assert not df.empty, "Indexes dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert df.index.droplevel(0).unique().to_list() == ['META', 'AAPL', 'AMZN', 'NFLX', 'GOOG'], \
        "Tickers are missing from dataframe"  # tickers
    assert list(df.columns) == ['close', 'high', 'low', 'open'], "Fields are missing from indexes dataframe."  # fields
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < timedelta(days=4), \
        "End date is more than 4 days ago."  # end date
    assert isinstance(df.close.dropna().iloc[-1], np.float64), "Close is not a numpy float."  # dtypes


def test_get_crypto(tiingo) -> None:
    """
    Test get crypto data method.
    """
    tg = tiingo
    data_req = DataRequest(tickers=['btc', 'eth', 'sol'], cat='crypto')
    df = tg.get_crypto(data_req)
    assert not df.empty, "Indexes dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert df.index.droplevel(0).unique().to_list() == ['BTC', 'ETH', 'SOL'], \
        "Tickers are missing from dataframe"  # tickers
    assert list(df.columns) == ['open', 'high', 'low', 'close', 'volume', 'volume_quote_ccy', 'trades '], \
        "Fields are missing from indexes dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp('2011-08-19 00:00:00'), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < timedelta(days=4), \
        "End date is more than 4 days ago."  # end date
    assert isinstance(df.close.dropna().iloc[-1], np.float64), "Close is not a numpy float."  # dtypes


def test_get_fx(tiingo) -> None:
    """
    Test get fx data method.
    """
    tg = tiingo
    data_req = DataRequest(tickers=['eur', 'gbp', 'jpy', 'cad', 'try', 'brl'], cat='fx', start_date='1990-01-01')
    df = tg.get_fx(data_req)
    assert not df.empty, "Indexes dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert df.index.droplevel(0).unique().to_list() == ['EURUSD', 'GBPUSD', 'USDJPY', 'USDCAD', 'USDTRY', 'USDBRL'], \
        "Tickers are missing from dataframe"  # tickers
    assert list(df.columns) == ['open', 'high', 'low', 'close'], \
        "Fields are missing from indexes dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp('1990-01-02 00:00:00'), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.unstack().index[-1] < timedelta(days=4), \
        "End date is more than 4 days ago."  # end date
    assert isinstance(df.close.dropna().iloc[-1], np.float64), "Close is not a numpy float."  # dtypes


def test_get_data(tiingo) -> None:
    """
    Test get OHLCV data method.
    """
    tg = tiingo
    data_req = DataRequest(tickers=['meta', 'aapl', 'amzn', 'nflx', 'goog'], cat='eqty')
    df = tg.get_data(data_req)
    assert not df.empty, "Indexes dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert df.index.droplevel(0).unique().to_list() == ['AAPL', 'AMZN', 'NFLX', 'META', 'GOOG'], \
        "Tickers are missing from dataframe"  # tickers
    assert list(df.columns) == ['close'], "Fields are missing from indexes dataframe."  # fields
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < timedelta(days=4), \
        "End date is more than 4 days ago."  # end date
    assert isinstance(df.close.dropna().iloc[-1], np.float64), "Close is not a numpy float."  # dtypes
