import numpy as np
import pandas as pd
import pytest
from cryptodatapy.extract.datarequest import DataRequest
from cryptodatapy.extract.libraries.ccxt_api import CCXT


@pytest.fixture
def datarequest():
    return DataRequest()


@pytest.fixture
def ccxt():
    return CCXT()


def test_categories(ccxt) -> None:
    """
    Test categories property.
    """
    cx = ccxt
    assert cx.categories == ['crypto'], "Category should be 'crypto'."


def test_categories_error(ccxt) -> None:
    """
    Test categories errors.
    """
    cx = ccxt
    with pytest.raises(ValueError):
        cx.categories = ['real_estate', 'art']


def test_exchanges(ccxt) -> None:
    """
    Test exchanges property.
    """
    cx = ccxt
    assert 'binance' in cx.exchanges, "Exchanges list is missing 'binance'."


def test_get_exchanges_info(ccxt) -> None:
    """
    Test get exchanges info method.
    """
    cx = ccxt
    assert cx.get_exchanges_info(exch='ftx').loc['ftx', 'name'] == 'FTX', \
        "Exchanges info is missing 'Binance'."


def test_assets(ccxt) -> None:
    """
    Test assets property.
    """
    cx = ccxt
    assert 'BTC' in cx.assets['binance'], "Assets list is missing 'BTC'."


def test_get_assets_info(ccxt) -> None:
    """
    Test get assets info method.
    """
    cx = ccxt
    assert cx.get_assets_info().loc['BTC', 'id'] == 'BTC', "Asset info is missing 'Bitcoin'."


def test_markets(ccxt) -> None:
    """
    Test markets info method.
    """
    cx = ccxt
    assert 'ETH/BTC' in cx.markets['binance'], "Markets list is missing 'ETH/BTC'."


def test_get_markets_info(ccxt) -> None:
    """
    Test get markets info method.
    """
    cx = ccxt
    assert cx.get_markets_info().loc['ETH/BTC', 'base'] == 'ETH', "Markets info is incorrect."


def test_market_types(ccxt) -> None:
    """
    Test market types.
    """
    cx = ccxt
    assert cx.market_types == ['spot', 'future', 'perpetual_future', 'option'], "Some market types are missing'."


def test_market_types_error(ccxt) -> None:
    """
    Test market types errors.
    """
    cx = ccxt
    with pytest.raises(ValueError):
        cx.market_types = ['swaps']


def test_fields(ccxt) -> None:
    """
    Test fields property.
    """
    cx = ccxt
    assert cx.fields == ['open', 'high', 'low', 'close', 'volume', 'funding_rate'], "Fields list is missing incorrect."


def test_frequencies(ccxt) -> None:
    """
    Test frequencies property.
    """
    cx = ccxt
    assert cx.frequencies['binance'] == {'1m': '1m', '3m': '3m', '5m': '5m', '15m': '15m', '30m': '30m', '1h': '1h',
                                         '2h': '2h', '4h': '4h', '6h': '6h', '8h': '8h', '12h': '12h', '1d': '1d',
                                         '3d': '3d', '1w': '1w', '1M': '1M'}, "Frequencies list is missing 'd'."


def test_frequencies_error(ccxt) -> None:
    """
    Test frequencies error.
    """
    cx = ccxt
    with pytest.raises(TypeError):
        cx.frequencies = 5


def test_get_frequencies_info(ccxt) -> None:
    """
    Test get markets info method.
    """
    cx = ccxt
    assert cx.get_frequencies_info(exch='ftx')['ftx']['15m'] == '900', "Frequencies info is incorrect."


def test_rate_limit(ccxt) -> None:
    """
    Test rate limit property.
    """
    cx = ccxt
    assert cx.rate_limit['binance'] == 50, "Rate limit is incorrect'."


def test_get_rate_limit_info(ccxt) -> None:
    """
    Test get rate limit info method.
    """
    cx = ccxt
    assert cx.get_rate_limit_info(exch='ftx')['ftx'] == 28.57, "Rate limit info is incorrect."


def test_get_ohlcv(ccxt, datarequest) -> None:
    """
    Test get OHLCV data method.
    """
    cx = ccxt
    data_req = datarequest
    df = cx.get_ohlcv(data_req)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert list(df.index.droplevel(0).unique()) == ['BTC'], "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == ['open', 'high', 'low', 'close', 'volume'], \
        "Fields are missing from dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp('2017-08-17'), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < pd.Timedelta(days=3), \
        "End date is more than 72h ago."  # end date
    assert isinstance(df.close.dropna().iloc[-1], np.float64), "Close is not a numpy float."  # dtypes

# TODO: add open interest test
# def test_open_interest(ccxt) -> None:
#     """
#     Test get funding rates data method.
#     """
#     pass


def test_get_funding_rates(ccxt) -> None:
    """
    Test get funding rates data method.
    """
    cx = ccxt
    data_req = DataRequest(mkt_type='perpetual_future')
    df = cx.get_funding_rates(data_req)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert df.index.droplevel(0).unique() == ['BTC'], "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == ['funding_rate'], "Fields are missing from dataframe."  # fields
    assert df.index[0][0] < pd.Timestamp('2019-10-10'), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < pd.Timedelta(days=1), \
        "End date is more than 24h ago."  # end date
    assert isinstance(df.funding_rate.dropna().iloc[-1], np.float64), "Funding rate is not a numpy float."  # dtypes


def test_get_data_integration(ccxt) -> None:
    """
    Test get data methods integration.
    """
    cx = ccxt
    data_req = DataRequest(tickers=['btc', 'eth'], fields=['close', 'funding_rate'], mkt_type='perpetual_future')
    df = cx.get_data(data_req)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert list(df.index.droplevel(0).unique()) == ['BTC', 'ETH'], "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == ['close', 'funding_rate'], "Fields are missing from dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp('2019-09-08 00:00:00'), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < pd.Timedelta(days=3), \
        "End date is more than 72h ago."  # end date
    assert isinstance(df.close.dropna().iloc[-1], np.float64), "Close is not a numpy float."  # dtypes
    assert isinstance(df.funding_rate.dropna().iloc[-1], np.float64), "Funding rate is not a numpy float."  # dtypes


if __name__ == "__main__":
    pytest.main()
