import numpy as np
import pandas as pd
import pytest
from cryptodatapy.extract.datarequest import DataRequest
from cryptodatapy.extract.data_vendors.coinmetrics_api import CoinMetrics
from datetime import datetime


@pytest.fixture
def datarequest():
    return DataRequest()


@pytest.fixture
def coinmetrics():
    return CoinMetrics()


def test_categories(coinmetrics) -> None:
    """
    Test categories property.
    """
    cm = coinmetrics
    assert cm.categories == ['crypto'], "Category should be 'crypto'."


def test_categories_error(coinmetrics) -> None:
    """
    Test categories errors.
    """
    cm = coinmetrics
    with pytest.raises(ValueError):
        cm.categories = ['real_estate', 'art']


def test_exchanges(coinmetrics) -> None:
    """
    Test exchanges property.
    """
    cm = coinmetrics
    assert 'binance' in cm.exchanges, "Exchanges list is missing 'binance'."


def test_get_exchanges_info(coinmetrics) -> None:
    """
    Test get exchanges info method.
    """
    cm = coinmetrics
    assert cm.get_exchanges_info().loc['binance', 'min_time'] == '2017-07-14T04:00:00.510000000Z', \
        "Exchanges info is missing 'Binance'."


def test_assets(coinmetrics) -> None:
    """
    Test assets property.
    """
    cm = coinmetrics
    assert 'btc' in cm.assets, "Assets list is missing 'BTC'."


def test_get_assets_info(coinmetrics) -> None:
    """
    Test get assets info method.
    """
    cm = coinmetrics
    assert cm.get_assets_info().loc['btc', 'full_name'] == 'Bitcoin', "Asset info is missing 'Bitcoin'."


def test_indexes(coinmetrics) -> None:
    """
    Test indexes property.
    """
    cm = coinmetrics
    assert 'CMBI10' in cm.indexes, "Index list is missing 'CMBI10'."


def test_get_indexes_info(coinmetrics) -> None:
    """
    Test get indexes info method.
    """
    cm = coinmetrics
    assert cm.get_indexes_info().loc['CMBI10', 'full_name'] == 'CMBI 10 Index', "Index info is missing 'CMBI10'."


def test_markets(coinmetrics) -> None:
    """
    Test markets info method.
    """
    cm = coinmetrics
    assert 'binance-btc-usdt-spot' in cm.markets, "Markets list is missing 'binance-btc-usdt-spot'."


def test_market_types(coinmetrics) -> None:
    """
    Test market types.
    """
    cm = coinmetrics
    assert cm.market_types == ['spot', 'perpetual_future', 'future', 'option'], "Some market types are missing'."


def test_market_types_error(coinmetrics) -> None:
    """
    Test market types errors.
    """
    cm = coinmetrics
    with pytest.raises(ValueError):
        cm.market_types = ['swaps']


def test_fields_close(coinmetrics) -> None:
    """
    Test fields property.
    """
    cm = coinmetrics
    assert 'price_close' in cm.fields, "Fields list is missing 'price_close'."


def test_fields_active_addresses(coinmetrics) -> None:
    """
    Test fields property.
    """
    cm = coinmetrics
    assert 'AdrActCnt' in cm.fields, "Fields list is missing 'AdrActCnt'."


def test_frequencies(coinmetrics) -> None:
    """
    Test frequencies property.
    """
    cm = coinmetrics
    assert 'd' in cm.frequencies, "Frequencies list is missing 'd'."


def test_frequencies_error(coinmetrics) -> None:
    """
    Test frequencies error.
    """
    cm = coinmetrics
    with pytest.raises(TypeError):
        cm.frequencies = 5


def test_get_avail_fields_info(coinmetrics) -> None:
    """
    Test get assets with available fields method.
    """
    cm = coinmetrics
    data_req = DataRequest(fields=['add_act', 'tx_count'])
    assets_list = cm.get_avail_assets_info(data_req)
    assert len(assets_list) != 0, "Assets list was returned empty."  # non empty
    assert isinstance(assets_list, list), "Should be a list."  # list
    assert 'eth' in assets_list, "Tickers are missing from assets list."  # tickers


def test_get_indexes(coinmetrics) -> None:
    """
    Test get indexes data method.
    """
    cm = coinmetrics
    data_req = DataRequest(tickers=['cmbi10'])
    df = cm.get_indexes(data_req)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert list(df.index.droplevel(0).unique()) == ['CMBI10'], "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == ['close'], "Fields are missing from dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp('2017-01-04'), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < pd.Timedelta(days=3), \
        "End date is more than 72h ago."  # end date
    assert isinstance(df.close.dropna().iloc[-1], np.float64), "Close is not a numpy float."  # dtypes


def test_get_institutions(coinmetrics) -> None:
    """
    Test get institutions data method.
    """
    cm = coinmetrics
    data_req = DataRequest(inst='grayscale', source_fields=['btc_shares_outstanding'])
    df = cm.get_institutions(data_req)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert list(df.index.droplevel(0).unique()) == ['grayscale'], "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == ['btc_shares_outstanding'], "Fields are missing from dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp('2019-01-02'), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < pd.Timedelta(days=7), \
        "End date is more than 7 days ago."  # end date
    assert isinstance(df.btc_shares_outstanding.dropna().iloc[-1], np.int64), \
        "Shares outstanding is not a numpy int."  # dtypes


def test_get_ohlcv(coinmetrics, datarequest) -> None:
    """
    Test get OHLCV data method.
    """
    cm = coinmetrics
    data_req = datarequest
    df = cm.get_ohlcv(data_req)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert list(df.index.droplevel(0).unique()) == ['BTC'], "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == ['open', 'high', 'low', 'close', 'volume', 'vwap'], \
        "Fields are missing from dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp('2017-08-17 00:00:00'), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < pd.Timedelta(days=3), \
        "End date is more than 72h ago."  # end date
    assert isinstance(df.close.dropna().iloc[-1], np.float64), "Close is not a numpy float."  # dtype


def test_get_onchain(coinmetrics) -> None:
    """
    Test get on-chain data method.
    """
    cm = coinmetrics
    data_req = DataRequest(fields='add_act')
    df = cm.get_onchain(data_req)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert list(df.index.droplevel(0).unique()) == ['BTC'], "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == ['add_act'], "Fields are missing from dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp('2009-01-09'), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < pd.Timedelta(days=3), \
        "End date is more than 72h ago."  # end date
    assert isinstance(df.add_act.dropna().iloc[-1], np.int64), "Active addresses is not a numpy int."  # dtype


def test_get_open_interest(coinmetrics) -> None:
    """
    Test get open interest data method.
    """
    cm = coinmetrics
    data_req = DataRequest(mkt_type='perpetual_future')
    df = cm.get_open_interest(data_req)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert list(df.index.droplevel(0).unique()) == ['BTC'], "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == ['oi'], "Fields are missing from dataframe."  # fields
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[0][0] < pd.Timedelta(days=5), \
        "Start date is more than 5 days ago."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < pd.Timedelta(days=1), \
        "End date is more than 24h ago."  # end date
    assert isinstance(df.oi.dropna().iloc[-1], np.float64), "Open interest is not a numpy float."  # dtype


def test_get_funding_rates(coinmetrics) -> None:
    """
    Test get funding rates data method.
    """
    cm = coinmetrics
    data_req = DataRequest(mkt_type='perpetual_future')
    df = cm.get_funding_rates(data_req)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert list(df.index.droplevel(0).unique()) == ['BTC'], "Tickers are missing from dataframe."  # tickers
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."  # multiindex
    assert list(df.columns) == ['funding_rate'], "Fields are missing from dataframe."  # fields
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < pd.Timedelta(days=1), \
        "End date is more than 24h ago."  # end date
    assert isinstance(df.funding_rate.dropna().iloc[-1], np.float64)  # dtypes


def test_get_trades(coinmetrics) -> None:
    """
    Test get trades data method.
    """
    cm = coinmetrics
    data_req = DataRequest(freq='tick', start_date=datetime.utcnow() - pd.Timedelta(seconds=30))
    df = cm.get_trades(data_req)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert list(df.index.droplevel(0).unique()) == ['BTC'], "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == ['trade_size', 'trade_price', 'trade_side'], "Fields are missing from dataframe."
    assert pd.Timestamp.utcnow() - df.index[-1][0] < pd.Timedelta(days=1), \
        "End date is more than 24h ago."  # end date
    assert isinstance(df.trade_price.dropna().iloc[-1], np.float64)  # dtypes


def test_get_quotes(coinmetrics) -> None:
    """
    Test get quotes data method.
    """
    cm = coinmetrics
    data_req = DataRequest(freq='tick', start_date=datetime.utcnow() - pd.Timedelta(seconds=30))
    df = cm.get_quotes(data_req)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert list(df.index.droplevel(0).unique()) == ['BTC'], "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == ['ask', 'ask_size', 'bid', 'bid_size'], "Fields are missing from dataframe."  # fields
    assert pd.Timestamp.utcnow() - df.index[-1][0] < pd.Timedelta(days=1), \
        "End date is more than 24h ago."  # end date
    assert isinstance(df.ask.dropna().iloc[-1], np.float64)  # dtypes


def test_get_data_integration(coinmetrics) -> None:
    """
    Test get data methods integration.
    """
    cm = coinmetrics
    data_req = DataRequest(tickers=['btc', 'eth'], fields=['close', 'add_act'])
    df = cm.get_data(data_req)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert list(df.index.droplevel(0).unique()) == ['BTC', 'ETH'], "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == ['close', 'add_act'], "Fields are missing from dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp('2009-01-09'), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < pd.Timedelta(days=3), \
        "End date is more than 72h ago."  # end date
    assert isinstance(df.close.dropna().iloc[-1], np.float64), "Close is not a numpy float."  # dtypes
    assert isinstance(df.add_act.dropna().iloc[-1], np.int64), "Active addresses is not a numpy int."  # dtypes


if __name__ == "__main__":
    pytest.main()
