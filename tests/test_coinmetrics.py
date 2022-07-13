# import libraries
import pandas as pd
from datetime import datetime, timedelta
from cryptodatapy.data_requests.datarequest import DataRequest
from cryptodatapy.data_vendors.coinmetrics_api import CoinMetrics
from cryptodatapy.util.convertparams import ConvertParams
import pytest


@pytest.fixture
def coinmetrics():
    return CoinMetrics()


@pytest.fixture
def datarequest():
    return DataRequest()


def test_source_type(coinmetrics) -> None:
    """
    Test source type for Coin Metrics.
    """
    cm = coinmetrics
    assert cm.source_type == 'data_vendor', "Source type should be 'data_vendor'."


def test_source_type_error(coinmetrics) -> None:
    """
    Test source type errors for Coin Metrics.
    """
    cm = coinmetrics
    with pytest.raises(ValueError):
        cm.source_type = 'anecdotal'


def test_categories(coinmetrics) -> None:
    """
    Test categories for Coin Metrics.
    """
    cm = coinmetrics
    assert cm.categories == ['crypto'], "Category should be 'crypto'."


def test_categories_error(coinmetrics) -> None:
    """
    Test categories errors for Coin Metrics.
    """
    cm = coinmetrics
    with pytest.raises(ValueError):
        cm.categories = ['real_estate', 'art']


def test_assets(coinmetrics) -> None:
    """
    Test assets list for Coin Metrics.
    """
    cm = coinmetrics
    assert 'btc' in cm.assets, "Assets list is missing 'BTC'."


def test_get_assets_info(coinmetrics) -> None:
    """
    Test get assets info for Coin Metrics.
    """
    cm = coinmetrics
    assert cm.get_assets_info().loc['btc', 'full_name'] == 'Bitcoin', "Asset info is missing 'Bitcoin'."


def test_indexes(coinmetrics) -> None:
    """
    Test indexes for Coin Metrics.
    """
    cm = coinmetrics
    assert 'CMBI10' in cm.indexes, "Index list is missing 'CMBI10'."


def test_get_indexes_info(coinmetrics) -> None:
    """
    Test get indexes info for Coin Metrics.
    """
    cm = coinmetrics
    assert cm.get_indexes_info().loc['CMBI10', 'full_name'] == 'CMBI 10 Index', "Index info is missing 'CMBI10'."


def test_markets(coinmetrics) -> None:
    """
    Test markets info for Coin Metrics.
    """
    cm = coinmetrics
    assert 'binance-btc-usdt-spot' in cm.markets, "Markets list is missing 'binance-btc-usdt-spot'."


def test_market_types(coinmetrics) -> None:
    """
    Test market types for Coin Metrics.
    """
    cm = coinmetrics
    assert cm.market_types == ['spot', 'perpetual_future', 'future', 'option'], "Some market types are missing'."


def test_market_types_error(coinmetrics) -> None:
    """
    Test market types errors for Coin Metrics.
    """
    cm = coinmetrics
    with pytest.raises(ValueError):
        cm.market_types = ['swaps']


def test_fields_close(coinmetrics) -> None:
    """
    Test close field for Coin Metrics.
    """
    cm = coinmetrics
    assert 'price_close' in cm.fields, "Fields list is missing 'price_close'."


def test_fields_active_addresses(coinmetrics) -> None:
    """
    Test active addresses field for Coin Metrics.
    """
    cm = coinmetrics
    assert 'AdrActCnt' in cm.fields, "Fields list is missing 'AdrActCnt'."


def test_frequencies(coinmetrics) -> None:
    """
    Test frequencies for Coin Metrics.
    """
    cm = coinmetrics
    assert 'd' in cm.frequencies, "Frequencies list is missing 'd'."


def test_frequencies_error(coinmetrics) -> None:
    """
    Test frequencies error for Coin Metrics.
    """
    cm = coinmetrics
    with pytest.raises(ValueError):
        cm.frequencies = '1hour'


def test_exchanges(coinmetrics) -> None:
    """
    Test exchanges for Coin Metrics.
    """
    cm = coinmetrics
    assert 'binance' in cm.exchanges, "Exchanges list is missing 'binance'."


def test_get_exchanges_info(coinmetrics) -> None:
    """
    Test exchanges info for Coin Metrics.
    """
    cm = coinmetrics
    assert cm.get_exchanges_info().loc['binance', 'min_time'] == '2017-07-14T04:00:00.510000000Z', \
        "Exchanges info is missing 'Binance'."


def test_fetch_indexes(coinmetrics) -> None:
    """
    Test indexes data retrieval from Coin Metrics API client.
    """
    cm = coinmetrics
    data_req = DataRequest(tickers=['cmbi10'])
    df = cm.fetch_indexes(data_req)
    assert not df.empty, "Dataframe was returned empty."
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."
    assert list(df.columns) == ['close'], "Fields are missing from dataframe."
    assert df.index[0][0] == datetime(2017, 1, 4).date(), "Wrong start date."
    assert df.index[-1][0] == datetime.utcnow().date(), "End date is yesterday's date."


def test_fetch_institutions(coinmetrics) -> None:
    """
    Test institutions data retrieval from Coin Metrics API client.
    """
    cm = coinmetrics
    data_req = DataRequest(inst='grayscale', source_fields=['btc_shares_outstanding'])
    df = cm.fetch_institutions(data_req)
    assert not df.empty, "Dataframe was returned empty."
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."
    assert list(df.columns) == ['btc_shares_outstanding'], "Fields are missing from dataframe."
    assert df.index[0][0] == datetime(2019, 1, 2).date(), "Wrong start date."
    assert df.index[-1][0] >= datetime.utcnow().date() - timedelta(days=7), "End date is yesterday's date."


def test_fetch_ohlcv(coinmetrics, datarequest) -> None:
    """
    Test OHLCV data retrieval from Coin Metrics API client.
    """
    cm = coinmetrics
    data_req = datarequest
    df = cm.fetch_ohlcv(data_req)
    assert not df.empty, "Dataframe was returned empty."
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."
    assert list(df.columns) == ['open', 'high', 'low', 'close', 'volume', 'vwap'], "Fields are missing from dataframe."
    assert df.index[0][0] == datetime(2017, 8, 18).date(), "Wrong start date."
    assert df.index[-1][0] >= datetime.utcnow().date() - timedelta(days=1), "End date is 2 days ago."


def test_fetch_onchain(coinmetrics) -> None:
    """
    Test on-chain data retrieval from Coin Metrics API client.
    """
    cm = coinmetrics
    data_req = DataRequest(fields='add_act')
    df = cm.fetch_onchain(data_req)
    assert not df.empty, "Dataframe was returned empty."
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."
    assert list(df.columns) == ['add_act'], "Fields are missing from dataframe."
    assert df.index[0][0] == datetime(2009, 1, 9).date(), "Wrong start date."
    assert df.index[-1][0] >= datetime.utcnow().date() - timedelta(days=1), "End date is 2 days ago."


def test_fetch_open_interest(coinmetrics) -> None:
    """
    Test open interest data retrieval from Coin Metrics API client.
    """
    cm = coinmetrics
    data_req = DataRequest(mkt_type='perpetual_future')
    df = cm.fetch_open_interest(data_req)
    assert not df.empty, "Dataframe was returned empty."
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."
    assert list(df.columns) == ['oi'], "Fields are missing from dataframe."
    assert df.index[0][0] == datetime.utcnow().date() - timedelta(days=1), "Wrong start date."
    assert df.index[-1][0] == datetime.utcnow().date(), "End date is 2 days ago."


def test_fetch_funding_rates(coinmetrics) -> None:
    """
    Test funding rates data retrieval from Coin Metrics API client.
    """
    cm = coinmetrics
    data_req = DataRequest(mkt_type='perpetual_future')
    df = cm.fetch_funding_rates(data_req)
    assert not df.empty, "Dataframe was returned empty."
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."
    assert list(df.columns) == ['funding_rate'], "Fields are missing from dataframe."
    assert df.index[-1][0] == datetime.utcnow().date(), "End date is 2 days ago."


def test_fetch_trades(coinmetrics) -> None:
    """
    Test trades data retrieval from Coin Metrics API client.
    """
    cm = coinmetrics
    data_req = DataRequest(freq='tick', start_date=datetime.utcnow() - timedelta(seconds=30))
    df = cm.fetch_trades(data_req)
    assert not df.empty, "Dataframe was returned empty."
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."
    assert list(df.columns) == ['trade_size', 'trade_price', 'trade_side'], "Fields are missing from dataframe."
    assert df.index[-1][0].date() == datetime.utcnow().date(), "End date is 2 days ago."


def test_fetch_quotes(coinmetrics) -> None:
    """
    Test quotes data retrieval from Coin Metrics API client.
    """
    cm = coinmetrics
    data_req = DataRequest(freq='tick', start_date=datetime.utcnow() - timedelta(seconds=30))
    df = cm.fetch_quotes(data_req)
    assert not df.empty, "Dataframe was returned empty."
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."
    assert list(df.columns) == ['ask', 'ask_size', 'bid', 'bid_size'], "Fields are missing from dataframe."
    assert df.index[-1][0].date() == datetime.utcnow().date(), "End date is 2 days ago."


def test_fetch_data_integration(coinmetrics) -> None:
    """
    Test integration of data retrieval methods for Coin Metrics API.
    """
    cm = coinmetrics
    data_req = DataRequest(tickers=['btc', 'eth'], fields=['close', 'add_act'])
    df = cm.fetch_data(data_req)
    assert not df.empty, "Dataframe was returned empty."
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."
    assert list(df.index.droplevel(0).unique()) == ['BTC', 'ETH'], "Tickers are missing from dataframe."
    assert list(df.columns) == ['close', 'add_act'], "Fields are missing from dataframe."
    assert df.index[0][0] == datetime(2009, 1, 9).date(), "Wrong start date."
    assert df.index[-1][0] >= datetime.utcnow().date() - timedelta(days=1), "End date is 2 days ago."


if __name__ == "__main__":
    pytest.main()
