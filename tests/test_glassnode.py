import pandas as pd
import numpy as np
from cryptodatapy.extract.datarequest import DataRequest
from cryptodatapy.extract.data_vendors.glassnode_api import Glassnode
import pytest


@pytest.fixture
def datarequest():
    return DataRequest()


@pytest.fixture
def glassnode():
    return Glassnode()

def test_categories(glassnode) -> None:
    """
    Test categories property.
    """
    gn = glassnode
    assert gn.categories == ['crypto'], "Category should be 'crypto'."


def test_categories_error(glassnode) -> None:
    """
    Test categories errors.
    """
    gn = glassnode
    with pytest.raises(ValueError):
        gn.categories = ['real_estate', 'art']


def test_assets(glassnode) -> None:
    """
    Test assets property.
    """
    gn = glassnode
    assert 'BTC' in gn.assets, "Assets list is missing 'BTC'."


def test_get_assets_info(glassnode) -> None:
    """
    Test get assets info method.
    """
    gn = glassnode
    assert gn.get_assets_info().loc['BTC', 'name'] == 'Bitcoin', "Asset info is missing 'Bitcoin'."


def test_market_types(glassnode) -> None:
    """
    Test market types.
    """
    gn = glassnode
    assert gn.market_types == ['spot', 'perpetual_future', 'future', 'option'], "Some market types are missing'."


def test_market_types_error(glassnode) -> None:
    """
    Test market types errors.
    """
    gn = glassnode
    with pytest.raises(ValueError):
        gn.market_types = ['swaps']


def test_fields_close(glassnode) -> None:
    """
    Test close field.
    """
    gn = glassnode
    assert 'market/price_usd_ohlc' in gn.fields, "Fields list is missing 'price_close'."


def test_fields_active_addresses(glassnode) -> None:
    """
    Test active addresses field.
    """
    gn = glassnode
    assert 'addresses/active_count' in gn.fields, "Fields list is missing 'AdrActCnt'."


def test_frequencies(glassnode) -> None:
    """
    Test frequencies.
    """
    gn = glassnode
    assert 'd' in gn.frequencies, "Frequencies list is missing 'd'."


def test_frequencies_error(glassnode) -> None:
    """
    Test frequencies error.
    """
    gn = glassnode
    with pytest.raises(TypeError):
        gn.frequencies = 5


def test_get_data_integration(glassnode) -> None:
    """
    Test integration of data retrieval methods.
    """
    gn = glassnode
    data_req = DataRequest(tickers=['btc', 'eth'], freq='d', fields=['close', 'add_act', 'tx_count'])
    df = gn.get_data(data_req)
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


if __name__ == "__main__":
    pytest.main()
