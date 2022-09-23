import numpy as np
import pandas as pd
import pytest

from cryptodatapy.extract.datarequest import DataRequest
from cryptodatapy.extract.libraries.pandasdr_api import PandasDataReader
from cryptodatapy.util.datacredentials import DataCredentials


@pytest.fixture
def pdr():
    return PandasDataReader()


@pytest.fixture
def fred_data_resp():
    df = pd.read_csv('tests/data/fred_df.csv', index_col='DATE')
    df.index = pd.to_datetime(df.index)
    return df


def test_wrangle_fred_data_resp(pdr, fred_data_resp) -> None:
    """
    Tests wrangling of Fred data response.
    """
    data_req = DataRequest(source='fred', tickers=['US_CB_MB', 'US_UE_Rate'], cat='macro')
    df = pdr.wrangle_data_resp(data_req, fred_data_resp)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert (df == 0).sum().sum() == 0, "Dataframe has missing values."
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."  # multiindex
    assert set(df.index.droplevel(0).unique()) == {'US_CB_MB', 'US_UE_Rate'}, "Columns are missing or incorrect."
    assert list(df.columns) == ['actual'], "Missing columns."  # fields
    assert isinstance(df.actual.iloc[-1], np.float64), "Actual should be a numpy float."  # dtypes


@pytest.fixture
def yahoo_data_resp():
    df = pd.read_csv('tests/data/yahoo_df.csv', header=[0, 1], index_col=0)
    return df


def test_wrangle_yahoo_data_resp(pdr, yahoo_data_resp) -> None:
    """
    Tests wrangling of Yahoo data response.
    """
    data_req = DataRequest(source='yahoo', tickers=['spy', 'tlt', 'gld'])
    df = pdr.wrangle_data_resp(data_req, yahoo_data_resp)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert (df == 0).sum().sum() == 0, "Dataframe has missing values."
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."  # multiindex
    assert set(df.index.droplevel(0).unique()) == {'GLD', 'SPY', 'TLT'}, "Columns are missing or incorrect."
    assert set(df.columns) == {'close', 'close_adj', 'high', 'low', 'open', 'volume'}, "Missing columns."  # fields
    assert isinstance(df.close.iloc[-1], np.float64), "Actual should be a numpy float."  # dtypes


def test_check_params(pdr) -> None:
    """
    Test parameter values before calling API.
    """
    data_req = DataRequest()
    with pytest.raises(ValueError):
        pdr.check_params(data_req)
    data_req = DataRequest(source='yahoo', cat='crypto')
    with pytest.raises(ValueError):
        pdr.check_params(data_req)
    data_req = DataRequest(source='fred', cat='rates', freq='1h')
    with pytest.raises(ValueError):
        pdr.check_params(data_req)
    data_req = DataRequest(source='yahoo', cat='eqty', fields='trades')
    with pytest.raises(ValueError):
        pdr.check_params(data_req)


def test_integration_get_data_fred(pdr) -> None:
    """
    Test integration of get data method.
    """
    data_req = DataRequest(source='fred', cat='macro', tickers=['US_UE_Rate', 'US_CB_MB'], fields='actual')
    df = pdr.get_data(data_req)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(
        df.index, pd.MultiIndex
    ), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(
        df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert set(df.index.droplevel(0).unique()) == {'US_CB_MB', 'US_UE_Rate'}, \
        "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == [
        "actual"
    ], "Fields are missing from dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp('1948-01-01 00:00:00'), "Wrong start date."  # start date
    assert isinstance(
        df.actual.dropna().iloc[-1], np.float64
    ), "Actual is not a numpy float."  # dtypes


def test_integration_get_data_yahoo(pdr) -> None:
    """
    Test integration of get data method.
    """
    data_req = DataRequest(source='yahoo', cat='eqty', tickers=['spy', 'tlt', 'gld'], fields=['close', 'volume'])
    df = pdr.get_data(data_req)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(
        df.index, pd.MultiIndex
    ), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(
        df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert set(df.index.droplevel(0).unique()) == {'SPY', 'TLT', 'GLD'}, \
        "Tickers are missing from dataframe."  # tickers
    assert set(df.columns) == {'close', 'volume'}, "Fields are missing from dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp('1993-01-29 00:00:00'), "Wrong start date."  # start date
    assert isinstance(df.close.dropna().iloc[-1], np.float64), "Close is not a numpy float."  # dtypes
    assert isinstance(df.volume.dropna().iloc[-1], np.int64), "Volume is not a numpy int."  # dtypes


if __name__ == "__main__":
    pytest.main()
