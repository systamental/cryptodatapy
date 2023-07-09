import numpy as np
import pandas as pd
import pytest

from cryptodatapy.extract.datarequest import DataRequest
from cryptodatapy.extract.libraries.dbnomics_api import DBnomics


@pytest.fixture
def db():
    return DBnomics()


@pytest.fixture
def data_req():
    return DataRequest()


@pytest.fixture
def db_data_req():
    return pd.read_csv('data/db_series_df.csv', index_col=0)


def test_wrangle_data_resp(db, data_req, db_data_req) -> None:
    """
    Test wrangling of data response into tidy data format.
    """
    df = db.wrangle_data_resp(data_req, db_data_req)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert df.shape[1] == 1, "Dataframe should have one column."
    assert (df == 0).sum().sum() == 0, "Dataframe has missing values."
    assert isinstance(df.index, pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert list(df.columns) == ['actual'], "Missing columns."  # fields
    assert isinstance(df.actual.iloc[-1], np.float64), "Actual should be a numpy float."  # dtypes


def test_check_params(db) -> None:
    """
    Test parameter values before calling API.
    """
    data_req = DataRequest(cat='crypto')
    with pytest.raises(ValueError):
        db.check_params(data_req)
    data_req = DataRequest(freq='tick')
    with pytest.raises(ValueError):
        db.check_params(data_req)
    data_req = DataRequest(fields=['forecasts'])
    with pytest.raises(ValueError):
        db.check_params(data_req)


def test_integration_get_data(db) -> None:
    """
    Test integration of get data method.
    """
    data_req = DataRequest(
        tickers=[
            "US_GDP_Sh_PPP",
            "EZ_GDP_Sh_PPP",
            "CN_GDP_Sh_PPP",
            "US_Credit/GDP_HH",
            "WL_Credit_Banks",
        ],
        fields="actual",
        cat="macro",
    )
    df = db.get_data(data_req)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(
        df.index, pd.MultiIndex
    ), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(
        df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert set(df.index.droplevel(0).unique()) == {
        "US_Credit/GDP_HH",
        "WL_Credit_Banks",
        "CN_GDP_Sh_PPP",
        "EZ_GDP_Sh_PPP",
        "US_GDP_Sh_PPP",
    }, "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == [
        "actual"
    ], "Fields are missing from dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp(
        "1947-10-01 00:00:00"
    ), "Wrong start date."  # start date
    assert isinstance(
        df.actual.dropna().iloc[-1], np.float64
    ), "Actual is not a numpy float."  # dtypes


if __name__ == "__main__":
    pytest.main()
