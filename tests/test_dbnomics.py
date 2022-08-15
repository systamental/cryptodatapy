import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from cryptodatapy.data_requests.datarequest import DataRequest
from cryptodatapy.data_vendors.dbnomics_api import DBnomics
import pytest


@pytest.fixture
def dbnomics():
    return DBnomics()


def test_source_type(dbnomics) -> None:
    """
    Test source type property.
    """
    db = dbnomics
    assert db.source_type == 'library', "Source type should be 'library'."


def test_source_type_error(dbnomics) -> None:
    """
    Test source type errors.
    """
    db = dbnomics
    with pytest.raises(ValueError):
        db.source_type = 'anecdotal'


def test_categories(dbnomics) -> None:
    """
    Test categories property.
    """
    db = dbnomics
    assert db.categories == ['macro'], "Incorrect categories."


def test_categories_error(dbnomics) -> None:
    """
    Test categories errors.
    """
    db = dbnomics
    with pytest.raises(ValueError):
        db.categories = ['real_estate', 'art']

def test_fields(dbnomics) -> None:
    """
    Test fields property.
    """
    db = dbnomics
    assert db.fields['macro'] == ['actual'], "Fields for macro cat are incorrect."

def test_fields_error(dbnomics) -> None:
    """
    Test categories errors.
    """
    db = dbnomics
    with pytest.raises(ValueError):
        db.get_fields_info(data_type='market')

def test_get_data(dbnomics) -> None:
    """
    Test get data method.
    """
    db = dbnomics
    data_req = DataRequest(tickers=['US_GDP_Sh_PPP', 'EZ_GDP_Sh_PPP', 'CN_GDP_Sh_PPP', 'US_Credit/GDP_HH',
                                    'WL_Credit_Banks'], fields='actual', cat='macro')
    df = db.get_data(data_req)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert list(df.index.droplevel(0).unique()) == ['US_GDP_Sh_PPP', 'EZ_GDP_Sh_PPP', 'CN_GDP_Sh_PPP',
                                                    'US_Credit/GDP_HH', 'WL_Credit_Banks'], \
        "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == ['actual'], "Fields are missing from dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp('1980-01-01 00:00:00'), "Wrong start date."  # start date
    assert isinstance(df.actual.dropna().iloc[-1], np.float64), "Actual is not a numpy float."  # dtypes


if __name__ == "__main__":
    pytest.main()


