import numpy as np
import pandas as pd
import pytest
from cryptodatapy.extract.datarequest import DataRequest
from cryptodatapy.extract.libraries.pandasdr_api import PandasDataReader
from cryptodatapy.util.datacredentials import DataCredentials


@pytest.fixture
def pandasdr():
    return PandasDataReader()


def test_categories(pandasdr) -> None:
    """
    Test categories property.
    """
    pdr = pandasdr
    assert pdr.categories == ['fx', 'rates', 'eqty', 'credit', 'macro'], "Incorrect categories."


def test_categories_error(pandasdr) -> None:
    """
    Test categories errors.
    """
    pdr = pandasdr
    with pytest.raises(ValueError):
        pdr.categories = ['real_estate', 'art']


def test_fields(pandasdr) -> None:
    """
    Test fields property.
    """
    pdr = pandasdr
    assert pdr.fields == {'fx': ['open', 'high', 'low', 'close', 'volume'],
                          'rates': ['open', 'high', 'low', 'close', 'volume'],
                          'eqty': ['open', 'high', 'low', 'close', 'volume'],
                          'credit': ['open', 'high', 'low', 'close', 'volume'],
                          'macro': ['actual']}, "Fields are incorrect."


def test_fields_error(pandasdr) -> None:
    """
    Test categories errors.
    """
    pdr = pandasdr
    with pytest.raises(ValueError):
        pdr.get_fields_info(data_type='on-chain')


def test_frequencies(pandasdr) -> None:
    """
    Test frequencies property
    """
    pdr = pandasdr
    assert pdr.frequencies == {
                                 'crypto': ['d', 'w', 'm', 'q', 'y'],
                                 'fx': ['d', 'w', 'm', 'q', 'y'],
                                 'rates': ['d', 'w', 'm', 'q', 'y'],
                                 'eqty': ['d', 'w', 'm', 'q', 'y'],
                                 'credit': ['d', 'w', 'm', 'q', 'y'],
                                 'macro': ['d', 'w', 'm', 'q', 'y']
                                }, "Incorrect data frequencies."


def test_api_key(pandasdr) -> None:
    """
    Test api key property
    """
    pdr = pandasdr
    data_cred = DataCredentials()
    assert pdr.api_key == {'fred': None, 'yahoo': None, 'av-daily': data_cred.av_api_key,
                           'av-forex-daily': data_cred.av_api_key}

def test_get_data_av_fx(pandasdr) -> None:
    """
    Test get data method.
    """
    pdr = pandasdr
    data_req = DataRequest(data_source='av-forex-daily', tickers=['eur', 'gbp', 'jpy'], start_date='1990-01-01',
                           fields=['close'], cat='fx')
    df = pdr.get_data(data_req)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert set(df.index.droplevel(0).unique()) == {'EURUSD', 'GBPUSD', 'USDJPY'}, \
        "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == ['close'], "Fields are missing from dataframe."  # fields
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[0][0] > pd.Timedelta(days=3780), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < pd.Timedelta(days=4), \
        "End date is more than 4 days ago."  # end date
    assert isinstance(df.close.dropna().iloc[-1], np.float64), "Actual is not a numpy float."  # dtypes


def test_get_data_fred(pandasdr) -> None:
    """
    Test get data method for fred.
    """
    pdr = pandasdr
    data_req = DataRequest(data_source='fred', tickers=['US_Credit_BAA_Spread', 'US_BE_Infl_10Y', 'US_Eqty_Vol_Idx',
                                                        'EM_Eqty_Vol_Idx'], fields='close', cat='eqty')
    df = pdr.get_data(data_req)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert set(df.index.droplevel(0).unique()) == {'EM_Eqty_Vol_Idx', 'US_BE_Infl_10Y', 'US_Credit_BAA_Spread',
                                                   'US_Eqty_Vol_Idx'}, "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == ['close'], "Fields are missing from dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp('1986-01-02 00:00:00'), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < pd.Timedelta(days=4), \
        "End date is more than 4 days ago."  # end date
    assert isinstance(df.close.dropna().iloc[-1], np.float64), "Actual is not a numpy float."  # dtypes


def test_get_data_yahoo(pandasdr) -> None:
    """
    Test get data method for yahoo.
    """
    pdr = pandasdr
    data_req = DataRequest(data_source='yahoo', tickers=['AAPL', 'MSFT', 'SPY', 'TLT', 'QQQ'], fields=['close'],
                           cat='eqty')
    df = pdr.get_data(data_req)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert set(df.index.droplevel(0).unique()) == {'AAPL', 'MSFT', 'QQQ', 'SPY', 'TLT'}, \
        "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == ['close'], "Fields are missing from dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp('1980-12-12 00:00:00'), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < pd.Timedelta(days=4), \
        "End date is more than 4 days ago."  # end date
    assert isinstance(df.close.dropna().iloc[-1], np.float64), "Actual is not a numpy float."  # dtypes


if __name__ == "__main__":
    pytest.main()
