import numpy as np
import pandas as pd
import pytest
import pickle

from cryptodatapy.extract.web.aqr import AQR
from cryptodatapy.extract.datarequest import DataRequest


@pytest.fixture
def aqr():
    return AQR()


@pytest.fixture
def aqr_data_dict():
    with open('data/aqr_dict.pickle', 'rb') as f:
        return pickle.load(f)


def test_set_excel_params(aqr) -> None:
    """
    Test excel parameter values.
    """
    data_req = DataRequest(tickers=['WL_FX_Carry'])
    params = aqr.set_excel_params(data_req, ticker='WL_FX_Carry')
    assert params['file'] == 'Century-of-Factor-Premia-', "Wrong file name."  # file name
    assert params['freq'] == 'Monthly', "Wrong frequency."  # freq
    assert params['format'] == 'xlsx', "Wrong file format."  # format
    assert params['sheet'] == 'Century of Factor Premia', "Wrong sheet name."  # sheet name
    assert params['url'] == 'https://www.aqr.com/-/media/AQR/Documents/Insights/Data-Sets/' + \
                            'Century-of-Factor-Premia-Monthly.xlsx', "Wrong url."  # url
    assert params['index_col'] == 'Unnamed: 0', "Wrong index column."  # index col
    assert params['header'] == 18, "Wrong header row."  # header row
    data_req = DataRequest(tickers=['US_Eqty_Val'], freq='d')
    params = aqr.set_excel_params(data_req, ticker='US_Eqty_Val')
    assert params['file'] == 'The-Devil-in-HMLs-Details-Factors-', "Wrong file name."  # file name
    assert params['freq'] == 'Daily', "Wrong frequency."  # freq
    assert params['format'] == 'xlsx', "Wrong file format."  # format
    assert params['sheet'] == 'HML FF', "Wrong sheet name."  # sheet name
    assert params['url'] == 'https://www.aqr.com/-/media/AQR/Documents/Insights/Data-Sets/' + \
                            'The-Devil-in-HMLs-Details-Factors-Daily.xlsx', "Wrong url."  # url
    assert params['index_col'] == 'DATE', "Wrong index column."  # index col
    assert params['header'] == 18, "Wrong header row."  # header row


def test_wrangle_data_resp(aqr, aqr_data_dict) -> None:
    """
    Test wrangling of data response into tidy data format.
    """
    data_req = DataRequest(tickers=['US_Eqty_Val'], freq='d')
    df = aqr.wrangle_data_resp(data_req, aqr_data_dict)
    assert not df.empty, "Dataframe was returned empty."  # non-empty
    assert isinstance(df.unstack().index, pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert (df.loc[:, df.columns != 'dividend'] == 0).sum().sum() == 0, "Dataframe has missing values."
    assert list(df.columns) == ['er'], "Missing columns."  # fields
    assert isinstance(df.er.iloc[-1], np.float64), "Close price should be a numpy float."  # dtypes


def test_check_params(aqr) -> None:
    """
    Test parameter values before calling API.
    """
    data_req = DataRequest(tickers='US_Eqty_Val')
    with pytest.raises(ValueError):
        aqr.check_params(data_req)
    data_req = DataRequest(tickers='US_Eqty_Val', freq='5min')
    with pytest.raises(ValueError):
        aqr.check_params(data_req)
    data_req = DataRequest(tickers='US_Eqty_Val', fields=['close'])
    with pytest.raises(ValueError):
        aqr.check_params(data_req)


def test_get_data(aqr) -> None:
    """
    Test get data integration method with get_tidy_data and check_params.
    """
    data_req = DataRequest(tickers=["US_Eqty_Val", "US_Eqty_Mom"], cat="eqty", fields='er')
    df = aqr.get_data(data_req)
    assert not df.empty, "Indexes dataframe was returned empty."  # non-empty
    assert isinstance(
        df.index, pd.MultiIndex
    ), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(
        df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert df.index.droplevel(0).unique().to_list() == [
        "US_Eqty_Val",
        "US_Eqty_Mom",
    ], "Tickers are missing from dataframe"  # tickers
    assert set(df.columns) == {
        "er"
    }, "Fields are missing from indexes dataframe."  # fields
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < pd.Timedelta(
        days=250
    ), "End date is more than 5 days ago."  # end date
    assert isinstance(
        df.er.dropna().iloc[-1], np.float64
    ), "Close is not a numpy float."  # dtypes


if __name__ == "__main__":
    pytest.main()
