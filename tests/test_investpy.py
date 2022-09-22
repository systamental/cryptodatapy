import numpy as np
import pandas as pd
import pytest

from cryptodatapy.extract.datarequest import DataRequest
from cryptodatapy.extract.libraries.investpy_api import InvestPy


@pytest.fixture
def ip():
    return InvestPy()


@pytest.fixture
def econ_cal():
    return pd.read_csv('tests/data/ip_econ_cal.csv', index_col=0)


def test_get_macro(ip, econ_cal) -> None:
    """
    Test get macro series from econ calendar.
    """
    data_req = DataRequest(tickers=['US_Manuf_PMI', 'EZ_Infl_CPI_YoY'], cat='macro',
                           fields=['actual', 'expected', 'surprise'])
    df = ip.get_macro(data_req, econ_cal)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."  # multiindex
    assert set(df.index.droplevel(0).unique()) == {'US_Manuf_PMI', 'EZ_Infl_CPI_YoY'}, \
        "Columns are missing or incorrect."
    assert list(df.columns) == ['actual', 'expected', 'previous', 'surprise'], "Missing columns."  # fields
    assert isinstance(df.actual.iloc[-1], np.float64), "Actual should be a numpy float."  # dtypes


def test_check_params(ip) -> None:
    """
    Test parameter values before calling API.
    """
    data_req = DataRequest()
    with pytest.raises(ValueError):
        ip.check_params(data_req)
    data_req = DataRequest(tickers=['EZ_Manuf_PMI', 'CN_Manuf_PMI'], cat='macro', fields=['actual', 'forecast'])
    with pytest.raises(ValueError):
        ip.check_params(data_req)
    data_req = DataRequest(tickers=['EZ_Manuf_PMI', 'CN_Manuf_PMI'], cat='macro', freq='tick')  # fred
    with pytest.raises(ValueError):
        ip.check_params(data_req)
    data_req = DataRequest(tickers=['EZ_Manuf_PMI', 'CN_Manuf_PMI'], cat='crypto')
    with pytest.raises(ValueError):
        ip.check_params(data_req)


def test_integration_get_data(ip) -> None:
    """
    Test integration of get_updated_econ_calendar, get_all_ctys_eco_cals and get_macro in the get_data method.
    """
    data_req = DataRequest(
        tickers=["CN_Manuf_PMI", "EZ_M3"],
        fields=["actual", "expected", "previous", "surprise"],
        cat="macro",
        start_date="2019-01-01",
    )
    df = ip.get_data(data_req)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(
        df.index, pd.MultiIndex
    ), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(
        df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert set(df.index.droplevel(0).unique()) == {
        "CN_Manuf_PMI",
        "EZ_M3",
    }, "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == [
        "actual",
        "expected",
        "previous",
        "surprise",
    ], "Fields are missing from dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp(
        "2019-01-01 00:00:00"
    ), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < pd.Timedelta(
        days=30
    ), "End date is more than a month ago."  # end date
    assert isinstance(
        df.actual.dropna().iloc[-1], np.float64
    ), "Actual is not a numpy float."  # dtypes


if __name__ == "__main__":
    pytest.main()
