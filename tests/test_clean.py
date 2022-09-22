import numpy as np
import pandas as pd
import pytest

from cryptodatapy.transform.clean import CleanData


# get data for testing
@pytest.fixture
def raw_oc_data():
    return pd.read_csv('tests/data/cm_raw_oc_df.csv', index_col=[0, 1], parse_dates=['date'])

@pytest.fixture
def raw_ohlcv_data():
    return pd.read_csv('tests/data/cm_raw_ohlcv_df.csv', index_col=[0, 1], parse_dates=['date'])


def test_clean_data_integration_filter_outliers(raw_ohlcv_data) -> None:
    """
    Test clean data pipeline - filter outliers.
    """
    # clean data - filter outliers
    df = CleanData(raw_ohlcv_data).filter_outliers().get(attr="df")
    # assert statements
    assert df.shape == raw_ohlcv_data.shape, "Filtered dataframe changed shape."  # shape
    assert isinstance(
        df.index, pd.MultiIndex
    ), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(
        df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert all(
        df.isnull().sum() >= raw_ohlcv_data.isnull().sum()
    ), "No outliers were filtered in dataframe."  # filtered
    assert not any(
        (df.describe().loc["max"] == np.inf) & (df.describe().loc["min"] == -np.inf)
    ), "Inf values found in the dataframe"  # inf
    assert isinstance(
        df.close.dropna().iloc[-1], np.float64
    ), "Filtered close is not a numpy float."  # dtypes


def test_clean_data_integration_repair_outliers(raw_ohlcv_data) -> None:
    """
    Test clean data pipeline - repair outliers.
    """
    # clean data - repair outliers
    df = CleanData(raw_ohlcv_data).filter_outliers().repair_outliers().get(attr="df")
    # assert statements
    assert df.shape == raw_ohlcv_data.shape, "Filtered dataframe changed shape."  # shape
    assert isinstance(
        df.index, pd.MultiIndex
    ), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(
        df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert all(
        df.iloc[-200:].isnull().sum() == 0
    ), "Missing/filtered values were not repaired."  # missing vals
    assert not any(
        (df.describe().loc["max"] == np.inf) & (df.describe().loc["min"] == -np.inf)
    ), "Inf values found in the dataframe"  # inf
    assert isinstance(
        df.close.dropna().iloc[-1], np.float64
    ), "Filtered close is not a numpy float."  # dtypes


def test_clean_data_integration_filter_avg_trading_val(raw_ohlcv_data) -> None:
    """
    Test clean data pipeline - filter average trading value.
    """
    # clean data - filter avg trading val
    df = CleanData(raw_ohlcv_data).filter_outliers().filter_avg_trading_val().get(attr="df")
    # assert statements
    assert df.shape == raw_ohlcv_data.shape, "Filtered dataframe changed shape."  # shape
    assert isinstance(
        df.index, pd.MultiIndex
    ), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(
        df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert (
        df.loc[pd.IndexSlice[:"2019-01-01", "ADA"], "close"].isnull().sum() > 0
    ), "Cardano average trading value before 2019 should be below threshold and replaced by NaNs"  # filtered vals
    assert not any(
        (df.describe().loc["max"] == np.inf) & (df.describe().loc["min"] == -np.inf)
    ), "Inf values found in the dataframe"  # inf
    assert isinstance(
        df.close.dropna().iloc[-1], np.float64
    ), "Filtered close is not a numpy float."  # dtypes


def test_clean_data_integration_filter_missing_vals_gaps(raw_ohlcv_data) -> None:
    """
    Test clean data pipeline - filter missing values gaps.
    """
    # clean data - filter outliers
    df = (
        CleanData(raw_ohlcv_data)
        .filter_outliers()
        .repair_outliers()
        .filter_avg_trading_val()
        .filter_missing_vals_gaps()
        .get(attr="df")
    )
    # assert statements
    assert df.shape == raw_ohlcv_data.shape, "Filtered dataframe changed shape."  # shape
    assert isinstance(
        df.index, pd.MultiIndex
    ), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(
        df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert df.loc[pd.IndexSlice[:, "ADA"], "close"].dropna().index[0][
        0
    ] == pd.Timestamp(
        "2020-01-31 00:00:00"
    ), "Start date should be '2020-01-31 00:00:00' for ADA after removing missing values gaps."  # filtered vals
    assert not any(
        (df.describe().loc["max"] == np.inf) & (df.describe().loc["min"] == -np.inf)
    ), "Inf values found in the dataframe"  # inf
    assert isinstance(
        df.close.dropna().iloc[-1], np.float64
    ), "Filtered close is not a numpy float."  # dtypes


def test_clean_data_integration_filter_min_obs(raw_ohlcv_data) -> None:
    """
    Test clean data pipeline - filter minimum observations.
    """
    # clean data - filter outliers
    df = (
        CleanData(raw_ohlcv_data)
        .filter_outliers()
        .repair_outliers()
        .filter_avg_trading_val()
        .filter_missing_vals_gaps()
        .filter_min_nobs(min_obs=1000)
        .get(attr="df")
    )
    # assert statements
    assert df.shape != raw_ohlcv_data.shape, "Filtered dataframe changed shape."  # shape
    assert isinstance(
        df.index, pd.MultiIndex
    ), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(
        df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert "ADA" not in list(
        df.index.droplevel(0).unique()
    ), "ADA should be removed from dataframe"  # filt min obs
    assert not any(
        (df.describe().loc["max"] == np.inf) & (df.describe().loc["min"] == -np.inf)
    ), "Inf values found in the dataframe"  # inf
    assert isinstance(
        df.close.dropna().iloc[-1], np.float64
    ), "Filtered close is not a numpy float."  # dtypes


def test_clean_data_integration_filter_tickers(raw_ohlcv_data) -> None:
    """
    Test clean data pipeline - filter tickers.
    """
    # clean data - filter outliers
    df = (
        CleanData(raw_ohlcv_data)
        .filter_outliers()
        .repair_outliers()
        .filter_avg_trading_val()
        .filter_missing_vals_gaps()
        .filter_min_nobs()
        .filter_tickers(tickers_list="BTC")
        .get(attr="df")
    )
    # assert statements
    assert df.shape != raw_ohlcv_data.shape, "Filtered dataframe changed shape."  # shape
    assert isinstance(
        df.index, pd.MultiIndex
    ), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(
        df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert "BTC" not in list(
        df.index.droplevel(0).unique()
    ), "BTC should be removed from dataframe"  # filt tickers
    assert not any(
        (df.describe().loc["max"] == np.inf) & (df.describe().loc["min"] == -np.inf)
    ), "Inf values found in the dataframe"  # inf
    assert isinstance(
        df.close.dropna().iloc[-1], np.float64
    ), "Filtered close is not a numpy float."  # dtypes


if __name__ == "__main__":
    pytest.main()
