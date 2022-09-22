from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from cryptodatapy.transform.filter import Filter
from cryptodatapy.transform.od import OutlierDetection

# get data for testing
@pytest.fixture
def raw_oc_data():
    return pd.read_csv('tests/data/cc_raw_oc_df.csv', index_col=[0, 1], parse_dates=['date'])

@pytest.fixture
def raw_ohlcv_data():
    return pd.read_csv('tests/data/cc_raw_ohlcv_df.csv', index_col=[0, 1], parse_dates=['date'])



def test_filter_atr(raw_ohlcv_data) -> None:
    """
    Test filter ATR method.
    """
    # outlier detection
    outliers_dict = OutlierDetection(raw_ohlcv_data).atr()
    filt_df = outliers_dict["filt_vals"]
    # assert statements
    assert filt_df.shape == raw_ohlcv_data.shape, "Filtered dataframe changed shape."  # shape
    assert isinstance(
        filt_df.index, pd.MultiIndex
    ), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(
        filt_df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert all(
        filt_df.loc[:, :"close"].notna().sum() / raw_ohlcv_data.loc[:, :"close"].notna().sum()
        > 0.95
    ), "Some series have more than 5% of values filtered as outliers in dataframe."  # % filtered
    assert not any(
        (filt_df.describe().loc["max"] == np.inf)
        & (filt_df.describe().loc["min"] == -np.inf)
    ), "Inf values found in the dataframe"  # inf
    assert isinstance(
        filt_df.close.dropna().iloc[-1], np.float64
    ), "Filtered close is not a numpy float."  # dtypes


def test_filter_iqr(raw_oc_data) -> None:
    """
    Test filter IQR method.
    """
    # outlier detection
    outliers_dict = OutlierDetection(raw_oc_data).iqr()
    filt_df = outliers_dict["filt_vals"]
    # assert statements
    assert filt_df.shape == raw_oc_data.shape, "Filtered dataframe changed shape."  # shape
    assert isinstance(
        filt_df.index, pd.MultiIndex
    ), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(
        filt_df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert all(
        filt_df.notna().sum() / raw_oc_data.notna().sum() > 0.90
    ), "Some series have more than 10% of values filtered as outliers in dataframe."  # % filtered
    assert not any(
        (filt_df.describe().loc["max"] == np.inf)
        & (filt_df.describe().loc["min"] == -np.inf)
    ), "Inf values found in the dataframe"  # inf
    assert isinstance(
        filt_df.close.dropna().iloc[-1], np.float64
    ), "Filtered close is not a numpy float."  # dtypes
    assert isinstance(
        filt_df.add_act.dropna().iloc[-1], np.int64
    ), "Filtered active addresses are not a numpy int."  # dtypes
    assert isinstance(
        filt_df.tx_count.dropna().iloc[-1], np.int64
    ), "Filtered transaction count is not a numpy int."  # dtypes


def test_filter_mad(raw_oc_data) -> None:
    """
    Test filter MAD method.
    """
    # outlier detection
    df = OutlierDetection(raw_oc_data).mad()
    filt_df = df["filt_vals"]
    # assert statements
    assert filt_df.shape == raw_oc_data.shape, "Filtered dataframe changed shape."  # shape
    assert isinstance(
        filt_df.index, pd.MultiIndex
    ), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(
        filt_df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert all(
        filt_df.notna().sum() / raw_oc_data.notna().sum() > 0.80
    ), "Some series have more than 20% of values filtered as outliers in dataframe."  # % filtered
    assert not any(
        (filt_df.describe().loc["max"] == np.inf)
        & (filt_df.describe().loc["min"] == -np.inf)
    ), "Inf values found in the dataframe"  # inf
    assert isinstance(
        filt_df.close.dropna().iloc[-1], np.float64
    ), "Filtered close is not a numpy float."  # dtypes
    assert isinstance(
        filt_df.add_act.dropna().iloc[-1], np.int64
    ), "Filtered active addresses is not a numpy int."  # dtypes
    assert isinstance(
        filt_df.tx_count.dropna().iloc[-1], np.int64
    ), "Filtered transaction count is not a numpy int."  # dtypes


def test_filter_zscore(raw_oc_data) -> None:
    """
    Test filter z-score method.
    """
    # outlier detection
    df = OutlierDetection(raw_oc_data).z_score(thresh_val=2)
    filt_df = df["filt_vals"]
    # assert statements
    assert filt_df.shape == raw_oc_data.shape, "Filtered dataframe changed shape."  # shape
    assert isinstance(
        filt_df.index, pd.MultiIndex
    ), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(
        filt_df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert all(
        filt_df.notna().sum() / raw_oc_data.notna().sum() > 0.9
    ), "Some series have more than 10% of values filtered as outliers in dataframe."  # % filtered
    assert not any(
        (filt_df.describe().loc["max"] == np.inf)
        & (filt_df.describe().loc["min"] == -np.inf)
    ), "Inf values found in the dataframe"  # inf
    assert isinstance(
        filt_df.close.dropna().iloc[-1], np.float64
    ), "Filtered close is not a numpy float."  # dtypes
    assert isinstance(
        filt_df.add_act.dropna().iloc[-1], np.int64
    ), "Filtered active addresses is not a numpy int."  # dtypes
    assert isinstance(
        filt_df.tx_count.dropna().iloc[-1], np.int64
    ), "Filtered transaction count is not a numpy int."  # dtypes


def test_filter_ewma(raw_oc_data) -> None:
    """
    Test filter exponential weighted moving average method.
    """
    # outlier detection
    df = OutlierDetection(raw_oc_data).ewma(thresh_val=1.5)
    filt_df, outliers_df = df["filt_vals"], df["outliers"]
    # assert statements
    assert filt_df.shape == raw_oc_data.shape, "Filtered dataframe changed shape."  # shape
    assert isinstance(
        filt_df.index, pd.MultiIndex
    ), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(
        filt_df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert all(
        filt_df.notna().sum() / raw_oc_data.notna().sum() > 0.9
    ), "Some series have more than 10% of values filtered as outliers in dataframe."  # % filtered
    assert not any(
        (filt_df.describe().loc["max"] == np.inf)
        & (filt_df.describe().loc["min"] == -np.inf)
    ), "Inf values found in the dataframe"  # inf
    assert isinstance(
        filt_df.close.dropna().iloc[-1], np.float64
    ), "Filtered close is not a numpy float."  # dtypes
    assert isinstance(
        filt_df.add_act.dropna().iloc[-1], np.int64
    ), "Filtered active addresses is not a numpy int."  # dtypes
    assert isinstance(
        filt_df.tx_count.dropna().iloc[-1], np.int64
    ), "Filtered transaction count is not a numpy int."  # dtypes


def test_filter_seasonal_decomp(raw_oc_data) -> None:
    """
    Test filter seasonal decomposition method.
    """
    # outlier detection
    df = OutlierDetection(raw_oc_data).seasonal_decomp(thresh_val=10)
    filt_df = df["filt_vals"]
    # assert statements
    assert filt_df.shape == raw_oc_data.shape, "Filtered dataframe changed shape."  # shape
    assert isinstance(
        filt_df.index, pd.MultiIndex
    ), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(
        filt_df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert all(
        filt_df.notna().sum() / raw_oc_data.notna().sum() > 0.7
    ), "Some series have more than 30% of values filtered as outliers in dataframe."  # % filtered
    assert not any(
        (filt_df.describe().loc["max"] == np.inf)
        & (filt_df.describe().loc["min"] == -np.inf)
    ), "Inf values found in the dataframe"  # inf
    assert isinstance(
        filt_df.close.dropna().iloc[-1], np.float64
    ), "Filtered close is not a numpy float."  # dtypes
    assert isinstance(
        filt_df.add_act.dropna().iloc[-1], np.int64
    ), "Filtered active addresses is not a numpy int."  # dtypes
    assert isinstance(
        filt_df.tx_count.dropna().iloc[-1], np.int64
    ), "Filtered transaction count is not a numpy int."  # dtypes


def test_filter_stl(raw_oc_data) -> None:
    """
    Test filter seasonal decomposition method.
    """
    # outlier detection
    df = OutlierDetection(raw_oc_data).stl(thresh_val=10)
    filt_df = df["filt_vals"]
    # assert statements
    assert filt_df.shape == raw_oc_data.shape, "Filtered dataframe changed shape."  # shape
    assert isinstance(
        filt_df.index, pd.MultiIndex
    ), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(
        filt_df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert all(
        filt_df.notna().sum() / raw_oc_data.notna().sum() > 0.7
    ), "Some series have more than 30% of values filtered as outliers in dataframe."  # % filtered
    assert not any(
        (filt_df.describe().loc["max"] == np.inf)
        & (filt_df.describe().loc["min"] == -np.inf)
    ), "Inf values found in the dataframe"  # inf
    assert isinstance(
        filt_df.close.dropna().iloc[-1], np.float64
    ), "Filtered close is not a numpy float."  # dtypes
    assert isinstance(
        filt_df.add_act.dropna().iloc[-1], np.int64
    ), "Filtered active addresses is not a numpy int."  # dtypes
    assert isinstance(
        filt_df.tx_count.dropna().iloc[-1], np.int64
    ), "Filtered transaction count is not a numpy int."  # dtypes


def test_filter_prophet(raw_oc_data) -> None:
    """
    Test filter prophet method.
    """
    # outlier detection
    df = OutlierDetection(raw_oc_data).prophet()
    filt_df = df["filt_vals"]
    # assert statements
    assert filt_df.shape == raw_oc_data.shape, "Filtered dataframe changed shape."  # shape
    assert isinstance(
        filt_df.index, pd.MultiIndex
    ), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(
        filt_df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert all(
        filt_df.notna().sum() / raw_oc_data.notna().sum() > 0.8
    ), "Some series have more than 20% of values filtered as outliers in dataframe."  # % filtered
    assert not any(
        (filt_df.describe().loc["max"] == np.inf)
        & (filt_df.describe().loc["min"] == -np.inf)
    ), "Inf values found in the dataframe"  # inf
    assert isinstance(
        filt_df.close.dropna().iloc[-1], np.float64
    ), "Filtered close is not a numpy float."  # dtypes
    assert isinstance(
        filt_df.add_act.dropna().iloc[-1], np.int64
    ), "Filtered active addresses is not a numpy int."  # dtypes
    assert isinstance(
        filt_df.tx_count.dropna().iloc[-1], np.int64
    ), "Filtered transaction count is not a numpy int."  # dtypes


def test_filter_avg_trading_vals(raw_ohlcv_data) -> None:
    """
    Test filter average trading value below threshold.
    """
    # outlier detection
    filt_df = Filter(raw_ohlcv_data).avg_trading_val(thresh_val=10000000, window_size=30)
    # assert statements
    assert filt_df.shape == raw_ohlcv_data.shape, "Filtered dataframe changed shape."  # shape
    assert isinstance(
        filt_df.index, pd.MultiIndex
    ), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(
        filt_df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert (
        filt_df.loc[pd.IndexSlice[:"2011-01-01", "BTC"], "close"].isnull().sum() > 0
    ), "Bitcoin average trading value before 2011 should be below threshold and replaced by NaNs"  # filtered vals
    assert not any(
        (filt_df.describe().loc["max"] == np.inf)
        & (filt_df.describe().loc["min"] == -np.inf)
    ), "Inf values found in the dataframe"  # inf
    assert isinstance(
        filt_df.close.dropna().iloc[-1], np.float64
    ), "Filtered close is not a numpy float."  # dtypes


def test_filter_missing_vals_gaps(raw_ohlcv_data) -> None:
    """
    Test filter missing values gap.
    """
    # outlier detection
    gaps_df = Filter(raw_ohlcv_data).avg_trading_val(thresh_val=10000000, window_size=30)
    filt_df = Filter(gaps_df).missing_vals_gaps(gap_window=30)
    # assert statements
    assert filt_df.shape == raw_ohlcv_data.shape, "Filtered dataframe changed shape."  # shape
    assert isinstance(
        filt_df.index, pd.MultiIndex
    ), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(
        filt_df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert filt_df.loc[pd.IndexSlice[:, "BTC"], "close"].dropna().index[0][
        0
    ] == pd.Timestamp(
        "2014-12-09 00:00:00"
    ), "Start date should be 2014-12-09 00:00:00 for BTC after removing missing values gaps."  # filtered vals
    assert not any(
        (filt_df.describe().loc["max"] == np.inf)
        & (filt_df.describe().loc["min"] == -np.inf)
    ), "Inf values found in the dataframe"  # inf
    assert isinstance(
        filt_df.close.dropna().iloc[-1], np.float64
    ), "Filtered close is not a numpy float."  # dtypes


def test_filter_min_nobs(raw_ohlcv_data) -> None:
    """
    Test filter minimum number of observations.
    """
    # create short series
    start_date = datetime.utcnow() - pd.Timedelta(days=50)
    raw_ohlcv_data.loc[pd.IndexSlice[:start_date, "BTC"], :] = np.nan
    filt_df = Filter(raw_ohlcv_data).min_nobs()
    # assert statements
    assert filt_df.shape != raw_ohlcv_data.shape, "Filtered dataframe changed shape."  # shape
    assert isinstance(
        filt_df.index, pd.MultiIndex
    ), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(
        filt_df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert "BTC" not in list(
        filt_df.index.droplevel(0).unique()
    ), "BTC should be removed from dataframe"  # filt vals
    assert not any(
        (filt_df.describe().loc["max"] == np.inf)
        & (filt_df.describe().loc["min"] == -np.inf)
    ), "Inf values found in the dataframe"  # inf
    assert isinstance(
        filt_df.close.dropna().iloc[-1], np.float64
    ), "Filtered close is not a numpy float."  # dtypes


def test_filter_tickers(raw_ohlcv_data) -> None:
    """
    Test filter tickers from dataframe.
    """
    # tickers list
    tickers_list = ["BTC"]
    filt_df = Filter(raw_ohlcv_data).tickers(tickers_list=tickers_list)
    # assert statements
    assert filt_df.shape != raw_ohlcv_data.shape, "Filtered dataframe changed shape."  # shape
    assert isinstance(
        filt_df.index, pd.MultiIndex
    ), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(
        filt_df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert "BTC" not in list(
        filt_df.index.droplevel(0).unique()
    ), "BTC should be removed from dataframe"  # filt vals
    assert not any(
        (filt_df.describe().loc["max"] == np.inf)
        & (filt_df.describe().loc["min"] == -np.inf)
    ), "Inf values found in the dataframe"  # inf
    assert isinstance(
        filt_df.close.dropna().iloc[-1], np.float64
    ), "Filtered close is not a numpy float."  # dtypes


if __name__ == "__main__":
    pytest.main()
