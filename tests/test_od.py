import numpy as np
import pandas as pd
import pytest

from cryptodatapy.extract.data_vendors.cryptocompare_api import CryptoCompare
from cryptodatapy.extract.datarequest import DataRequest
from cryptodatapy.transform.od import OutlierDetection

# get data for testing
cc = CryptoCompare()
raw_data = cc.get_data(
    DataRequest(tickers=["BTC", "ETH", "ADA"], fields=["close", "add_act", "tx_count"])
)
raw_ohlc = cc.get_data(
    DataRequest(
        tickers=["BTC", "ETH", "ADA"], fields=["open", "high", "low", "close", "volume"]
    )
)


@pytest.fixture
def raw_df():
    return raw_data


@pytest.fixture
def ohlc_df():
    return raw_ohlc


def test_od_atr(ohlc_df) -> None:
    """
    Test outlier detection ATR method.
    """
    # outlier detection
    df = OutlierDetection(ohlc_df).atr()
    outliers_df = df["outliers"]
    # assert statements
    assert (
        outliers_df.shape == ohlc_df.shape
    ), "Outliers dataframe changed shape."  # shape
    assert isinstance(
        outliers_df.index, pd.MultiIndex
    ), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(
        outliers_df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert all(
        outliers_df.loc[:, :"close"].notna().sum()
        / ohlc_df.loc[:, :"close"].notna().sum()
        < 0.05
    ), "Some series have more than 5% of values filtered as outliers in dataframe."  # % outliers
    assert not any(
        (outliers_df.describe().loc["max"] == np.inf)
        & (outliers_df.describe().loc["min"] == -np.inf)
    ), "Inf values found in the dataframe"  # inf


def test_od_iqr(raw_df) -> None:
    """
    Test outlier detection IQR method.
    """
    # outlier detection
    df = OutlierDetection(raw_df).iqr()
    outliers_df = df["outliers"]
    # assert statements
    assert (
        outliers_df.shape == raw_df.shape
    ), "Outliers dataframe changed shape."  # shape
    assert isinstance(
        outliers_df.index, pd.MultiIndex
    ), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(
        outliers_df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert all(
        outliers_df.notna().sum() / raw_df.notna().sum() < 0.05
    ), "Some series have more than 5% of values detected as outliers in dataframe."  # % outliers
    assert not any(
        (outliers_df.describe().loc["max"] == np.inf)
        & (outliers_df.describe().loc["min"] == -np.inf)
    ), "Inf values found in the dataframe"  # inf


def test_od_mad(raw_df) -> None:
    """
    Test outlier detection MAD method.
    """
    # outlier detection
    df = OutlierDetection(raw_df).mad()
    outliers_df = df["outliers"]
    # assert statements
    assert (
        outliers_df.shape == raw_df.shape
    ), "Outliers dataframe changed shape."  # shape
    assert isinstance(
        outliers_df.index, pd.MultiIndex
    ), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(
        outliers_df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert all(
        outliers_df.notna().sum() / raw_df.notna().sum() < 0.2
    ), "Some series have more than 20% of values detected as outliers in dataframe."  # % outliers
    assert not any(
        (outliers_df.describe().loc["max"] == np.inf)
        & (outliers_df.describe().loc["min"] == -np.inf)
    ), "Inf values found in the dataframe"  # inf


def test_od_zscore(raw_df) -> None:
    """
    Test outlier detection z-score method.
    """
    # outlier detection
    df = OutlierDetection(raw_df).z_score(thresh_val=2)
    outliers_df = df["outliers"]
    # assert statements
    assert (
        outliers_df.shape == raw_df.shape
    ), "Outliers dataframe changed shape."  # shape
    assert isinstance(
        outliers_df.index, pd.MultiIndex
    ), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(
        outliers_df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert all(
        outliers_df.notna().sum() / raw_df.notna().sum() < 0.05
    ), "Some series have more than 5% of values detected as outliers in dataframe."  # % outliers
    assert not any(
        (outliers_df.describe().loc["max"] == np.inf)
        & (outliers_df.describe().loc["min"] == -np.inf)
    ), "Inf values found in the dataframe"  # inf


def test_od_ewma(raw_df) -> None:
    """
    Test outlier detection exponential weighted moving average method.
    """
    # outlier detection
    df = OutlierDetection(raw_df).ewma(thresh_val=1.5)
    outliers_df = df["outliers"]
    # assert statements
    assert (
        outliers_df.shape == raw_df.shape
    ), "Outliers dataframe changed shape."  # shape
    assert isinstance(
        outliers_df.index, pd.MultiIndex
    ), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(
        outliers_df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert all(
        outliers_df.notna().sum() / raw_df.notna().sum() < 0.05
    ), "Some series have more than 5% of values detected as outliers in dataframe."  # % outliers
    assert not any(
        (outliers_df.describe().loc["max"] == np.inf)
        & (outliers_df.describe().loc["min"] == -np.inf)
    ), "Inf values found in the dataframe"  # inf


def test_od_seasonal_decomp(raw_df) -> None:
    """
    Test outlier detection seasonal decomposition method.
    """
    # outlier detection
    df = OutlierDetection(raw_df).seasonal_decomp(thresh_val=10)
    outliers_df = df["outliers"]
    # assert statements
    assert (
        outliers_df.shape == raw_df.shape
    ), "Outliers dataframe changed shape."  # shape
    assert isinstance(
        outliers_df.index, pd.MultiIndex
    ), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(
        outliers_df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert all(
        outliers_df.notna().sum() / raw_df.notna().sum() < 0.2
    ), "Some series have more than 20% of values detected as outliers in dataframe."  # % outliers
    assert not any(
        (outliers_df.describe().loc["max"] == np.inf)
        & (outliers_df.describe().loc["min"] == -np.inf)
    ), "Inf values found in the dataframe"  # inf


def test_od_stl(raw_df) -> None:
    """
    Test outlier detection seasonal decomposition method.
    """
    # outlier detection
    df = OutlierDetection(raw_df).stl(thresh_val=10)
    outliers_df = df["outliers"]
    # assert statements
    assert (
        outliers_df.shape == raw_df.shape
    ), "Outliers dataframe changed shape."  # shape
    assert isinstance(
        outliers_df.index, pd.MultiIndex
    ), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(
        outliers_df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert all(
        outliers_df.notna().sum() / raw_df.notna().sum() < 0.25
    ), "Some series have more than 25% of values detected as outliers in dataframe."  # % outliers
    assert not any(
        (outliers_df.describe().loc["max"] == np.inf)
        & (outliers_df.describe().loc["min"] == -np.inf)
    ), "Inf values found in the dataframe"  # inf


def test_od_prophet(raw_df) -> None:
    """
    Test outlier detection prophet method.
    """
    # outlier detection
    df = OutlierDetection(raw_df).prophet()
    outliers_df = df["outliers"]
    # assert statements
    assert (
        outliers_df.shape == raw_df.shape
    ), "Outliers dataframe changed shape."  # shape
    assert isinstance(
        outliers_df.index, pd.MultiIndex
    ), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(
        outliers_df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert all(
        outliers_df.notna().sum() / raw_df.notna().sum() < 0.1
    ), "Some series have more than 10% of values detected as outliers in dataframe."  # % outliers
    assert not any(
        (outliers_df.describe().loc["max"] == np.inf)
        & (outliers_df.describe().loc["min"] == -np.inf)
    ), "Inf values found in the dataframe"  # inf


if __name__ == "__main__":
    pytest.main()
