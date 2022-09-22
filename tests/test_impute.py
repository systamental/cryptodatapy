import numpy as np
import pandas as pd
import pytest

from cryptodatapy.transform.filter import Filter
from cryptodatapy.transform.impute import Impute
from cryptodatapy.transform.od import OutlierDetection

# get data for testing
@pytest.fixture
def raw_oc_data():
    return pd.read_csv('tests/data/cm_raw_oc_data.csv', index_col=[0, 1], parse_dates=['date'])

@pytest.fixture
def oc_filt_df(raw_oc_data):
    outliers_dict = OutlierDetection(raw_oc_data).stl(thresh_val=10)
    filt_df = Filter(raw_oc_data).outliers(outliers_dict)
    return filt_df


def test_impute_fwd_fill(oc_filt_df, raw_oc_data) -> None:
    """
    Test filter average trading value below threshold.
    """
    # impute
    imp_df = Impute(oc_filt_df).fwd_fill()
    # assert statements
    assert imp_df.shape == raw_oc_data.shape, "Filtered dataframe changed shape."  # shape
    assert isinstance(
        imp_df.index, pd.MultiIndex
    ), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(
        imp_df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert all(
        oc_filt_df.isna().sum() > imp_df.isna().sum()
    ), "Some missing values were not imputed."  # imp vals
    assert not any(
        (imp_df.describe().loc["max"] == np.inf)
        & (imp_df.describe().loc["min"] == -np.inf)
    ), "Inf values found in the dataframe"  # inf
    assert isinstance(
        imp_df.close.dropna().iloc[-1], np.float64
    ), "Imputed close is not a numpy float."  # dtypes


def test_impute_interpolate(oc_filt_df, raw_oc_data) -> None:
    """
    Test filter average trading value below threshold.
    """
    # impute
    imp_df = Impute(oc_filt_df).interpolate()
    # assert statements
    assert imp_df.shape == raw_oc_data.shape, "Filtered dataframe changed shape."  # shape
    assert isinstance(
        imp_df.index, pd.MultiIndex
    ), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(
        imp_df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert all(
        oc_filt_df.isna().sum() > imp_df.isna().sum()
    ), "Some missing values were not imputed."  # imp vals
    assert not any(
        (imp_df.describe().loc["max"] == np.inf)
        & (imp_df.describe().loc["min"] == -np.inf)
    ), "Inf values found in the dataframe"  # inf
    assert isinstance(
        imp_df.close.dropna().iloc[-1], np.float64
    ), "Imputed close is not a numpy float."  # dtypes


def test_impute_fcst(oc_filt_df, raw_oc_data) -> None:
    """
    Test filter average trading value below threshold.
    """
    # impute
    imp_df = Impute(oc_filt_df).interpolate()
    # assert statements
    assert imp_df.shape == raw_oc_data.shape, "Filtered dataframe changed shape."  # shape
    assert isinstance(
        imp_df.index, pd.MultiIndex
    ), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(
        imp_df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert all(
        oc_filt_df.isna().sum() > imp_df.isna().sum()
    ), "Some missing values were not imputed."  # imp vals
    assert not any(
        (imp_df.describe().loc["max"] == np.inf)
        & (imp_df.describe().loc["min"] == -np.inf)
    ), "Inf values found in the dataframe"  # inf
    assert isinstance(
        imp_df.close.dropna().iloc[-1], np.float64
    ), "Imputed close is not a numpy float."  # dtypes


if __name__ == "__main__":
    pytest.main()
