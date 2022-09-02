import pandas as pd
import numpy as np
from datetime import datetime
from datetime import datetime, timedelta
from cryptodatapy.data_requests.datarequest import DataRequest
from cryptodatapy.data_vendors.cryptocompare_api import CryptoCompare
from cryptodatapy.data_cleaning.od import OutlierDetection
from cryptodatapy.data_cleaning.filter import Filter
from cryptodatapy.data_cleaning.impute import Impute
import pytest


# get data for testing
cc = CryptoCompare()
oc_df = cc.get_data(DataRequest(tickers=['BTC', 'ETH', 'ADA'], fields=['close', 'add_act', 'tx_count']))


@pytest.fixture
def oc_filt_df():
    outliers_dict = OutlierDetection(oc_df).stl()
    filt_df = Filter(oc_df).outliers(outliers_dict)
    return filt_df


def test_impute_fwd_fill(oc_filt_df) -> None:
    """
    Test filter average trading value below threshold.
    """
    # impute
    imp_df = Impute(oc_filt_df).fwd_fill()
    # assert statements
    assert imp_df.shape == oc_df.shape, "Filtered dataframe changed shape."  # shape
    assert isinstance(imp_df.index, pd.MultiIndex), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(imp_df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert all(oc_filt_df.isna().sum() > imp_df.isna().sum()), "Some missing values were not imputed."  # imp vals
    assert not any((imp_df.describe().loc['max'] == np.inf) &
                   (imp_df.describe().loc['min'] == -np.inf)), "Inf values found in the dataframe"  # inf
    assert isinstance(imp_df.close.dropna().iloc[-1], np.float64), "Imputed close is not a numpy float."  # dtypes


def test_impute_interpolate(oc_filt_df) -> None:
    """
    Test filter average trading value below threshold.
    """
    # impute
    imp_df = Impute(oc_filt_df).interpolate()
    # assert statements
    assert imp_df.shape == oc_df.shape, "Filtered dataframe changed shape."  # shape
    assert isinstance(imp_df.index, pd.MultiIndex), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(imp_df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert all(oc_filt_df.isna().sum() > imp_df.isna().sum()), "Some missing values were not imputed."  # imp vals
    assert not any((imp_df.describe().loc['max'] == np.inf) &
                   (imp_df.describe().loc['min'] == -np.inf)), "Inf values found in the dataframe"  # inf
    assert isinstance(imp_df.close.dropna().iloc[-1], np.float64), "Imputed close is not a numpy float."  # dtypes


def test_impute_fcst(oc_filt_df) -> None:
    """
    Test filter average trading value below threshold.
    """
    # impute
    imp_df = Impute(oc_filt_df).interpolate()
    # assert statements
    assert imp_df.shape == oc_df.shape, "Filtered dataframe changed shape."  # shape
    assert isinstance(imp_df.index, pd.MultiIndex), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(imp_df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert all(oc_filt_df.isna().sum() > imp_df.isna().sum()), "Some missing values were not imputed."  # imp vals
    assert not any((imp_df.describe().loc['max'] == np.inf) &
                   (imp_df.describe().loc['min'] == -np.inf)), "Inf values found in the dataframe"  # inf
    assert isinstance(imp_df.close.dropna().iloc[-1], np.float64), "Imputed close is not a numpy float."  # dtypes


if __name__ == "__main__":
    pytest.main()
