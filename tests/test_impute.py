import numpy as np
import pandas as pd
import pytest

from cryptodatapy.transform.impute import Impute
from cryptodatapy.transform.od import OutlierDetection


@pytest.fixture
def raw_oc_data():
    return pd.read_csv('data/cm_raw_oc_df.csv', index_col=[0, 1], parse_dates=['date'])


@pytest.fixture
def filtered_data(raw_oc_data):
    od = OutlierDetection(raw_oc_data, thresh_val=10)
    od.stl()
    return od.filtered_df


@pytest.fixture
def yhat_data(raw_oc_data):
    od = OutlierDetection(raw_oc_data, thresh_val=10)
    od.stl()
    return od.yhat


class TestImpute:
    """
    Test class for Impute.
    """
    @pytest.fixture(autouse=True)
    def imp_instance(self, filtered_data):
        self.imp_instance = Impute(filtered_data)

    def test_initialization(self) -> None:
        """
        Test initialization.
        """
        # types
        assert isinstance(self.imp_instance, Impute)
        assert isinstance(self.imp_instance.filtered_df, pd.DataFrame)
        assert (self.imp_instance.filtered_df.dtypes == 'float64').all()

    def test_impute_fwd_fill(self, raw_oc_data) -> None:
        """
        Test impute missing values with forward fill.
        """
        # impute
        self.imp_instance.fwd_fill()

        # assert statements
        assert self.imp_instance.imputed_df.shape == self.imp_instance.filtered_df.shape, \
            "Filtered dataframe changed shape."
        assert isinstance(self.imp_instance.imputed_df.index, pd.MultiIndex), "Dataframe should be multiIndex."
        assert isinstance(self.imp_instance.imputed_df.index.droplevel(1), pd.DatetimeIndex), \
            "Index is not DatetimeIndex."
        assert all(self.imp_instance.filtered_df.isna().sum() > self.imp_instance.imputed_df.isna().sum()), \
            "Some missing values were not imputed."
        assert not any((self.imp_instance.imputed_df.describe().loc['max'] == np.inf) &
                       (self.imp_instance.imputed_df.describe().loc['min'] == -np.inf)), \
            "Inf values found in the dataframe"
        assert (self.imp_instance.imputed_df.dtypes == 'float64').all(), "Imputed close is not a float."

    def test_impute_interpolate(self, raw_oc_data) -> None:
        """
        Test impute missing values with interpolation.
        """
        # impute
        self.imp_instance.interpolate()

        # assert statements
        assert self.imp_instance.imputed_df.shape == self.imp_instance.filtered_df.shape, \
            "Filtered dataframe changed shape."
        assert isinstance(self.imp_instance.imputed_df.index, pd.MultiIndex), "Dataframe should be multiIndex."
        assert isinstance(self.imp_instance.imputed_df.index.droplevel(1), pd.DatetimeIndex), \
            "Index is not DatetimeIndex."
        assert all(self.imp_instance.filtered_df.isna().sum() > self.imp_instance.imputed_df.isna().sum()), \
            "Some missing values were not imputed."
        assert not any((self.imp_instance.imputed_df.describe().loc['max'] == np.inf) &
                       (self.imp_instance.imputed_df.describe().loc['min'] == -np.inf)), \
            "Inf values found in the dataframe"
        assert (self.imp_instance.imputed_df.dtypes == 'Float64').all(), "Imputed close is not a float."

    def test_impute_fcst(self, yhat_data) -> None:
        """
        Test impute missing values with forecasts.
        """
        # impute
        self.imp_instance.fcst(yhat_data)

        # assert statements
        assert self.imp_instance.imputed_df.shape == self.imp_instance.filtered_df.shape, \
            "Filtered dataframe changed shape."
        assert isinstance(self.imp_instance.imputed_df.index, pd.MultiIndex), "Dataframe should be multiIndex."
        assert isinstance(self.imp_instance.imputed_df.index.droplevel(1), pd.DatetimeIndex), \
            "Index is not DatetimeIndex."
        assert all(self.imp_instance.filtered_df.isna().sum() > self.imp_instance.imputed_df.isna().sum()), \
            "Some missing values were not imputed."
        assert not any((self.imp_instance.imputed_df.describe().loc['max'] == np.inf) &
                       (self.imp_instance.imputed_df.describe().loc['min'] == -np.inf)), \
            "Inf values found in the dataframe"
        assert (self.imp_instance.imputed_df.dtypes == 'Float64').all(), "Imputed close is not a float."


if __name__ == "__main__":
    pytest.main()
