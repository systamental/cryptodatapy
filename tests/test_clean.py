import numpy as np
import pandas as pd
import pytest

from cryptodatapy.transform.clean import CleanData


# get data for testing
@pytest.fixture
def raw_oc_data():
    return pd.read_csv('data/cm_raw_oc_df.csv', index_col=[0, 1], parse_dates=['date'])


@pytest.fixture
def raw_ohlcv_data():
    return pd.read_csv('data/cm_raw_ohlcv_df.csv', index_col=[0, 1], parse_dates=['date'])


class TestClean:
    """
    Test class for Clean.
    """
    @pytest.fixture(autouse=True)
    def clean_instance(self, raw_ohlcv_data):
        self.clean_instance = CleanData(raw_ohlcv_data)

    def test_initialization(self) -> None:
        """
        Test initialization.
        """
        # types
        assert isinstance(self.clean_instance, CleanData)
        assert isinstance(self.clean_instance.df, pd.DataFrame)
        assert (self.clean_instance.df.dtypes == 'float64').all()

    def test_clean_filter_outliers(self) -> None:
        """
        Test clean data - filter outliers.
        """
        # clean data - filter outliers
        self.clean_instance.filter_outliers()

        # assert statements
        assert self.clean_instance.filtered_df.shape == self.clean_instance.df.shape, \
            "Filtered dataframe changed shape."
        assert isinstance(self.clean_instance.filtered_df.index, pd.MultiIndex), "Dataframe should be multiIndex."
        assert isinstance(self.clean_instance.filtered_df.index.droplevel(1), pd.DatetimeIndex), \
            "Index is not DatetimeIndex."
        assert all(self.clean_instance.filtered_df.isnull().sum() >= self.clean_instance.df.isnull().sum()), \
            "No outliers were filtered in dataframe."
        assert not any((self.clean_instance.filtered_df.describe().loc['max'] == np.inf) &
                       (self.clean_instance.filtered_df.describe().loc['min'] == -np.inf)), \
            "Inf values found in the dataframe"
        assert (self.clean_instance.filtered_df.dtypes == 'Float64').all(), "Filtered data is not a float."

    def test_clean_repair_outliers(self) -> None:
        """
        Test clean data - repair outliers.
        """
        # clean data - repair outliers
        self.clean_instance.repair_outliers()

        # assert statements
        assert self.clean_instance.repaired_df.shape == self.clean_instance.df.shape, \
            "Filtered dataframe changed shape."
        assert isinstance(self.clean_instance.repaired_df.index, pd.MultiIndex), "Dataframe should be multiIndex."
        assert isinstance(self.clean_instance.repaired_df.index.droplevel(1), pd.DatetimeIndex), \
            "Index is not DatetimeIndex."
        assert all(self.clean_instance.repaired_df.isnull().sum() == 0), "Missing/filtered values were not repaired."
        assert not any((self.clean_instance.repaired_df.describe().loc['max'] == np.inf) &
                       (self.clean_instance.repaired_df.describe().loc['min'] == -np.inf)), \
            "Inf values found in the dataframe"
        assert (self.clean_instance.repaired_df.dtypes == 'Float64').all(), "Filtered data is not a float."

    def test_clean_filter_avg_trading_val(self) -> None:
        """
        Test clean data - filter average trading value.
        """
        # clean data - filter avg trading val
        self.clean_instance.filter_avg_trading_val()

        # assert statements
        assert self.clean_instance.filtered_df.shape == self.clean_instance.df.shape, \
            "Filtered dataframe changed shape."
        assert isinstance(self.clean_instance.filtered_df.index, pd.MultiIndex), "Dataframe should be multiIndex."
        assert isinstance(self.clean_instance.filtered_df.index.droplevel(1), pd.DatetimeIndex), \
            "Index is not DatetimeIndex."
        assert all(self.clean_instance.filtered_df.isnull().sum() > 0), "No outliers were filtered in dataframe."
        assert not any((self.clean_instance.filtered_df.describe().loc['max'] == np.inf) &
                       (self.clean_instance.filtered_df.describe().loc['min'] == -np.inf)), \
            "Inf values found in the dataframe"
        assert (self.clean_instance.filtered_df.dtypes == 'float64').all(), "Filtered data is not a float."

    def test_clean_filter_missing_vals_gaps(self) -> None:
        """
        Test clean data - filter missing values gaps.
        """
        # clean data - filter missing values gaps
        self.clean_instance.filter_missing_vals_gaps()

        # assert statements
        assert self.clean_instance.filtered_df.shape == self.clean_instance.df.shape, \
            "Filtered dataframe changed shape."
        assert isinstance(self.clean_instance.filtered_df.index, pd.MultiIndex), "Dataframe should be multiIndex."
        assert isinstance(self.clean_instance.filtered_df.index.droplevel(1), pd.DatetimeIndex), \
            "Index is not DatetimeIndex."
        assert self.clean_instance.df.loc[pd.IndexSlice[:, "ADA"], "close"].dropna().index[0][0] == \
               pd.Timestamp('2018-04-17 00:00:00'), \
            "Start date should be '2018-04-17 00:00:00' for ADA after removing missing values gaps."
        assert not any((self.clean_instance.filtered_df.describe().loc['max'] == np.inf) &
                       (self.clean_instance.filtered_df.describe().loc['min'] == -np.inf)), \
            "Inf values found in the dataframe"
        assert (self.clean_instance.filtered_df.dtypes == 'float64').all(), "Filtered close is not a float."

    def test_clean_filter_min_obs(self) -> None:
        """
        Test clean data - filter minimum observations.
        """
        # clean data - filter min obs
        self.clean_instance.filter_min_nobs()

        # assert statements
        assert self.clean_instance.filtered_df.shape == self.clean_instance.df.shape, \
            "Filtered dataframe changed shape."
        assert isinstance(self.clean_instance.filtered_df.index, pd.MultiIndex), "Dataframe should be multiIndex."
        assert isinstance(self.clean_instance.filtered_df.index.droplevel(1), pd.DatetimeIndex), \
            "Index is not DatetimeIndex."
        assert not any((self.clean_instance.filtered_df.describe().loc['max'] == np.inf) &
                       (self.clean_instance.filtered_df.describe().loc['min'] == -np.inf)), \
            "Inf values found in the dataframe"
        assert (self.clean_instance.filtered_df.dtypes == 'float64').all(), "Filtered data is not a float."

    def test_clean_filter_tickers(self) -> None:
        """
        Test clean data - filter tickers.
        """
        # clean data - filter tickers
        self.clean_instance.filter_outliers()
        self.clean_instance.filter_tickers(tickers_list=["BTC"])

        # assert statements
        assert self.clean_instance.filtered_df.shape == self.clean_instance.df.shape, \
            "Filtered dataframe changed shape."
        assert isinstance(self.clean_instance.filtered_df.index, pd.MultiIndex), "Dataframe should be multiIndex."
        assert isinstance(self.clean_instance.filtered_df.index.droplevel(1), pd.DatetimeIndex), \
            "Index is not DatetimeIndex."
        assert "BTC" not in list(self.clean_instance.filtered_df.index.droplevel(0).unique()), \
            "BTC should be removed from dataframe"
        assert not any((self.clean_instance.filtered_df.describe().loc['max'] == np.inf) &
                       (self.clean_instance.filtered_df.describe().loc['min'] == -np.inf)), \
            "Inf values found in the dataframe"
        assert (self.clean_instance.filtered_df.dtypes == 'Float64').all(), "Filtered close is not a float."


if __name__ == "__main__":
    pytest.main()
