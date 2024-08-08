from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from cryptodatapy.transform.filter import Filter


@pytest.fixture
def raw_oc_data():
    return pd.read_csv('data/cc_raw_oc_df.csv', index_col=['date', 'ticker'], parse_dates=['date'])


@pytest.fixture
def raw_ohlcv_data():
    return pd.read_csv('data/cc_raw_ohlcv_df.csv', index_col=['date', 'ticker'], parse_dates=['date'])


class TestFilter:
    """
    Test class for OutlierDetection.
    """
    @pytest.fixture(autouse=True)
    def filter_instance(self, raw_ohlcv_data):
        self.filter_instance = Filter(raw_ohlcv_data)

    @pytest.fixture(autouse=True)
    def filter_short_instance(self, raw_ohlcv_data):
        # create short series
        start_date = datetime.utcnow() - pd.Timedelta(days=50)
        raw_ohlcv_data.loc[pd.IndexSlice[:start_date, "BTC"], :] = np.nan
        self.filter_short_instance = Filter(raw_ohlcv_data)

    def test_initialization(self) -> None:
        """
        Test initialization.
        """
        # types
        assert isinstance(self.filter_instance, Filter)
        assert isinstance(self.filter_instance.df, pd.DataFrame)
        assert (self.filter_instance.df.dtypes == 'float64').all()

    def test_filter_avg_trading_val(self) -> None:
        """
        Test filter average trading value below threshold.
        """
        # outlier detection
        filtered_df = self.filter_instance.avg_trading_val(thresh_val=1000000, window_size=30)

        # assert statements
        assert filtered_df.shape == self.filter_instance.raw_df.shape, "Filtered dataframe changed shape."
        assert isinstance(filtered_df.index, pd.MultiIndex), "Dataframe should be multiIndex."
        assert isinstance(filtered_df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."
        assert (filtered_df.loc[pd.IndexSlice[:"2011-01-01", "BTC"], "close"].isnull().sum() > 0), \
            "Bitcoin average trading value before 2011 should be below threshold and replaced by NaNs"
        assert not any((filtered_df.describe().loc["max"] == np.inf) &
                       (filtered_df.describe().loc["min"] == -np.inf)), "Inf values found in the dataframe"
        assert (filtered_df.dtypes == 'float64').all(), "Filtered close is not a numpy float."

    def test_filter_missing_vals_gaps(self) -> None:
        """
        Test filter missing values gap.
        """
        # outlier detection
        gaps_df = self.filter_instance.avg_trading_val(thresh_val=10000000, window_size=30)
        filt_df = Filter(gaps_df).missing_vals_gaps(gap_window=30)

        # assert statements
        assert filt_df.shape == self.filter_instance.raw_df.shape, "Filtered dataframe changed shape."
        assert isinstance(filt_df.index, pd.MultiIndex), "Dataframe should be multiIndex."
        assert isinstance(filt_df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."
        assert filt_df.loc[pd.IndexSlice[:, "BTC"], "close"].dropna().index[0][0] == \
               pd.Timestamp("2014-12-09 00:00:00"), \
            "Start date should be 2014-12-09 00:00:00 for BTC after removing missing values gaps."
        assert not any((filt_df.describe().loc["max"] == np.inf) &
                       (filt_df.describe().loc["min"] == -np.inf)), "Inf values found in the dataframe"
        assert (filt_df.dtypes == 'float64').all(), "Filtered close is not a numpy float."

    def test_filter_min_nobs(self) -> None:
        """
        Test filter minimum number of observations.
        """
        # outlier detection
        filt_df = self.filter_short_instance.min_nobs()

        # assert statements
        assert filt_df.shape != self.filter_instance.raw_df.shape, "Filtered dataframe changed shape."
        assert isinstance(filt_df.index, pd.MultiIndex), "Dataframe should be multiIndex."
        assert isinstance(filt_df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."
        assert "BTC" not in list(filt_df.index.droplevel(0).unique()), "BTC should be removed from dataframe"
        assert not any((filt_df.describe().loc["max"] == np.inf) &
                       (filt_df.describe().loc["min"] == -np.inf)), "Inf values found in the dataframe"
        assert (filt_df.dtypes == 'float64').all(), "Filtered close is not a numpy float."

    def test_filter_tickers(self) -> None:
        """
        Test filter tickers from dataframe.
        """
        # tickers list
        tickers_list = ["BTC"]
        filt_df = self.filter_instance.tickers(tickers_list=tickers_list)

        # assert statements
        assert filt_df.shape != self.filter_instance.raw_df.shape, "Filtered dataframe changed shape."
        assert isinstance(filt_df.index, pd.MultiIndex), "Dataframe should be multiIndex."
        assert isinstance(filt_df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."
        assert "BTC" not in list(filt_df.index.droplevel(0).unique()), "BTC should be removed from dataframe"
        assert not any((filt_df.describe().loc["max"] == np.inf) &
                       (filt_df.describe().loc["min"] == -np.inf)), "Inf values found in the dataframe"
        assert (filt_df.dtypes == 'float64').all(), "Filtered close is not a numpy float."


if __name__ == "__main__":
    pytest.main()
