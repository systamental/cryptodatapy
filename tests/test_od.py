import pandas as pd
import numpy as np
import pytest

from cryptodatapy.transform.od import OutlierDetection


@pytest.fixture
def raw_oc_data():
    return pd.read_csv('data/cc_raw_oc_df.csv', index_col=[0, 1], parse_dates=['date'])


@pytest.fixture
def raw_ohlcv_data():
    return pd.read_csv('data/cc_raw_ohlcv_df.csv', index_col=[0, 1], parse_dates=['date'])


class TestOutlierDetection:
    """
    Test class for OutlierDetection.
    """
    @pytest.fixture(autouse=True)
    def od_oc_instance(self, raw_oc_data):
        self.od_oc_instance = OutlierDetection(raw_oc_data)

    @pytest.fixture(autouse=True)
    def od_ohlcv_instance(self, raw_ohlcv_data):
        self.od_instance = OutlierDetection(raw_ohlcv_data)

    @pytest.fixture(autouse=True)
    def od_excl_instance(self, raw_ohlcv_data):
        self.od_excl_instance = OutlierDetection(raw_ohlcv_data, excl_cols=['volume'])

    def test_initialization(self) -> None:
        """
        Test initialization.
        """
        # types
        assert isinstance(self.od_instance, OutlierDetection)
        assert isinstance(self.od_oc_instance, OutlierDetection)
        assert isinstance(self.od_instance.df, pd.DataFrame)
        assert isinstance(self.od_oc_instance.df, pd.DataFrame)
        assert (self.od_instance.df.dtypes == 'float64').all()
        assert (self.od_oc_instance.df.dtypes == 'float64').all()

    def test_od_atr(self) -> None:
        """
        Test outlier detection ATR method.
        """
        # atr
        self.od_instance.atr()
        self.od_excl_instance.atr()

        # shape
        assert (
            self.od_instance.outliers.shape == self.od_instance.df.shape,
            self.od_excl_instance.outliers.shape == self.od_excl_instance.df.shape
        ), "Outliers dataframe changed shape."
        # multiindex
        assert isinstance(self.od_instance.outliers.index, pd.MultiIndex), "Dataframe should be multiIndex."
        assert isinstance(self.od_excl_instance.outliers.index, pd.MultiIndex), "Dataframe should be multiIndex."
        # datetimeindex
        assert isinstance(self.od_instance.outliers.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."
        assert isinstance(self.od_excl_instance.outliers.index.droplevel(1), pd.DatetimeIndex), \
            "Index is not DatetimeIndex."
        # dtypes
        assert (self.od_instance.outliers.dtypes == 'float64').all(), "Outliers dataframe should be float64."
        assert (self.od_excl_instance.outliers.dtypes == 'float64').all(), "Outliers dataframe should be float64."
        # % outliers
        assert ((self.od_excl_instance.outliers.notna().sum() / self.od_excl_instance.df.notna().sum()) < 0.05).all(), \
            "Some series have more than 5% of values detected as outliers in dataframe."
        # check inf
        assert not any(
            (self.od_instance.outliers.describe().loc["max"] == np.inf)
            & (self.od_instance.outliers.describe().loc["min"] == -np.inf)
        ), "Inf values found in the dataframe."
        assert not any(
            (self.od_excl_instance.outliers.describe().loc["max"] == np.inf)
            & (self.od_excl_instance.outliers.describe().loc["min"] == -np.inf)
        ), "Inf values found in the dataframe."

    def test_od_iqr(self) -> None:
        """
        Test outlier detection ATR method.
        """
        # atr
        self.od_instance.iqr()
        self.od_oc_instance.iqr()
        self.od_excl_instance.iqr()

        # shape
        assert (
            self.od_instance.outliers.shape == self.od_instance.df.shape,
            self.od_oc_instance.outliers.shape == self.od_oc_instance.df.shape,
            self.od_excl_instance.outliers.shape == self.od_excl_instance.df.shape
        ), "Outliers dataframe changed shape."
        # multiindex
        assert isinstance(self.od_instance.outliers.index, pd.MultiIndex), "Dataframe should be multiIndex."
        assert isinstance(self.od_oc_instance.outliers.index, pd.MultiIndex), "Dataframe should be multiIndex."
        assert isinstance(self.od_excl_instance.outliers.index, pd.MultiIndex), "Dataframe should be multiIndex."
        # datetimeindex
        assert isinstance(self.od_instance.outliers.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."
        assert isinstance(self.od_oc_instance.outliers.index.droplevel(1), pd.DatetimeIndex), \
            "Index is not DatetimeIndex."
        assert isinstance(self.od_excl_instance.outliers.index.droplevel(1), pd.DatetimeIndex), \
            "Index is not DatetimeIndex."
        # dtypes
        assert (self.od_instance.outliers.dtypes == 'Float64').all(), "Outliers dataframe should be Float64."
        assert ((self.od_oc_instance.outliers.dtypes == 'Float64') |
                (self.od_oc_instance.outliers.dtypes == 'Int64')).all(), "Outliers dataframe should be Float64."
        assert (self.od_excl_instance.outliers.dtypes == 'Float64').all(), "Outliers dataframe should be Float64."
        # % outliers
        assert ((self.od_excl_instance.outliers.notna().sum() / self.od_excl_instance.df.notna().sum()) < 0.05).all(), \
            "Some series have more than 5% of values detected as outliers in dataframe."
        # check inf
        assert not any(
            (self.od_instance.outliers.describe().loc["max"] == np.inf)
            & (self.od_instance.outliers.describe().loc["min"] == -np.inf)
        ), "Inf values found in the dataframe."
        assert not any(
            (self.od_excl_instance.outliers.describe().loc["max"] == np.inf)
            & (self.od_excl_instance.outliers.describe().loc["min"] == -np.inf)
        ), "Inf values found in the dataframe."

    def test_od_mad(self) -> None:
        """
        Test outlier detection MAD method.
        """
        # thresh
        self.od_instance.thresh_val = 10
        self.od_oc_instance.thresh_val = 10
        self.od_excl_instance.thresh_val = 10

        # mad
        self.od_instance.mad()
        self.od_oc_instance.mad()
        self.od_excl_instance.mad()

        # shape
        assert (
            self.od_instance.outliers.shape == self.od_instance.df.shape,
            self.od_oc_instance.outliers.shape == self.od_oc_instance.df.shape,
            self.od_excl_instance.outliers.shape == self.od_excl_instance.df.shape
        ), "Outliers dataframe changed shape."
        # multiindex
        assert isinstance(self.od_instance.outliers.index, pd.MultiIndex), "Dataframe should be multiIndex."
        assert isinstance(self.od_oc_instance.outliers.index, pd.MultiIndex), "Dataframe should be multiIndex."
        assert isinstance(self.od_excl_instance.outliers.index, pd.MultiIndex), "Dataframe should be multiIndex."
        # datetimeindex
        assert isinstance(self.od_instance.outliers.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."
        assert isinstance(self.od_oc_instance.outliers.index.droplevel(1), pd.DatetimeIndex), \
            "Index is not DatetimeIndex."
        assert isinstance(self.od_excl_instance.outliers.index.droplevel(1), pd.DatetimeIndex), \
            "Index is not DatetimeIndex."
        # dtypes
        assert (self.od_instance.outliers.dtypes == 'Float64').all(), "Outliers dataframe should be Float64."
        assert ((self.od_oc_instance.outliers.dtypes == 'Float64') |
                (self.od_oc_instance.outliers.dtypes == 'Int64')).all(), "Outliers dataframe should be Float64."
        assert (self.od_excl_instance.outliers.dtypes == 'Float64').all(), "Outliers dataframe should be Float64."
        # % outliers
        assert ((self.od_excl_instance.outliers.notna().sum() / self.od_excl_instance.df.notna().sum()) < 0.2).all(), \
            "Some series have more than 20% of values detected as outliers in dataframe."
        # check inf
        assert not any(
            (self.od_instance.outliers.describe().loc["max"] == np.inf)
            & (self.od_instance.outliers.describe().loc["min"] == -np.inf)
        ), "Inf values found in the dataframe."
        assert not any(
            (self.od_oc_instance.outliers.describe().loc["max"] == np.inf)
            & (self.od_oc_instance.outliers.describe().loc["min"] == -np.inf)
        ), "Inf values found in the dataframe."
        assert not any(
            (self.od_excl_instance.outliers.describe().loc["max"] == np.inf)
            & (self.od_excl_instance.outliers.describe().loc["min"] == -np.inf)
        ), "Inf values found in the dataframe."

    def test_od_zscore(self) -> None:
        """
        Test outlier detection z-score method.
        """
        # thresh
        self.od_instance.thresh_val = 2
        self.od_oc_instance.thresh_val = 2
        self.od_excl_instance.thresh_val = 2

        # z-score
        self.od_instance.z_score()
        self.od_oc_instance.z_score()
        self.od_excl_instance.z_score()

        # shape
        assert (
            self.od_instance.outliers.shape == self.od_instance.df.shape,
            self.od_oc_instance.outliers.shape == self.od_oc_instance.df.shape,
            self.od_excl_instance.outliers.shape == self.od_excl_instance.df.shape
        ), "Outliers dataframe changed shape."
        # multiindex
        assert isinstance(self.od_instance.outliers.index, pd.MultiIndex), "Dataframe should be multiIndex."
        assert isinstance(self.od_oc_instance.outliers.index, pd.MultiIndex), "Dataframe should be multiIndex."
        assert isinstance(self.od_excl_instance.outliers.index, pd.MultiIndex), "Dataframe should be multiIndex."
        # datetimeindex
        assert isinstance(self.od_instance.outliers.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."
        assert isinstance(self.od_oc_instance.outliers.index.droplevel(1), pd.DatetimeIndex), \
            "Index is not DatetimeIndex."
        assert isinstance(self.od_excl_instance.outliers.index.droplevel(1), pd.DatetimeIndex), \
            "Index is not DatetimeIndex."
        # dtypes
        assert (self.od_instance.outliers.dtypes == 'Float64').all(), "Outliers dataframe should be Float64."
        assert ((self.od_oc_instance.outliers.dtypes == 'Float64') |
                (self.od_oc_instance.outliers.dtypes == 'Int64')).all(), "Outliers dataframe should be Float64."
        assert (self.od_excl_instance.outliers.dtypes == 'Float64').all(), "Outliers dataframe should be Float64."
        # % outliers
        assert ((self.od_excl_instance.outliers.notna().sum() / self.od_excl_instance.df.notna().sum()) < 0.05).all(), \
            "Some series have more than 5% of values detected as outliers in dataframe."
        # check inf
        assert not any(
            (self.od_instance.outliers.describe().loc["max"] == np.inf)
            & (self.od_instance.outliers.describe().loc["min"] == -np.inf)
        ), "Inf values found in the dataframe."
        assert not any(
            (self.od_oc_instance.outliers.describe().loc["max"] == np.inf)
            & (self.od_oc_instance.outliers.describe().loc["min"] == -np.inf)
        ), "Inf values found in the dataframe."
        assert not any(
            (self.od_excl_instance.outliers.describe().loc["max"] == np.inf)
            & (self.od_excl_instance.outliers.describe().loc["min"] == -np.inf)
        ), "Inf values found in the dataframe."

    def test_od_ewma(self) -> None:
        """
        Test outlier detection exponential weighted moving average method.
        """
        # thresh
        self.od_instance.thresh_val = 1.5
        self.od_oc_instance.thresh_val = 1.5
        self.od_excl_instance.thresh_val = 1.5

        # ewma
        self.od_instance.ewma()
        self.od_oc_instance.ewma()
        self.od_excl_instance.ewma()

        # shape
        assert (
            self.od_instance.outliers.shape == self.od_instance.df.shape,
            self.od_oc_instance.outliers.shape == self.od_oc_instance.df.shape,
            self.od_excl_instance.outliers.shape == self.od_excl_instance.df.shape
        ), "Outliers dataframe changed shape."
        # multiindex
        assert isinstance(self.od_instance.outliers.index, pd.MultiIndex), "Dataframe should be multiIndex."
        assert isinstance(self.od_oc_instance.outliers.index, pd.MultiIndex), "Dataframe should be multiIndex."
        assert isinstance(self.od_excl_instance.outliers.index, pd.MultiIndex), "Dataframe should be multiIndex."
        # datetimeindex
        assert isinstance(self.od_instance.outliers.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."
        assert isinstance(self.od_oc_instance.outliers.index.droplevel(1), pd.DatetimeIndex), \
            "Index is not DatetimeIndex."
        assert isinstance(self.od_excl_instance.outliers.index.droplevel(1), pd.DatetimeIndex), \
            "Index is not DatetimeIndex."
        # dtypes
        assert (self.od_instance.outliers.dtypes == 'Float64').all(), "Outliers dataframe should be Float64."
        assert ((self.od_oc_instance.outliers.dtypes == 'Float64') |
                (self.od_oc_instance.outliers.dtypes == 'Int64')).all(), "Outliers dataframe should be Float64."
        assert (self.od_excl_instance.outliers.dtypes == 'Float64').all(), "Outliers dataframe should be Float64."
        # % outliers
        assert ((self.od_excl_instance.outliers.notna().sum() / self.od_excl_instance.df.notna().sum()) < 0.05).all(), \
            "Some series have more than 5% of values detected as outliers in dataframe."
        # check inf
        assert not any(
            (self.od_instance.outliers.describe().loc["max"] == np.inf)
            & (self.od_instance.outliers.describe().loc["min"] == -np.inf)
        ), "Inf values found in the dataframe."
        assert not any(
            (self.od_oc_instance.outliers.describe().loc["max"] == np.inf)
            & (self.od_oc_instance.outliers.describe().loc["min"] == -np.inf)
        ), "Inf values found in the dataframe."
        assert not any(
            (self.od_excl_instance.outliers.describe().loc["max"] == np.inf)
            & (self.od_excl_instance.outliers.describe().loc["min"] == -np.inf)
        ), "Inf values found in the dataframe."

    def test_od_seasonal_decomp(self) -> None:
        """
        Test outlier detection seasonal decomposition method.
        """
        # thresh
        self.od_oc_instance.thresh_val = 10

        # seasonal decomposition
        self.od_oc_instance.seasonal_decomp()

        # shape
        assert (
            self.od_oc_instance.outliers.shape == self.od_oc_instance.df.shape
        ), "Outliers dataframe changed shape."
        # multiindex
        assert isinstance(self.od_oc_instance.outliers.index, pd.MultiIndex), "Dataframe should be multiIndex."
        # datetimeindex
        assert isinstance(self.od_oc_instance.outliers.index.droplevel(1), pd.DatetimeIndex), \
            "Index is not DatetimeIndex."
        # dtypes
        assert ((self.od_oc_instance.outliers.dtypes == 'Float64') |
                (self.od_oc_instance.outliers.dtypes == 'Int64')).all(), "Outliers dataframe should be Float64."
        # % outliers
        assert ((self.od_oc_instance.outliers.notna().sum() / self.od_oc_instance.df.notna().sum()) < 0.2).all(), \
            "Some series have more than 20% of values detected as outliers in dataframe."
        # check inf
        assert not any(
            (self.od_oc_instance.outliers.describe().loc["max"] == np.inf)
            & (self.od_oc_instance.outliers.describe().loc["min"] == -np.inf)
        ), "Inf values found in the dataframe."

    def test_od_stl(self) -> None:
        """
        Test outlier detection seasonal decomposition method.
        """
        # thresh
        self.od_instance.thresh_val = 10
        self.od_oc_instance.thresh_val = 10
        self.od_excl_instance.thresh_val = 10

        # seasonal decomposition
        self.od_instance.stl()
        self.od_oc_instance.stl()
        self.od_excl_instance.stl()

        # shape
        assert (
            self.od_instance.outliers.shape == self.od_instance.df.shape,
            self.od_oc_instance.outliers.shape == self.od_oc_instance.df.shape,
            self.od_excl_instance.outliers.shape == self.od_excl_instance.df.shape
        ), "Outliers dataframe changed shape."
        # multiindex
        assert isinstance(self.od_instance.outliers.index, pd.MultiIndex), "Dataframe should be multiIndex."
        assert isinstance(self.od_oc_instance.outliers.index, pd.MultiIndex), "Dataframe should be multiIndex."
        assert isinstance(self.od_excl_instance.outliers.index, pd.MultiIndex), "Dataframe should be multiIndex."
        # datetimeindex
        assert isinstance(self.od_instance.outliers.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."
        assert isinstance(self.od_oc_instance.outliers.index.droplevel(1), pd.DatetimeIndex), \
            "Index is not DatetimeIndex."
        assert isinstance(self.od_excl_instance.outliers.index.droplevel(1), pd.DatetimeIndex), \
            "Index is not DatetimeIndex."
        # dtypes
        assert (self.od_instance.outliers.dtypes == 'Float64').all(), "Outliers dataframe should be Float64."
        assert ((self.od_oc_instance.outliers.dtypes == 'Float64') |
                (self.od_oc_instance.outliers.dtypes == 'Int64')).all(), "Outliers dataframe should be Float64."
        assert (self.od_excl_instance.outliers.dtypes == 'Float64').all(), "Outliers dataframe should be Float64."
        # % outliers
        assert ((self.od_excl_instance.outliers.notna().sum() / self.od_excl_instance.df.notna().sum()) < 0.25).all(), \
            "Some series have more than 25% of values detected as outliers in dataframe."
        # check inf
        assert not any(
            (self.od_instance.outliers.describe().loc["max"] == np.inf)
            & (self.od_instance.outliers.describe().loc["min"] == -np.inf)
        ), "Inf values found in the dataframe."
        assert not any(
            (self.od_oc_instance.outliers.describe().loc["max"] == np.inf)
            & (self.od_oc_instance.outliers.describe().loc["min"] == -np.inf)
        ), "Inf values found in the dataframe."
        assert not any(
            (self.od_excl_instance.outliers.describe().loc["max"] == np.inf)
            & (self.od_excl_instance.outliers.describe().loc["min"] == -np.inf)
        ), "Inf values found in the dataframe."

    def test_od_prophet(self) -> None:
        """
        Test outlier detection seasonal decomposition method.
        """
        # prophet
        self.od_oc_instance.prophet()

        # shape
        assert (
            self.od_oc_instance.outliers.shape == self.od_oc_instance.df.shape,
        ), "Outliers dataframe changed shape."
        # multiindex
        assert isinstance(self.od_oc_instance.outliers.index, pd.MultiIndex), "Dataframe should be multiIndex."
        # datetimeindex
        assert isinstance(self.od_oc_instance.outliers.index.droplevel(1), pd.DatetimeIndex), \
            "Index is not DatetimeIndex."
        # dtypes
        assert ((self.od_oc_instance.outliers.dtypes == 'Float64') |
                (self.od_oc_instance.outliers.dtypes == 'Int64')).all(), "Outliers dataframe should be Float64."
        # % outliers
        assert ((self.od_oc_instance.outliers.notna().sum() / self.od_oc_instance.df.notna().sum()) < 0.2).all(), \
            "Some series have more than 20% of values detected as outliers in dataframe."
        # check inf
        assert not any(
            (self.od_oc_instance.outliers.describe().loc["max"] == np.inf)
            & (self.od_oc_instance.outliers.describe().loc["min"] == -np.inf)
        ), "Inf values found in the dataframe."


if __name__ == "__main__":
    pytest.main()
