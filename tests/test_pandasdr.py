import numpy as np
import pandas as pd
import pytest

from cryptodatapy.extract.datarequest import DataRequest
from cryptodatapy.extract.libraries.pandasdr_api import PandasDataReader


@pytest.fixture
def fred_data_resp():
    df = pd.read_csv('data/fred_df.csv', index_col='DATE')
    df.index = pd.to_datetime(df.index)
    return df


@pytest.fixture
def yahoo_data_resp():
    df = pd.read_csv('data/yahoo_df.csv', header=[0, 1], index_col=0)
    return df


class TestPandasDataReader:
    """
    Test PandasDataReader class.
    """
    @pytest.fixture(autouse=True)
    def setup(self):
        self.pdr_instance = PandasDataReader()

    def test_init(self) -> None:
        """
        Test PandasDataReader initialization.
        """
        assert isinstance(self.pdr_instance, PandasDataReader), "Object is not PandasDataReader."

    def test_get_fields_info(self) -> None:
        """
        Test get fields info method.
        """
        self.pdr_instance.get_fields_info()

        assert isinstance(self.pdr_instance.fields, dict), "Fields info is not a dictionary."

    def test_get_frequencies_info(self) -> None:
        """
        Test get frequencies info method.
        """
        self.pdr_instance.get_frequencies_info()

        assert isinstance(self.pdr_instance.frequencies, list), "Frequencies info is not a list."
        assert self.pdr_instance.frequencies == ["d", "w", "m", "q", "y", "av-intraday", "av-daily",
                                                 "av-weekly", "av-monthly", "av-daily-adjusted",
                                                 "av-weekly-adjusted", "av-monthly-adjusted",
                                                 "av-forex-daily"], "Frequencies are incorrect."

    def test_convert_params_error(self) -> None:
        """
        Test convert params method with incorrect parameters.
        """
        data_req = DataRequest(source='yahoo', cat='crypto', tickers=['btc'], fields='close')
        with pytest.raises(ValueError):
            self.pdr_instance.convert_params(data_req)

    def test_get_series(self):
        """
        Test get series method.
        """
        data_req = DataRequest(source='fred', cat='macro', tickers=['US_UE_Rate', 'US_MB'], fields='actual')
        df = self.pdr_instance.get_series(data_req)

        assert not df.empty, "Dataframe was returned empty."
        assert isinstance(df.index, pd.DatetimeIndex), "Index is not DatetimeIndex."
        assert (df.dtypes == 'float64').all(), "Dataframe should have float64 dtype."

    def test_wrangle_data_resp(self, yahoo_data_resp):
        """
        Test wrangle data response method.
        """
        data_req = DataRequest(source='yahoo', tickers=['SPY', 'TLT', 'GLD'],
                               fields=['open', 'high', 'low', 'close', 'volume'], cat='eqty')
        df = self.pdr_instance.wrangle_data_resp(data_req, yahoo_data_resp)

        assert not df.empty, "Dataframe was returned empty."
        assert (df == 0).sum().sum() == 0, "Dataframe has missing values."
        assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."
        assert set(df.index.droplevel(0).unique()) == {'GLD', 'SPY', 'TLT'}, "Columns are missing or incorrect."
        assert set(df.columns) == {'close', 'high', 'low', 'open', 'volume'}, "Missing columns."
        assert (df[['open', 'high', 'low', 'close']].dtypes == 'Float64').all(), \
            "Dataframe should have Float64 dtype."
        assert (df['volume'].dtypes == 'Int64'), "Dataframe should have Int64 dtype."

    def test_get_data(self):
        """
        Test get data method.
        """
        data_req = DataRequest(source='fred', cat='macro', tickers=['US_UE_Rate', 'US_MB'], fields='actual')
        df = self.pdr_instance.get_data(data_req)

        assert not df.empty, "Dataframe was returned empty."
        assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."
        assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."
        assert set(df.index.droplevel(0).unique()) == {'US_MB', 'US_UE_Rate'}, "Columns are missing or incorrect."
        assert list(df.columns) == ['actual'], "Missing columns."
        assert (df.dtypes == 'Float64').all(), "Dataframe should have Float64 dtype."


if __name__ == "__main__":
    pytest.main()
