from __future__ import annotations
from typing import Union
import pandas as pd
import logging

from cryptodatapy.transform.wranglers.base_wrangler import BaseDataWrangler

logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)


class CoinMetricsWrangler(BaseDataWrangler):
    """
    Handles CoinMetrics API specific data wrangling for both time series data
    and metadata (info) responses. Inherits common data processing from
    BaseDataWrangler.
    """

    def __init__(self, data_req, data_resp):
        """
        Initializes the CoinMetricsWrangler and the BaseDataWrangler parent.
        """
        super().__init__(data_req, data_resp)

    # --- Metadata Wrangling ---
    def wrangle_assets_info(self, as_list: bool = False) -> Union[pd.DataFrame, list]:
        """
        Wrangles DefiLlama chains info.

        Parameters
        ----------
        as_list: bool
            If True, returns a list of chain names instead of DataFrame.

        Returns
        -------
        Union[pd.DataFrame, list]
            Wrangled DataFrame or list of chain names.
        """
        df = self.data_resp.to_dataframe().copy()
        df.set_index("asset", inplace=True)
        df = df.sort_index()

        return list(df.index) if as_list else df

    def wrangle_markets_info(self, as_list: bool = False) -> Union[pd.DataFrame, list]:
        """
        Wrangles CoinMetrics markets info.

        Parameters
        ----------
        as_list: bool
            If True, returns a list of market names instead of DataFrame.

        Returns
        -------
        Union[pd.DataFrame, list]
            Wrangled DataFrame or list of market names.
        """
        df = self.data_resp.to_dataframe().copy()
        df.set_index("market", inplace=True)
        df = df.sort_index()

        return list(df.index) if as_list else df

    def wrangle_exchanges_info(self, as_list: bool = False) -> Union[pd.DataFrame, list]:
        """
        Wrangles CoinMetrics exchanges info.

        Parameters
        ----------
        as_list: bool
            If True, returns a list of exchange names instead of DataFrame.

        Returns
        -------
        Union[pd.DataFrame, list]
            Wrangled DataFrame or list of exchange names.
        """
        df = self.data_resp.to_dataframe().copy()
        df.set_index("exchange", inplace=True)
        df = df.sort_index()

        return list(df.index) if as_list else df

    def wrangle_indexes_info(self, as_list: bool = False) -> Union[pd.DataFrame, list]:
        """
        Wrangles CoinMetrics indexes info.

        Parameters
        ----------
        as_list: bool
            If True, returns a list of index names instead of DataFrame.

        Returns
        -------
        Union[pd.DataFrame, list]
            Wrangled DataFrame or list of index names.
        """
        df = self.data_resp.to_dataframe().copy()
        df.set_index("index", inplace=True)
        df = df.sort_index()

        return list(df.index) if as_list else df

    def wrangle_fields_info(self, as_list: bool = False) -> Union[pd.DataFrame, list]:
        """
        Wrangles CoinMetrics fields info.

        Parameters
        ----------
        as_list: bool
            If True, returns a list of field names instead of DataFrame.

        Returns
        -------
        Union[pd.DataFrame, list]
            Wrangled DataFrame or list of field names.
        """
        df = self.data_resp.to_dataframe().copy()
        df.set_index("metric", inplace=True)
        df = df.sort_index()

        return list(df.index) if as_list else df

    def wrangle_available_fields(self, as_list: bool = False) -> Union[pd.DataFrame, list]:
        """
        Wrangles CoinMetrics available fields info.

        Parameters
        ----------
        as_list: bool
            If True, returns a list of available field names instead of DataFrame.

        Returns
        -------
        Union[pd.DataFrame, list]
            Wrangled DataFrame or list of available field names.
        """
        df = self.data_resp.to_dataframe().copy()
        df.set_index("metric", inplace=True)
        df = df.sort_index()

        return list(df.index.unique()) if as_list else df

    def _wrangle_ticker(self) -> None:
        """
        Helper function to wrangle ticker symbols.
        """
        if 'ticker' in self.data_resp.columns and all(self.data_resp.ticker.str.contains('-')):
            self.data_resp['ticker'] = self.data_resp.ticker.str.split(pat='-', expand=True)[1].str.upper()
        elif 'ticker' in self.data_resp.columns and self.data_req.mkt_type == 'perpetual_future':
            self.data_resp['ticker'] = self.data_resp.ticker.str.replace('USDT', '')
        elif 'ticker' in self.data_resp.columns:
            self.data_resp['ticker'] = self.data_resp.ticker.str.upper()

    def wrangle_time_series(self) -> pd.DataFrame:
        """
        Processes CoinMetrics time series data into a tidy, multi-index DataFrame.

        Returns
        -------
        pd.DataFrame
            Consolidated DataFrame of all time series data.
        """
        # convert fields to lib
        self._convert_fields_to_lib(data_source='coinmetrics')
        # convert ticker
        self._wrangle_ticker()
        # set index and sort
        self._set_index_and_sort(index_cols=['date', 'ticker'])
        # resample
        self._resample()
        # reorder columns
        self._reorder_columns()
        # clean
        self._clean_data()
        # type conversion
        self._convert_types()

        return self.data_resp

    def wrangle(self) -> pd.DataFrame:
        """
        Wrangles CoinMetrics time series data into standardized tidy DataFrame.

        Returns
        -------
        pd.DataFrame
            Wrangled time series DataFrame.
        """
        self.wrangle_time_series()
