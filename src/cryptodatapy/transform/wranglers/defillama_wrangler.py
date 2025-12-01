from __future__ import annotations
from typing import Union, Optional, Dict, Any
import pandas as pd
import logging

from cryptodatapy.transform.wranglers.base_wrangler import BaseDataWrangler

logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)


class DefiLlamaWrangler(BaseDataWrangler):
    """
    Handles DefiLlama API specific data wrangling for both time series data
    and metadata (info) responses. Inherits common data processing from
    BaseDataWrangler.
    """

    def __init__(self, data_req, data_resp):
        """
        Initializes the DefiLlamaWrangler and the BaseDataWrangler parent.

        data_resp here is expected to be a Dict/List for info requests or
        a List[Dict[str, Any]] from the adapter for time series data.
        """
        super().__init__(data_req, data_resp)

    # --- Metadata Wrangling ---
    def wrangle_chains_info(self,
                            remove_missing: Optional[list] = None,
                            as_list: bool = False) -> Union[pd.DataFrame, list]:
        """
        Wrangles DefiLlama chains info.

        Parameters
        ----------
        remove_missing: Optional[list]
            List of columns to check for missing values to remove rows.
        as_list: bool
            If True, returns a list of chain names instead of DataFrame.

        Returns
        -------
        Union[pd.DataFrame, list]
            Wrangled DataFrame or list of chain names.
        """
        df = pd.DataFrame(self.data_resp).copy()
        df.set_index("name", inplace=True)
        df = df.sort_values(by='tvl', ascending=False)
        df.rename(columns={'tokenSymbol': 'ticker'}, inplace=True)

        if remove_missing is not None:
            df = df.dropna(subset=remove_missing)

        return list(df.index) if as_list else df

    def wrangle_protocols_info(self,
                               remove_missing: Optional[list] = None,
                               as_list: bool = False) -> Union[pd.DataFrame, list]:
        """
        Wrangles DefiLlama protocols info.

        Parameters
        ----------
        remove_missing: Optional[list]
            List of columns to check for missing values to remove rows.
        as_list: bool
            If True, returns a list of chain names instead of DataFrame.

        Returns
        -------
        Union[pd.DataFrame, list]
            Wrangled DataFrame or list of chain names.
        """
        df = pd.DataFrame(self.data_resp).copy()
        df.set_index("name", inplace=True)
        df = df.sort_values(by='tvl', ascending=False)
        df.rename(columns={'symbol': 'ticker'}, inplace=True)

        if remove_missing is not None:
            df = df.dropna(subset=remove_missing)

        return list(df.index) if as_list else df

    def wrangle_fees_info(self,
                          remove_missing: Optional[list] = None,
                          as_list: bool = False) -> Union[pd.DataFrame, list]:
        """
        Wrangles DefiLlama fees info.

        Parameters
        ----------
        remove_missing: Optional[list]
            List of columns to check for missing values to remove rows.
        as_list: bool
            If True, returns a list of protocol names instead of DataFrame.

        Returns
        -------
        Union[pd.DataFrame, list]
            Wrangled DataFrame or list of protocol names.
        """
        # Specific handling for fees response structure
        df = pd.DataFrame(self.data_resp.get('protocols', []))

        df.set_index("name", inplace=True)
        df = df.sort_values(by='totalAllTime', ascending=False)

        if remove_missing is not None:
            df = df.dropna(subset=remove_missing)

        return list(df.index) if as_list else df

    def wrangle_stablecoins_info(self,
                                 remove_missing: Optional[list] = None,
                                 as_list: bool = False) -> Union[pd.DataFrame, list]:
        """
        Wrangles DefiLlama stablecoins info.

        Parameters
        ----------
        remove_missing: Optional[list]
            List of columns to check for missing values to remove rows.
        as_list: bool
            If True, returns a list of stablecoin names instead of DataFrame.

        Returns
        -------
        Union[pd.DataFrame, list]
            Wrangled DataFrame or list of stablecoin names.
        """
        # Specific handling for stablecoins response structure
        df = pd.DataFrame(self.data_resp.get('peggedAssets', [])).set_index('name')

        if remove_missing is not None:
            df = df.dropna(subset=remove_missing)

        return list(df.index) if as_list else df

    def wrangle_yields_info(self) -> Union[pd.DataFrame, list]:
        """
        Wrangle DefiLlama yields info.

        Returns
        -------
        Union[pd.DataFrame, list]
            Wrangled DataFrame of yields info.
        """
        df = pd.DataFrame(self.data_resp['data']).sort_values(by='tvlUsd', ascending=False).set_index('pool')

        return df

    # --- Time Series Data Wrangling ---

    def _process_single_timeseries(self, raw_resp: Dict[str, Any]) -> pd.DataFrame:
        """
        Converts a single raw API response (dict) into a clean DataFrame
        with appropriate index and metadata columns.

        Parameters
        ----------
        raw_resp : Dict[str, Any]
            Raw response dictionary containing 'metadata' and 'data' keys.

        Returns
        -------
        pd.DataFrame
            Processed DataFrame for the single time series.
        """
        metadata = raw_resp['metadata']
        data = raw_resp['data']

        # check if data is valid
        # list
        if isinstance(data, list) and data:
            df = pd.DataFrame(data).copy()
        # dict
        elif isinstance(data, dict) and data:
            if metadata['type'] == 'protocol' and metadata['field'] == 'tvl_usd':
                df = pd.DataFrame(data['tvl'])
            else:
                df = pd.DataFrame(data['totalDataChart'])
        else:
            logger.warning(f"No time series data found for {metadata['ticker']}/{metadata['field']}.")
            return pd.DataFrame()

        # Convert date field from Unix timestamp (seconds)
        if 'date' in df.columns:
            # Drop the timezone data if it exists
            df['date'] = pd.to_datetime(df['date'], unit='s').dt.normalize()
        else:
            df['date'] = pd.to_datetime(df[0], unit='s').dt.normalize()

        # Add metadata columns
        df['ticker'] = metadata['ticker']
        df['type'] = metadata['type']
        df['category'] = metadata['category']

        # convert value column to standard field name
        old_col_name = df.columns[1]
        df = df.rename(columns={old_col_name: metadata['field']})

        # keep only relevant fields
        df = df.reset_index()[['date', 'ticker', 'type', 'category', metadata['field'],]]

        return df

    def wrangle_time_series(self) -> pd.DataFrame:
        """
        Processes the list of raw time series data responses and consolidates them
        into a single, tidy, multi-index DataFrame.

        Returns
        -------
        pd.DataFrame
            Consolidated DataFrame of all time series data.
        """
        if not isinstance(self.data_resp, list) or not self.data_resp:
            logger.warning("No raw data responses provided for time series wrangling.")
            return pd.DataFrame()

        all_processed_dfs = []

        # process each raw response into a clean DataFrame
        for raw_resp in self.data_resp:
            try:
                df_single = self._process_single_timeseries(raw_resp)
                if not df_single.empty:
                    all_processed_dfs.append(df_single)
            except Exception as e:
                ticker = raw_resp['metadata'].get('ticker', 'Unknown')
                field = raw_resp['metadata'].get('field', 'Unknown')
                logger.error(f"Error processing time series for {ticker}/{field}: {e}")

        if not all_processed_dfs:
            return pd.DataFrame()

        # consolidate all DataFrames into a single one
        # Uses outer join to combine all data frames on their index ('date')
        all_dfs = pd.concat(all_processed_dfs, axis=0)

        df = all_dfs.groupby(['date', 'ticker']).last()

        return df.sort_index()

    def wrangle(self) -> pd.DataFrame:
        """
        Orchestrates the transformation of DefiLlama time series data.

        Returns
        -------
        pd.DataFrame
            Wrangled dataframe into tidy data format.
        """
        # Check for time series data structure (List of Dictionaries from the adapter)
        if isinstance(self.data_resp, list) and all('metadata' in d and 'data' in d for d in self.data_resp):
            self.data_resp = self.wrangle_time_series()

        if isinstance(self.data_resp, pd.DataFrame):
            # generic wrangling steps from BaseDataWrangler class
            self._filter_dates()
            self._resample()
            self._clean_data()
            self._convert_types()

            return self.data_resp

        return pd.DataFrame()
