from __future__ import annotations
from typing import Union, Optional
import pandas as pd
import logging

from cryptodatapy.transform.wranglers.base_wrangler import BaseDataWrangler

logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)


class CCXTWrangler(BaseDataWrangler):
    """
    Handles CCXT API specific data wrangling for both time series data
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
        self.data_resp.load_markets()
        df = pd.DataFrame(self.data_resp.currencies).T
        df.index.name = "ticker"

        return df.index.to_list() if as_list else df

    def wrangle_markets_info(self,
                             quote_ccy: Optional[str] = None,
                             mkt_type: Optional[str] = None,
                             as_list: bool = False) -> Union[pd.DataFrame, list]:
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
        # get assets on exchange
        df = pd.DataFrame(self.data_resp.load_markets()).T
        df.index.name = "ticker"

        # Quote currency filter
        if quote_ccy is not None:
            df = df[df.quote == quote_ccy.upper()]

        # Market type filter
        if mkt_type == "perpetual_future":
            # CCXT uses 'swap' for perpetuals
            if not df[df.type == "swap"].empty:
                df = df[df.type == "swap"]
            else:
                df = df[df.type == "future"]
        elif mkt_type in ["spot", "future", "option"]:
            df = df[df.type == mkt_type]

        return df.index.to_list() if as_list else df

    def _convert_dates(self) -> None:
        """
        Converts 'date' column from milliseconds to datetime.
        """
        if 'date' in self.data_resp.columns:
            self.data_resp['date'] = pd.to_datetime(self.data_resp['date'], unit='ms', utc=True)

    def wrangle_ohlcv(self) -> pd.DataFrame:
        """
        Wrangler for OHLCV data. Pulls from self.data_resp['fetchOHLCV'].
        """
        data = self.data_resp.get('fetchOHLCV', {})
        processed_chunks = []

        for ticker, obs_list in data.items():
            if not obs_list:
                continue
            df = pd.DataFrame(obs_list, columns=['date', 'open', 'high', 'low', 'close', 'volume'])
            df['ticker'] = ticker
            processed_chunks.append(df)

        return pd.concat(processed_chunks, ignore_index=True) if processed_chunks else pd.DataFrame()

    def wrangle_funding_rate(self) -> pd.DataFrame:
        """
        Wrangler for Funding Rate data. Pulls from self.data_resp['fetchFundingRateHistory'].
        """
        data = self.data_resp.get('fetchFundingRateHistory', {})
        processed_chunks = []

        for ticker, obs_list in data.items():
            if not obs_list:
                continue
            ticker_data = [
                {
                    'date': obs.get('timestamp'),
                    'ticker': ticker,
                    'funding_rate': obs.get('fundingRate')
                }
                for obs in obs_list
            ]
            processed_chunks.append(pd.DataFrame(ticker_data))

        return pd.concat(processed_chunks, ignore_index=True) if processed_chunks else pd.DataFrame()

    def wrangle_oi(self) -> pd.DataFrame:
        """
        Wrangler for Open Interest data. Pulls from self.data_resp['fetchOpenInterestHistory'].
        """
        data = self.data_resp.get('fetchOpenInterestHistory', {})
        processed_chunks = []

        for ticker, obs_list in data.items():
            if not obs_list:
                continue
            ticker_data = [
                {
                    'date': obs.get('timestamp'),
                    'ticker': ticker,
                    'oi': obs.get('openInterestAmount'),
                    'oi_value': obs.get('openInterestValue'),
                    'base_volume': obs.get('baseVolume'),
                    'quote_volume': obs.get('quoteVolume')
                }
                for obs in obs_list
            ]
            processed_chunks.append(pd.DataFrame(ticker_data))

        return pd.concat(processed_chunks, ignore_index=True) if processed_chunks else pd.DataFrame()

    def wrangle(self) -> pd.DataFrame:
        """
        Orchestrates the full wrangling pipeline for CCXT data.
        """
        # Guard against non-dict response (e.g. if raw exchange object was passed)
        if not isinstance(self.data_resp, dict):
            logger.error("data_resp must be a dictionary of CCXT responses to use wrangle().")
            return pd.DataFrame()

        # 1. Define the mapping
        method_map = {
            'fetchOHLCV': self.wrangle_ohlcv,
            'fetchFundingRateHistory': self.wrangle_funding_rate,
            'fetchOpenInterestHistory': self.wrangle_oi
        }

        # 2. Collect all available DataFrames
        dfs_to_merge = []

        # We cache the original dict response because individual wranglers access it
        original_resp = self.data_resp

        for key, parse_func in method_map.items():
            if key in original_resp and original_resp[key]:
                try:
                    # Individual wranglers (wrangle_ohlcv, etc) now pull from self.data_resp
                    df_part = parse_func()
                    if not df_part.empty:
                        dfs_to_merge.append(df_part)
                except Exception as e:
                    logger.error(f"Error parsing {key}: {e}")

        # 3. Handle empty case
        if not dfs_to_merge:
            logger.warning("No data found in CCXT response to wrangle.")
            return pd.DataFrame()

        # 4. Merge all dataframes on [date, ticker]
        final_df = dfs_to_merge[0]
        for next_df in dfs_to_merge[1:]:
            final_df = pd.merge(final_df, next_df, on=['date', 'ticker'], how='outer')

        # 5. Overwrite self.data_resp with the merged DataFrame so pipeline works
        self.data_resp = final_df

        # 6. Execute the Standardized Pipeline (Inherited from BaseDataWrangler)
        self._convert_dates()
        self._set_index_and_sort(index_cols=['date', 'ticker'])
        self._resample()
        self._reorder_columns(requested_fields=True)
        self._clean_data()
        self._convert_types()

        return self.data_resp
