import logging
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import yfinance as yf
import pandas_datareader.data as web
from pandas_datareader import wb

from cryptodatapy.extract.datarequest import DataRequest
from cryptodatapy.extract.libraries.library import Library
from cryptodatapy.transform.convertparams import ConvertParams
from cryptodatapy.transform.wrangle import WrangleData
from cryptodatapy.util.datacredentials import DataCredentials

# data credentials
data_cred = DataCredentials()


class PandasDataReader(Library):
    """
    Retrieves data from Pandas Data Reader API.
    """
    def __init__(
            self,
            categories: Union[str, List[str]] = ["fx", "rates", "eqty", "cmdty", "credit", "macro"],
            exchanges: Optional[List[str]] = None,
            indexes: Optional[Dict[str, List[str]]] = None,
            assets: Optional[Dict[str, List[str]]] = None,
            markets: Optional[Dict[str, List[str]]] = None,
            market_types: List[str] = ["spot", "future"],
            fields: Optional[Dict[str, List[str]]] = None,
            frequencies: Optional[Dict[str, List[str]]] = ["d", "w", "m", "q", "y"],
            base_url: Optional[str] = None,
            api_key: Optional[str] = None,
            max_obs_per_call: Optional[int] = None,
            rate_limit: Optional[Any] = None,
    ):
        """
        Constructor

        Parameters
        ----------
        categories: list or str, {'crypto', 'fx', 'rates', 'eqty', 'cmdty', 'credit', 'macro', 'alt'}
            List or string of available categories, e.g. ['crypto', 'fx', 'alt'].
        exchanges: list, optional, default None
            List of available exchanges, e.g. ['Binance', 'Coinbase', 'Kraken', 'FTX', ...].
        indexes: dictionary, optional, default None
            Dictionary of available indexes, by cat-indexes key-value pairs,  e.g. [{'eqty': ['SPX', 'N225'],
            'rates': [.... , ...}.
        assets: dictionary, optional, default None
            Dictionary of available assets, by cat-assets key-value pairs,  e.g. {'rates': ['Germany 2Y', 'Japan 10Y',
            ...], 'eqty: ['SPY', 'TLT', ...], ...}.
        markets: dictionary, optional, default None
            Dictionary of available markets, by cat-markets key-value pairs,  e.g. [{'fx': ['EUR/USD', 'USD/JPY', ...],
            'crypto': ['BTC/ETH', 'ETH/USDT', ...}.
        market_types: list
            List of available market types e.g. [spot', 'perpetual_future', 'future', 'option'].
        fields: dictionary, optional, default None
            Dictionary of available fields, by cat-fields key-value pairs,  e.g. {'cmdty': ['date', 'open', 'high',
            'low', 'close', 'volume'], 'macro': ['actual', 'previous', 'expected', 'surprise']}
        frequencies: dictionary
            Dictionary of available frequencies, by cat-frequencies key-value pairs, e.g. {'fx':
            ['d', 'w', 'm', 'q', 'y'], 'rates': ['d', 'w', 'm', 'q', 'y'], 'eqty': ['d', 'w', 'm', 'q', 'y'], ...}.
        base_url: str, optional, default None
            Base url used for GET requests. If not provided, default is set to base_url stored in DataCredentials.
        api_key: dictionary
            Api keys for data source by source-api key key-value pairs, e.g. {'av-daily' :'dcf13983adf7dfa79a0df',
            'fred' : dcf13983adf7dfa79a0df', ...}.
            If not provided, default is set to api_key stored in DataCredentials.
        max_obs_per_call: int, optional, default None
            Maximum number of observations returned per API call. If not provided, default is set to
            api_limit stored in DataCredentials.
        rate_limit: Any, optional, default None
            Number of API calls made and left, by time frequency.
        """
        Library.__init__(
            self,
            categories,
            exchanges,
            indexes,
            assets,
            markets,
            market_types,
            fields,
            frequencies,
            base_url,
            api_key,
            max_obs_per_call,
            rate_limit,
        )
        self.data_req = None
        self.data = pd.DataFrame()

    @staticmethod
    def get_vendors_info():
        """
        Get vendors info.
        """
        print(
            f"See providers page to find available vendors: {data_cred.pdr_vendors_url} "
        )

    @staticmethod
    def get_exchanges_info() -> None:
        """
        Get exchanges info.
        """
        print(
            f"See specific data vendor for available exchanges: {data_cred.pdr_vendors_url}"
        )

    @staticmethod
    def get_indexes_info() -> None:
        """
        Get indexes info.
        """
        print(
            f"See specific data vendor for available indexes: {data_cred.pdr_vendors_url}"
        )

    @staticmethod
    def get_assets_info() -> None:
        """
        Get assets info.
        """
        print(
            f"See specific data vendor for available assets: {data_cred.pdr_vendors_url} "
        )

    @staticmethod
    def get_markets_info() -> None:
        """
        Get markets info.
        """
        print(
            f"See specific data vendor for available markets: {data_cred.pdr_vendors_url}"
        )

    def get_fields_info(self) -> Dict[str, List[str]]:
        """
        Get fields info.

        Returns
        -------
        fields: dictionary
            Dictionary with info on available fields, by category.
        """
        if self.fields is None:
            self.fields = {
                "fx": ["open", "high", "low", "close", "volume", "close_adj", "er"],
                "rates": ["open", "high", "low", "close", "volume", "close_adj", "er"],
                "eqty": ["open", "high", "low", "close", "volume", "close_adj", "er"],
                "cmdty": ["open", "high", "low", "close", "volume", "close_adj", "er"],
                "credit": ["open", "high", "low", "close", "volume", "close_adj", "er"],
                "macro": ["actual"],
            }

        # fields cat
        if self.data_req is not None:
            self.fields = self.fields[self.data_req.cat]

        return self.fields

    def get_frequencies_info(self) -> Dict[str, Union[str, int]]:
        """
        Get frequencies info.

        Returns
        -------
        freq: dictionary
            Dictionary with info on available frequencies.
        """
        if self.frequencies is None:
            self.frequencies = {
                "crypto": ["d", "w", "m", "q", "y"],
                "fx": ["d", "w", "m", "q", "y"],
                "rates": ["d", "w", "m", "q", "y"],
                "cmdty": ["d", "w", "m", "q", "y"],
                "eqty": ["d", "w", "m", "q", "y"],
                "credit": ["d", "w", "m", "q", "y"],
                "macro": ["d", "w", "m", "q", "y"],
            }

        return self.frequencies

    @staticmethod
    def get_rate_limit_info() -> None:
        """
        Get rate limit info.
        """
        print(f"See specific data vendor for rate limits: {data_cred.pdr_vendors_url}")

    def convert_params(self, data_req: DataRequest) -> DataRequest:
        """
        Converts data request parameters to source format.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        data_req: DataRequest
            Parameters of data request in source format.
        """
        # convert params to source format
        if self.data_req is None:
            self.data_req = getattr(ConvertParams(data_req), f"to_{data_req.source}")()

        # check cat
        if self.data_req.cat not in self.categories:
            raise ValueError(
                f"Select a valid category. Valid categories are: {self.categories}."
            )

        # check tickers
        if not self.data_req.source_tickers:
            raise ValueError("No tickers provided for data request.")

        # check freq
        if self.data_req.source_freq not in self.frequencies:
            raise ValueError(
                f"{self.data_req.source_freq} frequency is not available. "
                f"Use the '.frequencies' attribute to check available frequencies."
            )

        # mkt type
        if self.data_req.mkt_type not in self.market_types:
            raise ValueError(
                f"{self.data_req.mkt_type} is not available for {self.data_req.exch}."
            )

        # check fields
        if self.fields is None:
            self.get_fields_info()
        if not any(field in self.fields for field in self.data_req.fields):
            raise ValueError(
                f"{self.data_req.fields} fields are not available for {self.data_req.cat}."
            )

        return self.data_req

    def get_series(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Gets series from python client.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame
            Dataframe with DatetimeIndex and actual values (col) for requested series.

        """
        # convert params to source format
        if self.data_req is None:
            self.convert_params(data_req)

        try:
            # yahoo
            if self.data_req.source == "yahoo":
                # fetch yf data
                self.data = yf.download(self.data_req.source_tickers,
                                        self.data_req.source_start_date,
                                        self.data_req.source_end_date)

            # fama-french
            elif data_req.source == "famafrench":
                for ticker in self.data_req.source_tickers:
                    df1 = web.DataReader(ticker,
                                         self.data_req.source,
                                         self.data_req.source_start_date,
                                         self.data_req.source_end_date)
                    self.data = pd.concat([self.data, df1[0]], axis=1)

            # world bank
            elif data_req.source == "wb":
                for ticker in self.data_req.source_tickers:
                    df1 = wb.download(indicator=ticker,
                                      country=self.data_req.countries,
                                      start=self.data_req.source_start_date,
                                      end=self.data_req.source_end_date)
                    self.data = pd.concat([self.data, df1], axis=1)

            # other pdr data
            else:
                self.data = web.DataReader(self.data_req.source_tickers,
                                           self.data_req.source,
                                           self.data_req.source_start_date,
                                           self.data_req.source_end_date)

        except Exception as e:
            logging.warning(e)
            logging.warning(f"Failed to get data for source tickers: {self.data_req.source_tickers}.")

        else:
            return self.data

    def wrangle_data_resp(self, data_req: DataRequest, data_resp: pd.DataFrame) -> pd.DataFrame:
        """
        Wrangle data response.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.
        data_resp: pd.DataFrame
            Data response from data request.

        Returns
        -------
        df: pd.DataFrame
            Wrangled dataframe with DatetimeIndex (level 0), ticker (level 1), and values for market or macro series
            for selected fields (cols), in tidy format.
        """
        if self.data_req is None:
            self.convert_params(data_req)

        # wrangle data resp
        df = getattr(WrangleData(self.data_req, data_resp), self.data_req.source)()

        return df

    def get_tidy_data(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Gets data from FRED and wrangles the data response into tidy data format.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            Dataframe with DatetimeIndex (level 0), tickers (level 1) and actual values (cols),
            in tidy data format.
        """
        # convert params to source format
        if self.data_req is None:
            self.convert_params(data_req)

        # get series
        data_resp = self.get_series(self.data_req)

        # wrangle data resp
        df = self.wrangle_data_resp(self.data_req, data_resp)

        return df

    def get_data(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get data.

        Parameters
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and values for selected fields (cols),
            in tidy format.
        """
        # convert params to source format
        if self.data_req is None:
            self.convert_params(data_req)

        # get tidy data
        self.data = self.get_tidy_data(self.data_req)

        # check if df empty
        if self.data.empty:
            raise Exception(
                "No data returned. Check data request parameters and try again."
            )

        return self.data.sort_index()
