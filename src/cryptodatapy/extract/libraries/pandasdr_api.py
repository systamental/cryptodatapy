import logging
from typing import Any, Dict, List, Optional

import pandas as pd
import yfinance as yf
from pandas_datareader.data import DataReader as pdr_fetch
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
            categories=None,
            exchanges: Optional[List[str]] = None,
            indexes: Optional[Dict[str, List[str]]] = None,
            assets: Optional[Dict[str, List[str]]] = None,
            markets: Optional[Dict[str, List[str]]] = None,
            market_types=None,
            fields: Optional[Dict[str, List[str]]] = None,
            frequencies=None,
            base_url: Optional[str] = None,
            api_key=None,
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

        if api_key is None:
            self.api_key = {
                "fred": None,
                "yahoo": None,
                "fama_french": None
            }
        if frequencies is None:
            self.frequencies = {
                "crypto": ["d", "w", "m", "q", "y"],
                "fx": ["d", "w", "m", "q", "y"],
                "rates": ["d", "w", "m", "q", "y"],
                "cmdty": ["d", "w", "m", "q", "y"],
                "eqty": ["d", "w", "m", "q", "y"],
                "credit": ["d", "w", "m", "q", "y"],
                "macro": ["d", "w", "m", "q", "y"],
            }
        if market_types is None:
            self.market_types = ["spot"]
        if categories is None:
            self.categories = ["fx", "rates", "eqty", "cmdty", "credit", "macro"]
        if fields is None:
            self.fields = self.get_fields_info()

    @staticmethod
    def get_vendors_info():
        """
        Get vendors info.
        """
        print(
            f"See providers page to find available vendors: {data_cred.pdr_vendors_url} "
        )

    def get_exchanges_info(self) -> None:
        """
        Get exchanges info.
        """
        print(
            f"See specific data vendor for available exchanges: {data_cred.pdr_vendors_url}"
        )

    def get_indexes_info(self) -> None:
        """
        Get indexes info.
        """
        print(
            f"See specific data vendor for available indexes: {data_cred.pdr_vendors_url}"
        )

    def get_assets_info(self) -> None:
        """
        Get assets info.
        """
        print(
            f"See specific data vendor for available assets: {data_cred.pdr_vendors_url} "
        )

    def get_markets_info(self) -> None:
        """
        Get markets info.
        """
        print(
            f"See specific data vendor for available markets: {data_cred.pdr_vendors_url}"
        )

    @staticmethod
    def get_fields_info(
            data_type: Optional[str] = "market", cat: Optional[str] = None
    ) -> Dict[str, List[str]]:
        """
        Get fields info.

        Parameters
        ----------
        data_type: str, {'market', 'on-chain', 'off-chain'}, default 'market'
            Type of data.
        cat: str, {'crypto', 'eqty', 'fx', 'rates', 'cmdty', 'macro'}, optional, default None
            Asset class or time series category.

        Returns
        -------
        fields: dictionary
            Dictionary with info on available fields, by category.
        """
        if data_type == "on-chain":
            raise ValueError(
                "Pandas Data Reader is a market data aggregator of market and off-chain data."
            )

        # list of fields
        market_fields_list = ["open", "high", "low", "close", "volume", "close_adj", "er"]
        macro_fields_list = ["actual"]

        # fields dict
        fields = {
            "fx": market_fields_list,
            "rates": market_fields_list,
            "eqty": market_fields_list,
            "cmdty": market_fields_list,
            "credit": market_fields_list,
            "macro": macro_fields_list,
        }

        # fields obj
        if cat is not None:
            fields = fields[cat]

        return fields

    def get_rate_limit_info(self) -> None:
        """
        Get rate limit info.
        """
        print(f"See specific data vendor for rate limits: {data_cred.pdr_vendors_url}")

    @staticmethod
    def get_series(data_req: DataRequest) -> pd.DataFrame:
        """
        Gets series from DBnomics python client.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame
            Dataframe with DatetimeIndex and actual values (col) for requested series.

        """
        # convert data request parameters to source format
        conv_data_req = getattr(ConvertParams(data_req), f"to_{data_req.source}")()

        try:
            # fetch yahoo data
            if data_req.source == "yahoo":
                # fetch yf data
                df = yf.download(conv_data_req["tickers"],
                                 conv_data_req["start_date"],
                                 conv_data_req["end_date"])

            # fetch fama-french data
            elif data_req.source == "famafrench":
                df = pd.DataFrame()
                for ticker in conv_data_req["tickers"]:
                    df1 = pdr_fetch(ticker,
                                    data_req.source,
                                    conv_data_req["start_date"],
                                    conv_data_req["end_date"])
                    df = pd.concat([df, df1[0]], axis=1)

            # featch wb data
            elif data_req.source == "wb":
                df = pd.DataFrame()
                for ticker in conv_data_req["tickers"]:
                    df1 = wb.download(indicator=ticker,
                                      country=conv_data_req['ctys'],
                                      start=conv_data_req["start_date"],
                                      end=conv_data_req["end_date"])
                    df = pd.concat([df, df1], axis=1)

            # fetch pdr data
            else:
                df = pdr_fetch(conv_data_req["tickers"],
                               data_req.source,
                               conv_data_req["start_date"],
                               conv_data_req["end_date"])

        except Exception as e:
            logging.warning(e)
            logging.warning(f"Failed to get data for: {conv_data_req['tickers']}.")

        else:

            return df

    @staticmethod
    def wrangle_data_resp(
            data_req: DataRequest, data_resp: pd.DataFrame) -> pd.DataFrame:
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
        # wrangle data resp
        df = getattr(WrangleData(data_req, data_resp), data_req.source)()

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
        # change to get series
        df = self.get_series(data_req)
        # wrangle data resp
        df = self.wrangle_data_resp(data_req, df)

        return df

    def check_params(self, data_req: DataRequest) -> None:
        """
        Checks the data request parameters before requesting data to reduce API calls
        and improve efficiency.

        """
        # check data source
        if data_req.source not in ['fred', 'yahoo', 'famafrench', 'wb']:
            raise ValueError(
                "Select a Pandas-datareader supported data source for the data request."
            )

        # check cat
        if data_req.cat not in self.categories:
            raise ValueError(
                f"Select a valid category. Valid categories are: {self.categories}."
            )
        # check freq
        if data_req.freq not in self.frequencies[data_req.cat]:
            raise ValueError(
                f"Invalid data frequency. Valid data frequencies are: {self.frequencies}."
            )
        # check fields
        if not any(field in self.fields[data_req.cat] for field in data_req.fields):
            raise ValueError(f"Invalid fields. Valid data fields are: {self.fields}.")

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
        # check params
        self.check_params(data_req)

        # get tidy data
        df = self.get_tidy_data(data_req)

        # check if df empty
        if df.empty:
            raise Exception(
                "No data returned. Check data request parameters and try again."
            )

        return df.sort_index()