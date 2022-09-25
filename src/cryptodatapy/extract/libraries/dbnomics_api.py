from typing import Dict, List, Optional

import dbnomics
import pandas as pd

from cryptodatapy.extract.datarequest import DataRequest
from cryptodatapy.extract.libraries.library import Library
from cryptodatapy.transform.convertparams import ConvertParams
from cryptodatapy.transform.wrangle import WrangleData
from cryptodatapy.util.datacredentials import DataCredentials

# data credentials
data_cred = DataCredentials()


class DBnomics(Library):
    """
    Retrieves data from DBnomics API.
    """

    def __init__(
            self,
            categories=None,
            exchanges: Optional[List[str]] = None,
            indexes: Optional[List[str]] = None,
            assets: Optional[List[str]] = None,
            markets: Optional[List[str]] = None,
            market_types: Optional[List[str]] = None,
            fields: Optional[Dict[str, List[str]]] = None,
            frequencies=None,
            base_url: Optional[str] = None,
            api_key: Optional[str] = None,
            max_obs_per_call: Optional[int] = None,
            rate_limit: Optional[str] = None,
    ):
        """
        Constructor

        Parameters
        ----------
        categories: list or str, {'crypto', 'fx', 'rates', 'eqty', 'commodities', 'credit', 'macro', 'alt'}
            List or string of available categories, e.g. ['crypto', 'fx', 'alt'].
        exchanges: list, optional, default None
            List of available exchanges, e.g. ['Binance', 'Coinbase', 'Kraken', 'FTX', ...].
        indexes: list, optional, default None
            List of available indexes, e.g. ['mvda', 'bvin'].
        assets: list, optional, default None
            List of available assets, e.g. ['btc', 'eth'].
        markets: list, optional, default None
            List of available markets as base asset/quote currency pairs, e.g. ['btcusdt', 'ethbtc'].
        market_types: list, optional, default None
            List of available market types, e.g. [spot', 'perpetual_future', 'future', 'option'].
        fields: dictionary, optional, default None
            Dictionary of available fields, by category-fields key-value pairs,
             e.g. {'macro': 'actual', 'expected', 'suprise'}.
        frequencies: dictionary
            Dictionary of available frequencies, by category-freq key-value pairs, e.g. ['tick', '1min', '5min',
            '10min', '20min', '30min', '1h', '2h', '4h', '8h', 'd', 'w', 'm']
        base_url: str, optional, default None
            Base url used in GET requests. If not provided, default is set to base url stored in DataCredentials.
        api_key: str, optional, default None
            Api key. If not provided, default is set to cryptocompare_api_key stored in DataCredentials.
        max_obs_per_call: int, optional, default None
            Maximum number of observations returns per API call.
        rate_limit: str, optional, default None
            Number of API calls made and left by frequency.
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

        if frequencies is None:
            self.frequencies = {"macro": ["d", "w", "m", "q", "y"]}
        if categories is None:
            self.categories = ["macro"]
        if fields is None:
            self.fields = self.get_fields_info()

    @staticmethod
    def get_vendors_info():
        """
        Gets available vendors info.
        """
        print(
            f"See providers page to find available vendors: {data_cred.dbnomics_vendors_url} "
        )

    def get_assets_info(self) -> None:
        """
        Gets available assets info.
        """
        print(f"See search page for available assets: {data_cred.dbnomics_search_url} ")

    def get_indexes_info(self) -> None:
        """
        Gets available indexes info.
        """
        return None

    def get_markets_info(self) -> None:
        """
        Gets market pairs info.
        """
        return None

    def get_fields_info(self, cat: Optional[str] = None) -> Dict[str, List[str]]:
        """
        Gets fields info.

        Parameters
        ----------
        cat: str, {'crypto', 'eqty', 'fx', 'rates', 'cmdty', 'macro', 'alt'}, default None
            Asset class or time series category.

        Returns
        -------
        fields: dictionary
            Dictionary with info on available fields, by category.
        """

        # list of fields
        macro_fields_list = ["actual"]

        # fields dict
        fields = {
            "macro": macro_fields_list,
        }

        # fields obj
        if cat is not None:
            fields = fields[cat]

        return fields

    def get_exchanges_info(self) -> None:
        """
        Gets exchanges info.
        """
        return None

    def get_rate_limit_info(self) -> None:
        """
        Gets rate limit info.
        """
        return None

    @staticmethod
    def get_series(ticker: str) -> pd.DataFrame:
        """
        Gets series from DBnomics python client.

        Parameters
        ----------
        ticker: str
            Ticker symbol/identifier of time series.

        Returns
        -------
        df: pd.DataFrame
            Dataframe with DatetimeIndex and actual values (col) for requested series.

        """
        return dbnomics.fetch_series(ticker)

    @staticmethod
    def wrangle_data_resp(data_req: DataRequest, data_resp: pd.DataFrame) -> pd.DataFrame:
        """
        Wrangle data response.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.
        data_resp: pd.DataFrame
            Data response from client.

        Returns
        -------
        df: pd.DataFrame
            Wrangled dataframe with DatetimeIndex (level 0), ticker (level 1), and values for macro time series
            for selected fields (cols), in tidy format.
        """
        # wrangle data resp
        df = WrangleData(data_req, data_resp).dbnomics()

        return df

    def get_tidy_data(self, data_req: DataRequest, ticker: str) -> pd.DataFrame:
        """
        Submits data request to Python client and wrangles the data response into tidy data format.

        Parameters
        ----------
        data_req: DataRequest
            Data request parameters in CryptoDataPy format.
        ticker: str
            Tickery symbol/identifier for time series.

        Returns
        -------
        df: pd.DataFrame
            Dataframe with DatetimeIndex and field values (col) wrangled into tidy data format.
        """
        # get entire data history
        df = self.get_series(ticker)
        # wrangle df
        df = self.wrangle_data_resp(data_req, df)

        return df

    def check_params(self, data_req: DataRequest) -> None:
        """
        Checks the parameters of the data request before requesting data to reduce API calls
        and improve efficiency.

        """
        # check cat
        if data_req.cat not in self.categories:
            raise ValueError(
                f"Invalid category. Valid categories are: {self.categories}."
            )

        # check freq
        if data_req.freq not in self.frequencies[data_req.cat]:
            raise ValueError(
                f"Invalid data frequency. Valid data frequencies are: {self.frequencies}."
            )

        # check fields
        if not any([field in self.fields[data_req.cat] for field in data_req.fields]):
            raise ValueError(
                "Invalid fields. See fields property for available fields."
            )

    def get_data(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get data macro data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1) and values macro or off-chain fields (cols).
        """
        # convert data req params to DBnomics format
        db_data_req = ConvertParams(data_req).to_dbnomics()

        # check params
        self.check_params(data_req)

        # emtpy df
        df = pd.DataFrame()

        # get data from dbnomics
        for db_ticker, dr_ticker in zip(db_data_req["tickers"], data_req.tickers):

            # loop through tickers
            df0 = self.get_tidy_data(data_req, db_ticker)

            # add ticker to index
            if data_req.source_tickers is None:
                df0["ticker"] = dr_ticker
            else:
                df0["ticker"] = db_ticker
            df0.set_index(["ticker"], append=True, inplace=True)
            # stack ticker dfs
            df = pd.concat([df, df0])

        # check if df empty
        if df.empty:
            raise Exception(
                "No data returned. Check data request parameters and try again."
            )

        # filter df for desired fields and typecast
        fields = [field for field in data_req.fields if field in df.columns]
        df = df.loc[:, fields]

        return df.sort_index()
