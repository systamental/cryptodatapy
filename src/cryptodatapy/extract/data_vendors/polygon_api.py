import logging
from time import sleep
from typing import Any, Dict, List, Optional
import pandas as pd

from polygon import RESTClient


from cryptodatapy.extract.data_vendors.datavendor import DataVendor
from cryptodatapy.extract.datarequest import DataRequest
from cryptodatapy.transform.convertparams import ConvertParams
from cryptodatapy.transform.wrangle import WrangleData
from cryptodatapy.util.datacredentials import DataCredentials

# data credentials
data_cred = DataCredentials()


class Polygon(DataVendor):
    """
    Retrieves data from Tiingo API.
    """

    def __init__(
            self,
            categories: List[str] = ["crypto", "fx", "eqty", 'rates', "bonds", "cmdty", "index"],
            exchanges: Optional[Dict[str, List[str]]] = None,
            indexes: Optional[Dict[str, List[str]]] = None,
            assets: Optional[Dict[str, List[str]]] = None,
            markets: Optional[Dict[str, List[str]]] = None,
            market_types: List[str] = ["spot", "future", "option"],
            fields: Dict[str, List[str]] = None,
            frequencies: List[str] = ["1s", "1min", "1h", "d", "w", "m", "q", "y"],
            base_url: str = data_cred.tiingo_base_url,
            api_key: str = data_cred.polygon_api_key,
            api_endpoints: Optional[Dict[str, str]] = None,
            max_obs_per_call: Optional[int] = None,
            rate_limit: Optional[Any] = None,
    ):
        """
        Constructor

        Parameters
        ----------
        categories: list or str, {'crypto', 'fx', 'rates', 'eqty', 'commodities', 'credit', 'macro', 'alt'}
            List or string of available categories, e.g. ['crypto', 'fx', 'alt'].
        exchanges: dictionary, optional, default None
            Dictionary with available exchanges, by cat-exchanges key-value pairs,  e.g. {'eqty' : ['NYSE', 'DAX', ...],
            'crypto' : ['binance', 'ftx', ....]}.
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
            Dictionary of available fields, by cat-fields key-value pairs,  e.g. {'eqty': ['date', 'open', 'high',
            'low', 'close', 'volume'], 'fx': ['date', 'open', 'high', 'low', 'close']}
        frequencies: list
            List of available frequencies, e.g. ['tick', '1min', '5min', '10min', '20min', '30min', '1h', '2h', '4h',
            '8h', 'd', 'w', 'm']
        base_url: str
            Base url used for GET requests. If not provided, default is set to base_url stored in DataCredentials.
        api_endpoints: dict, optional, default None
            Dictionary with available API endpoints. If not provided, default is set to api_endpoints stored in
            DataCredentials.
        api_key: str
            Api key, e.g. 'dcf13983adf7dfa79a0dfa35adf'. If not provided, default is set to
            api_key stored in DataCredentials.
        max_obs_per_call: int, default None
            Maximum number of observations returned per API call. If not provided, default is set to
            api_limit stored in DataCredentials.
        rate_limit: pd.DataFrame, optional, Default None
            Number of API calls made and left, by time frequency.
        """
        super().__init__(
            categories, exchanges, indexes, assets, markets, market_types,
            fields, frequencies, base_url, api_endpoints, api_key, max_obs_per_call, rate_limit
        )

        if api_key is None:
            raise TypeError("Set your Polygon api key in environment variables as 'POLYGON_API_KEY' or "
                            "add it as an argument when instantiating the class. To get an api key, visit: "
                            "https://polygon.io/dashboard/")

        self.data_req = None
        self.data = pd.DataFrame()
        self.client = RESTClient(self.api_key)

    def get_exchanges_info(self):
        """
        Get exchanges info from Polygon API.

        Returns
        -------
        pd.DataFrame
            DataFrame with exchanges info.
        """
        pass

    def get_indexes_info(self):
        """
        Get indexes info from Polygon API.

        Returns
        -------
        pd.DataFrame
            DataFrame with indexes info.
        """
        pass

    def get_assets_info(self):
        """
        Get assets info from Polygon API.

        Returns
        -------
        pd.DataFrame
            DataFrame with assets info.
        """
        pass

    def get_markets_info(self):
        """
        Get markets info from Polygon API.

        Returns
        -------
        pd.DataFrame
            DataFrame with markets info.
        """
        pass

    def get_fields_info(self, data_type: Optional[str]):
        """
        Get fields info from Polygon API.

        Parameters
        ----------
        data_type: str, optional
            Data type for which to get fields info. If None, returns all fields info.

        Returns
        -------
        pd.DataFrame
            DataFrame with fields info.
        """
        pass

    def get_rate_limit_info(self):
        """
        Get rate limit info from Polygon API.

        Returns
        -------
        pd.DataFrame
            DataFrame with rate limit info.
        """
        pass

    def req_data(self,
                 ticker: str,
                 multiplier: int,
                 timespan: str,
                 from_: str,
                 to: str
                 ) -> List:
        """
        Request data from Polygon API.

        Parameters
        ----------
        ticker: str
            Ticker symbol for the asset.
        multiplier: int
            Multiplier for the aggregation.
        timespan: str
            Timespan for the aggregation, e.g. 'minute', 'hour', 'day'.
        from_: str
            Start date for the data request in 'YYYY-MM-DD' format.
        to: str
            End date for the data request in 'YYYY-MM-DD' format.

        Returns
        -------
        List: List of aggregated data from Polygon API.
        """

        aggs = []
        for a in self.client.list_aggs(
                f"C:{ticker}",
                multiplier,
                timespan,
                from_,
                to,
                adjusted="true",
                sort="asc",
                limit=self.max_obs_per_call if self.max_obs_per_call else 500
        ):
            aggs.append(a)

        if not aggs:
            logging.warning(f"No data found for ticker {ticker} in the specified date range.")

        return aggs

    @staticmethod
    def wrangle_data_resp(data_req: DataRequest, data_resp: Dict[str, Any]) -> pd.DataFrame:
        """
        Wrangle data response.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.
        data_resp: dictionary
            Data response from data request in JSON format.
        Returns
        -------
        df: pd.DataFrame
            Wrangled dataframe with DatetimeIndex and market data for selected fields (cols), in tidy format.
        """
        # wrangle data resp
        df = WrangleData(data_req, data_resp).polygon()

        return df

    def get_tidy_data(self, data_req: DataRequest, ticker) -> pd.DataFrame:
        """
        Submits data request and wrangles the data response into tidy data format.

        Parameters
        ----------
        data_req: DataRequest

        Returns
        -------
        df: pd.DataFrame
            Dataframe with DatetimeIndex and field values (col) wrangled into tidy data format.
        """
        # convert data request parameters to CryptoCompare format
        self.data_req = ConvertParams(data_req).to_polygon()

        # get entire data history
        df = self.req_data(
            ticker=ticker,
            multiplier=1,
            timespan=self.data_req.source_freq,
            from_=self.data_req.source_start_date,
            to=self.data_req.source_end_date,
        )

        # wrangle df
        df = self.wrangle_data_resp(self.data_req, df)

        return df

    def get_all_tickers(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Loops list of tickers, retrieves data in tidy format for each ticker and stores it in a
        multiindex dataframe.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            Dataframe with DatetimeIndex (level 0), ticker (level 1) and values for fields (cols), in tidy data format.
        """
        # convert data request parameters to CryptoCompare format
        self.data_req = ConvertParams(data_req).to_polygon()

        # empty df to add data
        df = pd.DataFrame()

        if self.data_req.cat == 'fx':
            for market, ticker in zip(self.data_req.source_markets, self.data_req.tickers):
                try:
                    df0 = self.get_tidy_data(self.data_req, market)
                except Exception as e:
                    logging.info(f"Failed to get fx data for {market} after many attempts: {e}.")
                else:
                    # add ticker to index
                    df0['ticker'] = ticker.upper()
                    df0.set_index(['ticker'], append=True, inplace=True)
                    # concat df and df1
                    df = pd.concat([df, df0])

                # sleep to avoid hitting API rate limits
                sleep(self.data_req.pause)

        elif self.data_req.cat == 'eqty':
            for ticker in self.data_req.tickers:
                try:
                    df0 = self.get_tidy_data(self.data_req, ticker)
                except Exception as e:
                    logging.info(f"Failed to get eqty data for {ticker} after many attempts: {e}.")
                else:
                    # add ticker to index
                    df0['ticker'] = ticker.upper()
                    df0.set_index(['ticker'], append=True, inplace=True)
                    # concat df and df1
                    df = pd.concat([df, df0])

                # sleep to avoid hitting API rate limits
                sleep(self.data_req.pause)

        else:
            raise NotImplementedError(
                f"Data category '{self.data_req.cat}' is not implemented for Polygon API. "
                "Supported categories are: 'fx', 'eqty'."
            )

        return df.sort_index()

    def check_params(self, data_req: DataRequest) -> None:
        """
        Checks the parameters of the data request before requesting data to reduce API calls
        and improve efficiency.

        """
        self.data_req = ConvertParams(data_req).to_polygon()

        # get metadata
        # self.get_assets_info(as_list=True)
        # self.get_fields_info()

        # check cat
        if self.data_req.cat is None:
            raise ValueError(
                f"Cat cannot be None. Please provide category. Categories include: {self.categories}."
            )

        # # check assets
        # if self.data_req.cat == 'eqty':
        #     if not any([ticker.upper() in self.assets[self.data_req.cat] for ticker in self.data_req.source_tickers]):
        #         raise ValueError(
        #             f"Selected eqty tickers are not available. Use assets attribute to see available eqty tickers."
        #         )
        # elif self.data_req.cat == 'fx':
        #     if not any([ticker in self.assets[self.data_req.cat] for ticker in self.data_req.source_markets]):
        #         raise ValueError(
        #             f"Selected crypto tickers are not available.
        #             Use assets attribute to see available crypto tickers."
        #         )

        # # check fields
        # if not any([field in self.fields[data_req.cat] for field in self.data_req.fields]):
        #     raise ValueError(
        #         f"Selected fields are not available. Use fields attribute to see available fields."
        #     )

    def get_data(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get market data (eqty, fx, crypto).

        Parameters
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and values for market or series data
            for selected fields (cols), in tidy format.
        """
        # check data req params
        self.check_params(data_req)

        # get data
        try:
            df = self.get_all_tickers(data_req)

        except Exception as e:
            logging.warning(e)
            raise Exception(
                "No data returned. Check data request parameters and try again."
            )

        return df
