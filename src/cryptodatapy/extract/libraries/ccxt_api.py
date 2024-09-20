import logging
from time import sleep
from typing import Any, Dict, List, Optional, Union

import ccxt
import pandas as pd

from cryptodatapy.extract.datarequest import DataRequest
from cryptodatapy.extract.libraries.library import Library
from cryptodatapy.transform.convertparams import ConvertParams
from cryptodatapy.transform.wrangle import WrangleData
from cryptodatapy.util.datacredentials import DataCredentials

# data credentials
data_cred = DataCredentials()


class CCXT(Library):
    """
    Retrieves data from CCXT API.
    """
    def __init__(
            self,
            categories: Union[str, List[str]] = "crypto",
            exchanges: Optional[List[str]] = None,
            indexes: Optional[List[str]] = None,
            assets: Optional[Dict[str, List[str]]] = None,
            markets: Optional[Dict[str, List[str]]] = None,
            market_types: List[str] = ["spot", "future", "perpetual_future", "option"],
            fields: Optional[List[str]] = None,
            frequencies: Optional[Dict[str, List[str]]] = None,
            base_url: Optional[str] = None,
            api_key: Optional[str] = None,
            max_obs_per_call: Optional[int] = 10000,
            rate_limit: Optional[Any] = None,
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
        assets: dictionary, optional, default None
            Dictionary of available assets, by exchange-assets key-value pairs, e.g. {'ftx': 'btc', 'eth', ...}.
        markets: dictionary, optional, default None
            Dictionary of available markets as base asset/quote currency pairs, by exchange-markets key-value pairs,
             e.g. {'kraken': btcusdt', 'ethbtc', ...}.
        market_types: list
            List of available market types, e.g. [spot', 'perpetual_future', 'future', 'option'].
        fields: list, optional, default None
            List of available fields, e.g. ['open', 'high', 'low', 'close', 'volume'].
        frequencies: dict, optional, default None
            Dictionary of available frequencies, by exchange-frequencies key-value pairs,
            e.g. {'binance' :  '5min', '10min', '20min', '30min', '1h', '2h', '4h', '8h', 'd', 'w', 'm'}.
        base_url: str, optional, default None
            Base url used for GET requests. If not provided, default is set to base_url stored in DataCredentials.
        api_key: str, optional, default None
            Api key, e.g. 'dcf13983adf7dfa79a0dfa35adf'. If not provided, default is set to
            api_key stored in DataCredentials.
        max_obs_per_call: int, optional, default 10,000
            Maximum number of observations returned per API call. If not provided, default is set to
            api_limit stored in DataCredentials.
        rate_limit: Any, optional, Default None
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

    def get_exchanges_info(self) -> List[str]:
        """
        Get exchanges info.

        Returns
        -------
        exch: list or pd.DataFrame
            List or dataframe with info on supported exchanges.
        """
        self.exchanges = ccxt.exchanges

        return self.exchanges

    def get_indexes_info(self) -> None:
        """
        Get indexes info.
        """
        return None

    def get_assets_info(
            self,
            exch: str = "binance",
            as_list: bool = False
    ) -> Union[pd.DataFrame, List[str]]:
        """
        Get assets info.

        Parameters
        ----------
        exch: str, default 'binance'
            Name of exchange.
        as_list: bool, default False
            Returns assets info for selected exchanges as list.

        Returns
        -------
        assets: list or pd.DataFrame
            Dataframe with info on available assets or list of assets.
        """
        # inst exch
        exchange = getattr(ccxt, exch)()

        # get assets on exchange and create df
        exchange.load_markets()
        self.assets = pd.DataFrame(exchange.currencies).T
        self.assets.index.name = "ticker"

        # as list of assets
        if as_list:
            self.assets = self.assets.index.to_list()

        return self.assets

    def get_markets_info(
            self,
            exch: str = "binance",
            quote_ccy: Optional[str] = None,
            mkt_type: Optional[str] = None,
            as_list: bool = False,
    ) -> Union[pd.DataFrame, List[str]]:
        """
        Get markets info.

        Parameters
        ----------
        exch: str, default 'binance'
            Name of exchange.
        quote_ccy: str, optional, default None
            Quote currency.
        mkt_type: str,  {'spot', 'future', 'perpetual_future', 'option'}, optional, default None
            Market type.
        as_list: bool, default False
            Returns markets info as list for selected exchange.

        Returns
        -------
        markets: list or pd.DataFrame
            List or dataframe with info on available markets, by exchange.
        """
        # inst exch
        exchange = getattr(ccxt, exch)()

        # get assets on exchange
        self.markets = pd.DataFrame(exchange.load_markets()).T
        self.markets.index.name = "ticker"

        # quote ccy
        if quote_ccy is not None:
            self.markets = self.markets[self.markets.quote == quote_ccy.upper()]

        # mkt type
        if mkt_type == "perpetual_future":
            if self.markets[self.markets.type == "swap"].empty:
                self.markets = self.markets[self.markets.type == "future"]
            else:
                self.markets = self.markets[self.markets.type == "swap"]
        elif mkt_type == "spot" or mkt_type == "future" or mkt_type == "option":
            self.markets = self.markets[self.markets.type == mkt_type]

        # dict of assets
        if as_list:
            self.markets = self.markets.index.to_list()

        return self.markets

    def get_fields_info(self) -> List[str]:
        """
        Get fields info.

        Returns
        -------
        fields: list
            List of available fields.
        """
        # list of fields
        self.fields = ["open", "high", "low", "close", "volume", "funding_rate"]

        return self.fields

    def get_frequencies_info(self, exch: str = "binance") -> Dict[str, List[str]]:
        """
        Get frequencies info.

        Parameters
        ----------
        exch: str, default 'binance'
            Name of exchange for which to get available assets.

        Returns
        -------
        freq: dictionary
            Dictionary with info on available frequencies.
        """
        # inst exch and load mkts
        exchange = getattr(ccxt, exch)()
        exchange.load_markets()

        # freq dict
        self.frequencies = exchange.timeframes

        return self.frequencies

    def get_rate_limit_info(self, exch: str = "binance") -> Dict[str, Union[str, int]]:
        """
        Get rate limit info.

        Parameters
        ----------
        exch: str, default 'binance'
            Name of exchange.

        Returns
        -------
        rate_limit: dictionary
            Dictionary with exchange and required minimal delay between HTTP requests that exchange in milliseconds.
        """
        # inst exch
        exchange = getattr(ccxt, exch)()

        self.rate_limit = {
            "exchange rate limit": "delay in milliseconds between two consequent HTTP requests to the same exchange",
            exch: exchange.rateLimit
        }
        return self.rate_limit

    def get_metadata(self) -> None:
        """
        Get CCXT metadata.
        """
        if self.exchanges is None:
            self.exchanges = self.get_exchanges_info()
        if self.market_types is None:
            self.market_types = ["spot", "future", "perpetual_future", "option"]
        if self.assets is None:
            self.assets = self.get_assets_info(as_list=True)
        if self.markets is None:
            self.markets = self.get_markets_info(as_list=True)
        if self.fields is None:
            self.fields = self.get_fields_info()
        if self.frequencies is None:
            self.frequencies = self.get_frequencies_info()
        if self.rate_limit is None:
            self.rate_limit = self.get_rate_limit_info()

    def req_data(self,
                 data_req: DataRequest,
                 data_type: str,
                 ticker: str,
                 start_date: str = None,
                 end_date: str = None,
                 ) -> pd.DataFrame:
        """
        Sends data request to Python client.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.
        data_type: str, {'ohlcv', 'funding_rates'},
            Data type to retrieve.
        ticker: str
            Ticker symbol to request data for.
        start_date: str
            Start date in 'YYYY-MM-DD' format.
        end_date: str
            End date in 'YYYY-MM-DD' format.


        Returns
        -------
        df: pd.DataFrame
            Dataframe with datetime, ticker/identifier, and field/col values.
        """
        # convert data request parameters to CCXT format
        cx_data_req = ConvertParams(data_req).to_ccxt()
        if start_date is None:
            start_date = cx_data_req['start_date']
        if end_date is None:
            end_date = cx_data_req['end_date']

        # data types
        data_types = {'ohlcv': 'fetchOHLCV', 'funding_rates': 'fetchFundingRateHistory'}

        # inst exch
        exch = getattr(ccxt, cx_data_req['exch'])()
        data_resp = []

        try:
            if data_type == 'ohlcv':
                data_resp = getattr(exch, data_types[data_type])(
                    ticker,
                    cx_data_req["freq"],
                    since=start_date,
                    limit=self.max_obs_per_call,
                    params={'until': end_date}
                )
            elif data_type == 'funding_rates':
                data_resp = getattr(exch, data_types[data_type])(
                    ticker,
                    since=start_date,
                    limit=1000,
                    params={'until': end_date}
                )

            return data_resp

        except Exception as e:
            logging.warning(f"Failed to get {data_type} data for {ticker}.")
            logging.warning(e)

            return None

    def fetch_all_ohlcv_hist(self, data_req: DataRequest, ticker: str) -> pd.DataFrame:
        """
        Submits get requests to API until entire OHLCV history has been collected. Only necessary when
        number of observations is larger than the maximum number of observations per call.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.
        ticker: str
            Ticker symbol.

        Returns
        -------
        df: pd.DataFrame
            Dataframe with entire data history retrieved.
        """
        # convert data request parameters to CCXT format and set start date
        cx_data_req = ConvertParams(data_req).to_ccxt()
        start_date = cx_data_req['start_date']
        end_date = cx_data_req['end_date']

        # create empty df
        df = pd.DataFrame()

        # while loop condition
        missing_vals, attempts = True, 0

        # run a while loop until all data collected
        while missing_vals and attempts < cx_data_req['trials']:

            data_resp = self.req_data(data_req=data_req,
                                      data_type='ohlcv',
                                      ticker=ticker,
                                      start_date=start_date,
                                      end_date=end_date)

            if data_resp is None:
                attempts += 1
                sleep(self.get_rate_limit_info(exch=cx_data_req['exch'])[cx_data_req['exch']] / 1000)
                logging.warning(
                    f"Failed to pull data on attempt #{attempts}."
                )
                if attempts == cx_data_req["trials"]:
                    logging.warning(
                        f"Failed to get OHLCV data from {cx_data_req['exch']} for {ticker} after many attempts."
                    )
                    return None

            else:
                # name cols and create df
                header = ["datetime", "open", "high", "low", "close", "volume"]
                data = pd.DataFrame(data_resp, columns=header)
                df = pd.concat([df, data])

                # check if all data has been extracted
                time_diff = cx_data_req["end_date"] - df.datetime.iloc[-1]
                if pd.Timedelta(milliseconds=time_diff) < pd.Timedelta(cx_data_req["freq"]):
                    missing_vals = False
                # missing data, infinite loop
                elif df.datetime.iloc[-1] == df.datetime.iloc[-2]:
                    missing_vals = False
                    logging.warning(f"Missing recent OHLCV data for {ticker}.")
                # reset end date and pause before calling API
                else:
                    # change end date
                    start_date = df.datetime.iloc[-1]

                # rate limit
                sleep(self.get_rate_limit_info(exch=cx_data_req['exch'])[cx_data_req['exch']] / 1000)

        return df

    def fetch_all_funding_hist(self, data_req: DataRequest, ticker: str) -> pd.DataFrame:
        """
        Submits get requests to API until entire funding rate history has been collected. Only necessary when
        number of observations is larger than the maximum number of observations per call.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.
        ticker: str
            Ticker symbol.|

        Returns
        -------
        df: pd.DataFrame
            Dataframe with entire data history retrieved.
        """
        # convert data request parameters to CCXT format and set start date
        cx_data_req = ConvertParams(data_req).to_ccxt()
        start_date = cx_data_req['start_date']
        end_date = cx_data_req['end_date']

        # create empty df
        df = pd.DataFrame()
        # while loop condition
        missing_vals, attempts = True, 0

        # run a while loop until all data collected
        while missing_vals and attempts < cx_data_req['trials']:

            # data req
            data_resp = self.req_data(data_req=data_req,
                                      data_type='funding_rates',
                                      ticker=ticker,
                                      start_date=start_date,
                                      end_date=end_date)

            if data_resp is None:
                attempts += 1
                sleep(self.get_rate_limit_info(exch=cx_data_req['exch'])[cx_data_req['exch']] / 1000)
                logging.warning(
                    f"Failed to pull data on attempt #{attempts}."
                )
                if attempts == cx_data_req["trials"]:
                    logging.warning(
                        f"Failed to get funding_rates from {cx_data_req['exch']} for {ticker} after many attempts."
                    )
                    return None

            else:
                # add to df
                data = pd.DataFrame(data_resp)
                df = pd.concat([df, data])
                # check if all data has been extracted
                time_diff = pd.to_datetime(
                    cx_data_req["end_date"], unit="ms"
                ) - pd.to_datetime(data.datetime.iloc[-1]).tz_localize(None)
                if time_diff < pd.Timedelta("8h"):
                    missing_vals = False
                # missing data, infinite loop
                elif df.datetime.iloc[-1] == df.datetime.iloc[-2]:
                    missing_vals = False
                    logging.warning(f"Missing recent funding rate data for {ticker}.")
                # reset end date and pause before calling API
                else:
                    # change end date
                    start_date = data.timestamp.iloc[-1]

                # rate limit
                sleep(self.get_rate_limit_info(exch=cx_data_req['exch'])[cx_data_req['exch']] / 1000)

        return df

    @staticmethod
    def wrangle_data_resp(data_req: DataRequest, data_resp: pd.DataFrame) -> pd.DataFrame:
        """
        Wrangle data response.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.
        data_resp: pd.DataFrame
            Data response from GET request.

        Returns
        -------
        df: pd.DataFrame
            Wrangled dataframe with DatetimeIndex and values in tidy format.
        """

        return WrangleData(data_req, data_resp).ccxt()

    def fetch_tidy_ohlcv(self, data_req: DataRequest, ticker: str) -> pd.DataFrame:
        """
        Gets entire OHLCV history and wrangles the data response into tidy data format.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.
        ticker: str
            Ticker symbol.

        Returns
        -------
        df: pd.DataFrame
            Dataframe with entire data history retrieved and wrangled into tidy data format.
        """
        # get entire data history
        df = self.fetch_all_ohlcv_hist(data_req, ticker)

        # wrangle df
        if df is not None:
            df = self.wrangle_data_resp(data_req, df)

        return df

    def fetch_tidy_funding_rates(self, data_req: DataRequest, ticker: str) -> pd.DataFrame:
        """
        Gets entire funding rates history and wrangles the data response into tidy data format.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.
        ticker: str
            Ticker symbol.

        Returns
        -------
        df: pd.DataFrame
            Dataframe with entire data history retrieved and wrangled into tidy data format.
        """
        # get entire data history
        df = self.fetch_all_funding_hist(data_req, ticker)

        # wrangle df
        if df is not None:
            df = self.wrangle_data_resp(data_req, df)

        return df

    def check_params(self, data_req) -> None:
        """
        Checks if data request parameters are valid.

        """
        # convert data request parameters to CCXT format
        cx_data_req = ConvertParams(data_req).to_ccxt()

        # inst exch
        exch = getattr(ccxt, cx_data_req['exch'])()

        # check tickers
        tickers = self.get_assets_info(exch=cx_data_req["exch"], as_list=True)
        if not any([ticker.upper() in tickers for ticker in cx_data_req["tickers"]]):
            raise ValueError(
                f"Assets are not available. Use assets attribute to check available assets for {cx_data_req['exch']}")

        # check tickers
        fields = self.get_fields_info()
        if not any([field in fields for field in data_req.fields]):
            raise ValueError(
                f"Fields are not available. Use fields attribute to check available fields."
            )

        # check freq
        if cx_data_req["freq"] not in exch.timeframes:
            raise ValueError(
                f"{data_req.freq} is not available for {cx_data_req['exch']}."
            )

        # check if ohlcv avail on exch
        if any([field in self.fields[:-1] for field in data_req.fields]) and \
                not exch.has["fetchOHLCV"]:
            raise ValueError(
                f"OHLCV data is not available for {cx_data_req['exch']}."
                f" Try another exchange or data request."
            )

        # check if funding avail on exch
        if any([field == 'funding_rate' for field in data_req.fields]) and \
                not exch.has["fetchFundingRateHistory"]:
            raise ValueError(
                f"Funding rates are not available for {cx_data_req['exch']}."
                f" Try another exchange or data request."
            )

        # check if perp future
        if any([field == 'funding_rate' for field in data_req.fields]) and \
                data_req.mkt_type == "spot":
            raise ValueError(
                f"Funding rates are not available for spot markets."
                f" Market type must be perpetual futures."
            )

    def fetch_ohlcv(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Loops list of tickers, retrieves OHLCV data for each ticker in tidy format and stores it in a
        multiindex dataframe.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            Dataframe with DatetimeIndex (level 0), ticker (level 1) and OHLCV values (cols), in tidy data format.
        """
        # convert data request parameters to CCXT format
        cx_data_req = ConvertParams(data_req).to_ccxt()

        # check params
        self.check_params(data_req)

        # empty df to add data
        df = pd.DataFrame()

        # loop through tickers
        for mkt, ticker in zip(cx_data_req['mkts'], data_req.tickers):

            df0 = self.fetch_tidy_ohlcv(data_req, mkt)

            if df0 is not None:
                # add ticker to index
                df0['ticker'] = ticker.upper()
                df0.set_index(['ticker'], append=True, inplace=True)
                # concat df and df1
                df = pd.concat([df, df0])

        return df

    def fetch_funding_rates(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Loops list of tickers, retrieves funding rates data for each ticker in tidy format and stores it in a
        multiindex dataframe.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            Dataframe with DatetimeIndex (level 0), ticker (level 1) and OHLCV values (cols), in tidy data format.
        """
        # convert data request parameters to CCXT format
        cx_data_req = ConvertParams(data_req).to_ccxt()

        # check params
        self.check_params(data_req)

        # empty df to add data
        df = pd.DataFrame()

        # loop through tickers
        for mkt, ticker in zip(cx_data_req['mkts'], data_req.tickers):

            df0 = self.fetch_tidy_funding_rates(data_req, mkt)

            if df0 is not None:
                # add ticker to index
                df0['ticker'] = ticker.upper()
                df0.set_index(['ticker'], append=True, inplace=True)
                # concat df and df1
                df = pd.concat([df, df0])

        return df

    def get_data(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get data specified by data request.

        Parameters
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and values for selected fields (cols).
        """
        # empty df
        df = pd.DataFrame()

        # get OHLCV data
        ohlcv_list = ["open", "high", "low", "close", "volume"]
        if any([field in ohlcv_list for field in data_req.fields]):
            df0 = self.fetch_ohlcv(data_req)
            df = pd.concat([df, df0])

        # get funding rate data
        if any([field == "funding_rate" for field in data_req.fields]):
            df1 = self.fetch_funding_rates(data_req)
            df = pd.concat([df, df1], axis=1)

        # check if df empty
        if df.empty:
            raise Exception(
                "No data returned. Check data request parameters and try again."
            )

        # filter df for desired fields and typecast
        fields = [field for field in data_req.fields if field in df.columns]
        df = df.loc[:, fields]

        return df.sort_index()
