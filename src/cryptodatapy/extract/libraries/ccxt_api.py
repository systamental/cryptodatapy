import logging
from time import sleep
from typing import Any, Dict, List, Optional, Union

import ccxt
import numpy as np
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

        self.exchanges = self.get_exchanges_info(as_list=True)
        self.assets = self.get_assets_info(as_dict=True)
        self.markets = self.get_markets_info(as_dict=True)
        self.fields = self.get_fields_info()
        self.frequencies = self.get_frequencies_info()
        self.rate_limit = self.get_rate_limit_info()

    @staticmethod
    def get_exchanges_info(
        exch: Optional[str] = None, as_list: bool = False
    ) -> Union[List[str], pd.DataFrame]:
        """
        Get exchanges info.

        Parameters
        ----------
        exch: str, default None
            Name of exchange.
        as_list: bool, default False
            Returns exchanges info as list.

        Returns
        -------
        exch: list or pd.DataFrame
            List or dataframe with info on supported exchanges.
        """
        # list
        if as_list:
            exchanges = ccxt.exchanges
        else:
            if exch is not None:
                exchanges = [exch]
            else:
                exchanges = ccxt.exchanges
                print(
                    "Getting info on all supported exchanges can take a few minutes. Change exchange parameter"
                    " for info on a specific exchange."
                )

            # exch df
            exch_df = pd.DataFrame(
                index=exchanges,
                columns=[
                    "id",
                    "name",
                    "countries",
                    "urls",
                    "version",
                    "api",
                    "has",
                    "timeframes",
                    "timeout",
                    "rateLimit",
                    "userAgent",
                    "verbose",
                    "markets",
                    "symbols",
                    "currencies",
                    "markets_by_id",
                    "currencies_by_id",
                    "api_key",
                    "secret",
                    "uid",
                    "options",
                ],
            )
            # extract exch info
            for row in exch_df.iterrows():
                try:
                    exchange = getattr(ccxt, row[0])()
                    exchange.load_markets()
                except Exception:
                    exch_df.loc[row[0], :] = np.nan
                else:
                    for col in exch_df.columns:
                        try:
                            exch_df.loc[row[0], col] = str(getattr(exchange, str(col)))
                        except Exception:
                            exch_df.loc[row[0], col] = np.nan
            # set index name
            exch_df.index.name = "exchange"
            exchanges = exch_df

        return exchanges

    @staticmethod
    def get_indexes_info():
        """
        Get indexes info.
        """
        return None

    def get_assets_info(
        self, exch: str = "binance", as_dict: bool = False
    ) -> Union[pd.DataFrame, Dict[str, List[str]]]:
        """
        Get assets info.

        Parameters
        ----------
        exch: str, default 'binance'
            Name of exchange.
        as_dict: bool, default False
            Returns assets info as dictionary, with exchange-assets key-value pairs.

        Returns
        -------
        assets: dictionary or pd.DataFrame
            Dictionary or dataframe with info on available assets.
        """
        # exch
        if exch not in self.exchanges:
            raise ValueError(
                f"{exch} is not a supported exchange. Try another exchange."
            )
        else:
            exchange = getattr(ccxt, exch)()

        # get assets on exchange
        exchange.load_markets()
        assets = pd.DataFrame(exchange.currencies).T
        assets.index.name = "ticker"

        # dict of assets
        if as_dict:
            assets_dict = {exch: assets.index.to_list()}
            assets = assets_dict

        return assets

    def get_markets_info(
        self,
        exch: str = "binance",
        quote_ccy: Optional[str] = None,
        mkt_type: Optional[str] = None,
        as_dict: bool = False,
    ) -> Union[Dict[str, List[str]], pd.DataFrame]:
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
        as_dict: bool, default False
            Returns markets info as dict with exchange-markets key-values pair.

        Returns
        -------
        markets: dictionary or pd.DataFrame
            Dictionary or dataframe with info on available markets, by exchange.
        """
        if exch not in self.exchanges:
            raise ValueError(
                f"{exch} is not a supported exchange. Try another exchange."
            )
        else:
            exchange = getattr(ccxt, exch)()

        # get assets on exchange
        markets = pd.DataFrame(exchange.load_markets()).T
        markets.index.name = "ticker"

        # quote ccy
        if quote_ccy is not None:
            markets = markets[markets.quote == quote_ccy.upper()]

        # mkt type
        if mkt_type == "perpetual_future":
            if markets[markets.type == "swap"].empty:
                markets = markets[markets.type == "future"]
            else:
                markets = markets[markets.type == "swap"]
        elif mkt_type == "spot" or mkt_type == "future" or mkt_type == "option":
            markets = markets[markets.type == mkt_type]

        # dict of assets
        if as_dict:
            mkts_dict = {exch: markets.index.to_list()}
            markets = mkts_dict

        return markets

    @staticmethod
    def get_fields_info() -> List[str]:
        """
        Get fields info.

        Parameters
        ----------

        Returns
        -------
        fields: list
            List of available fields.
        """
        # list of fields
        fields = ["open", "high", "low", "close", "volume", "funding_rate"]

        return fields

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
        # exch
        if exch not in self.exchanges:
            raise ValueError(
                f"{exch} is not a supported exchange. Try another exchange."
            )
        else:
            exchange = getattr(ccxt, exch)()
        exchange.load_markets()

        # freq dict
        freq = {exch: exchange.timeframes}

        return freq

    def get_rate_limit_info(self, exch: str = "binance") -> Dict[str, int]:
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
        # exch
        if exch not in self.exchanges:
            raise ValueError(
                f"{exch} is not a supported exchange. Try another exchange."
            )
        else:
            exchange = getattr(ccxt, exch)()

        return {
            "exchange rate limit": "delay in milliseconds between two consequent HTTP requests to the same exchange",
            exch: exchange.rateLimit,
        }

    def get_ohlcv(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get OHLCV data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and OHLCV values (cols).
        """
        # convert data request parameters to CCXT format
        cx_data_req = ConvertParams(data_req, data_source="ccxt").convert_to_source()

        # check exchange
        if cx_data_req["exch"] not in self.exchanges:
            raise ValueError(
                f"{cx_data_req['exch']} is not a supported exchange. Try another exchange."
            )
        else:
            exch = getattr(ccxt, cx_data_req["exch"])()
        # check if ohlcv avail on exch
        if not exch.has["fetchOHLCV"]:
            raise ValueError(
                f"OHLCV data is not available for {cx_data_req['exch']}."
                f" Try another exchange or data request."
            )
        # check freq
        if cx_data_req["freq"] not in exch.timeframes:
            raise ValueError(
                f"{data_req.freq} is not available for {cx_data_req['exch']}."
            )

        # check tickers
        tickers = self.get_assets_info(exch=cx_data_req["exch"], as_dict=True)[
            cx_data_req["exch"]
        ]
        if not any(ticker.upper() in tickers for ticker in cx_data_req["tickers"]):
            raise ValueError(
                f"Assets are not available. Check available assets for {cx_data_req['exch']}"
                f" with get_assets_info method."
            )

        # get OHLCV
        df = pd.DataFrame()  # empty df to store data
        # loop through tickers
        for cx_ticker, dr_ticker in zip(cx_data_req["mkts"], data_req.tickers):

            # start date
            start_date = cx_data_req["start_date"]
            # create empty ohlcv df
            df0 = pd.DataFrame()
            # set number of attempts and bool for while loop
            attempts = 0

            # run a while loop in case the attempt fails
            while attempts < cx_data_req["trials"]:

                try:
                    data = exch.fetch_ohlcv(
                        cx_ticker,
                        cx_data_req["freq"],
                        since=start_date,
                        limit=self.max_obs_per_call,
                    )
                    assert data != []

                except AssertionError as e:
                    logging.warning(e)
                    attempts += 1
                    sleep(exch.rateLimit / 1000)
                    logging.warning(
                        f"Failed to pull data for {cx_ticker} after attempt #{str(attempts)}."
                    )
                    if attempts == cx_data_req["trials"]:
                        logging.warning(
                            f"Failed to pull data from {data_req.exch} for {cx_ticker} after many attempts."
                        )
                        break

                else:
                    # name cols and create df
                    header = ["datetime", "open", "high", "low", "close", "volume"]
                    data = pd.DataFrame(data, columns=header)
                    df0 = pd.concat([df0, data])
                    # check if all data has been extracted
                    time_diff = cx_data_req["end_date"] - df0.datetime.iloc[-1]
                    if pd.Timedelta(milliseconds=time_diff) < pd.Timedelta(
                        cx_data_req["freq"]
                    ):
                        break
                    # reset end date and pause before calling API
                    else:
                        # change end date
                        start_date = df0.datetime.iloc[-1]

                # rate limit
                sleep(exch.rateLimit / 1000)

            # wrangle data resp
            if not df0.empty:
                # wrangle data resp
                df1 = self.wrangle_data_resp(data_req, df0)
                # add ticker to index
                df1["ticker"] = dr_ticker.upper()
                df1.set_index(["ticker"], append=True, inplace=True)
                # stack ticker dfs
                df = pd.concat([df, df1])

        return df

    def get_funding_rates(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get funding rates data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and funding rates values (col).
        """
        # convert data request parameters to CCXT format
        cx_data_req = ConvertParams(data_req, data_source="ccxt").convert_to_source()

        # check exchange
        if cx_data_req["exch"] not in self.exchanges:
            raise ValueError(
                f"{cx_data_req['exch']} is not a supported exchange. Try another exchange."
            )
        else:
            exch = getattr(ccxt, cx_data_req["exch"])()

        # check if funding avail on exch
        if not exch.has["fetchFundingRateHistory"]:
            raise ValueError(
                f"Funding rates are not available for {cx_data_req['exch']}."
                f" Try another exchange or data request."
            )

        # check if perp future
        if data_req.mkt_type == "spot":
            raise ValueError(
                f"Funding rates are not available for spot markets."
                f" Market type must be perpetual futures."
            )

        # check tickers
        tickers = self.get_assets_info(exch=cx_data_req["exch"], as_dict=True)[
            cx_data_req["exch"]
        ]
        if not any(ticker.upper() in tickers for ticker in cx_data_req["tickers"]):
            raise ValueError(
                f"Assets are not available. Check available assets for {cx_data_req['exch']}"
                f" with asset property."
            )

        # get OHLCV
        df = pd.DataFrame()  # empty df to store data
        # loop through tickers
        for cx_ticker, dr_ticker in zip(cx_data_req["mkts"], data_req.tickers):

            # start date
            start_date = cx_data_req["start_date"]
            # create empty ohlcv df
            df0 = pd.DataFrame()
            # set number of attempts and bool for while loop
            attempts = 0

            # run a while loop to pull ohlcv prices in case the attempt fails
            while attempts < cx_data_req["trials"]:

                try:
                    data = exch.fetchFundingRateHistory(
                        cx_ticker, since=start_date, limit=1000
                    )
                    assert data != []

                except AssertionError as e:
                    logging.warning(e)
                    attempts += 1
                    sleep(exch.rateLimit / 1000)
                    logging.warning(
                        f"Failed to pull data for {cx_ticker} after attempt #{str(attempts)}."
                    )
                    if attempts == cx_data_req["trials"]:
                        logging.warning(
                            f"Failed to pull data from {data_req.exch} for {cx_ticker} after many attempts."
                        )
                        break

                else:
                    # add to df
                    data = pd.DataFrame(data)
                    df0 = pd.concat([df0, data])
                    # check if all data has been extracted
                    time_diff = pd.to_datetime(
                        cx_data_req["end_date"], unit="ms"
                    ) - pd.to_datetime(data.datetime.iloc[-1]).tz_localize(None)
                    if time_diff < pd.Timedelta("8h"):
                        break
                    # reset end date and pause before calling API
                    else:
                        # change end date
                        start_date = data.timestamp.iloc[-1]

                # rate limit
                sleep(exch.rateLimit / 1000)

            # wrangle data resp
            if not df0.empty:
                # wrangle data resp
                df1 = self.wrangle_data_resp(data_req, df0)
                # add ticker to index
                df1["ticker"] = dr_ticker.upper()
                df1.set_index(["ticker"], append=True, inplace=True)
                # stack ticker dfs
                df = pd.concat([df, df1])

        return df

    # TODO: get open interest method
    def get_open_interest(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Submits data request to CCXT API for open interest data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and open interest (col).
        """

        pass

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
        # convert data request parameters to CCXT format
        cx_data_req = ConvertParams(data_req, data_source="ccxt").convert_to_source()
        # empty df
        df = pd.DataFrame()

        # check fields
        fields_list = self.fields
        if not any(field in fields_list for field in data_req.fields):
            raise ValueError(
                f"Fields are not available. Check available fields for with fields property."
            )

        # get OHLCV data
        ohlcv_list = ["open", "high", "low", "close", "volume"]
        if any(field in ohlcv_list for field in cx_data_req["fields"]):
            df0 = self.get_ohlcv(data_req)
            df = pd.concat([df, df0])

        # get funding rate data
        if any(field == "funding_rate" for field in data_req.fields):
            df1 = self.get_funding_rates(data_req)
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

    @staticmethod
    def wrangle_data_resp(
        data_req: DataRequest, data_resp: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Wrangle raw data response.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.
        data_resp: pd.DataFrame
            Data response from API.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            Wrangled dataframe with DatetimeIndex (level 0), ticker (level 1), and values for selected fields (cols),
            in tidy format.
        """
        # wrangle data resp
        df = WrangleData(data_req, data_resp, data_source="ccxt").tidy_data()

        return df
