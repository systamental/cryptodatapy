import pandas as pd
import numpy as np
import logging
import requests
from datetime import datetime, timedelta
from time import sleep
from typing import Optional, Union, Any
from cryptodatapy.util.datacredentials import DataCredentials
from cryptodatapy.data_requests.datarequest import DataRequest
from cryptodatapy.util.convertparams import ConvertParams
from cryptodatapy.data_vendors.datavendor import DataVendor
import ccxt


# data credentials
data_cred = DataCredentials()


class CCXT(DataVendor):
    """
    Retrieves data from CCXT library.
    """

    def __init__(
            self,
            source_type: str = 'library',
            categories: Union[str, list[str]] = 'crypto',
            exchanges: Optional[list[str]] = None,
            indexes: Optional[list[str]] = None,
            assets: Optional[dict[str, list[str]]] = None,
            markets: Optional[dict[str, list[str]]] = None,
            market_types: list[str] = ['spot', 'future', 'perpetual_future', 'option'],
            fields: Optional[list[str]] = None,
            frequencies: Optional[dict[str, list[str]]] = None,
            base_url: Optional[str] = None,
            api_key: Optional[str] = None,
            max_obs_per_call: Optional[int] = 10000,
            rate_limit: Optional[Any] = None
    ):
        """
        Constructor

        Parameters
        ----------
        source_type: str, {'data_vendor', 'exchange', 'library', 'on-chain', 'web'}
            Type of data source, e.g. 'data_vendor', 'exchange', etc.
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

        DataVendor.__init__(self, source_type, categories, exchanges, indexes, assets, markets, market_types, fields,
                            frequencies, base_url, api_key, max_obs_per_call, rate_limit)

        # set exchanges
        if exchanges is None:
            self.exchanges = self.get_exchanges_info(as_list=True)
        # set assets
        if assets is None:
            self.assets = self.get_assets_info(as_dict=True)
        # set markets
        if markets is None:
            self.markets = self.get_markets_info(as_dict=True)
        # set fields
        if fields is None:
            self.fields = self.get_fields_info()
        # set fields
        if frequencies is None:
            self.frequencies = self.get_frequencies_info()
        # set rate limit
        if rate_limit is None:
            self.rate_limit = self.get_rate_limit_info()

    @staticmethod
    def get_exchanges_info(exchange=None, as_list=False) -> Union[pd.DataFrame, list[str]]:
        """
        Get exchanges info.

        Parameters
        ----------
        exchange: str, default None
            Name of exchange.
        as_list: bool, default False
            Returns exchanges info as list.

        Returns
        -------
        exch: list or pd.DataFrame
            List or dataframe with info for supported exchanges.
        """
        # list
        if as_list:
            exchanges = ccxt.exchanges
        else:
            if exchange is not None:
                exchanges = [exchange]
            else:
                print("Getting info on all supported exchanges can take a few minutes. Change exchange parameter"
                      " for info on a specific exchange.")
                exchanges = ccxt.exchanges
            # exch df
            exch_df = pd.DataFrame(index=exchanges, columns=['id', 'name', 'countries', 'urls', 'version', 'api',
                                                             'has', 'timeframes', 'timeout', 'rateLimit', 'userAgent',
                                                             'verbose', 'markets', 'symbols', 'currencies',
                                                             'markets_by_id', 'currencies_by_id', 'api_key', 'secret',
                                                             'uid', 'options'])
            # extract exch info
            for row in exch_df.iterrows():
                try:
                    exch = getattr(ccxt, row[0])()
                    exch.load_markets()
                except Exception:
                    exch_df.loc[row[0], :] = np.nan
                for col in exch_df.columns:
                    try:
                        exch_df.loc[row[0], col] = str(getattr(exch, str(col)))
                    except Exception:
                        exch_df.loc[row[0], col] = np.nan
            # set index name
            exch_df.index.name = 'exchange'
            exchanges = exch_df

        return exchanges

    @staticmethod
    def get_indexes_info():
        """
        Get indexes info.
        """
        return None

    def get_assets_info(self, exchange='binance', as_dict=False) -> Union[pd.DataFrame, dict[str, list[str]]]:
        """
        Get available assets info.

        Parameters
        ----------
        exchange: str, default 'binance'
            Name of exchange.
        as_dict: bool, default False
            Returns assets info as dictionary, with exchange-assets key-value pairs.

        Returns
        -------
        assets: list or pd.DataFrame
            List or dataframe with info for available assets.
        """
        # exch
        if exchange not in self.exchanges:
            raise ValueError(f"{exchange} is not a supported exchange. Try another exchange.")
        else:
            exch = getattr(ccxt, exchange)()

        # get assets on exchange
        markets = exch.load_markets()
        assets = pd.DataFrame(exch.currencies).T
        assets.index.name = 'ticker'

        # dict of assets
        if as_dict:
            assets_dict = {exchange: assets.index.to_list()}
            assets = assets_dict

        return assets

    def get_markets_info(self, exchange='binance', quote_ccy=None, mkt_type=None,
                         as_dict=False) -> Union[pd.DataFrame, dict[str, list[str]]]:
        """
        Get available markets info.

        Parameters
        ----------
        exchange: str, default 'binance'
            Name of exchange.
        quote_ccy: str, default None
            Quote currency.
        mkt_type: str,  {'spot', 'future', 'perpetual_future', 'option'}, default None
            Market type.
        as_dict: bool, default False
            Returns markets info as dict with exchange-markets key-values pair.

        Returns
        -------
        markets: dict or pd.DataFrame
            Dictionary or dataframe with info for available markets, by exchange.
        """
        if exchange not in self.exchanges:
            raise ValueError(f"{exchange} is not a supported exchange. Try another exchange.")
        else:
            exch = getattr(ccxt, exchange)()

        # get assets on exchange
        markets = pd.DataFrame(exch.load_markets()).T
        markets.index.name = 'ticker'

        # quote ccy
        if quote_ccy is not None:
            markets = markets[markets.quote == quote_ccy.upper()]

        # mkt type
        if mkt_type == 'perpetual_future':
            if markets[markets.type == 'swap'].empty:
                markets = markets[markets.type == 'future']
            else:
                markets = markets[markets.type == 'swap']
        elif mkt_type == 'spot' or mkt_type == 'future' or mkt_type == 'option':
            markets = markets[markets.type == mkt_type]

        # dict of assets
        if as_dict:
            mkts_dict = {exchange: markets.index.to_list()}
            markets = mkts_dict

        return markets

    @staticmethod
    def get_fields_info() -> list[str]:
        """
        Get fields info.

        Parameters
        ----------

        Returns
        -------
        fields_list: list
            List of available fields.
        """
        # list of fields
        fields = ['open', 'high', 'low', 'close', 'volume', 'funding_rate']

        return fields

    def get_frequencies_info(self, exchange='binance') -> dict[str, list[str]]:
        """
        Get frequencies info.

        Parameters
        ----------
        exchange: str, default 'binance'
            Name of exchange for which to get available assets.

        Returns
        -------
        assets: dictionary
            Dictionary with info on available frequencies.
        """
        # exch
        if exchange not in self.exchanges:
            raise ValueError(f"{exchange} is not a supported exchange. Try another exchange.")
        else:
            exch = getattr(ccxt, exchange)()
        markets = exch.load_markets()

        # freq dict
        freq = {exchange: exch.timeframes}

        return freq

    def get_rate_limit_info(self, exchange='binance'):
        """
        Get rate limit info.

        Parameters
        ----------
        exchange: str, default 'binance'
            Name of exchange.

        Returns
        -------
        rate_limit: int
            Required minimal delay between HTTP requests to the same exchange in milliseconds.
        """
        # exch
        if exchange not in self.exchanges:
            raise ValueError(f"{exchange} is not a supported exchange. Try another exchange.")
        else:
            exch = getattr(ccxt, exchange)()

        return {
            'exchange rate limit': 'delay in milliseconds between two consequent HTTP requests to the same exchange',
            exchange: exch.rateLimit}

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
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and OHLCV data (cols).
        """
        # convert data request parameters to CCXT format
        cx_data_req = ConvertParams(data_source='ccxt').convert_to_source(data_req)

        # check exchange
        if cx_data_req['exch'] not in self.exchanges:
            raise ValueError(f"{cx_data_req['exch']} is not a supported exchange. Try another exchange.")
        else:
            exch = getattr(ccxt, cx_data_req['exch'])()
        # check if ohlcv avail on exch
        if not exch.has['fetchOHLCV']:
            raise ValueError(f"OHLCV data is not available for {cx_data_req['exch']}."
                             f" Try another exchange or data request.")
        # check freq
        if cx_data_req['freq'] not in exch.timeframes:
            raise ValueError(f"{data_req.freq} is not available for {cx_data_req['exch']}.")

        # check tickers
        tickers = self.get_assets_info(exchange=cx_data_req['exch'], as_dict=True)[cx_data_req['exch']]
        if not any(ticker.upper() in tickers for ticker in cx_data_req['tickers']):
            raise ValueError(f"Assets are not available. Check available assets for {cx_data_req['exch']}"
                             f" with get_assets_info method.")

        # get OHLCV
        df = pd.DataFrame()  # empty df to store data
        # loop through tickers
        for cx_ticker, dr_ticker in zip(cx_data_req['mkts'], data_req.tickers):

            # start date
            start_date = cx_data_req['start_date']
            # create empty ohlcv df
            df0 = pd.DataFrame()
            # set number of attempts and bool for while loop
            attempts = 0

            # run a while loop in case the attempt fails
            while attempts < cx_data_req['trials']:

                try:
                    data = exch.fetch_ohlcv(cx_ticker, cx_data_req['freq'], since=start_date,
                                            limit=self.max_obs_per_call)
                    assert data != []

                except AssertionError as e:
                    logging.warning(e)
                    attempts += 1
                    sleep(exch.rateLimit / 1000)
                    logging.warning(f"Failed to pull data for {cx_ticker} after attempt #{str(attempts)}.")
                    if attempts == cx_data_req['trials']:
                        logging.warning(
                            f"Failed to pull data from {data_req.exch} for {cx_ticker} after many attempts.")
                        break

                else:
                    # name cols and create df
                    header = ['datetime', 'open', 'high', 'low', 'close', 'volume']
                    data = pd.DataFrame(data, columns=header)
                    df0 = pd.concat([df0, data])
                    # check if all data has been extracted
                    time_diff = cx_data_req['end_date'] - df0.datetime.iloc[-1]
                    if pd.Timedelta(milliseconds=time_diff) < pd.Timedelta(cx_data_req['freq']):
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
                df1['ticker'] = dr_ticker.upper()
                df1.set_index(['ticker'], append=True, inplace=True)
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
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and funding rates (col).
        """
        # convert data request parameters to CCXT format
        cx_data_req = ConvertParams(data_source='ccxt').convert_to_source(data_req)

        # check exchange
        if cx_data_req['exch'] not in self.exchanges:
            raise ValueError(f"{cx_data_req['exch']} is not a supported exchange. Try another exchange.")
        else:
            exch = getattr(ccxt, cx_data_req['exch'])()

        # check if funding avail on exch
        if not exch.has['fetchFundingRateHistory']:
            raise ValueError(f"Funding rates are not available for {cx_data_req['exch']}."
                             f" Try another exchange or data request.")

        # check if perp future
        if data_req.mkt_type == 'spot':
            raise ValueError(f"Funding rates are not available for spot markets."
                             f" Market type must be perpetual futures.")

        # check tickers
        tickers = self.get_assets_info(exchange=cx_data_req['exch'], as_dict=True)[cx_data_req['exch']]
        if not any(ticker.upper() in tickers for ticker in cx_data_req['tickers']):
            raise ValueError(f"Assets are not available. Check available assets for {cx_data_req['exch']}"
                             f" with asset property.")

        # get OHLCV
        df = pd.DataFrame()  # empty df to store data
        # loop through tickers
        for cx_ticker, dr_ticker in zip(cx_data_req['mkts'], data_req.tickers):

            # start date
            start_date = cx_data_req['start_date']
            # create empty ohlcv df
            df0 = pd.DataFrame()
            # set number of attempts and bool for while loop
            attempts = 0

            # run a while loop to pull ohlcv prices in case the attempt fails
            while attempts < cx_data_req['trials']:

                try:
                    data = exch.fetchFundingRateHistory(cx_ticker, since=start_date, limit=1000)
                    assert data != []

                except AssertionError as e:
                    logging.warning(e)
                    attempts += 1
                    sleep(exch.rateLimit / 1000)
                    logging.warning(f"Failed to pull data for {cx_ticker} after attempt #{str(attempts)}.")
                    if attempts == cx_data_req['trials']:
                        logging.warning(
                            f"Failed to pull data from {data_req.exch} for {cx_ticker} after many attempts.")
                        break

                else:
                    # add to df
                    data = pd.DataFrame(data)
                    df0 = pd.concat([df0, data])
                    # check if all data has been extracted
                    time_diff = pd.to_datetime(cx_data_req['end_date'], unit='ms') - pd.to_datetime(
                        data.datetime.iloc[-1]).tz_localize(None)
                    if time_diff < pd.Timedelta('8h'):
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
                df1['ticker'] = dr_ticker.upper()
                df1.set_index(['ticker'], append=True, inplace=True)
                # stack ticker dfs
                df = pd.concat([df, df1])

        return df

    # TODO: get open interest method
    # def get_open_interest(self, data_req: DataRequest) -> pd.DataFrame:
    #     """
    #     Submits data request to CCXT API for open interest data.
    #
    #     Parameters
    #     ----------
    #     data_req: DataRequest
    #         Parameters of data request in CryptoDataPy format.
    #
    #     Returns
    #     -------
    #     df: pd.DataFrame - MultiIndex
    #         DataFrame with DatetimeIndex (level 0), ticker (level 1), and open interest (col).
    #     """
    #     # TODO: get open interest
    #
    #     pass

    def get_data(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get data specified by data request.

        Parameters
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and data for selected fields (cols).
        """
        # convert data request parameters to CCXT format
        cx_data_req = ConvertParams(data_source='ccxt').convert_to_source(data_req)
        # empty df
        df = pd.DataFrame()

        # check fields
        fields_list = self.fields
        ohlcv_list = ['open', 'high', 'low', 'close', 'volume']
        if not any(field in fields_list for field in data_req.fields):
            raise ValueError(f"Fields are not available. Check available fields for with fields property.")

        # get OHLCV data
        if any(field in ohlcv_list for field in cx_data_req['fields']):
            df0 = self.get_ohlcv(data_req)
            df = pd.concat([df, df0])

        # get funding rate data
        if any(field == 'funding_rate' for field in data_req.fields):
            df1 = self.get_funding_rates(data_req)
            df = pd.concat([df, df1], axis=1)

        # check if df empty
        if df.empty:
            raise Exception('No data returned. Check data request parameters and try again.')

        # filter df for desired fields and typecast
        fields = [field for field in data_req.fields if field in df.columns]
        df = df.loc[:, fields]

        return df.sort_index()

    @staticmethod
    def wrangle_data_resp(data_req: DataRequest, data_resp: pd.DataFrame) -> pd.DataFrame:
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
            Wrangled dataframe with DatetimeIndex (level 0), ticker (level 1), and data for selected fields (cols),
            in tidy format.
        """
        # convert cols to cryptodatapy format
        df = ConvertParams(data_source='ccxt').convert_fields_to_lib(data_req, data_resp)

        # convert date and set datetimeindex
        if 'close' in df.columns:
            df['date'] = pd.to_datetime(df.date, unit='ms')
        elif 'funding_rate' in df.columns:
            df['date'] = pd.to_datetime(df.set_index('date').index).floor('S').tz_localize(None)
        # set index
        df = df.set_index('date').sort_index()

        # resample freq
        if 'funding_rate' in df.columns and data_req.freq in ['d', 'w', 'm', 'q', 'y']:
            df = (df.funding_rate + 1).cumprod().resample(data_req.freq).last().diff().to_frame()

        # remove dups and NaNs
        df = df[~df.index.duplicated()]  # duplicate rows
        df.dropna(how='all', inplace=True)  # remove entire row NaNs

        # type conversion
        df = ConvertParams().convert_dtypes(df)

        return df
