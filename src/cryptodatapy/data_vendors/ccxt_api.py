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
            categories: str = 'crypto',
            exchanges: list[str] = None,
            indexes: list[str] = None,
            assets: dict[str, list[str]] = None,
            markets: dict[str, list[str]] = None,
            market_types: list[str] = ['spot', 'future', 'perpetual_future', 'option'],
            fields: list[str] = None,
            frequencies: list[str] = None,
            base_url: str = None,
            api_key: str = None,
            max_obs_per_call: int = 10000,
            rate_limit: Any = None
    ):
        """
        Constructor

        Parameters
        ----------
        source_type: str, {'data_vendor', 'exchange', 'library', 'on-chain', 'web'}
            Type of data source, e.g. 'data_vendor', 'exchange', etc.
        categories: list[str], {'crypto', 'fx', 'rates', 'eqty', 'commodities', 'credit', 'macro', 'alt'}
            List of available categories, e.g. ['crypto', 'fx', 'alt']
        exchanges: list
            List of available exchanges, e.g. ['Binance', 'Coinbase', 'Kraken', 'FTX']
        indexes: list
            List of available indexes, e.g. ['mvda', 'bvin']
        assets: list
            List of available assets, e.g. ['btc', 'eth']
        markets: list
            List of available markets as asset/quote currency pairs, e.g. ['btcusdt', 'ethbtc']
        market_types: list
            List of available market types/contracts, e.g. [spot', 'perpetual_future', 'future', 'option']
        fields: list
            List of available fields, e.g. ['open', 'high', 'low', 'close', 'volume']
        frequencies: list
            List of available frequencies, e.g. ['tick', '1min', '5min', '10min', '20min', '30min', '1h', '2h', '4h',
            '8h', 'd', 'w', 'm']
        base_url: str
            Cryptocompare base url used in GET requests, e.g. 'https://min-api.cryptocompare.com/data/'
            If not provided, default is set to cryptocompare_base_url stored in DataCredentials
        api_key: str
            Cryptocompare api key, e.g. 'dcf13983adf7dfa79a0dfa35adf'
            If not provided, default is set to cryptocompare_api_key stored in DataCredentials
        max_obs_per_call: int
            Maximum number of observations returns per API call.
            If not provided, default is set to cryptocompare_api_limit storeed in DataCredentials
        rate_limit: pd.DataFrame
            Number of API calls made and left by frequency.
        """

        DataVendor.__init__(self, source_type, categories, exchanges, indexes, assets, markets, market_types, fields,
                            frequencies, base_url, api_key, max_obs_per_call, rate_limit)

        # set exchanges
        if exchanges is None:
            self.exchanges = self.get_exchanges_info(as_list=True)
        # set assets
        if assets is None:
            self.assets = self.get_assets_info(as_list=True)
        # set markets
        if markets is None:
            self.markets = self.get_markets_info(as_list=True)
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
        Gets list of supported exchanges.

        Parameters
        ----------
        None

        Returns
        -------
        exch: list
            List of supported exchanges.
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
                except Exception as e:
                    exch_df.loc[row[0], :] = np.nan
                for col in exch_df.columns:
                    try:
                        exch_df.loc[row[0], col] = str(getattr(exch, str(col)))
                    except Exception as e:
                        exch_df.loc[row[0], col] = np.nan
            # set index name
            exch_df.index.name = 'exchange'
            exchanges = exch_df

        return exchanges

    @staticmethod
    def get_indexes_info():
        """
        Gets indexes info.
        """
        print("CCXT does not provide data for indexes. See other data vendors.")

    def get_assets_info(self, exchange='binance', as_list=False) -> Union[pd.DataFrame, dict[str, list[str]]]:
        """
        Gets available assets info.

        Parameters
        ----------
        exchange: str, default 'binance'
            Name of exchange for which to get available assets.

        Returns
        -------
        assets: list
            List of available assets.
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

        # list of assets
        if as_list:
            assets_dict = {}
            assets_dict[exchange] = assets.index.to_list()
            assets = assets_dict

        return assets

    def get_markets_info(self, exchange='binance', quote_ccy=None, mkt_type=None,
                         as_list=False) -> Union[pd.DataFrame, dict[str, list[str]]]:
        """
        Gets available markets info.

        Parameters
        ----------
        exchange: str, default 'binance'
            Name of exchange for which to get available assets.
        as_dict: bool
            Returns markets info as dict with exchange-markets key-values pair.

        Returns
        -------
        markets: dataframe or dict
            DataFrame with markets info or dict of available markets.
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

        # list of assets
        if as_list:
            mkts_dict = {}
            mkts_dict[exchange] = markets.index.to_list()
            markets = mkts_dict

        return markets

    @staticmethod
    def get_fields_info() -> list[str]:
        """
        Gets fields info.

        Parameters
        ----------
        None

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
        Gets frequencies info.

        Parameters
        ----------
        exchange: str, default 'binance'
            Name of exchange for which to get available assets.
        as_list: bool
            Returns markets info as list of markets.

        Returns
        -------
        assets: dataframe or list
            DataFrame with markets info or list of available markets.
        """
        # exch
        if exchange not in self.exchanges:
            raise ValueError(f"{exchange} is not a supported exchange. Try another exchange.")
        else:
            exch = getattr(ccxt, exchange)()
        markets = exch.load_markets()

        # freq dict
        freq = {}
        freq[exchange] = exch.timeframes

        return freq

    def get_rate_limit_info(self, exchange='binance'):
        """
        Gets rate limit info.

        Parameters
        ----------
        exchange: str
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
        Submits data request to CCXT API for OHLCV data.

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
        tickers = self.get_assets_info(exchange=cx_data_req['exch'], as_list=True)[cx_data_req['exch']]
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

            # run a while loop to pull ohlcv prices in case the attempt fails
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
                    if attempts == 3:
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
        Submits data request to CCXT API for funding rate data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and funding rate (col).
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
        tickers = self.get_assets_info(exchange=cx_data_req['exch'], as_list=True)[cx_data_req['exch']]
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
                    if attempts == 3:
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

    def get_oi(self, data_req: DataRequest) -> pd.DataFrame:
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
        # TODO: get open interest

        pass

    def get_data(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Gets time series data specified by data request.

        Parameters
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and time series data for fields (cols).
        """
        # empty df
        df = pd.DataFrame()

        # check fields
        fields_list = self.fields
        ohlcv_list = ['open', 'high', 'low', 'close', 'volume']
        if not any(field in fields_list for field in cx_data_req['fields']):
            raise ValueError(f"Fields are not available. Check available fields for with fields property.")

        # get OHLCV data
        if any(field in ohlcv_list for field in cx_data_req['fields']):
            df0 = self.get_ohlcv(data_req)
            df = pd.concat([df, df0])

        # get funding rate data
        if any(field == 'funding_rate' for field in cx_data_req['fields']):
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
        Wrangles data response.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.
        data_resp: pd.DataFrame
            Data response from GET request.

        Returns
        -------
        df: pd.DataFrame
            Wrangled dataframe in tidy format.
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

        # remove bad data
        df = df[df != 0].dropna(how='all')  # 0 values
        df = df[~df.index.duplicated()]  # duplicate rows
        df.dropna(how='all', inplace=True)  # remove entire row NaNs

        # type conversion
        df = ConvertParams().convert_dtypes(df)

        return df
