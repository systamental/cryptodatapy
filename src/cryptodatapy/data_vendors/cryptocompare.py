# import libraries
import pandas as pd
import logging
import requests
from datetime import datetime, timedelta
from time import sleep
from typing import Optional, Union
from cryptodatapy.util.datacredentials import *
from cryptodatapy.util.convertparams import *
from cryptodatapy.data_vendors.datavendor import DataVendor
from cryptodatapy.data_requests.datarequest import DataRequest


# data credentials
data_cred = DataCredentials()


class CryptoCompare(DataVendor):
    """
    Retrieves data from CryptoCompare API.
    """

    def __init__(
            self,
            source_type: str = 'data_vendor',
            categories: list[str] = ['crypto'],
            assets: list[str] = None,
            indexes: list[str] = None,
            markets: list[str] = None,
            market_types: list[str] = ['spot'],
            fields: list[str] = None,
            frequencies: list[str] = ['min', 'h', 'd'],
            exchanges: list[str] = None,
            base_url: str = data_cred.cryptocompare_base_url,
            api_key: str = data_cred.cryptocompare_api_key,
            max_obs_per_call: int = 2000,
            rate_limit: pd.DataFrame = None
    ):
        """
        Constructor

        Parameters
        ----------
        source_type: str, {'data_vendor', 'exchange', 'library', 'on-chain', 'web'}
            Type of data source, e.g. 'data_vendor'
        categories: list[str], {'crypto', 'fx', 'rates', 'equities', 'commodities', 'credit', 'macro', 'alt'}
            List of available categories, e.g. ['crypto', 'fx', 'alt']
        assets: list[str]
            List of available assets, e.g. ['btc', 'eth']
        indexes: list[str]
            List of available indexes, e.g. ['mvda', 'bvin']
        markets: list[str]
            List of available markets as asset/quote currency pairs, e.g. ['btcusdt', 'ethbtc']
        market_types: list[str]
            List of available market types/contracts, e.g. [spot', 'perpetual_futures', 'futures', 'options']
        fields: list[str]
            List of available fields, e.g. ['open', 'high', 'low', 'close', 'volume']
        frequencies: list[str]
            List of available frequencies, e.g. ['tick', '1min', '5min', '10min', '20min', '30min', '1h', '2h', '4h',
            '8h', 'd', 'w', 'm']
        exchanges: list[str]
            List of available exchanges, e.g. ['Binance', 'Coinbase', 'Kraken', 'FTX']
        base_url: str
            Cryptocompare base url used in GET requests, e.g. 'https://min-api.cryptocompare.com/data/'
            If not provided, the data_cred.cryptocompare_base_url is read from DataCredentials
        api_key: str
            Cryptocompare api key, e.g. 'dcf13983adf7dfa79a0dfa35adf'
            If not provided, the data_cred.cryptocompare_api_key is read from DataCredentials
        max_obs_per_call: int, default 2000
            Maxiumu number of observations returns per API call.
        rate_limit: pd.DataFrame
            Number of API calls made and left by frequency.
        """

        DataVendor.__init__(self, source_type, categories, assets, indexes, markets, market_types, fields, frequencies,
                            exchanges, base_url, api_key, max_obs_per_call, rate_limit)

        # api key
        if api_key is None:
            raise TypeError(f"Set your api key. Alternatively, you can use the function "
                            f"{set_credential.__name__} which uses keyring to store your "
                            f"api key in {DataCredentials.__name__}.")
        # set assets
        if assets is None:
            self._assets = self.get_assets_info(as_list=True)
        # set indexes
        if indexes is None:
            self._indexes = self.get_indexes_info(as_list=True)
        # set markets
        if markets is None:
            self._markets = self.get_markets_info(as_list=True)
        # set fields
        if fields is None:
            self._fields = self.get_fields(data_type=None)
        # set exchanges
        if exchanges is None:
            self._exchanges = self.get_exchanges_info(as_list=True)
        # set rate limit
        if rate_limit is None:
            self._rate_limit = self.get_rate_limit_info()

    def get_assets_info(self, as_list=False) -> Union[pd.DataFrame, list[str]]:
        """
        Gets available assets info.

        Parameters
        ----------
        as_list: bool, default False
            If True, returns available assets as list.

        Returns
        -------
        assets: Union[pd.DataFrame, list[str]]
            Info on available assets.
        """
        try:  # try get request
            url = data_cred.cryptocompare_base_url + 'all/coinlist'
            params = {
                'api_key': self.api_key
            }
            r = requests.get(url, params=params)
            assert r.json()['Response'] == 'Success'

        except AssertionError as e:
            logging.warning(e)
            logging.warning(f"Failed to get asset info.")

        else:
            # format response
            assets = pd.DataFrame(r.json()['Data']).T
            # add index name
            assets.index.name = 'ticker'
            # asset list
            if as_list:
                assets = list(assets.index)
            return assets

    def get_top_market_cap_assets(self, n=100) -> list[str]:
        """
        Gets list of top assets by market cap.

        Parameters
        ----------
        n: int, default 100
            Number of assets to return sorted by market cap.

        Returns
        -------
        tickers: list[str]
            List of tickers for top n coins by market cap.
        """
        # check n value
        if n > 100:
            raise ValueError("Maximum number of assets is 100. Change n parameter and try again.")

        try:  # try get request
            url = data_cred.cryptocompare_base_url + 'top/mktcapfull?'
            params = {
                'limit': n,
                'tsym': 'USD',
                'api_key': self.api_key
            }
            r = requests.get(url, params=params)
            assert r.json()['Message'] == 'Success'

        except AssertionError as e:
            logging.warning(e)
            logging.warning(f"Failed to get market cap info.")

        else:
            # format response
            data = pd.DataFrame(r.json()['Data'])
            # create list of tickers
            tickers = []
            for i in range(0, int(n)):
                try:
                    tickers.append(data['RAW'][i]['USD']['FROMSYMBOL'])
                except:
                    logging.warning("Failed to pull ticker for coin #{}\n".format(str(i)))

            return tickers

    # get onchain info
    def get_onchain_info(self, as_list=False) -> Union[pd.DataFrame, list[str]]:
        """
        Gets on-chain data info.

        Parameters
        ----------
        as_list: bool, default False
            If True, returns available on-chain data as list.

        Returns
        -------
        onchain: Union[pd.DataFrame, list[str]]
            Info on available on-chain data.
        """
        try:  # try get request
            url = data_cred.cryptocompare_base_url + 'blockchain/list'
            params = {
                'api_key': self.api_key
            }
            r = requests.get(url, params=params)
            assert r.json()['Response'] == 'Success'

        except AssertionError as e:
            logging.warning(e)
            logging.warning(f"Failed to get index info.")

        else:
            # format response
            onchain = pd.DataFrame(r.json()['Data']).T
            # add index name
            onchain.index.name = 'ticker'
            # asset list
            if as_list:
                onchain = list(onchain.index)

            return onchain

    # get index info, or list
    def get_indexes_info(self, as_list=False) -> Union[pd.DataFrame, list[str]]:
        """
        Gets indexes info.

        Parameters
        ----------
        as_list: bool, default False
            If True, returns available indexes as list.

        Returns
        -------
        indexes: Union[pd.DataFrame, list[str]]
            Info on available indexes.
        """
        try:  # try get request
            url = data_cred.cryptocompare_base_url + 'index/list'
            params = {
                'api_key': self.api_key
            }
            r = requests.get(url, params=params)
            assert r.json()['Response'] == 'Success'

        except AssertionError as e:
            logging.warning(e)
            logging.warning(f"Failed to get index info.")

        else:
            # format response
            indexes = pd.DataFrame(r.json()['Data']).T
            # add index name
            indexes.index.name = 'ticker'
            # asset list
            if as_list:
                indexes = list(indexes.index)
            return indexes

    # get markets info, or list
    def get_markets_info(self, as_list=False) -> Union[dict, list[str]]:
        """
        Gets market pairs info.

        Parameters
        ----------
        as_list: bool, default False
            If True, returns available market pairs as list.

        Returns
        -------
        mkts: Union[dict, list[str]]
            Info on available market pairs.
        """
        try:  # try get request
            url = data_cred.cryptocompare_base_url + 'v2/cccagg/pairs'
            params = {
                'api_key': self.api_key
            }
            r = requests.get(url, params=params)
            assert r.json()['Response'] == 'Success'

        except AssertionError as e:
            logging.warning(e)
            logging.warning(f"Failed to get index info.")

        else:
            # format response
            data_resp = r.json()['Data']
            mkts = {}
            for asset in data_resp['pairs']:
                mkts[asset] = data_resp['pairs'][asset]['tsyms']

            # as list
            if as_list:
                pairs = []
                for asset in mkts.keys():
                    for quote in mkts[asset]:
                        pairs.append(str(asset + quote))
                mkts = pairs

            return mkts

    def get_fields(self, data_type: Optional[str]) -> list[str]:
        """
        Gets list of fields.

        Parameters
        ----------
        data_type: str, {'market', 'on-chain', 'off-chain'}, default None
            Type of data.

        Returns
        -------
        fields_list: list[str]
            List of available fields.
        """

        ohlcv_list, onchain_list, social_list = ['open', 'high', 'low', 'close', 'volume'], [], []

        try:  # try get request for on-chain data
            url = self.base_url + 'blockchain/latest?fsym=BTC'
            params = {
                'api_key': self.api_key
            }
            r = requests.get(url, params=params)
            assert r.json()['Response'] == 'Success'
            data_resp = r.json()['Data']

        except AssertionError as e:
            logging.warning(e)
            logging.warning(f"Failed to get on-chain data info.")

        else:
            # format onchain resp
            for key in list(data_resp):
                if key not in ['id', 'time', 'symbol', 'partner_symbol']:
                    onchain_list.append(key)

        try:  # try get request for social stats
            url = self.base_url + 'social/coin/histo/day'
            params = {
                'coinId': 1182,
                'api_key': self.api_key
            }
            r = requests.get(url, params=params)
            assert r.json()['Response'] == 'Success'
            data_resp = r.json()['Data']

        except AssertionError as e:
            logging.warning(e)
            logging.warning(f"Failed to get on-chain data info.")

        else:
            # format social resp
            for key in list(data_resp[0]):
                if key not in ['id', 'time', 'symbol', 'partner_symbol']:
                    social_list.append(key)

        # fields list
        if data_type == 'market':
            fields_list = ohlcv_list
        elif data_type == 'on-chain':
            fields_list = onchain_list
        elif data_type == 'off-chain':
            fields_list = social_list
        else:
            fields_list = ohlcv_list + onchain_list + social_list

        return fields_list

    def get_exchanges_info(self, as_list=False) -> Union[pd.DataFrame, list[str]]:
        """
        Gets exchanges info.

        Parameters
        ----------
        as_list: bool, default False
            If True, returns available exchanges as list.

        Returns
        -------
        indexes: Union[pd.DataFrame, list[str]]
            Info on available exchanges.
        """
        try:  # try get request
            url = data_cred.cryptocompare_base_url + 'exchanges/general'
            params = {
                'api_key': self.api_key
            }
            r = requests.get(url, params=params)
            assert r.json()['Response'] == 'Success'

        except AssertionError as e:
            logging.warning(e)
            logging.warning(f"Failed to get exchanges info.")

        else:
            # format response
            exch = pd.DataFrame(r.json()['Data']).T
            exch.set_index('Name', inplace=True)
            # asset list
            if as_list:
                exch = list(exch.index)
            return exch

    def get_news(self) -> pd.DataFrame:
        """
        Get news articles from various sources.


        Returns
        -------
        news: pd.DataFrame
            News articles from various sources with title, source, body, ...
        """
        try:  # try get request
            url = self.base_url + 'v2/news/?lang=EN'
            params = {
                'api_key': self.api_key
            }
            r = requests.get(url, params=params)
            assert r.json()['Message'] == 'News list successfully returned'

        except AssertionError as e:
            logging.warning(e)
            logging.warning(f"Failed to get news articles")

        else:
            news = pd.DataFrame(r.json()['Data'])
            return news

    def get_news_sources(self) -> pd.DataFrame:
        """
        Gets news sources.

        Returns
        -------
        news_sources: pd.DataFrame
            News source with name, language and image.
        """

        try:  # try get request
            url = self.base_url + 'news/feeds'
            params = {
                'api_key': self.api_key
            }
            r = requests.get(url, params=params)

        except AssertionError as e:
            logging.warning(e)
            logging.warning(f"Failed to get news articles")

        else:
            news_sources = pd.DataFrame(r.json()).set_index('key')
            return news_sources

    def get_rate_limit_info(self) -> pd.DataFrame:
        """
        Gets rate limit info.

        Returns
        ------
        rate_limit: pd.DataFrame
            DataFrame with API calls made and left by period (hour, day, month).
        """
        try:  # try get request
            url = 'https://min-api.cryptocompare.com/stats/rate/limit'
            params = {
                'api_key': self.api_key
            }
            r = requests.get(url, params=params)
            assert r.json()['Response'] == 'Success'

        except AssertionError as e:
            logging.warning(e)
            logging.warning(f"Failed to get rate limit info.")

        else:
            # format response
            rate_limit = pd.DataFrame(r.json()['Data'])
            # add index name
            rate_limit.index.name = 'frequency'
            return rate_limit

    def convert_data_req_params(self, data_req: DataRequest) -> dict[str, Union[str, int, list]]:
        """
        Converts CryptoDataPy data request parameters to CryptoCompare API format.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        cc_data_req: dict[str, Union[str, int, list]]
            Dictionary with data request parameters in Cryptocommpare format.
        """
        # convert tickers to uppercase
        tickers = [ticker.upper() for ticker in data_req.tickers]

        # convert freq format
        if data_req.freq is None:
            freq = 'histoday'
        elif data_req.freq[-3:] == 'min':
            freq = 'histominute'
        elif data_req.freq[-1] == 'h':
            freq = 'histohour'
        elif data_req.freq == 'd':
            freq = 'histoday'
        else:
            freq = 'histoday'

        # convert quote ccy
        # no quote
        if data_req.quote_ccy is None:
            quote_ccy = 'USD'
        else:
            quote_ccy = data_req.quote_ccy
        # quote ccy to uppercase and add to dict
        quote_ccy = quote_ccy.upper()

        # convert exch
        # no exch
        if data_req.exch is None:
            exch = 'CCCAGG'
        else:
            exch = data_req.exch

        # convert date format
        # min freq, Cryptocompare freemium will only return the past week of min OHLCV data
        if data_req.freq is not None and data_req.freq[-3:] == 'min':
            start_date = round((datetime.now() - timedelta(days=7)).timestamp())
        # no start date
        elif data_req.start_date is None:
            start_date = convert_datetime_to_unix_tmsp('2009-01-03 00:00:00')
        else:
            start_date = convert_datetime_to_unix_tmsp(data_req.start_date)
        # end date
        if data_req.end_date is None:
            end_date = convert_datetime_to_unix_tmsp(datetime.utcnow())
        else:
            end_date = convert_datetime_to_unix_tmsp(data_req.end_date)

        # add to params dict
        cc_data_req = {'tickers': tickers, 'frequency': freq, 'currency': quote_ccy, 'exchange': exch,
                       'start_date': start_date, 'end_date': end_date, 'fields': data_req.fields,
                       'trials': data_req.trials, 'pause': data_req.pause}

        return cc_data_req

    # wrangle OHLCV data resp
    @staticmethod
    def wrangle_ohlcv_resp(data_req: DataRequest, data_resp: pd.DataFrame) -> pd.DataFrame:
        """
        Wrangles OHLCV data response.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.
        data_resp: pd.DataFrame
            Data response from GET request.

        Returns
        -------
        df: pd.DataFrame
            Wrangled OHLCV dataframe in tidy format.
        """
        # make copy of df
        df = data_resp.copy()

        # create date and convert to datetime
        if 'date' not in df.columns and 'time' in df.columns:
            df['date'] = pd.to_datetime(df['time'], unit='s')
            df.drop(columns=['time'], inplace=True)

        # set datetimeindex and sort index by date
        df = df.set_index('date').sort_index()

        # rename volume col
        if 'volume' not in df.columns and 'volumefrom' in df.columns:
            df.rename(columns={'volumefrom': 'volume'}, inplace=True)
            # keep only ohlcv cols
            df = df.loc[:, ['open', 'high', 'low', 'close', 'volume']]
        else:
            # keep only ohlc cols
            df = df.loc[:, ['open', 'high', 'low', 'close']]

        # remove duplicate indexes/rows
        df = df[~df.index.duplicated()]

        # remove rows where close is 0
        df = df[df['close'] != 0].dropna()

        # filter for desired start to end date
        if data_req.start_date is not None:
            df = df[(df.index >= data_req.start_date)]
        if data_req.end_date is not None:
            df = df[(df.index <= data_req.end_date)]

        # resample frequency
        df = df.resample(data_req.freq).last()

        return df

    # wrangle onchain df
    @staticmethod
    def wrangle_onchain_resp(data_req: DataRequest, data_resp: pd.DataFrame) -> pd.DataFrame:
        """
        Wrangles on-chain data response.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.
        data_resp: pd.DataFrame
            Data response from GET request.

        Returns
        -------
        df: pd.DataFrame
            Wrangled on-chain dataframe in tidy format.
        """
        # make copy of df
        df = data_resp.copy()

        # create date and convert to datetime
        if 'date' not in df.columns and 'time' in df.columns:
            df['date'] = pd.to_datetime(df['time'], unit='s')
            df.drop(columns=['time'], inplace=True)

        # set datetimeindex and sort index by date
        df = df.set_index('date').sort_index()

        # drop id and
        if 'id' in df.columns:
            df = df.drop(columns=['id'])
        if 'symbol' in df.columns:
            df = df.drop(columns=['symbol'])

        # remove duplicate indexes/rows
        df = df[~df.index.duplicated()]

        # filter for desired start to end date
        if data_req.start_date is not None:
            df = df[(df.index >= data_req.start_date)]
        if data_req.end_date is not None:
            df = df[(df.index <= data_req.end_date)]

        # convert to float
        df = df.astype(float)

        # resample frequency
        df = df.resample(data_req.freq).last()

        return df

    # wrangle social data resp
    @staticmethod
    def wrangle_social_resp(data_req: DataRequest, data_resp: pd.DataFrame) -> pd.DataFrame:
        """
        Wrangles social stats data response.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.
        data_resp: pd.DataFrame
            Data response from GET request.

        Returns
        -------
        df: pd.DataFrame
            Wrangled social stats dataframe in tidy format.
        """
        # make copy of df
        df = data_resp.copy()

        # create date and convert to datetime
        if 'date' not in df.columns and 'time' in df.columns:
            df['date'] = pd.to_datetime(df['time'], unit='s')
            df.drop(columns=['time'], inplace=True)

        # set datetimeindex and sort index by date
        df = df.set_index('date').sort_index()

        # remove duplicate indexes/rows
        df = df[~df.index.duplicated()]

        # remove rows where all vals 0
        df = df[df != 0].dropna(how='all')

        # filter for desired start to end date
        if data_req.start_date is not None:
            df = df[(df.index >= data_req.start_date)]
        if data_req.end_date is not None:
            df = df[(df.index <= data_req.end_date)]

        # convert to float
        df = df.astype(float)

        # resample frequency
        df = df.resample(data_req.freq).last()

        return df

    def fetch_indexes(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Submits data request to API for indexes data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame
            DataFrame with DatetimeIndex and open, high, low and close index values (cols).
        """
        # convert data request parameters to CryptoCompare format
        cc_data_req = self.convert_data_req_params(data_req)
        # empty df to add data
        df = pd.DataFrame()

        # indexes list
        idx_list, tickers = self.indexes, []
        # keep only asset tickers
        for ticker in cc_data_req['tickers']:
            if ticker in idx_list:
                tickers.append(ticker)
            else:
                pass
        # raise error if all tickers are assets
        if len(tickers) == 0:
            raise ValueError('Tickers are all assets. Try again with get_OHLCV() method.')

        # loop through tickers
        for ticker in tickers:

            # start and end date
            end_date = cc_data_req['end_date']
            # create empty ohlc df
            df0 = pd.DataFrame()
            # set number of attempts and bool for while loop
            attempts = 0
            # run a while loop to pull ohlcv prices in case the attempt fails
            while attempts < cc_data_req['trials']:
                try:  # fetch index data
                    # get request
                    url = self.base_url + "index/" + cc_data_req['frequency'][:5] + "/" + cc_data_req['frequency'][5:]
                    params = {
                        'indexName': ticker,
                        'limit': self.max_obs_per_call,
                        'toTs': end_date,
                        'api_key': self.api_key
                    }
                    r = requests.get(url, params=params)
                    # resp message
                    assert r.json()['Response'] == 'Success'

                except AssertionError as e:
                    logging.warning(e)
                    attempts += 1
                    sleep(cc_data_req['pause'])
                    logging.warning(f"Failed to pull data for {ticker} after attempt #{str(attempts)}.")
                    if attempts == 3:
                        logging.warning(
                            f"Failed to pull data from Cryptocompare for {ticker} after many attempts"
                            f" due to following error: {str(r.json()['Message'])}.")
                        break

                except Exception as e:
                    logging.warning(e)
                    logging.warning(
                        "The data's response format has most likely changed.\n Review Cryptocompares response format"
                        " and make changes to AlphaFactory's code base.")
                    break

                else:
                    data = pd.DataFrame(r.json()['Data'])
                    # add data to empty df
                    df0 = pd.concat([df0, data])
                    # check if all data has been extracted
                    if len(data) < (self.max_obs_per_call + 1) or data.close[0] == 0 or data.close[0].astype(
                            str) == 'nan':
                        break
                    # reset end date and pause before calling API
                    else:
                        # change end date
                        end_date = data.time[0]
                        sleep(cc_data_req['pause'])

            # wrangle data resp
            if not df0.empty:
                df1 = self.wrangle_ohlcv_resp(data_req, df0)
                # add ticker to df0 and reset index
                df1['ticker'] = ticker
                df1 = df1.reset_index().set_index(['date', 'ticker']).sort_index()
                # concat df and df1
                df = pd.concat([df, df1])

        return df.sort_index()

    def fetch_ohlcv(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Submits data request to API for OHLCV data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and open, high, low and close prices (cols).
        """
        # convert data request parameters to CryptoCompare format
        cc_data_req = self.convert_data_req_params(data_req)
        # index tickers list
        idx_tickers_list = self.get_indexes_info(as_list=True)
        # empty df to add data
        df = pd.DataFrame()

        # indexes list
        idx_list, tickers = self.indexes, []
        # remove index tickers
        for ticker in cc_data_req['tickers']:
            if ticker in idx_list:
                pass
            else:
                tickers.append(ticker)
        # raise error if all tickers are indexes
        if len(tickers) == 0:
            raise ValueError('Tickers are all indexes. Try again with get_indexes() method.')

        # loop through tickers
        for ticker in tickers:

            # start and end date
            end_date = cc_data_req['end_date']
            # create empty ohlc df
            df0 = pd.DataFrame()
            # set number of attempts and bool for while loop
            attempts = 0
            # run a while loop to pull ohlcv prices in case the attempt fails
            while attempts < cc_data_req['trials']:
                try:  # fetch OHLCV data
                    # get request
                    url = self.base_url + f"v2/{cc_data_req['frequency']}"
                    params = {
                        'fsym': ticker,
                        'tsym': cc_data_req['currency'],
                        'limit': self.max_obs_per_call,
                        'e': cc_data_req['exchange'],
                        'toTs': end_date,
                        'api_key': self.api_key
                    }
                    r = requests.get(url, params=params)
                    # resp message
                    assert r.json()['Response'] == 'Success'

                except AssertionError as e:
                    logging.warning(e)
                    attempts += 1
                    sleep(cc_data_req['pause'])
                    logging.warning(f"Failed to pull data for {ticker} after attempt #{str(attempts)}.")
                    if attempts == 3:
                        logging.warning(
                            f"Failed to pull data from Cryptocompare for {ticker} after many attempts "
                            f"due to following error: {str(r.json()['Message'])}.")
                        break

                except Exception as e:
                    logging.warning(e)
                    logging.warning(" The data's response format has most likely changed."
                                    " Review Cryptocompare's response format and "
                                    "make changes to cryptodatapy if necessary.")
                    break

                else:
                    data = pd.DataFrame(r.json()['Data']['Data'])
                    # add data to empty df
                    df0 = pd.concat([df0, data])
                    # check if all data has been extracted
                    if len(data) < (self.max_obs_per_call + 1) or data.close[0] == 0 or data.close[0].astype(
                            str) == 'nan':
                        break
                    # reset end date and pause before calling API
                    else:
                        # change end date
                        end_date = data.time[0]
                        sleep(cc_data_req['pause'])

            # wrangle data resp
            if not df0.empty:
                df1 = self.wrangle_ohlcv_resp(data_req, df0)
                # add ticker to df0 and reset index
                df1['ticker'] = ticker
                df1 = df1.reset_index().set_index(['date', 'ticker']).sort_index()
                # concat df and df1
                df = pd.concat([df, df1])

        return df

    def fetch_onchain(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Submits data request to API for on-chain data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and on-chain data (cols).
        """
        # convert data request parameters to CryptoCompare format
        cc_data_req = self.convert_data_req_params(data_req)
        # empty df to add data
        df = pd.DataFrame()

        # check if frequency daily
        if cc_data_req['frequency'] != 'histoday':
            raise ValueError(f"On-chain data is only available on a daily frequency."
                             f" Change data request frequency to 'd' and try again.")

        # indexes list
        idx_list, tickers = self.indexes, []
        # remove index tickers
        for ticker in cc_data_req['tickers']:
            if ticker in idx_list:
                pass
            else:
                tickers.append(ticker)
        # raise error if all tickers are indexes
        if len(tickers) == 0:
            raise ValueError('Tickers are all indexes. Try again with get_indexes() method.')

        # loop through tickers
        for ticker in tickers:

            # start and end date
            end_date = cc_data_req['end_date']
            # create empty ohlc df
            df0 = pd.DataFrame()
            # set number of attempts and bool for while loop
            attempts = 0
            # run a while loop to pull on-chain data in case the attempt fails
            while attempts < cc_data_req['trials']:
                try:
                    # get request
                    url = self.base_url + f"blockchain/histo/day?"
                    params = {
                        'fsym': ticker,
                        'limit': self.max_obs_per_call,
                        'toTs': end_date,
                        'api_key': self.api_key
                    }
                    r = requests.get(url, params=params)
                    # resp message
                    assert r.json()['Response'] == 'Success'

                except AssertionError as e:
                    logging.warning(e)
                    attempts += 1
                    sleep(cc_data_req['pause'])
                    logging.warning(f"Failed to pull data for {ticker} after attempt #{str(attempts)}.")
                    if attempts == 3:
                        logging.warning(
                            f"Failed to pull data from Cryptocompare for {ticker} after many attempts "
                            f"due to following error: {str(r.json()['Message'])}.")
                        break

                except Exception as e:
                    logging.warning(e)
                    logging.warning(" The data's response format has most likely changed."
                                    " Review Cryptocompare's response format and "
                                    "make changes to cryptodatapy if necessary.")
                    break

                else:
                    data = pd.DataFrame(r.json()['Data']['Data'])
                    # add data to empty df
                    df0 = pd.concat([df0, data])
                    # check if all data has been extracted
                    if len(data) < (self.max_obs_per_call - 1) or all(data.iloc[0] == 0) or all(
                            data.iloc[0].astype(str) == 'nan'):
                        break
                    # reset end date and pause before calling API
                    else:
                        # change end date
                        end_date = data.time[0]
                        sleep(cc_data_req['pause'])

            # wrangle data resp
            if not df0.empty:
                df1 = self.wrangle_onchain_resp(data_req, df0)
                # add ticker to df0 and reset index
                df1['ticker'] = ticker
                df1 = df1.reset_index().set_index(['date', 'ticker']).sort_index()
                # concat df and df1
                df = pd.concat([df, df1])

        return df

    def fetch_social(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Submits data request to API for social stats.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and social stats (cols).
        """
        # convert data request parameters to CryptoCompare format
        cc_data_req = self.convert_data_req_params(data_req)
        # empty df to add data
        df = pd.DataFrame()

        # check frequency
        if cc_data_req['frequency'] != 'histoday' and cc_data_req['frequency'] != 'histohour':
            raise ValueError(f"On-chain data is only available on a daily and hourly frequency."
                             f" Change data request frequency to 'd' or '1h' and try again.")

        # indexes list
        idx_list, tickers = self.indexes, []
        # remove index tickers
        for ticker in cc_data_req['tickers']:
            if ticker in idx_list:
                pass
            else:
                tickers.append(ticker)
        # raise error if all tickers are indexes
        if len(tickers) == 0:
            raise ValueError('Tickers are all indexes. Try again with get_indexes() method.')

        # loop through tickers
        for ticker in tickers:

            # get coinId for ticker
            coin_id = int(self.get_assets_info().loc[ticker, 'Id'])
            # start and end date
            end_date = cc_data_req['end_date']
            # create empty ohlc df
            df0 = pd.DataFrame()
            # set number of attempts and bool for while loop
            attempts = 0
            # run a while loop to pull on-chain data in case the attempt fails
            while attempts < cc_data_req['trials']:
                try:
                    # get request
                    url = self.base_url + "social/coin/" + cc_data_req['frequency'][:5] + '/' \
                          + cc_data_req['frequency'][5:]
                    params = {
                        'coinId': coin_id,
                        'limit': self.max_obs_per_call,
                        'toTs': end_date,
                        'api_key': self.api_key
                    }
                    r = requests.get(url, params=params)
                    # resp message
                    assert r.json()['Response'] == 'Success'

                except AssertionError as e:
                    attempts += 1
                    sleep(cc_data_req['pause'])
                    logging.warning(e)
                    logging.warning(f"Failed to pull data for {ticker} after attempt #{str(attempts)}.")
                    if attempts == 3:
                        logging.warning(
                            f"Failed to pull data from Cryptocompare for {ticker} after many attempts "
                            f"due to following error: {str(r.json()['Message'])}.")
                        break

                except Exception as e:
                    logging.warning(e)
                    logging.warning(" The data's response format has most likely changed."
                                    " Review Cryptocompare's response format and "
                                    "make changes to cryptodatapy if necessary.")
                    break

                else:
                    data = pd.DataFrame(r.json()['Data'])
                    # add data to empty df
                    df0 = pd.concat([df0, data])
                    # check if all data has been extracted
                    if len(data) < (self.max_obs_per_call + 1) or all(data.drop(columns=['time']).iloc[0] == 0) or all(
                            data.drop(columns=['time']).iloc[0].astype(str) == 'nan'):
                        break
                    # reset end date and pause before calling API
                    else:
                        # change end date
                        end_date = data.time[0]
                        sleep(cc_data_req['pause'])

            # wrangle data resp
            if not df0.empty:
                df1 = self.wrangle_social_resp(data_req, df0)
                # add ticker to df0 and reset index
                df1['ticker'] = ticker
                df1 = df1.reset_index().set_index(['date', 'ticker']).sort_index()
                # concat df and df1
                df = pd.concat([df, df1])

        return df

    def fetch_data(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Fetches either OHLCV, on-chain or social stats data.

        Parameters
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and OHLCV, on-chain and/or social data (cols).
        """
        # convert data request parameters to CryptoCompare format
        cc_data_req = self.convert_data_req_params(data_req)

        # check if fields available
        fields_list = self.get_fields(data_type=None)
        if not all(i in fields_list for i in cc_data_req['fields']):
            raise ValueError('Fields are not available. Check available fields with get_fields() method and try again.')

        # fields list
        ohlcv_list = self.get_fields(data_type='market')
        onchain_list = self.get_fields(data_type='on-chain')
        offchain_list = self.get_fields(data_type='off-chain')
        # index tickers list
        idx_tickers_list = self.indexes
        # store in empty df
        df = pd.DataFrame()

        # fetch indexes data
        if any(i in idx_tickers_list for i in cc_data_req['tickers']):
            try:
                df0 = self.fetch_indexes(data_req)

            except Exception as e:
                logging.warning(e)
            else:
                df = pd.concat([df, df0])

        # fetch OHLCV data
        if any(i in ohlcv_list for i in cc_data_req['fields']):
            try:
                df1 = self.fetch_ohlcv(data_req)

            except Exception as e:
                logging.warning(e)
                # drop field
                for field in ohlcv_list:
                    if field in cc_data_req['fields']:
                        cc_data_req['fields'].remove(field)
            else:
                df = pd.concat([df, df1])

        # fetch on-chain data
        if any(i in onchain_list for i in cc_data_req['fields']):
            try:
                df2 = self.fetch_onchain(data_req)

            except Exception as e:
                logging.warning(e)
                # drop field
                for field in onchain_list:
                    if field in cc_data_req['fields']:
                        cc_data_req['fields'].remove(field)
            else:
                df = pd.concat([df, df2], axis=1)

        # fetch social stats data
        if any(i in offchain_list for i in cc_data_req['fields']):
            try:
                df3 = self.fetch_social(data_req)

            except Exception as e:
                logging.warning(e)
                # drop field
                for field in offchain_list:
                    if field in cc_data_req['fields']:
                        cc_data_req['fields'].remove(field)

            else:
                df = pd.concat([df, df3], axis=1)

        # check if df empty
        if df.empty:
            raise Exception('No data returned. Check data request parameters and try again.')

        # filter df for desired fields and sort index by date
        df = df.loc[:, cc_data_req['fields']]
        # remove ticker if only 1 ticker
        if len(data_req.tickers) == 1:
            df = df.droplevel('ticker')

        return df.sort_index()
