import pandas as pd
import logging
import requests
from datetime import datetime, timedelta
from time import sleep
from typing import Optional, Union
from cryptodatapy.util.datacredentials import DataCredentials
from cryptodatapy.util.convertparams import ConvertParams
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
            frequencies: list[str] = ['1min', '1h', 'd'],
            exchanges: list[str] = None,
            base_url: str = data_cred.cryptocompare_base_url,
            api_key: str = data_cred.cryptocompare_api_key,
            max_obs_per_call: int = int(data_cred.cryptocompare_api_limit),
            rate_limit: pd.DataFrame = None
    ):
        """
        Constructor

        Parameters
        ----------
        source_type: str, {'data_vendor', 'exchange', 'library', 'on-chain', 'web'}
            Type of data source, e.g. 'data_vendor', 'exchange', etc.
        categories: list[str], {'crypto', 'fx', 'rates', 'equities', 'commodities', 'credit', 'macro', 'alt'}
            List of available categories, e.g. ['crypto', 'fx', 'alt']
        assets: list
            List of available assets, e.g. ['btc', 'eth']
        indexes: list
            List of available indexes, e.g. ['mvda', 'bvin']
        markets: list
            List of available markets as asset/quote currency pairs, e.g. ['btcusdt', 'ethbtc']
        market_types: list
            List of available market types/contracts, e.g. [spot', 'perpetual_future', 'future', 'option']
        fields: list
            List of available fields, e.g. ['open', 'high', 'low', 'close', 'volume']
        frequencies: list
            List of available frequencies, e.g. ['tick', '1min', '5min', '10min', '20min', '30min', '1h', '2h', '4h',
            '8h', 'd', 'w', 'm']
        exchanges: list
            List of available exchanges, e.g. ['Binance', 'Coinbase', 'Kraken', 'FTX']
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

        DataVendor.__init__(self, source_type, categories, assets, indexes, markets, market_types, fields, frequencies,
                            exchanges, base_url, api_key, max_obs_per_call, rate_limit)

        # api key
        if api_key is None:
            raise TypeError(f"Set your api key. Alternatively, you can use the function "
                            f"{set_credential.__name__} which uses keyring to store your "
                            f"api key in {DataCredentials.__name__}.")
        # set assets
        if assets is None:
            self.assets = self.get_assets_info(as_list=True)
        # set indexes
        if indexes is None:
            self.indexes = self.get_indexes_info(as_list=True)
        # set markets
        if markets is None:
            self.markets = self.get_markets_info(as_list=True)
        # set fields
        if fields is None:
            self.fields = self.get_fields_info(data_type=None)
        # set exchanges
        if exchanges is None:
            self.exchanges = self.get_exchanges_info(as_list=True)
        # set rate limit
        if rate_limit is None:
            self.rate_limit = self.get_rate_limit_info()

    def get_assets_info(self, as_list=False) -> Union[pd.DataFrame, list[str]]:
        """
        Gets available assets info.

        Parameters
        ----------
        as_list: bool, default False
            If True, returns available assets as list.

        Returns
        -------
        assets: pd.DataFrame or list
            Info on available assets.
        """
        try:  # try get request
            url = self.base_url + 'all/coinlist'
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
        tickers: list
            List of tickers for top n coins ranked by market cap.
        """
        # check n value
        if n > 100:
            raise ValueError("Maximum number of assets is 100. Change n parameter and try again.")

        try:  # try get request
            url = self.base_url + 'top/mktcapfull?'
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

    def get_onchain_info(self, as_list=False) -> Union[pd.DataFrame, list[str]]:
        """
        Gets on-chain data info.

        Parameters
        ----------
        as_list: bool, default False
            If True, returns available on-chain data as list.

        Returns
        -------
        onchain: pd.DataFrame or list
            Info on available on-chain data.
        """
        try:  # try get request
            url = self.base_url + 'blockchain/list'
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
            # format date
            onchain['data_available_from'] = pd.to_datetime(onchain.data_available_from, unit='s')
            # add index name
            onchain.index.name = 'ticker'
            # asset list
            if as_list:
                onchain = list(onchain.index)

            return onchain

    def get_indexes_info(self, as_list=False) -> Union[pd.DataFrame, list[str]]:
        """
        Gets indexes info.

        Parameters
        ----------
        as_list: bool, default False
            If True, returns available indexes as list.

        Returns
        -------
        indexes: pd.DataFrame or list
            Info on available indexes.
        """
        try:  # try get request
            url = self.base_url + 'index/list'
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

    def get_markets_info(self, as_list=False) -> Union[dict, list[str]]:
        """
        Gets market pairs info.

        Parameters
        ----------
        as_list: bool, default False
            If True, returns available market pairs as list.

        Returns
        -------
        mkts: dictionary or list
            Info on available market pairs.
        """
        try:  # try get request
            url = self.base_url + 'v2/cccagg/pairs'
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

    def get_fields_info(self, data_type: Optional[str]) -> list[str]:
        """
        Gets fields info.

        Parameters
        ----------
        data_type: str, {'market', 'on-chain', 'off-chain'}, default None
            Type of data.

        Returns
        -------
        fields_list: list
            Info on available fields.
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
            url = data_cred.cryptocompare_base_url + 'social/coin/histo/day'
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
        indexes: pd.DataFrame or list
            Info on available exchanges.
        """
        try:  # try get request
            url = self.base_url + 'exchanges/general'
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
            DataFrame with API calls left and made by period (hour, day, month).
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

    def fetch_indexes(self, data_req: DataRequest, tidy_data=True) -> pd.DataFrame:
        """
        Submits data request to API for indexes data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.
        tidy_data: bool, default True
            Wrangles data response into tidy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1) and open, high, low and close index values (cols).
        """
        # convert data request parameters to CryptoCompare format
        cc_data_req = ConvertParams(data_source='cryptocompare').convert_to_source(data_req)
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
            raise ValueError("Tickers are all assets. Use '.indexes' property to"
                             " see available indexes.")

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
                    url = self.base_url + "index/" + cc_data_req['freq'][:5] + "/" + \
                          cc_data_req['freq'][5:]
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
            if not df0.empty and tidy_data:
                df1 = self.wrangle_data_resp(data_req, df0)
                # add ticker to df0 and reset index
                df1['ticker'] = ticker
                df1 = df1.reset_index().set_index(['date', 'ticker']).sort_index()
                # concat df and df1
                df = pd.concat([df, df1])
            elif not df0.empty:
                # add ticker to df0 and reset index
                df0['ticker'] = ticker
                df = pd.concat([df, df0])

        return df

    def fetch_ohlcv(self, data_req: DataRequest, tidy_data=True) -> pd.DataFrame:
        """
        Submits data request to API for OHLCV data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.
        tidy_data: bool, default True
            Wrangles data response into tidy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and open, high, low and close prices (cols).
        """
        # convert data request parameters to CryptoCompare format
        cc_data_req = ConvertParams(data_source='cryptocompare').convert_to_source(data_req)
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
            raise ValueError("Tickers are all indexes. Use '.assets' property to"
                             " see available assets.")

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
                    url = self.base_url + f"v2/{cc_data_req['freq']}"
                    params = {
                        'fsym': ticker,
                        'tsym': cc_data_req['quote_ccy'],
                        'limit': self.max_obs_per_call,
                        'e': cc_data_req['exch'],
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
            if not df0.empty and tidy_data:
                df1 = self.wrangle_data_resp(data_req, df0)
                # add ticker to df0 and reset index
                df1['ticker'] = ticker
                df1 = df1.reset_index().set_index(['date', 'ticker']).sort_index()
                # concat df and df1
                df = pd.concat([df, df1])
            elif not df0.empty:
                # add ticker to df0 and reset index
                df0['ticker'] = ticker
                df = pd.concat([df, df0])

        return df

    def fetch_onchain(self, data_req: DataRequest, tidy_data=True) -> pd.DataFrame:
        """
        Submits data request to API for on-chain data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.
        tidy_data: bool, default True
            Wrangles data response into tidy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and on-chain data (cols).
        """
        # convert data request parameters to CryptoCompare format
        cc_data_req = ConvertParams(data_source='cryptocompare').convert_to_source(data_req)
        # empty df to add data
        df = pd.DataFrame()

        # check if frequency daily
        if cc_data_req['freq'] != 'histoday':
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
            raise ValueError("Tickers are all indexes. Use '.assets' property to"
                             " see available assets.")

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
            if not df0.empty and tidy_data:
                df1 = self.wrangle_data_resp(data_req, df0)
                # add ticker to df0 and reset index
                df1 = df1.reset_index().set_index(['date', 'ticker']).sort_index()
                # concat df and df1
                df = pd.concat([df, df1])
            elif not df0.empty:
                # add ticker to df0 and reset index
                df = pd.concat([df, df0])

        return df

    def fetch_social(self, data_req: DataRequest, tidy_data=True) -> pd.DataFrame:
        """
        Submits data request to API for social stats.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.
        tidy_data: bool, default True
            Wrangles data response into tidy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and social stats (cols).
        """
        # convert data request parameters to CryptoCompare format
        cc_data_req = ConvertParams(data_source='cryptocompare').convert_to_source(data_req)
        # empty df to add data
        df = pd.DataFrame()

        # check frequency
        if cc_data_req['freq'] != 'histoday' and cc_data_req['freq'] != 'histohour':
            raise ValueError(f"Social stats data is only available on a daily and hourly frequency."
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
            raise ValueError("Tickers are all indexes. Use '.assets' property to"
                             " see available assets.")

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
                    url = self.base_url + "social/coin/" + cc_data_req['freq'][:5] + '/' \
                          + cc_data_req['freq'][5:]
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
            if not df0.empty and tidy_data:
                df1 = self.wrangle_data_resp(data_req, df0)
                # add ticker to df0 and reset index
                df1['ticker'] = ticker
                df1 = df1.reset_index().set_index(['date', 'ticker']).sort_index()
                # concat df and df1
                df = pd.concat([df, df1])
            elif not df0.empty:
                # add ticker to df0 and reset index
                df0['ticker'] = ticker
                df = pd.concat([df, df0])

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
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and OHLCV, on-chain and/or social data (cols)
            in tidy format.
        """
        # convert data request parameters to CryptoCompare format
        cc_data_req = ConvertParams(data_source='cryptocompare').convert_to_source(data_req)

        # check if fields available
        fields_list = self.get_fields_info(data_type=None)
        if not all(i in fields_list for i in cc_data_req['fields']):
            raise ValueError("Fields are not available. Use '.fields' property to see available fields.")

        # fields list
        ohlcv_list = self.get_fields_info(data_type='market')
        onchain_list = self.get_fields_info(data_type='on-chain')
        offchain_list = self.get_fields_info(data_type='off-chain')

        # create index tickers list and empty df
        idx_tickers_list, df = self.indexes, pd.DataFrame()

        # fetch indexes data
        if any(i in idx_tickers_list for i in cc_data_req['tickers']) and \
                any(i in ohlcv_list for i in cc_data_req['fields']):
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
            else:
                df = pd.concat([df, df1])

        # fetch on-chain data
        if any(i in onchain_list for i in cc_data_req['fields']):
            try:
                df2 = self.fetch_onchain(data_req)
            except Exception as e:
                logging.warning(e)
            else:
                df = pd.concat([df, df2], axis=1)

        # fetch social stats data
        if any(i in offchain_list for i in cc_data_req['fields']):
            try:
                df3 = self.fetch_social(data_req)
            except Exception as e:
                logging.warning(e)
            else:
                df = pd.concat([df, df3], axis=1)

        # check if df empty
        if df.empty:
            raise Exception('No data returned. Check data request parameters and try again.')

        # filter df for desired fields and sort index by date
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
        df = ConvertParams(data_source='cryptocompare').convert_fields_to_lib(data_req, data_resp)
        # format col order
        if 'volume' in df.columns:  # ohlcv data resp
            df = df.loc[:, ['date', 'open', 'high', 'low', 'close', 'volume']]
        elif 'volume' not in df.columns and 'close' in df.columns:  # indexes data resp
            df = df.loc[:, ['date', 'open', 'high', 'low', 'close']]

        # convert date and set datetimeindex
        df['date'] = pd.to_datetime(df['date'], unit='s')
        df = df.set_index('date').sort_index()

        # filter for desired start to end date
        if data_req.start_date is not None:
            df = df[(df.index >= data_req.start_date)]
        if data_req.end_date is not None:
            df = df[(df.index <= data_req.end_date)]

        # resample freq
        df = df.resample(data_req.freq).last()

        # remove bad data
        df = df[df != 0].dropna(how='all')  # 0 values
        df = df[~df.index.duplicated()]  # duplicate rows
        df.dropna(how='all', inplace=True)  # remove entire row NaNs

        return df
