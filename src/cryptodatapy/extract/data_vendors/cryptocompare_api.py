import logging
import pandas as pd
import requests
from cryptodatapy.extract.data_vendors.datavendor import DataVendor
from cryptodatapy.extract.datarequest import DataRequest
from cryptodatapy.transform.convertparams import ConvertParams
from cryptodatapy.transform.wrangle import WrangleData
from cryptodatapy.util.datacredentials import DataCredentials
from time import sleep
from typing import Optional, Union

# data credentials
data_cred = DataCredentials()


class CryptoCompare(DataVendor):
    """
    Retrieves data from CryptoCompare API.
    """
    def __init__(
            self,
            categories: list[str] = ['crypto'],
            exchanges: Optional[list[str]] = None,
            indexes: Optional[list[str]] = None,
            assets: Optional[list[str]] = None,
            markets: Optional[list[str]] = None,
            market_types: list[str] = ['spot'],
            fields: Optional[list[str]] = None,
            frequencies: list[str] = ['1min', '1h', 'd'],
            base_url: str = data_cred.cryptocompare_base_url,
            api_key: str = data_cred.cryptocompare_api_key,
            max_obs_per_call: int = 2000,
            rate_limit: Optional[pd.DataFrame] = None
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
            List of available markets as asset/quote currency pairs, e.g. ['btcusdt', 'ethbtc'].
        market_types: list
            List of available market types, e.g. [spot', 'perpetual_future', 'future', 'option'].
        fields: list, optional, default None
            List of available fields, e.g. ['open', 'high', 'low', 'close', 'volume'].
        frequencies: list
            List of available frequencies, e.g. ['tick', '1min', '5min', '10min', '20min', '30min', '1h', '2h', '4h',
            '8h', 'd', 'w', 'm']
        base_url: str
            Base url used for GET requests. If not provided, default is set to base_url stored in DataCredentials.
        api_key: str
            Api key, e.g. 'dcf13983adf7dfa79a0dfa35adf'. If not provided, default is set to
            api_key stored in DataCredentials.
        max_obs_per_call: int, default 2,000
            Maximum number of observations returned per API call. If not provided, default is set to
            api_limit stored in DataCredentials.
        rate_limit: pd.DataFrame, optional, Default None
            Number of API calls made and left, by time frequency.
        """
        DataVendor.__init__(self, categories, exchanges, indexes, assets, markets, market_types, fields,
                            frequencies, base_url, api_key, max_obs_per_call, rate_limit)
        # api key
        if api_key is None:
            raise TypeError(f"Set your api key. Alternatively, you can use the function "
                            f"{set_credential.__name__} which uses keyring to store your "
                            f"api key in {DataCredentials.__name__}.")
        self.exchanges = self.get_exchanges_info(as_list=True)
        self.indexes = self.get_indexes_info(as_list=True)
        self.assets = self.get_assets_info(as_list=True)
        self.markets = self.get_markets_info(as_list=True)
        self.fields = self.get_fields_info(data_type=None)
        self.rate_limit = self.get_rate_limit_info()

    def get_exchanges_info(self, as_list: bool = False) -> Union[list[str], pd.DataFrame]:
        """
        Get exchanges info.

        Parameters
        ----------
        as_list: bool, default False
            Returns exchanges info as list.

        Returns
        -------
        exch: list or pd.DataFrame
            List or dataframe with info for supported exchanges.
        """
        # set number of attempts and bool for while loop
        attempts = 0
        # run a while loop in case the attempt fails
        while attempts < 3:

            try:  # try get request
                url = self.base_url + 'exchanges/general'
                params = {
                    'api_key': self.api_key
                }
                r = requests.get(url, params=params)
                assert r.json()['Response'] == 'Success'

            except AssertionError as e:
                logging.warning(e)
                attempts += 1
                sleep(0.1)
                logging.warning(f"Failed to get exchanges info after attempt #{str(attempts)}.")
                if attempts == 3:
                    logging.warning("Failed to get exchanges info after many attempts.")
                    break

            else:
                # format response
                exch = pd.DataFrame(r.json()['Data']).T
                exch.set_index('Name', inplace=True)
                # asset list
                if as_list:
                    exch = list(exch.index)

                return exch

    def get_indexes_info(self, as_list: bool = False) -> Union[list[str], pd.DataFrame]:
        """
        Get indexes info.

        Parameters
        ----------
        as_list: bool, default False
            Returns indexes info as list.

        Returns
        -------
        indexes: list or pd.DataFrame
            List or dataframe with info on available indexes.
        """
        # set number of attempts and bool for while loop
        attempts = 0
        # run a while loop in case the attempt fails
        while attempts < 3:

            try:  # try get request
                url = self.base_url + 'index/list'
                params = {
                    'api_key': self.api_key
                }
                r = requests.get(url, params=params)
                assert r.json()['Response'] == 'Success'

            except AssertionError as e:
                logging.warning(e)
                attempts += 1
                sleep(0.1)
                logging.warning(f"Failed to get indexes info after attempt #{str(attempts)}.")
                if attempts == 3:
                    logging.warning("Failed to get indexes info after many attempts.")
                    break

            else:
                # format response
                indexes = pd.DataFrame(r.json()['Data']).T
                # add index name
                indexes.index.name = 'ticker'
                # asset list
                if as_list:
                    indexes = list(indexes.index)

                return indexes

    def get_assets_info(self, as_list: bool = False) -> Union[list[str], pd.DataFrame]:
        """
        Get assets info.

        Parameters
        ----------
        as_list: bool, default False
            Returns assets info as list.

        Returns
        -------
        assets: list or pd.DataFrame
            List or dataframe with info on available assets.
        """
        # set number of attempts and bool for while loop
        attempts = 0
        # run a while loop in case the attempt fails
        while attempts < 3:

            try:  # try get request
                url = self.base_url + 'all/coinlist'
                params = {
                    'api_key': self.api_key
                }
                r = requests.get(url, params=params)
                assert r.json()['Response'] == 'Success'

            except AssertionError as e:
                logging.warning(e)
                attempts += 1
                sleep(0.1)
                logging.warning(f"Failed to get asset info after attempt #{str(attempts)}.")
                if attempts == 3:
                    logging.warning("Failed to get asset info after many attempts.")
                    break

            else:
                # format response
                assets = pd.DataFrame(r.json()['Data']).T
                # add index name
                assets.index.name = 'ticker'
                # asset list
                if as_list:
                    assets = list(assets.index)

                return assets

    def get_top_market_cap_info(self, n: int = 100) -> list[str]:
        """
        Get list of top assets by market cap.

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

        # set number of attempts and bool for while loop
        attempts = 0
        # run a while loop in case the attempt fails
        while attempts < 3:

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
                attempts += 1
                sleep(0.1)
                logging.warning(f"Failed to get top market cap assets after attempt #{str(attempts)}.")
                if attempts == 3:
                    logging.warning("Failed to get top market cap assets after many attempts.")
                    break

            else:
                # format response
                data = pd.DataFrame(r.json()['Data'])
                # create list of tickers
                tickers = []
                for i in range(0, int(n)):
                    try:
                        ticker = data['RAW'][i]['USD']['FROMSYMBOL']
                        tickers.append(ticker)
                    except Exception as e:
                        logging.info(e)
                        logging.info(f"Failed to get data for ticker #{i}.")

                return tickers

    def get_markets_info(self, as_list: bool = False) -> Union[list[str], dict[str]]:
        """
        Get markets info.

        Parameters
        ----------
        as_list: bool, default False
            Returns markets info as list.

        Returns
        -------
        mkts: dictionary or list
            Dictionary or list with info on available markets.
        """
        # set number of attempts and bool for while loop
        attempts = 0
        # run a while loop in case the attempt fails
        while attempts < 3:

            try:  # try get request
                url = self.base_url + 'v2/cccagg/pairs'
                params = {
                    'api_key': self.api_key
                }
                r = requests.get(url, params=params)
                assert r.json()['Response'] == 'Success'

            except AssertionError as e:
                logging.warning(e)
                attempts += 1
                sleep(0.1)
                logging.warning(f"Failed to get markets info after attempt #{str(attempts)}.")
                if attempts == 3:
                    logging.warning("Failed to get markets info after many attempts.")
                    break

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

    def get_onchain_info(self, as_list: bool = False) -> Union[list[str], pd.DataFrame]:
        """
        Get on-chain data info.

        Parameters
        ----------
        as_list: bool, default False
            Returns available on-chain data as list.

        Returns
        -------
        onchain: list or pd.DataFrame
            list or dataframe with info on available on-chain data.
        """
        # set number of attempts and bool for while loop
        attempts = 0
        # run a while loop in case the attempt fails
        while attempts < 3:

            try:  # try get request
                url = self.base_url + 'blockchain/list'
                params = {
                    'api_key': self.api_key
                }
                r = requests.get(url, params=params)
                assert r.json()['Response'] == 'Success'

            except AssertionError as e:
                logging.warning(e)
                attempts += 1
                sleep(0.1)
                logging.warning(f"Failed to get on-chain info after attempt #{str(attempts)}.")
                if attempts == 3:
                    logging.warning(
                        f"Failed to get on-chain info from Cryptocompare after many attempts.")
                    break

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

    def get_fields_info(self, data_type: Optional[str] = None) -> list[str]:
        """
        Get fields info.

        Parameters
        ----------
        data_type: str, {'market', 'on-chain', 'off-chain'}, default None
            Type of data.

        Returns
        -------
        fields_list: list
            List of available fields.
        """
        ohlcv_list, onchain_list, social_list = ['open', 'high', 'low', 'close', 'volumefrom'], [], []

        # set number of attempts and bool for while loop
        attempts = 0
        # run a while loop in case the attempt fails
        while attempts < 3:

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
                attempts += 1
                sleep(0.1)
                logging.warning(f"Failed to get on-chain info after attempt #{str(attempts)}.")
                if attempts == 3:
                    logging.warning(
                        f"Failed to get on-chain info from Cryptocompare after many attempts.")
                    break

            else:
                # format onchain resp
                for key in list(data_resp):
                    if key not in ['id', 'time', 'symbol', 'partner_symbol']:
                        onchain_list.append(key)
                break

        # set number of attempts and bool for while loop
        attempts = 0
        # run a while loop in case the attempt fails
        while attempts < 3:

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
                attempts += 1
                sleep(0.1)
                logging.warning(f"Failed to get social stats info after attempt #{str(attempts)}.")
                if attempts == 3:
                    logging.warning(
                        f"Failed to get social stats info from Cryptocompare after many attempts.")
                    break

            else:
                # format social resp
                for key in list(data_resp[0]):
                    if key not in ['id', 'time', 'symbol', 'partner_symbol']:
                        social_list.append(key)
                break

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

    def get_rate_limit_info(self) -> pd.DataFrame:
        """
        Get rate limit info.

        Returns
        ------
        rate_limit: pd.DataFrame
            DataFrame with API calls left and made by period (hour, day, month).
        """
        # set number of attempts and bool for while loop
        attempts = 0
        # run a while loop in case the attempt fails
        while attempts < 3:

            try:  # try get request
                url = data_cred.cryptocompare_api_rate_limit
                params = {
                    'api_key': self.api_key
                }
                r = requests.get(url, params=params)
                assert r.json()['Response'] == 'Success'

            except AssertionError as e:
                logging.warning(e)
                attempts += 1
                sleep(0.1)
                logging.warning(f"Failed to get rate limit info after attempt #{str(attempts)}.")
                if attempts == 3:
                    logging.warning(
                        f"Failed to get rate limit info from Cryptocompare after many attempts.")
                    break

            else:
                # format response
                rate_limit = pd.DataFrame(r.json()['Data'])
                # add index name
                rate_limit.index.name = 'frequency'

                return rate_limit

    def get_news(self) -> pd.DataFrame:
        """
        Get news articles from various sources.

        Returns
        -------
        news: pd.DataFrame
            News articles from various sources with title, source, body, ...
        """
        # set number of attempts and bool for while loop
        attempts = 0
        # run a while loop in case the attempt fails
        while attempts < 3:

            try:  # try get request
                url = self.base_url + 'v2/news/?lang=EN'
                params = {
                    'api_key': self.api_key
                }
                r = requests.get(url, params=params)
                assert r.json()['Message'] == 'News list successfully returned'

            except AssertionError as e:
                logging.warning(e)
                attempts += 1
                sleep(0.1)
                logging.warning(f"Failed to get news after attempt #{str(attempts)}.")
                if attempts == 3:
                    logging.warning(
                        f"Failed to get news from Cryptocompare after many attempts.")
                    break

            else:
                news = pd.DataFrame(r.json()['Data'])

                return news

    def get_news_sources(self) -> pd.DataFrame:
        """
        Get news sources.

        Returns
        -------
        news_sources: pd.DataFrame
            News source info.
        """
        # set number of attempts and bool for while loop
        attempts = 0
        # run a while loop in case the attempt fails
        while attempts < 3:

            try:  # try get request
                url = self.base_url + 'news/feeds'
                params = {
                    'api_key': self.api_key
                }
                r = requests.get(url, params=params)

            except Exception as e:
                logging.warning(e)
                attempts += 1
                sleep(0.1)
                logging.warning(f"Failed to get news after attempt #{str(attempts)}.")
                if attempts == 3:
                    logging.warning("Failed to get news after many attempts.")
                    break

            else:
                news_sources = pd.DataFrame(r.json()).set_index('key')

                return news_sources

    def get_indexes(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get indexes data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1) and index values (cols).
        """
        # convert data request parameters to CryptoCompare format
        cc_data_req = ConvertParams(data_req, data_source='cryptocompare').convert_to_source()
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
            raise ValueError(f"Tickers are all assets. Available indexes include: {self.indexes}.")

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

                except Exception as e:
                    logging.warning(e)
                    attempts += 1
                    sleep(cc_data_req['pause'])
                    logging.warning(f"Failed to get data for {ticker} after attempt #{str(attempts)}.")
                    if attempts == 3:
                        logging.warning(
                            f"Failed to get data for {ticker} after many attempts.")
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
                df1 = self.wrangle_data_resp(data_req, df0)
                # add ticker to df0 and reset index
                df1['ticker'] = ticker
                df1 = df1.reset_index().set_index(['date', 'ticker']).sort_index()
                # concat df and df1
                df = pd.concat([df, df1])

        return df

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
        # convert data request parameters to CryptoCompare format
        cc_data_req = ConvertParams(data_req, data_source='cryptocompare').convert_to_source()
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
            raise ValueError(f"Tickers are all indexes. Use assets property to see available assets.")

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

                except Exception as e:
                    logging.warning(e)
                    attempts += 1
                    sleep(cc_data_req['pause'])
                    logging.warning(f"Failed to get data for {ticker} after attempt #{str(attempts)}.")
                    if attempts == 3:
                        logging.warning(
                            f"Failed to get data for {ticker} after many attempts.")
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
                df1 = self.wrangle_data_resp(data_req, df0)
                # add ticker to df0 and reset index
                df1['ticker'] = ticker
                df1 = df1.reset_index().set_index(['date', 'ticker']).sort_index()
                # concat df and df1
                df = pd.concat([df, df1])

        return df

    def get_onchain(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get on-chain data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and values for on-chain fields (cols).
        """
        # convert data request parameters to CryptoCompare format
        cc_data_req = ConvertParams(data_req, data_source='cryptocompare').convert_to_source()
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
            raise ValueError("Tickers are all indexes. Use assets property to see available assets.")

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

                except Exception as e:
                    logging.warning(e)
                    attempts += 1
                    sleep(cc_data_req['pause'])
                    logging.warning(f"Failed to get data for {ticker} after attempt #{str(attempts)}.")
                    if attempts == 3:
                        logging.warning(f"Failed to get data for {ticker} after many attempts.")
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
                df1 = self.wrangle_data_resp(data_req, df0)
                # add ticker to df0 and reset index
                df1 = df1.reset_index().set_index(['date', 'ticker']).sort_index()
                # concat df and df1
                df = pd.concat([df, df1])

        return df

    def get_social(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get social media data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and values for social media fields (cols).
        """
        # convert data request parameters to CryptoCompare format
        cc_data_req = ConvertParams(data_req, data_source='cryptocompare').convert_to_source()
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
            raise ValueError("Tickers are all indexes. Use assets property to see all available assets.")

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

                except Exception as e:
                    attempts += 1
                    sleep(cc_data_req['pause'])
                    logging.warning(e)
                    logging.warning(f"Failed to get data for {ticker} after attempt #{str(attempts)}.")
                    if attempts == 3:
                        logging.warning(
                            f"Failed to get data for {ticker} after many attempts.")
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
                df1 = self.wrangle_data_resp(data_req, df0)
                # add ticker to df0 and reset index
                df1['ticker'] = ticker
                df1 = df1.reset_index().set_index(['date', 'ticker']).sort_index()
                # concat df and df1
                df = pd.concat([df, df1])

        return df

    def get_data(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get either market, on-chain or social media data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and values for OHLCV, on-chain and/or
            social fields (cols), in tidy format.
        """
        # convert data request parameters to CryptoCompare format
        cc_data_req = ConvertParams(data_req, data_source='cryptocompare').convert_to_source()

        # check if fields available
        fields_list = self.get_fields_info(data_type=None)
        if not all(i in fields_list for i in cc_data_req['fields']):
            raise ValueError("Fields are not available. Use fields property to see all available fields.")

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
                df0 = self.get_indexes(data_req)
            except Exception as e:
                logging.warning(e)
            else:
                df = pd.concat([df, df0])

        # fetch OHLCV data
        if any(i in ohlcv_list for i in cc_data_req['fields']):
            try:
                df1 = self.get_ohlcv(data_req)
            except Exception as e:
                logging.warning(e)
            else:
                df = pd.concat([df, df1])

        # fetch on-chain data
        if any(i in onchain_list for i in cc_data_req['fields']):
            try:
                df2 = self.get_onchain(data_req)
            except Exception as e:
                logging.warning(e)
            else:
                df = pd.concat([df, df2], axis=1)

        # fetch social stats data
        if any(i in offchain_list for i in cc_data_req['fields']):
            try:
                df3 = self.get_social(data_req)
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
            Wrangled dataframe with DatetimeIndex (level 0), ticker (level 1), and values for market, on-chain or
            social media fields (cols), in tidy format.
        """
        # wrangle data resp
        df = WrangleData(data_req, data_resp, data_source='cryptocompare').tidy_data()

        return df
