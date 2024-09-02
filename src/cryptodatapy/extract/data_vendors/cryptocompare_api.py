import logging
from time import sleep
from typing import Dict, Optional, Union, Any

import pandas as pd

from cryptodatapy.extract.data_vendors.datavendor import DataVendor
from cryptodatapy.extract.datarequest import DataRequest
from cryptodatapy.transform.convertparams import ConvertParams
from cryptodatapy.transform.wrangle import WrangleData, WrangleInfo
from cryptodatapy.util.datacredentials import DataCredentials

# data credentials
data_cred = DataCredentials()

# url endpoints
urls = {'exchanges_info': 'exchanges/general', 'indexes_info': 'index/list', 'assets_info': 'all/coinlist',
        'markets_info': 'v2/cccagg/pairs', 'on-chain_tickers_info': 'blockchain/list',
        'on-chain_info': 'blockchain/latest?fsym=BTC', 'social_info': 'social/coin/histo/day',
        'news': 'v2/news/?lang=EN', 'news_sources': 'news/feeds', 'rate_limit_info': 'rate/limit',
        'top_mkt_cap_info': 'top/mktcapfull?', 'indexes': 'index/'}


class CryptoCompare(DataVendor):
    """
    Retrieves data from CryptoCompare API.
    """

    def __init__(
            self,
            categories=None,
            exchanges: Optional[list[str]] = None,
            indexes: Optional[list[str]] = None,
            assets: Optional[list[str]] = None,
            markets: Optional[list[str]] = None,
            market_types=None,
            fields: Optional[list[str]] = None,
            frequencies=None,
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

        if frequencies is None:
            self.frequencies = ['1min', '1h', 'd']
        if market_types is None:
            self.market_types = ['spot']
        if categories is None:
            self.categories = ['crypto']
        if api_key is None:
            raise TypeError("Set your CryptoCompare api key in environment variables as 'CRYPTOCOMPARE_API_KEY' or "
                            "add it as an argument when instantiating the class. To get an api key, visit: "
                            "https://min-api.cryptocompare.com/")
        if exchanges is None:
            self.exchanges = self.get_exchanges_info(as_list=True)
        if indexes is None:
            self.indexes = self.get_indexes_info(as_list=True)
        if assets is None:
            self.assets = self.get_assets_info(as_list=True)
        if markets is None:
            self.markets = self.get_markets_info(as_list=True)
        if fields is None:
            self.fields = self.get_fields_info(data_type=None)
        if rate_limit is None:
            self.rate_limit = self.get_rate_limit_info()

    def req_meta(self, info_type: str) -> Dict[str, Any]:
        """
        Request metadata.

        Parameters
        ----------
        info_type: str,  {'exchanges_info', 'indexes_info', 'assets_info', 'markets_info',
                          'on-chain_tickers_info', 'on-chain_info', 'social_info', 'news',
                          'news_sources', 'rate_limit_info', 'top_mkt_cap_info', 'indexes'}
            Type of metadata to request.

        Returns
        -------
        meta: dictionary
            Metadata in JSON format.
        """
        return DataRequest().get_req(url=self.base_url + urls[info_type], params={'api_key': self.api_key})

    def get_exchanges_info(self, as_list: bool = False) -> Union[list[str], pd.DataFrame]:
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
        # data req
        data_resp = self.req_meta(info_type='exchanges_info')
        # wrangle data resp
        exch = WrangleInfo(data_resp).cc_exch_info(as_list=as_list)

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
        # data req
        data_resp = self.req_meta(info_type='indexes_info')
        # wrangle data resp
        indexes = WrangleInfo(data_resp).cc_indexes_info(as_list=as_list)

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
        # data req
        data_resp = self.req_meta(info_type='assets_info')
        # wrangle data resp
        assets = WrangleInfo(data_resp).cc_assets_info(as_list=as_list)

        return assets

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
        # data req
        data_resp = self.req_meta(info_type='markets_info')
        # wrangle data resp
        mkts = WrangleInfo(data_resp).cc_mkts_info(as_list=as_list)

        return mkts

    def get_onchain_tickers_info(self, as_list: bool = False) -> Union[list[str], pd.DataFrame]:
        """
        Get on-chain data info.

        Parameters
        ----------
        as_list: bool, default False
            Returns available on-chain data as list.

        Returns
        -------
        tickers: list or pd.DataFrame
            list or dataframe with info on tickers with available on-chain data.
        """
        # data req
        data_resp = self.req_meta(info_type='on-chain_tickers_info')
        # wrangle data resp
        tickers = WrangleInfo(data_resp).cc_onchain_tickers_info(as_list=as_list)

        return tickers

    def req_onchain(self) -> Dict[str, Any]:
        """
        Get request for on-chain info.
        """
        return DataRequest().get_req(url=self.base_url + urls['on-chain_info'], params={'api_key': self.api_key})

    def get_onchain_info(self) -> list[str]:
        """
        Get on-chain fields info.

        Returns
        -------
        onchain_fields: list
            List of available on-chain fields.
        """
        # data req
        data_resp = self.req_meta(info_type='on-chain_info')
        # wrangle data resp
        onchain_fields = WrangleInfo(data_resp).cc_onchain_info()

        return onchain_fields

    def get_social_info(self) -> list[str]:
        """
        Get social fields info.

        Returns
        -------
        social_fields: list
            List of available social fields.
        """
        # data req
        data_resp = self.req_meta(info_type='social_info')
        # wrangle data resp
        social_fields = WrangleInfo(data_resp).cc_social_info()

        return social_fields

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
        ohlcv_list = ['open', 'high', 'low', 'close', 'volumefrom']

        # fields list
        if data_type == 'market':
            fields_list = ohlcv_list
        elif data_type == 'on-chain':
            fields_list = self.get_onchain_info()
        elif data_type == 'off-chain':
            fields_list = self.get_social_info()
        else:
            fields_list = ohlcv_list + self.get_onchain_info() + self.get_social_info()

        return fields_list

    def req_rate_limit(self) -> Dict[str, Any]:
        """
        Get request for rate limit info.
        """
        return DataRequest().get_req(url=self.base_url.replace('data', 'stats') + urls['rate_limit_info'],
                                     params={'api_key': self.api_key})

    def get_rate_limit_info(self) -> pd.DataFrame:
        """
        Get rate limit info.

        Returns
        ------
        rate_limit: pd.DataFrame
            DataFrame with API calls left and made by period (hour, day, month).
        """
        # data req
        data_resp = self.req_rate_limit()
        # wrangle data resp
        rate_limit = WrangleInfo(data_resp).cc_rate_limit_info()

        return rate_limit

    def get_news(self) -> pd.DataFrame:
        """
        Get news articles from various sources.

        Returns
        -------
        news: pd.DataFrame
            News articles from various sources with title, source, body, ...
        """
        # data req
        data_resp = self.req_meta(info_type='news')
        # wrangle data resp
        news = WrangleInfo(data_resp).cc_news()

        return news

    def get_news_sources(self) -> pd.DataFrame:
        """
        Get news sources.

        Returns
        -------
        news_sources: pd.DataFrame
            News source info.
        """
        # data req
        data_resp = self.req_meta(info_type='news_sources')
        # wrangle data resp
        news_sources = WrangleInfo(data_resp).cc_news_sources()

        return news_sources

    def req_top_mkt_cap(self, n: int = 100) -> Dict[str, Any]:
        """
        Get request for top mkt cap coins info.
        """
        return DataRequest().get_req(url=self.base_url + urls['top_mkt_cap_info'],
                                     params={
                                         'limit': n,
                                         'tsym': 'USD',
                                         'api_key': self.api_key
                                     })

    def get_top_mkt_cap_info(self, n: int = 100) -> list[str]:
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

        # date req
        data_resp = self.req_top_mkt_cap(n=n)
        # wrangle data resp
        tickers = WrangleInfo(data_resp).cc_top_mkt_cap_info()

        return tickers

    def set_urls_params(self, data_req: DataRequest, data_type: str, ticker: str) -> Dict[str, Union[str, int]]:
        """
        Sets url and params for get request.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.
        data_type: str, {'indexes', 'ohlcv', 'on-chain', 'social'}
            Data type to retrieve.
        ticker: str
            Ticker symbol.

        Returns
        -------
        dict: dictionary
            Dictionary with url and params values for get request.

        """
        # convert data req params
        cc_data_req = ConvertParams(data_req).to_cryptocompare()

        # set params
        if data_type == 'indexes':
            url = self.base_url + urls['indexes'] + cc_data_req['freq'][:5] + "/" + cc_data_req['freq'][5:]
            params = {
                'indexName': ticker,
                'toTs': cc_data_req['end_date'],
                'limit': self.max_obs_per_call,
                'api_key': self.api_key
            }
        elif data_type == 'ohlcv':
            url = self.base_url + f"v2/{cc_data_req['freq']}"
            params = {
                'fsym': ticker,
                'tsym': cc_data_req['quote_ccy'],
                'limit': self.max_obs_per_call,
                'e': cc_data_req['exch'],
                'toTs': cc_data_req['end_date'],
                'api_key': self.api_key
            }

        elif data_type == 'on-chain':
            url = self.base_url + f"blockchain/histo/day?"
            params = {
                'fsym': ticker,
                'limit': self.max_obs_per_call,
                'toTs': cc_data_req['end_date'],
                'api_key': self.api_key
            }

        elif data_type == 'social':
            url = self.base_url + "social/coin/" + cc_data_req['freq'][:5] + '/' + cc_data_req['freq'][5:]
            params = {
                'coinId': int(self.get_assets_info().loc[ticker.upper(), 'Id']),
                'limit': self.max_obs_per_call,
                'toTs': cc_data_req['end_date'],
                'api_key': self.api_key
            }

        else:
            url = self.base_url
            params = {}

        return {'url': url, 'params': params}

    def req_data(self, data_req: DataRequest, data_type: str, ticker: str) -> Dict[str, Any]:
        """
        Submits get request to CryptoCompare API.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.
        data_type: str, {'indexes', 'ohlcv', 'on-chain', 'social'}
            Data type to retrieve.
        ticker: str
            Ticker symbol.|

        Returns
        -------
        data_resp: dict
            Data response in JSON format.
        """
        # set params
        urls_params = self.set_urls_params(data_req, data_type, ticker)
        url, params = urls_params['url'], urls_params['params']

        # data req
        data_resp = DataRequest().get_req(url=url, params=params)

        return data_resp

    def get_all_data_hist(self, data_req: DataRequest, data_type: str, ticker: str) -> pd.DataFrame:
        """
        Submits get requests to API until entire data history has been collected. Only necessary when
        number of observations is larger than the maximum number of observations per call.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.
        data_type: str, {'indexes', 'ohlcv', 'on-chain', 'social'}
            Data type to retrieve.
        ticker: str
            Ticker symbol.

        Returns
        -------
        df: pd.DataFrame
            Dataframe with entire data history retrieved.

        """
        # convert data req params
        cc_data_req = ConvertParams(data_req).to_cryptocompare()

        # set params
        urls_params = self.set_urls_params(data_req, data_type, ticker)
        url, params = urls_params['url'], urls_params['params']

        # create empty df
        df = pd.DataFrame()
        # while loop condition
        missing_vals = True

        # run a while loop until all data collected
        while missing_vals:

            # data req
            data_resp = DataRequest().get_req(url=url, params=params)

            # add data resp to df
            if data_resp:
                if data_type == 'indexes' or data_type == 'social':
                    df1 = pd.DataFrame(data_resp['Data'])
                else:
                    df1 = pd.DataFrame(data_resp['Data']['Data'])
                df = pd.concat([df, df1])  # add data to empty df

                # check if all data has been extracted
                if len(df1) < (self.max_obs_per_call - 1) or df1.time[0] <= cc_data_req['start_date'] or \
                        all(df1.drop(columns=['time']).iloc[0] == 0) or \
                        all(df1.drop(columns=['time']).iloc[0].astype(str) == 'nan'):
                    missing_vals = False
                # reset end date and pause before calling API again
                else:
                    # change end date
                    params['toTs'] = df1.time[0]
                    sleep(0.1)

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

        return WrangleData(data_req, data_resp).cryptocompare()

    def get_tidy_data(self, data_req: DataRequest, data_type: str, ticker: str) -> pd.DataFrame:
        """
        Gets entire data history and wrangles the data response into tidy data format.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.
        data_type: str, {'indexes', 'ohlcv', 'on-chain', 'social'}
            Data type to retrieve.
        ticker: str
            Ticker symbol.

        Returns
        -------
        df: pd.DataFrame
            Dataframe with entire data history retrieved and wrangled into tidy data format.
        """
        # get entire data history
        df = self.get_all_data_hist(data_req, data_type, ticker)
        # wrangle df
        df = self.wrangle_data_resp(data_req, df)

        return df

    def get_all_tickers(self, data_req: DataRequest, data_type: str) -> pd.DataFrame:
        """
        Loops list of tickers, retrieves data in tidy format for each ticker and stores it in a
        multiindex dataframe.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.
        data_type: str, {'indexes', 'ohlcv', 'on-chain', 'social'}
            Data type to retrieve.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            Dataframe with DatetimeIndex (level 0), ticker (level 1) and values for fields (cols), in tidy data format.
        """
        # convert data request parameters to CryptoCompare format
        cc_data_req = ConvertParams(data_req).to_cryptocompare()

        # empty df to add data
        df = pd.DataFrame()

        # loop through tickers
        for ticker in cc_data_req['tickers']:

            try:
                df0 = self.get_tidy_data(data_req, data_type, ticker)
            except Exception:
                logging.info(f"Failed to get {data_type} data for {ticker} after many attempts.")
            else:
                # add ticker to index
                df0['ticker'] = ticker
                df0.set_index(['ticker'], append=True, inplace=True)
                # concat df and df1
                df = pd.concat([df, df0])

        return df

    def get_indexes(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Gets indexes data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and index values (cols), in tidy format.
        """
        # convert data request parameters to CryptoCompare format
        cc_data_req = ConvertParams(data_req).to_cryptocompare()

        # fields list
        market_fields = self.get_fields_info(data_type='market')

        # check if tickers and fields are correct
        if any([ticker in self.indexes for ticker in cc_data_req['tickers']]) and \
                any([field in market_fields for field in cc_data_req['fields']]):
            try:
                df = self.get_all_tickers(data_req, data_type='indexes')

            except Exception as e:
                logging.warning(e)

            else:
                return df

    def get_ohlcv(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Gets OHLCV data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and OHLCV values (cols), in tidy format.
        """
        # convert data request parameters to CryptoCompare format
        cc_data_req = ConvertParams(data_req).to_cryptocompare()

        # fields list
        market_fields = self.get_fields_info(data_type='market')

        # check if tickers and fields are correct
        if any([ticker in self.assets for ticker in cc_data_req['tickers']]) and \
                any([field in market_fields for field in cc_data_req['fields']]):
            try:
                df = self.get_all_tickers(data_req, data_type='ohlcv')

            except Exception as e:
                logging.warning(e)

            else:
                return df

    def get_onchain(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Gets on chain data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and values for on-chain fields (cols),
            in tidy format.
        """
        # convert data request parameters to CryptoCompare format
        cc_data_req = ConvertParams(data_req).to_cryptocompare()

        # check if frequency daily
        if cc_data_req['freq'] != 'histoday':
            raise ValueError(f"On-chain data is only available on a daily frequency."
                             f" Change data request frequency to 'd' and try again.")

        # fields list
        onchain_fields = self.get_fields_info(data_type='on-chain')

        # check if tickers and fields are correct
        if any([ticker in self.assets for ticker in cc_data_req['tickers']]) and \
                any([field in onchain_fields for field in cc_data_req['fields']]):
            try:
                df = self.get_all_tickers(data_req, data_type='on-chain')

            except Exception as e:
                logging.warning(e)

            else:
                return df

    def get_social(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Gets social media data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and values for social media fields (cols),
            in tidy format.
        """
        # convert data request parameters to CryptoCompare format
        cc_data_req = ConvertParams(data_req).to_cryptocompare()

        # check frequency
        if cc_data_req['freq'] == 'histominute':
            raise ValueError(f"Social media data is only available on a daily and hourly frequency."
                             f" Change data request frequency to 'd' or '1h' and try again.")

        # fields list
        social_fields = self.get_fields_info(data_type='off-chain')

        # check if tickers and fields are correct
        if any([ticker in self.assets for ticker in cc_data_req['tickers']]) and \
                any([field in social_fields for field in cc_data_req['fields']]):
            try:
                df = self.get_all_tickers(data_req, data_type='social')

            except Exception as e:
                logging.warning(e)

            else:
                return df

    def check_params(self, data_req: DataRequest) -> None:
        """
        Checks the parameters of the data request before requesting data to reduce API calls
        and improve efficiency.
        """
        # convert data request parameters to CryptoCompare format
        cc_data_req = ConvertParams(data_req).to_cryptocompare()

        # tickers lists
        assets_list = self.assets
        idx_list = self.indexes
        tickers_list = assets_list + idx_list

        # check tickers and fields
        if not all([ticker in tickers_list for ticker in cc_data_req['tickers']]):
            raise ValueError("Some assets are not available. "
                             "Check assets and indexes attributes to see all available asset and indexes.")

        if len(cc_data_req['fields']) == 0:
            raise ValueError("Some fields are not available. "
                             "Check fields attribute to see all available fields.")

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
        logging.info("Retrieving data request from CryptoCompare...")

        # check data req params
        self.check_params(data_req)

        # df to store data
        df = pd.DataFrame()

        # get indexes
        try:
            df0 = self.get_indexes(data_req)
        except Exception as e:
            logging.warning(e)
        else:
            df = pd.concat([df, df0])

        # get ohlcv
        try:
            df1 = self.get_ohlcv(data_req)
        except Exception as e:
            logging.warning(e)
        else:
            df = pd.concat([df, df1])

        # get onchain
        try:
            df2 = self.get_onchain(data_req)
        except Exception as e:
            logging.warning(e)
        else:
            df = pd.concat([df, df2], axis=1)

        # get social
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

        logging.info("Data retrieved from CryptoCompare.")

        return df.sort_index()
