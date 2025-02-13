import logging
from time import sleep
from typing import Dict, Optional, Union, Any, List

import pandas as pd

from cryptodatapy.extract.data_vendors.datavendor import DataVendor
from cryptodatapy.extract.datarequest import DataRequest
from cryptodatapy.transform.convertparams import ConvertParams
from cryptodatapy.transform.wrangle import WrangleData, WrangleInfo
from cryptodatapy.util.datacredentials import DataCredentials

# data credentials
data_cred = DataCredentials()


class CryptoCompare(DataVendor):
    """
    Retrieves data from CryptoCompare API.
    """

    def __init__(
            self,
            categories: List[str] = ['crypto'],
            exchanges: Optional[list[str]] = None,
            indexes: Optional[list[str]] = None,
            assets: Optional[list[str]] = None,
            markets: Optional[list[str]] = None,
            market_types: List[str] = ['spot'],
            fields: Optional[list[str]] = None,
            frequencies: List[str] = ['1min', '1h', 'd'],
            base_url: str = data_cred.cryptocompare_base_url,
            api_endpoints: Dict[str, str] = data_cred.cryptomcompare_endpoints,
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
        api_endpoints: dict, optional, default None
            Dictionary with available API endpoints. If not provided, default is set to api_endpoints stored in
            DataCredentials.
        api_key: str
            Api key, e.g. 'dcf13983adf7dfa79a0dfa35adf'. If not provided, default is set to
            api_key stored in DataCredentials.
        max_obs_per_call: int, default 2,000
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
            raise TypeError("Set your CryptoCompare api key in environment variables as 'CRYPTOCOMPARE_API_KEY' or "
                            "add it as an argument when instantiating the class. To get an api key, visit: "
                            "https://min-api.cryptocompare.com/")
        self.onchain_fields = None
        self.social_fields = None
        self.data_req = None
        self.data_resp = None
        self.data = pd.DataFrame()

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
        self.data_resp = DataRequest().get_req(url=self.base_url + self.api_endpoints[info_type],
                                               params={'api_key': self.api_key})
        return self.data_resp

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
        self.req_meta(info_type='exchanges_info')
        # wrangle data resp
        self.exchanges = WrangleInfo(self.data_resp).cc_exch_info(as_list=as_list)

        return self.exchanges

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
        self.req_meta(info_type='indexes_info')
        # wrangle data resp
        self.indexes = WrangleInfo(self.data_resp).cc_indexes_info(as_list=as_list)

        return self.indexes

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
        self.req_meta(info_type='assets_info')
        # wrangle data resp
        self.assets = WrangleInfo(self.data_resp).cc_assets_info(as_list=as_list)

        return self.assets

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
        self.req_meta(info_type='markets_info')
        # wrangle data resp
        self.markets = WrangleInfo(self.data_resp).cc_mkts_info(as_list=as_list)

        return self.markets

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
        self.req_meta(info_type='on-chain_tickers_info')
        # wrangle data resp
        tickers = WrangleInfo(self.data_resp).cc_onchain_tickers_info(as_list=as_list)

        return tickers

    def get_onchain_info(self) -> list[str]:
        """
        Get on-chain fields info.

        Returns
        -------
        onchain_fields: list
            List of available on-chain fields.
        """
        # data req
        self.req_meta(info_type='on-chain_info')
        # wrangle data resp
        self.onchain_fields = WrangleInfo(self.data_resp).cc_onchain_info()

        return self.onchain_fields

    def get_social_info(self) -> list[str]:
        """
        Get social fields info.

        Returns
        -------
        social_fields: list
            List of available social fields.
        """
        # data req
        self.req_meta(info_type='social_info')
        # wrangle data resp
        self.social_fields = WrangleInfo(self.data_resp).cc_social_info()

        return self.social_fields

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

        # fields
        if data_type == 'market':
            self.fields = ohlcv_list
        elif data_type == 'on-chain':
            self.fields = self.get_onchain_info()
        elif data_type == 'off-chain':
            self.fields = self.get_social_info()
        else:
            self.fields = ohlcv_list + self.get_onchain_info() + self.get_social_info()

        return self.fields

    def req_rate_limit(self) -> Dict[str, Any]:
        """
        Get request for rate limit info.
        """
        self.data_resp = DataRequest().get_req(url=self.base_url.replace('data', 'stats') +
                                               self.api_endpoints['rate_limit_info'],
                                               params={'api_key': self.api_key})
        return self.data_resp

    def get_rate_limit_info(self) -> pd.DataFrame:
        """
        Get rate limit info.

        Returns
        ------
        rate_limit: pd.DataFrame
            DataFrame with API calls left and made by period (hour, day, month).
        """
        # data req
        self.req_rate_limit()
        # wrangle data resp
        self.rate_limit = WrangleInfo(self.data_resp).cc_rate_limit_info()

        return self.rate_limit

    def get_news(self) -> pd.DataFrame:
        """
        Get news articles from various sources.

        Returns
        -------
        news: pd.DataFrame
            News articles from various sources with title, source, body, ...
        """
        # data req
        self.req_meta(info_type='news')
        # wrangle data resp
        news = WrangleInfo(self.data_resp).cc_news()

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
        self.req_meta(info_type='news_sources')
        # wrangle data resp
        news_sources = WrangleInfo(self.data_resp).cc_news_sources()

        return news_sources

    def req_top_mkt_cap(self, n: int = 100) -> Dict[str, Any]:
        """
        Get request for top mkt cap coins info.
        """
        self.data_resp = DataRequest().get_req(
            url=self.base_url + self.api_endpoints['top_mkt_cap_info'],
            params={
                'limit': n,
                'tsym': 'USD',
                'api_key': self.api_key
            }
        )
        return self.data_resp

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
        self.req_top_mkt_cap(n=n)
        # wrangle data resp
        tickers = WrangleInfo(self.data_resp).cc_top_mkt_cap_info()

        return tickers

    def set_urls_params(self, data_req: DataRequest, data_type: str, ticker: str) -> Dict[str, Union[str, Any]]:
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
        self.data_req = ConvertParams(data_req).to_cryptocompare()

        # set params
        if data_type == 'indexes':
            url = (self.base_url + self.api_endpoints['indexes'] + self.data_req.source_freq[:5] + "/" +
                   self.data_req.source_freq[5:])
            params = {
                'indexName': ticker,
                'toTs': self.data_req.source_end_date,
                'limit': self.max_obs_per_call,
                'api_key': self.api_key
            }
        elif data_type == 'ohlcv':
            url = self.base_url + f"v2/{self.data_req.source_freq}"
            params = {
                'fsym': ticker,
                'tsym': self.data_req.quote_ccy,
                'limit': self.max_obs_per_call,
                'e': self.data_req.exch,
                'toTs': self.data_req.source_end_date,
                'api_key': self.api_key
            }

        elif data_type == 'on-chain':
            url = self.base_url + f"blockchain/histo/day?"
            params = {
                'fsym': ticker,
                'limit': self.max_obs_per_call,
                'toTs': self.data_req.source_end_date,
                'api_key': self.api_key
            }

        elif data_type == 'social':
            url = self.base_url + "social/coin/" + self.data_req.source_freq[:5] + '/' + self.data_req.source_freq[5:]
            params = {
                'coinId': int(self.get_assets_info().loc[ticker.upper(), 'Id']),
                'limit': self.max_obs_per_call,
                'toTs': self.data_req.source_end_date,
                'api_key': self.api_key
            }

        else:
            url = self.base_url
            params = {}

        return url, params

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
        url, params = self.set_urls_params(data_req, data_type, ticker)

        # data req
        self.data_req = data_req.get_req(url=url, params=params)

        return self.data_req

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
        self.data_req = ConvertParams(data_req).to_cryptocompare()

        # set params
        url, params = self.set_urls_params(data_req, data_type, ticker)

        # create empty df
        df = pd.DataFrame()
        # while loop condition
        missing_vals = True

        # run a while loop until all data collected
        while missing_vals:

            # data req
            self.data_resp = DataRequest().get_req(url=url, params=params)

            # add data resp to df
            if self.data_resp:
                if data_type == 'indexes' or data_type == 'social':
                    df1 = pd.DataFrame(self.data_resp['Data'])
                else:
                    df1 = pd.DataFrame(self.data_resp['Data']['Data'])
                df = pd.concat([df, df1])  # add data to empty df

                # check if all data has been extracted
                if len(df1) < (self.max_obs_per_call - 1) or df1.time[0] <= self.data_req.source_start_date or \
                        all(df1.drop(columns=['time']).iloc[0] == 0) or \
                        all(df1.drop(columns=['time']).iloc[0].astype(str) == 'nan'):
                    missing_vals = False
                # reset end date and pause before calling API again
                else:
                    params['toTs'] = df1.time[0]
                    sleep(self.data_req.pause)

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
        self.data_req = ConvertParams(data_req).to_cryptocompare()

        # empty df to add data
        df = pd.DataFrame()

        # loop through tickers
        for ticker in self.data_req.source_tickers:

            try:
                df0 = self.get_tidy_data(data_req, data_type, ticker)
            except Exception as e:
                logging.info(f"Failed to get {data_type} data for {ticker} after many attempts: {e}.")
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
        # convert params
        self.data_req = ConvertParams(data_req).to_cryptocompare()

        # get indexes
        try:
            df = self.get_all_tickers(data_req, data_type='indexes')
            return df

        except Exception as e:
            logging.warning(e)

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
        # convert params
        self.data_req = ConvertParams(data_req).to_cryptocompare()

        # get ohlcv
        try:
            df = self.get_all_tickers(data_req, data_type='ohlcv')
            return df

        except Exception as e:
            logging.warning(e)

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
        # convert params
        self.data_req = ConvertParams(data_req).to_cryptocompare()

        # get on-chain
        try:
            df = self.get_all_tickers(data_req, data_type='on-chain')
            return df

        except Exception as e:
            logging.warning(e)

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
        # convert params
        self.data_req = ConvertParams(data_req).to_cryptocompare()

        # get social
        try:
            df = self.get_all_tickers(data_req, data_type='social')
            return df
        except Exception as e:
            logging.warning(e)

    def check_params(self, data_req: DataRequest) -> None:
        """
        Checks the parameters of the data request before requesting data to reduce API calls
        and improve efficiency.
        """
        # convert params
        self.data_req = ConvertParams(data_req).to_cryptocompare()

        # tickers list
        self.get_assets_info(as_list=True)
        self.get_indexes_info(as_list=True)
        tickers_list = self.assets + self.indexes

        # fields
        self.get_fields_info()
        self.get_onchain_info()
        self.get_social_info()

        # tickers
        if not all([ticker in tickers_list for ticker in self.data_req.source_tickers]):
            raise ValueError("Some assets are not available. "
                             "Check available assets and indexes with get_assets_info() or get_indexes_info().")

        # fields
        if not all([field in self.fields for field in self.data_req.source_fields]):
            raise ValueError("Some fields are not available. "
                             "Check available fields with get_fields_info().")

        # check freq
        if self.data_req.source_freq not in ['histoday', 'histohour', 'histominute']:
            raise ValueError(f"Frequency {self.data_req.source_freq} not available. "
                             f"Check available frequencies with get_frequencies().")

        # on-chain freq
        if self.data_req.source_fields in self.onchain_fields and self.data_req.source_freq != 'histoday':
            raise ValueError(f"On-chain data is only available on a daily frequency."
                             f" Change data request frequency to 'd' and try again.")

        # social freq
        if self.data_req.source_fields in self.social_fields and self.data_req.source_freq == 'histominute':
            raise ValueError(f"Social media data is only available on a daily and hourly frequency."
                             f" Change data request frequency to 'd' or '1h' and try again.")

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
        # check data req params
        self.check_params(data_req)

        # get indexes
        if any([ticker in self.indexes for ticker in data_req.source_tickers]):
            try:
                df0 = self.get_indexes(data_req)
            except Exception as e:
                logging.warning(e)
            else:
                self.data = pd.concat([self.data, df0])

        # get ohlcv
        if any([field in ['open', 'high', 'low', 'close', 'volumefrom'] for field in data_req.source_fields]):
            try:
                df1 = self.get_ohlcv(data_req)
            except Exception as e:
                logging.warning(e)
            else:
                self.data = pd.concat([self.data, df1])

        # get onchain
        if any([field in self.onchain_fields for field in data_req.source_fields]):
            try:
                df2 = self.get_onchain(data_req)
            except Exception as e:
                logging.warning(e)
            else:
                self.data = pd.concat([self.data, df2], axis=1)

        # get social
        if any([field in self.social_fields for field in data_req.source_fields]):
            try:
                df3 = self.get_social(data_req)
            except Exception as e:
                logging.warning(e)
            else:
                self.data = pd.concat([self.data, df3], axis=1)

        # check if df empty
        if self.data.empty:
            raise Exception('No data returned. Check data request parameters and try again.')

        # filter df for desired fields and sort index by date
        fields = [field for field in data_req.fields if field in self.data.columns]
        self.data = self.data.loc[:, fields].sort_index()

        return self.data
