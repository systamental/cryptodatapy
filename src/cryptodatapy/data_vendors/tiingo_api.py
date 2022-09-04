import logging
import pandas as pd
import requests
from cryptodatapy.data_requests.datarequest import DataRequest
from cryptodatapy.data_vendors.datavendor import DataVendor
from cryptodatapy.util.convertparams import ConvertParams
from cryptodatapy.util.datacredentials import DataCredentials
# from datetime import datetime, timedelta
from time import sleep
from typing import Optional, Union, Any

# data credentials
data_cred = DataCredentials()


class Tiingo(DataVendor):
    """
    Retrieves data from Tiingo API.
    """
    def __init__(
            self,
            source_type: str = 'data_vendor',
            categories: list[str] = ['crypto', 'fx', 'eqty'],
            exchanges: Optional[dict[str, list[str]]] = None,
            indexes: Optional[dict[str, list[str]]] = None,
            assets: Optional[dict[str, list[str]]] = None,
            markets: Optional[dict[str, list[str]]] = None,
            market_types: list[str] = ['spot'],
            fields: dict[str, list[str]] = None,
            frequencies: list[str] = ['1min', '5min', '10min', '15min', '30min',
                                      '1h', '2h', '4h', '8h', 'd', 'w', 'm', 'q', 'y'],
            base_url: str = data_cred.tiingo_base_url,
            api_key: str = data_cred.tiingo_api_key,
            max_obs_per_call: Optional[int] = None,
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
        api_key: str
            Api key, e.g. 'dcf13983adf7dfa79a0dfa35adf'. If not provided, default is set to
            api_key stored in DataCredentials.
        max_obs_per_call: int, default None
            Maximum number of observations returned per API call. If not provided, default is set to
            api_limit stored in DataCredentials.
        rate_limit: pd.DataFrame, optional, Default None
            Number of API calls made and left, by time frequency.
        """
        DataVendor.__init__(self, source_type, categories, exchanges, indexes, assets, markets, market_types, fields,
                            frequencies, base_url, api_key, max_obs_per_call, rate_limit)
        # api key
        if api_key is None:
            raise TypeError(f"Set your api key. Alternatively, you can use the function "
                            f"{set_credential.__name__} which uses keyring to store your "
                            f"api key in {DataCredentials.__name__}.")
        # set exchanges
        if exchanges is None:
            self.exchanges = self.get_exchanges_info()
        # set assets
        if assets is None:
            self.assets = self.get_assets_info(as_dict=True)
        # set fields
        if fields is None:
            self.fields = self.get_fields_info()

    def get_exchanges_info(self, cat: Optional[str] = None) -> Union[dict[str, list[str]], pd.DataFrame]:
        """
        Get exchanges info.

        Parameters
        ----------
        cat: str, optional, default None
            Category (asset or time series) to filter on.

        Returns
        -------
        exch: dictionary or pd.DataFrame
            Dictionary or dataframe with info for supported exchanges.
        """
        eqty_exch_list = list(self.get_assets_info(cat='eqty', as_dict=False).exchange.unique())
        crypto_exch_list = ['Apeswap', 'ASCENDEX', 'Balancer (Mainnet)', 'Balancer (Polygon)', 'Bancor', 'BHEX',
                            'Bibox', 'Bilaxy', 'Binance', 'Bitfinex', 'Bitflyer', 'Bithumb', 'Bitmart',
                            'Bitstamp', 'Bittrex', 'Bybit', 'GDAX (Coinbase)', 'Cryptopia',
                            'Curve (Including various factory pools)', 'DFYN', 'FTX', 'Gatecoin', 'Gate.io', 'Gemini'
                                                                                                             'HitBTC',
                            'Huobi', 'Indodax', 'Kraken', 'Kucoin', 'LAToken', 'Lbank', 'Lydia', 'MDEX',
                            'MEXC', 'OKex', 'Orca', 'P2PB2B', 'Pancakeswap', 'Pangolin', 'Poloniex', 'Quickswap',
                            'Raydium', 'Saberswap', 'Serum DEX', 'Spiritswap', 'Spookyswap', 'Sushiswap (Mainnet)',
                            'Sushiswap (Polygon)', 'Terraswap', 'Trader Joe', 'UniswapV2', 'UniswapV3 (Mainnet)',
                            'UniswapV3 (Arbitrum)', 'Upbit', 'Wualtswap (Polygon)', 'Yobit']
        # exch dict
        exch = {'crypto': crypto_exch_list, 'eqty': eqty_exch_list}
        # cat
        if cat is not None:
            exch = exch[cat]

        return exch

    @staticmethod
    def get_indexes_info():
        """
        Get indexes info.
        """
        return None

    def get_assets_info(self, cat: Optional[str] = None, as_dict: bool = False) -> \
            Union[dict[str, list[str]], pd.DataFrame]:
        """
        Get assets info.

        Parameters
        ----------
        cat: str, {'crypto', 'eqty', 'fx'}, optional, default None
            Asset class or time series category, e.g. 'crypto', 'fx', 'macro', 'alt', etc.
        as_dict: bool, default False
            Returns assets info as dictionary, by category.

        Returns
        -------
        assets: dictionary or pd.DataFrame
            Dictionary or dataframe with info on available assets, by category.
        """
        # store assets info in dict
        assets_dict = {}

        # eqty assets info
        try:
            assets_dict['eqty'] = pd.read_csv(f"https://apimedia.tiingo.com/docs/tiingo/daily/"
                                              'supported_tickers.zip').set_index('ticker')

        except Exception as e:
            logging.warning(e)
            raise Exception(f"Failed to get eqty {cat} info.")

        # crypto assets info
        try:
            url = self.base_url + 'crypto'
            headers = {
                'Content-Type': 'application/json',
                'Authorization': f"Token {self.api_key}"
            }
            r = requests.get(url, headers=headers)
            r.raise_for_status()
            assets_dict['crypto'] = pd.DataFrame(r.json()).set_index('ticker')

        except Exception as e:
            logging.warning(e)
            raise Exception(f"Failed to get {cat} asset info.")

        # fx assets info
        url = 'https://api.tiingo.com/documentation/forex'
        assets_dict['fx'] = f"For more information, see FX documentation: {url}."

        # as dict
        if as_dict:
            list_dict = {'eqty': [], 'crypto': [], 'fx': []}
            for asset in list_dict.keys():
                if asset == 'fx':
                    list_dict[asset].append(assets_dict[asset])
                else:
                    list_dict[asset].extend(assets_dict[asset].index.to_list())
            assets_dict = list_dict

        # filter cat
        if cat is not None:
            assets_dict = assets_dict[cat]

        return assets_dict

    @staticmethod
    def get_markets_info():
        """
        Get markets info.
        """
        return None

    @staticmethod
    def get_fields_info(cat: Optional[str] = None) -> dict[str, list[str]]:
        """
        Get fields info.

        Parameters
        ----------
        cat: str, {'crypto', 'eqty', 'fx'}, optional, default None
            Asset class or time series category, e.g. 'crypto', 'fx', 'macro', 'alt', etc.

        Returns
        -------
        fields: dictionary
            Dictionary with info on available fields, by category.
        """
        # list of fields
        equities_fields_list = ['open', 'high', 'low', 'close', 'volume', 'open_adj', 'high_adj', 'close_adj',
                                'dividend', 'split']
        crypto_fields_list = ['open', 'high', 'low', 'close', 'trades', 'volume', 'volume_quote_ccy']
        fx_fields_list = ['open', 'high', 'low', 'close']

        # fields dict
        fields = {'crypto': crypto_fields_list,
                  'fx': fx_fields_list,
                  'eqty': equities_fields_list
                  }
        # fields obj
        if cat is not None:
            fields = fields[cat]

        return fields

    @staticmethod
    def get_rate_limit_info():
        """
        Get rate limit info.
        """
        return None

    def get_eqty(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get equities daily data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1) and equities OHLCV values (cols).
        """
        # convert data request parameters to CryptoCompare format
        tg_data_req = ConvertParams(data_source='tiingo').convert_to_source(data_req)
        # empty df to add data
        df = pd.DataFrame()

        # check tickers
        tickers = self.get_assets_info(cat='eqty', as_dict=True)
        if not any(ticker.upper() in tickers for ticker in tg_data_req['tickers']):
            raise ValueError("Assets are not available. To explore available assets use assets property.")

        # loop through tickers
        for ticker in tg_data_req['tickers']:

            # set number of attempts and bool for while loop
            attempts = 0
            # run a while loop to pull ohlcv prices in case the attempt fails
            while attempts < tg_data_req['trials']:

                try:  # try get request
                    url = self.base_url + f"daily/{ticker}/prices"
                    headers = {
                        'Content-Type': 'application/json',
                        'Authorization': f"Token {self.api_key}"
                    }
                    params = {
                        'startDate': tg_data_req['start_date'],
                        'endDate': tg_data_req['end_date'],
                    }
                    r = requests.get(url, headers=headers, params=params)
                    r.raise_for_status()
                    assert len(r.json()) != 0

                except Exception as e:
                    logging.warning(e)
                    attempts += 1
                    sleep(tg_data_req['pause'])
                    logging.warning(f"Failed to get data for {ticker} after attempt #{str(attempts)}.")
                    if attempts == tg_data_req['trials']:
                        logging.warning(
                            f"Failed to get data for {ticker} after many attempts.")
                        break

                else:
                    df0 = pd.DataFrame(r.json())
                    # wrangle data resp
                    df1 = self.wrangle_data_resp(data_req, df0)
                    # add ticker to index
                    df1['ticker'] = ticker.upper()
                    df1.set_index(['ticker'], append=True, inplace=True)
                    # stack ticker dfs
                    df = pd.concat([df, df1])
                    break

        return df

    def get_eqty_iex(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get equities intraday data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1) and equity intrady OHLCV values (cols).
        """
        # convert data request parameters to Tiingo format
        tg_data_req = ConvertParams(data_source='tiingo').convert_to_source(data_req)
        # empty df to add data
        df = pd.DataFrame()

        # check tickers
        tickers = self.get_assets_info(cat='eqty', as_dict=True)
        if not any(ticker.upper() in tickers for ticker in tg_data_req['tickers']):
            raise ValueError("Assets are not available. To explore available assets use assets property.")

        # loop through tickers
        for ticker in tg_data_req['tickers']:

            # set number of attempts and bool for while loop
            attempts = 0
            # run a while loop to pull ohlcv prices in case the attempt fails
            while attempts < tg_data_req['trials']:

                try:  # try get request
                    url = f"https://api.tiingo.com/iex/{ticker}/prices"
                    headers = {
                        'Content-Type': 'application/json',
                        'Authorization': f"Token {self.api_key}"
                    }
                    params = {
                        'startDate': tg_data_req['start_date'],
                        'endDate': tg_data_req['end_date'],
                        'resampleFreq': tg_data_req['freq']
                    }
                    r = requests.get(url, headers=headers, params=params)
                    r.raise_for_status()
                    assert len(r.json()) != 0

                except Exception as e:
                    logging.warning(e)
                    attempts += 1
                    sleep(tg_data_req['pause'])
                    logging.warning(f"Failed to get data for {ticker} after attempt #{str(attempts)}.")
                    if attempts == tg_data_req['trials']:
                        logging.warning(
                            f"Failed to get data for {ticker} after many attempts.")
                        break

                else:
                    df0 = pd.DataFrame(r.json())
                    # wrangle data resp
                    df1 = self.wrangle_data_resp(data_req, df0)
                    # add ticker to index
                    df1['ticker'] = ticker.upper()
                    df1.set_index(['ticker'], append=True, inplace=True)
                    # stack ticker dfs
                    df = pd.concat([df, df1])
                    break

        return df

    def get_crypto(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get crypto data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1) and crypto OHLCV values (cols).
        """
        # convert data request parameters to Tiingo format
        tg_data_req = ConvertParams(data_source='tiingo').convert_to_source(data_req)
        # empty df to add data
        df = pd.DataFrame()

        # check tickers
        tickers = self.get_assets_info(cat='crypto', as_dict=True)
        if not any(ticker + tg_data_req['quote_ccy'] in tickers for ticker in tg_data_req['tickers']):
            raise ValueError("Assets are not available."
                             "To explore available assets use assets property.")

        # loop through tickers
        for ticker, mkt in zip(tg_data_req['tickers'], tg_data_req['mkts']):

            # set number of attempts and bool for while loop
            attempts = 0
            # run a while loop to pull ohlcv prices in case the attempt fails
            while attempts < tg_data_req['trials']:

                try:  # try get request
                    url = self.base_url + 'crypto/prices'
                    headers = {
                        'Content-Type': 'application/json',
                        'Authorization': f"Token {self.api_key}"
                    }
                    params = {
                        'tickers': mkt,
                        'startDate': tg_data_req['start_date'],
                        'endDate': tg_data_req['end_date'],
                        'resampleFreq': tg_data_req['freq']
                    }
                    r = requests.get(url, headers=headers, params=params)
                    r.raise_for_status()
                    assert len(r.json()) != 0

                except Exception as e:
                    logging.warning(e)
                    attempts += 1
                    sleep(tg_data_req['pause'])
                    logging.warning(f"Failed to get data for {mkt} after attempt #{str(attempts)}.")
                    if attempts == tg_data_req['trials']:
                        logging.warning(f"Failed to get data for {mkt} after many attempts.")
                        break

                else:
                    df0 = pd.DataFrame(r.json()[0]['priceData'])
                    # wrangle data resp
                    df1 = self.wrangle_data_resp(data_req, df0)
                    # add ticker to index
                    df1['ticker'] = ticker.upper()
                    df1.set_index(['ticker'], append=True, inplace=True)
                    # stack ticker dfs
                    df = pd.concat([df, df1])
                    break

        return df

    def get_fx(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get FX data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1) and FX OHLC values (cols).
        """
        # convert data request parameters to Tiingo format
        tg_data_req = ConvertParams(data_source='tiingo').convert_to_source(data_req)
        # empty df to add data
        df = pd.DataFrame()

        # loop through tickers
        for ticker, mkt in zip(tg_data_req['tickers'], tg_data_req['mkts']):

            # set number of attempts and bool for while loop
            attempts = 0
            # run a while loop to pull ohlcv prices in case the attempt fails
            while attempts < tg_data_req['trials']:

                try:  # try get request
                    url = self.base_url + f"fx/{mkt}/prices"
                    headers = {
                        'Content-Type': 'application/json',
                        'Authorization': f"Token {self.api_key}"
                    }
                    params = {
                        'tickers': mkt,
                        'startDate': tg_data_req['start_date'],
                        'endDate': tg_data_req['end_date'],
                        'resampleFreq': tg_data_req['freq']
                    }
                    r = requests.get(url, headers=headers, params=params)
                    r.raise_for_status()
                    assert len(r.json()) != 0

                except Exception as e:
                    logging.warning(e)
                    attempts += 1
                    sleep(tg_data_req['pause'])
                    logging.warning(f"Failed to get data for {mkt} after attempt #{str(attempts)}.")
                    if attempts == tg_data_req['trials']:
                        logging.warning(f"Failed to get data for {mkt} after many attempts.")
                        break

                else:
                    df0 = pd.DataFrame(r.json())
                    # wrangle data resp
                    df1 = self.wrangle_data_resp(data_req, df0)
                    # add ticker to index
                    df1['ticker'] = mkt.upper()
                    df1.set_index(['ticker'], append=True, inplace=True)
                    # stack ticker dfs
                    df = pd.concat([df, df1])
                    break

        return df

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
        # empty df
        df = pd.DataFrame()

        # check cat
        if data_req.cat is None:
            raise ValueError(f"Please provide category. Categories include: {self.categories}.")

        # check fields
        if not any(field in self.fields[data_req.cat] for field in data_req.fields):
            raise ValueError(f"Those fields are not available. Available fields include: {self.fields}.")

        # get data
        try:
            # get eqty intraday OHLCV data
            if data_req.cat == 'eqty' and data_req.freq in self.frequencies[:self.frequencies.index('d')]:
                df = self.get_eqty_iex(data_req)
            # get eqty daily OHLCV data
            elif data_req.cat == 'eqty' and data_req.freq in self.frequencies[self.frequencies.index('d'):]:
                df = self.get_eqty(data_req)
            # get crypto OHLCV data
            elif data_req.cat == 'crypto':
                df = self.get_crypto(data_req)
            # get fx OHLCV data
            elif data_req.cat == 'fx':
                df = self.get_fx(data_req)

        except Exception as e:
            logging.warning(e)
            raise Exception('No data returned. Check data request parameters and try again.')

        else:
            # filter df for desired fields and typecast
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
            Data response from data request.

        Returns
        -------
        df: pd.DataFrame
            Wrangled dataframe with DatetimeIndex (level 0), ticker (level 1), and market data
             for selected fields (cols), in tidy format.
        """
        # convert cols to cryptodatapy format
        df = ConvertParams(data_source='tiingo').convert_fields_to_lib(data_req, data_resp)

        # convert date and set datetimeindex
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date').sort_index()
        df.index = df.index.tz_localize(None)

        # resample freq
        df = df.resample(data_req.freq).last()
        # re-format datetimeindex
        if data_req.freq in ['d', 'w', 'm', 'q']:
            df.reset_index(inplace=True)
            df['date'] = pd.to_datetime(df.date.dt.date)
            df.set_index('date', inplace=True)

        # remove dups and NaNs
        df = df[~df.index.duplicated()]  # duplicate rows
        df.dropna(how='all', inplace=True)  # remove entire row NaNs

        # type conversion
        df = df.apply(pd.to_numeric, errors='ignore').convert_dtypes()

        return df
