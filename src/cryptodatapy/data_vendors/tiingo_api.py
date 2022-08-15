import pandas as pd
import logging
import requests
from datetime import datetime, timedelta
from time import sleep
from typing import Optional, Union, Any
from cryptodatapy.util.datacredentials import DataCredentials
from cryptodatapy.data_requests.datarequest import DataRequest
from cryptodatapy.util.convertparams import ConvertParams
from cryptodatapy.data_vendors.datavendor import DataVendor


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
            exchanges: list[str] = None,
            assets: list[str] = None,
            indexes: list[str] = None,
            markets: list[str] = None,
            market_types: list[str] = ['spot'],
            fields: dict[str, list[str]] = None,
            frequencies: list[str] = ['1min', '5min', '10min', '15min', '30min',
                                      '1h', '2h', '4h', '8h', 'd', 'w', 'm', 'q', 'y'],
            base_url: str = data_cred.tiingo_base_url,
            api_key: str = data_cred.tiingo_api_key,
            max_obs_per_call: int = 2000,
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

        DataVendor.__init__(self, source_type, categories, exchanges, assets, indexes, markets, market_types, fields,
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
            self.assets = self.get_assets_info(as_list=True)
        # set fields
        if fields is None:
            self.fields = self.get_fields_info()

    def get_exchanges_info(self, cat=None) -> Union[pd.DataFrame, list[str]]:
        """
        Gets exchanges info.

        Parameters
        ----------
        cat: str, {'crypto', 'eqty', 'fx'}, default 'crypto'
            Asset class or time series category, e.g. 'crypto', 'fx', 'macro', 'alt', etc.


        Returns
        -------
        indexes: pd.DataFrame or list
            Info on available exchanges.
        """
        eqty_exch_list = list(self.get_assets_info(cat='eqty').exchange.unique())
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

    def get_assets_info(self, cat='crypto', quote_ccy='usd', as_list=False) -> Union[pd.DataFrame, list[str]]:
        """
        Gets available assets info.

        Parameters
        ----------
        cat: str, {'crypto', 'eqty', 'fx'}, default 'crypto'
            Asset class or time series category, e.g. 'crypto', 'fx', 'macro', 'alt', etc.
        quote_ccy: str, default 'usd'
            Quote currency.
        as_list: bool, default False
            If True, returns available assets as list.

        Returns
        -------
        assets: pd.DataFrame or list
            Info on available assets, by category.
        """
        # crypto

        if cat == 'eqty':
            try:
                assets = pd.read_csv('https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip')
            except Exception as e:
                logging.warning(e)
                raise Exception(f"Failed to get eqty {cat} info.")

        elif cat == 'crypto':
            try:
                url = self.base_url + cat
                headers = {
                    'Content-Type': 'application/json',
                    'Authorization': f"Token {self.api_key}"
                }
                r = requests.get(url, headers=headers)
                r.raise_for_status()
                assets = pd.DataFrame(r.json())
            except Exception as e:
                logging.warning(e)
                raise Exception(f"Failed to get {cat} asset info.")

        elif cat == 'fx':
            url = 'https://api.tiingo.com/documentation/forex'
            raise Exception(f"For more information, see FX documentation: {url}.")

        else:
            raise ValueError(f"Asset info is only available for cat: {self.categories}.")

        # create index
        assets.set_index('ticker', inplace=True)
        # filter by quote ccy
        if quote_ccy is not None and cat == 'crypto':
            assets = assets[assets.quoteCurrency == quote_ccy]
        elif quote_ccy is not None and cat == 'eqty':
            assets = assets[assets.priceCurrency == quote_ccy.upper()]
        # asset list
        if as_list:
            assets = list(assets.index)

        return assets

    @staticmethod
    def get_indexes_info():
        """
        Gets indexes info.
        """
        return None

    @staticmethod
    def get_markets_info():
        """
        Gets market pairs info.
        """
        return None

    @staticmethod
    def get_fields_info(data_type: Optional[str] = 'market', cat=None) -> dict[str, list[str]]:
        """
        Gets fields info.

        Parameters
        ----------
        data_type: str, {'market', 'on-chain', 'off-chain'}, default 'market'
            Type of data.
        cat: str, {'crypto', 'eqty', 'fx'}, default None
            Asset class or time series category, e.g. 'crypto', 'fx', 'macro', 'alt', etc.

        Returns
        -------
        fields_list: dict
            Info on available fields, by category.
        """
        if data_type != 'market':
            raise Exception("No on-chain or off-chain data available."
                            " Tiingo provides market, news and fundamental equity data.")
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
        Gets rate limit info.
        """
        return None

    def get_eqty_daily(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get equity OHLCV data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and OHLCV fields (cols).
        """
        # convert data request parameters to CryptoCompare format
        tg_data_req = ConvertParams(data_source='tiingo').convert_to_source(data_req)
        # empty df to add data
        df = pd.DataFrame()

        # check tickers
        tickers = self.get_assets_info(cat='eqty', as_list=True)
        if not any(ticker.upper() in tickers for ticker in tg_data_req['tickers']):
            raise ValueError("Assets are not available."
                             "Check available assets with get_assets_info(cat='eqty') method.")

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
                    logging.warning(f"Failed to pull data for {ticker} after attempt #{str(attempts)}.")
                    if attempts == tg_data_req['trials']:
                        logging.warning(
                            f"Failed to pull data from Tiingo for {ticker} after many attempts.")
                        break

                else:
                    df0 = pd.DataFrame(r.json())
                    # wrangle data resp
                    if not df0.empty:
                        # wrangle data resp
                        df1 = self.wrangle_data_resp(data_req, df0)
                        # add ticker to index
                        df1['ticker'] = ticker.upper()
                        df1.set_index(['ticker'], append=True, inplace=True)
                        # stack ticker dfs
                        df = pd.concat([df, df1])
                        break
                    else:
                        logging.warning("No data was returned. Check equity tickers.")
                        break

        return df

    def get_eqty_iex(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Submits data request to API for IEX intraday equity OHLCV data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and OHLCV data (cols).
        """
        # convert data request parameters to Tiingo format
        tg_data_req = ConvertParams(data_source='tiingo').convert_to_source(data_req)
        # empty df to add data
        df = pd.DataFrame()

        # check tickers
        tickers = self.get_assets_info(cat='eqty', as_list=True)
        if not any(ticker.upper() in tickers for ticker in tg_data_req['tickers']):
            raise ValueError("Assets are not available."
                             "Check available assets with get_assets_info(cat='eqty') method.")

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
                    logging.warning(f"Failed to pull data for {ticker} after attempt #{str(attempts)}.")
                    if attempts == tg_data_req['trials']:
                        logging.warning(
                            f"Failed to pull data from Tiingo for {ticker} after many attempts.")
                        break

                else:
                    df0 = pd.DataFrame(r.json())
                    # wrangle data resp
                    if not df0.empty:
                        # wrangle data resp
                        df1 = self.wrangle_data_resp(data_req, df0)
                        # add ticker to index
                        df1['ticker'] = ticker.upper()
                        df1.set_index(['ticker'], append=True, inplace=True)
                        # stack ticker dfs
                        df = pd.concat([df, df1])
                        break
                    else:
                        logging.warning("No data was returned. Check equity tickers.")
                        break
        return df

    def get_crypto(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Submits data request to API for crypto OHLCV data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and OHLCV data (cols).
        """
        # convert data request parameters to Tiingo format
        tg_data_req = ConvertParams(data_source='tiingo').convert_to_source(data_req)
        # empty df to add data
        df = pd.DataFrame()

        # check tickers
        tickers = self.assets
        if not any(ticker + tg_data_req['quote_ccy'] in tickers for ticker in tg_data_req['tickers']):
            raise ValueError("Assets are not available."
                             "Check available assets with get_assets_info() method.")

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
                    logging.warning(f"Failed to pull data for {mkt} after attempt #{str(attempts)}.")
                    if attempts == tg_data_req['trials']:
                        logging.warning(
                            f"Failed to pull data from Tiingo for {mkt} after many attempts.")
                        break

                else:
                    df0 = pd.DataFrame(r.json()[0]['priceData'])
                    # wrangle data resp
                    if not df0.empty:
                        # wrangle data resp
                        df1 = self.wrangle_data_resp(data_req, df0)
                        # add ticker to index
                        df1['ticker'] = ticker.upper()
                        df1.set_index(['ticker'], append=True, inplace=True)
                        # stack ticker dfs
                        df = pd.concat([df, df1])
                        break
                    else:
                        logging.warning("No data was returned. Check crypto tickers.")
                        break

        return df

    def get_fx(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Submits data request to API for FX OHLCV data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and OHLCV data (cols).
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
                    logging.warning(f"Failed to pull data for {mkt} after attempt #{str(attempts)}.")
                    if attempts == tg_data_req['trials']:
                        logging.warning(
                            f"Failed to pull data from Tiingo for {mkt} after many attempts.")
                        break

                else:
                    df0 = pd.DataFrame(r.json())
                    # wrangle data resp
                    if not df0.empty:
                        # wrangle data resp
                        df1 = self.wrangle_data_resp(data_req, df0)
                        # add ticker to index
                        df1['ticker'] = mkt.upper()
                        df1.set_index(['ticker'], append=True, inplace=True)
                        # stack ticker dfs
                        df = pd.concat([df, df1])
                        break
                    else:
                        logging.warning("No data was returned. Check fx tickers.")
                        break
        return df

    def get_data(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Gets OHLCV data for any category.

        Parameters
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and OHLCV data (cols).
        """
        # empty df
        df = pd.DataFrame()

        # check cat
        if data_req.cat is None:
            raise ValueError(f"Please provide category. Categories include: {self.categories}.")

        # check fields
        if not any(field in self.fields[data_req.cat] for field in data_req.fields):
            raise ValueError(f"Those fields are not available. Available fields include: {self.fields}.")

        # get eqty intraday OHLCV data
        elif data_req.cat == 'eqty' and \
                data_req.freq in self.frequencies[:self.frequencies.index('d')]:
            try:
                df = self.get_eqty_iex(data_req)
            except Exception as e:
                logging.warning(e)

        # get eqty daily OHLCV data
        elif data_req.cat == 'eqty' and \
                data_req.freq in self.frequencies[self.frequencies.index('d'):]:
            try:
                df = self.get_eqty_daily(data_req)
            except Exception as e:
                logging.warning(e)

        # get crypto OHLCV data
        elif data_req.cat == 'crypto':
            try:
                df = self.get_crypto(data_req)
            except Exception as e:
                logging.warning(e)

        # get fx OHLCV data
        elif data_req.cat == 'fx':
            try:
                df = self.get_fx(data_req)
            except Exception as e:
                logging.warning(e)

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
        df = ConvertParams().convert_dtypes(df)

        return df
