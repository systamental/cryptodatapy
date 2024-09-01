import logging
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from cryptodatapy.extract.data_vendors.datavendor import DataVendor
from cryptodatapy.extract.datarequest import DataRequest
from cryptodatapy.transform.convertparams import ConvertParams
from cryptodatapy.transform.wrangle import WrangleData
from cryptodatapy.util.datacredentials import DataCredentials

# data credentials
data_cred = DataCredentials()


class Tiingo(DataVendor):
    """
    Retrieves data from Tiingo API.
    """

    def __init__(
            self,
            categories=None,
            exchanges: Optional[Dict[str, List[str]]] = None,
            indexes: Optional[Dict[str, List[str]]] = None,
            assets: Optional[Dict[str, List[str]]] = None,
            markets: Optional[Dict[str, List[str]]] = None,
            market_types=None,
            fields: Dict[str, List[str]] = None,
            frequencies=None,
            base_url: str = data_cred.tiingo_base_url,
            api_key: str = data_cred.tiingo_api_key,
            max_obs_per_call: Optional[int] = None,
            rate_limit: Optional[Any] = None,
    ):
        """
        Constructor

        Parameters
        ----------
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
        DataVendor.__init__(
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

        if frequencies is None:
            self.frequencies = [
                "1min",
                "5min",
                "10min",
                "15min",
                "30min",
                "1h",
                "2h",
                "4h",
                "8h",
                "d",
                "w",
                "m",
                "q",
                "y",
            ]
        if market_types is None:
            self.market_types = ["spot"]
        if categories is None:
            self.categories = ["crypto", "fx", "eqty"]
        if api_key is None:
            raise TypeError("Set your Tiingo api key in environment variables as 'TIINGO_API_KEY' or "
                            "add it as an argument when instantiating the class. To get an api key, visit: "
                            "https://www.tiingo.com/")
        if exchanges is None:
            self.exchanges = self.get_exchanges_info()
        if assets is None:
            self.assets = self.get_assets_info(as_list=True)
        if fields is None:
            self.fields = self.get_fields_info()

    def get_exchanges_info(
            self, cat: Optional[str] = None
    ) -> Union[Dict[str, List[str]], pd.DataFrame]:
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
        eqty_exch_list = list(self.get_assets_info(cat="eqty").exchange.unique())
        crypto_exch_list = [
            "Apeswap",
            "ASCENDEX",
            "Balancer (Mainnet)",
            "Balancer (Polygon)",
            "Bancor",
            "BHEX",
            "Bibox",
            "Bilaxy",
            "Binance",
            "Bitfinex",
            "Bitflyer",
            "Bithumb",
            "Bitmart",
            "Bitstamp",
            "Bittrex",
            "Bybit",
            "GDAX (Coinbase)",
            "Cryptopia",
            "Curve (Including various factory pools)",
            "DFYN",
            "FTX",
            "Gatecoin",
            "Gate.io",
            "Gemini" "HitBTC",
            "Huobi",
            "Indodax",
            "Kraken",
            "Kucoin",
            "LAToken",
            "Lbank",
            "Lydia",
            "MDEX",
            "MEXC",
            "OKex",
            "Orca",
            "P2PB2B",
            "Pancakeswap",
            "Pangolin",
            "Poloniex",
            "Quickswap",
            "Raydium",
            "Saberswap",
            "Serum DEX",
            "Spiritswap",
            "Spookyswap",
            "Sushiswap (Mainnet)",
            "Sushiswap (Polygon)",
            "Terraswap",
            "Trader Joe",
            "UniswapV2",
            "UniswapV3 (Mainnet)",
            "UniswapV3 (Arbitrum)",
            "Upbit",
            "Wualtswap (Polygon)",
            "Yobit",
        ]
        # exch dict
        exch = {"crypto": crypto_exch_list, "eqty": eqty_exch_list}
        # cat
        if cat is not None:
            exch = exch[cat]

        return exch

    def get_indexes_info(self) -> None:
        """
        Get indexes info.
        """
        return None

    @staticmethod
    def get_eqty_info(as_list: bool = False) -> Union[List[str], pd.DataFrame]:
        """
        Get equity markets info.

        Parameters
        ----------
        as_list: bool, default False
            Returns eqty info as list.

        Returns
        -------
        eqty: list or pd.DataFrame
            List or dataframe with info on available equities.
        """
        # eqty info
        try:
            eqty = pd.read_csv(
                f"https://apimedia.tiingo.com/docs/tiingo/daily/"
                "supported_tickers.zip"
            ).set_index("ticker")

        except Exception as e:
            logging.warning(e)
            raise Exception(f"Failed to get eqty info.")

        # as list
        if as_list:
            eqty = eqty.index.to_list()

        return eqty

    def req_crypto(self) -> Dict[str, Any]:
        """
        Submit get request for crypto metadata.
        """
        # req crypto asset info
        return DataRequest().get_req(url=self.base_url + 'crypto', params={},
                                     headers={
                                         "Content-Type": "application/json",
                                         "Authorization": f"Token {self.api_key}",
                                     })

    def get_crypto_info(self, as_list: bool = False) -> Union[List[str], pd.DataFrame]:
        """
        Get cryptoassets info.

        Parameters
        ----------
        as_list: bool, default False
            Returns cryptoasset info as list.

        Returns
        -------
        crypto: list or pd.DataFrame
            List or dataframe with info on available cryptoassets.
        """
        # data req
        data_resp = self.req_crypto()
        # wrangle data resp
        crypto = pd.DataFrame(data_resp).set_index('ticker')
        # as list
        if as_list:
            crypto = crypto.index.to_list()

        return crypto

    def get_assets_info(
            self, cat: Optional[str] = None, as_list: bool = False
    ) -> Union[Dict[str, List[str]], pd.DataFrame]:
        """
        Get assets info.

        Parameters
        ----------
        cat: str, {'crypto', 'eqty', 'fx'}, optional, default None
            Asset class or time series category, e.g. 'crypto', 'fx', etc.
        as_list: bool, default False
            Returns asset info as list.

        Returns
        -------
        assets_info: dictionary
            Dictionary of dataframes or lists with info on available assets, by category.
        """
        eqty = self.get_eqty_info()  # eqty info
        crypto = self.get_crypto_info()  # crypto info
        fx_url = "https://api.tiingo.com/documentation/forex"  # fx info

        # as list
        if as_list:
            eqty = eqty.index.to_list()
            crypto = crypto.index.to_list()

        # add to dict
        assets_info = {'eqty': eqty,
                       'crypto': crypto,
                       'fx': f"For more information, see FX documentation: {fx_url}."}

        # filter cat
        if cat is not None:
            assets_info = assets_info[cat]

        return assets_info

    def get_markets_info(self) -> None:
        """
        Get markets info.
        """
        return None

    def get_fields_info(self, cat: Optional[str] = None) -> Dict[str, List[str]]:
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
        equities_fields_list = [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "open_adj",
            "high_adj",
            "close_adj",
            "dividend",
            "split",
        ]
        crypto_fields_list = [
            "open",
            "high",
            "low",
            "close",
            "trades",
            "volume",
            "volume_quote_ccy",
        ]
        fx_fields_list = ["open", "high", "low", "close"]

        # fields dict
        fields = {
            "crypto": crypto_fields_list,
            "fx": fx_fields_list,
            "eqty": equities_fields_list,
        }
        # fields obj
        if cat is not None:
            fields = fields[cat]

        return fields

    def get_rate_limit_info(self) -> None:
        """
        Get rate limit info.
        """
        return None

    def set_urls_params(self, data_req: DataRequest, data_type: str, ticker: str) -> Dict[str, Union[str, int]]:
        """
        Sets url and params for get request.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.
        data_type: str, {'eqty', 'iex', 'crypto', 'fx'}
            Data type to retrieve.
        ticker: str
            Ticker symbol.

        Returns
        -------
        dict: dictionary
            Dictionary with url and params values for get request.

        """
        # convert data req params
        tg_data_req = ConvertParams(data_req).to_tiingo()

        url, params, headers = None, {}, {}

        # eqty daily
        if data_type == 'eqty':
            url = self.base_url + f"daily/prices"
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Token {self.api_key}",
            }
            params = {
                "tickers": ticker,
                "startDate": tg_data_req["start_date"],
                "endDate": tg_data_req["end_date"],
            }

        # eqty intraday
        elif data_type == 'iex':
            url = f"https://api.tiingo.com/iex/{ticker}/prices"
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Token {self.api_key}",
            }
            params = {
                "startDate": tg_data_req["start_date"],
                "endDate": tg_data_req["end_date"],
                "resampleFreq": tg_data_req["freq"],
            }

        # crypto
        elif data_type == 'crypto':
            url = self.base_url + "crypto/prices"
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Token {self.api_key}",
            }
            params = {
                "tickers": ticker,
                "startDate": tg_data_req["start_date"],
                "endDate": tg_data_req["end_date"],
                "resampleFreq": tg_data_req["freq"],
            }

        # fx
        elif data_type == 'fx':
            # url = f"https://api.tiingo.com/tiingo/fx/{ticker}/prices"
            url = self.base_url + f"fx/prices"
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Token {self.api_key}",
            }
            params = {
                "tickers": ticker,
                "startDate": tg_data_req["start_date"],
                "endDate": tg_data_req["end_date"],
                "resampleFreq": tg_data_req["freq"],
            }

        return {'url': url, 'params': params, 'headers': headers}

    def req_data(self, data_req: DataRequest, data_type: str, ticker: str) -> Dict[str, Any]:
        """
        Submits get request to Tiingo API.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.
        data_type: str, {'eqty', 'iex', 'crypto', 'fx'}
            Data type to retrieve.
        ticker: str
            Ticker symbol.

        Returns
        -------
        data_resp: dict
            Data response in JSON format.
        """
        # set params
        urls_params = self.set_urls_params(data_req, data_type, ticker)
        url, params, headers = urls_params['url'], urls_params['params'], urls_params['headers']

        # data req
        data_resp = DataRequest().get_req(url=url, params=params, headers=headers)

        return data_resp

    @staticmethod
    def wrangle_data_resp(data_req: DataRequest, data_resp: Dict[str, Any], data_type: str) -> pd.DataFrame:
        """
        Wrangle data response.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.
        data_resp: dictionary
            Data response from data request in JSON format.
        data_type: str, {'eqty', 'iex', 'crypto', 'fx'}
            Data type retrieved.

        Returns
        -------
        df: pd.DataFrame
            Wrangled dataframe with DatetimeIndex and market data for selected fields (cols), in tidy format.
        """
        # wrangle data resp
        df = WrangleData(data_req, data_resp).tiingo(data_type)

        return df

    def get_tidy_data(self, data_req: DataRequest, data_type: str, ticker: str) -> pd.DataFrame:
        """
        Submits data request and wrangles the data response into tidy data format.

        Parameters
        ----------
        data_req: DataRequest
            Data request parameters in CryptoDataPy format.
        data_type: str, {'eqty', 'iex', 'crypto', 'fx'}
            Data type to retrieve.
        ticker: str
            Requested ticker symbol.

        Returns
        -------
        df: pd.DataFrame
            Dataframe with DatetimeIndex and field values (col) wrangled into tidy data format.
        """
        # get entire data history
        df = self.req_data(data_req, data_type, ticker)
        # wrangle df
        df = self.wrangle_data_resp(data_req, df, data_type)

        return df

    def get_all_tickers(self, data_req: DataRequest, data_type: str) -> pd.DataFrame:
        """
        Loops list of tickers, retrieves data in tidy format for each ticker and stores it in a
        multiindex dataframe.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.
        data_type: str, {'eqty', 'iex', 'crypto', 'fx'}
            Data type to retrieve.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            Dataframe with DatetimeIndex (level 0), ticker (level 1) and values for fields (cols), in tidy data format.
        """
        # convert data request parameters to CryptoCompare format
        tg_data_req = ConvertParams(data_req).to_tiingo()

        # empty df to add data
        df = pd.DataFrame()

        if data_type == 'crypto':
            # loop through mkts
            for mkt, ticker in zip(tg_data_req['mkts'], data_req.tickers):
                try:
                    df0 = self.get_tidy_data(data_req, data_type, mkt)
                    print(df0)
                except Exception:
                    logging.info(f"Failed to get {data_type} data for {ticker} after many attempts.")
                else:
                    # add ticker to index
                    df0['ticker'] = ticker.upper()
                    df0.set_index(['ticker'], append=True, inplace=True)
                    # concat df and df1
                    df = pd.concat([df, df0])

        elif data_type == 'fx':
            # loop through mkts
            for mkt, ticker in zip(tg_data_req['mkts'], data_req.tickers):
                try:
                    df0 = self.get_tidy_data(data_req, data_type, mkt)
                except Exception:
                    logging.info(f"Failed to get {data_type} data for {mkt} after many attempts.")
                else:
                    # add ticker to index
                    df0['ticker'] = mkt.upper()
                    df0.set_index(['ticker'], append=True, inplace=True)
                    # concat df and df1
                    df = pd.concat([df, df0])

        else:
            # loop through mkts
            for tg_ticker, dr_ticker in zip(tg_data_req['tickers'], data_req.tickers):
                try:
                    df0 = self.get_tidy_data(data_req, data_type, tg_ticker)
                except Exception:
                    logging.info(f"Failed to get {data_type} data for {dr_ticker} after many attempts.")
                else:
                    # add ticker to index
                    df0['ticker'] = dr_ticker.upper()
                    df0.set_index(['ticker'], append=True, inplace=True)
                    # concat df and df1
                    df = pd.concat([df, df0])

        return df

    def get_eqty(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get daily eqty data.

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
        tg_data_req = ConvertParams(data_req).to_tiingo()

        # check tickers
        if any([ticker.upper() in self.assets['eqty'] for ticker in tg_data_req['tickers']]) and \
                any([field in self.fields['eqty'] for field in data_req.fields]):
            try:
                df = self.get_all_tickers(data_req, data_type='eqty')

            except Exception as e:
                logging.warning(e)

            else:
                return df

    def get_eqty_iex(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get daily eqty data.

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
        tg_data_req = ConvertParams(data_req).to_tiingo()

        # check tickers
        if any([ticker.upper() in self.assets['eqty'] for ticker in tg_data_req['tickers']]) and \
                any([field in self.fields['eqty'] for field in data_req.fields]):
            try:
                df = self.get_all_tickers(data_req, data_type='iex')

            except Exception as e:
                logging.warning(e)

            else:
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
        # convert data request parameters to CryptoCompare format
        tg_data_req = ConvertParams(data_req).to_tiingo()

        # check tickers
        if any([ticker in self.assets['crypto'] for ticker in tg_data_req['mkts']]) and \
                any([field in self.fields['crypto'] for field in data_req.fields]):

            try:
                df = self.get_all_tickers(data_req, data_type='crypto')

            except Exception as e:
                logging.warning(e)

            else:
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
        # convert data request parameters to CryptoCompare format
        tg_data_req = ConvertParams(data_req).to_tiingo()

        # check tickers
        if any([field in self.fields['fx'] for field in data_req.fields]):

            try:
                df = self.get_all_tickers(data_req, data_type='fx')

            except Exception as e:
                logging.warning(e)

            else:
                return df

    def check_params(self, data_req: DataRequest) -> None:
        """
        Checks the parameters of the data request before requesting data to reduce API calls
        and improve efficiency.

        """
        tg_data_req = ConvertParams(data_req).to_tiingo()

        # check cat
        if data_req.cat is None:
            raise ValueError(
                f"Cat cannot be None. Please provide category. Categories include: {self.categories}."
            )

        # check assets
        if not any([ticker.upper() in self.assets[data_req.cat] for ticker in tg_data_req['tickers']]) and \
                data_req.cat != 'fx':
            raise ValueError(
                f"Selected tickers are not available. Use assets attribute to see available tickers."
            )

            # check fields
        if not any([field in self.fields[data_req.cat] for field in data_req.fields]):
            raise ValueError(
                f"Selected fields are not available. Use fields attribute to see available fields."
            )

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
        logging.info("Retrieving data request from Tiingo...")

        # check data req params
        self.check_params(data_req)

        # df to store data
        df = pd.DataFrame()

        # get data
        try:
            # get eqty intraday OHLCV data
            if (
                    data_req.cat == "eqty"
                    and data_req.freq in self.frequencies[:self.frequencies.index('d')]
            ):
                df = self.get_eqty_iex(data_req)

            # get eqty daily OHLCV data
            elif (
                    data_req.cat == "eqty"
                    and data_req.freq in self.frequencies[self.frequencies.index('d'):]
            ):
                df = self.get_eqty(data_req)

            # get crypto OHLCV data
            elif data_req.cat == "crypto":
                df = self.get_crypto(data_req)

            # get fx OHLCV data
            elif data_req.cat == "fx":
                df = self.get_fx(data_req)

        except Exception as e:
            logging.warning(e)
            raise Exception(
                "No data returned. Check data request parameters and try again."
            )

        else:
            # filter df for desired fields and typecast
            fields = [field for field in data_req.fields if field in df.columns]
            df = df.loc[:, fields]

            return df.sort_index()
