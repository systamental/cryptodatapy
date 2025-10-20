import logging
from typing import Any, Dict, List, Optional, Union
import pandas as pd
import asyncio
import random
from time import sleep
import ccxt
import ccxt.async_support as ccxt_async
from tqdm.asyncio import tqdm

from cryptodatapy.extract.datarequest import DataRequest
from cryptodatapy.extract.libraries.library import Library
from cryptodatapy.transform.convertparams import ConvertParams
from cryptodatapy.transform.wrangle import WrangleData
from cryptodatapy.util.datacredentials import DataCredentials

# data credentials
data_cred = DataCredentials()


class CCXT(Library):
    """
    Retrieves data from CCXT API.
    """

    def __init__(
            self,
            categories: Union[str, List[str]] = "crypto",
            exchanges: Optional[List[str]] = None,
            indexes: Optional[List[str]] = None,
            assets: Optional[Dict[str, List[str]]] = None,
            markets: Optional[Dict[str, List[str]]] = None,
            market_types: List[str] = ["spot", "future", "perpetual_future", "option"],
            fields: Optional[List[str]] = ["open", "high", "low", "close", "volume", "funding_rate", 'oi'],
            frequencies: Optional[Dict[str, Union[str, int]]] = None,
            base_url: Optional[str] = None,
            api_key: Optional[str] = None,
            max_obs_per_call: Optional[int] = 1000,
            rate_limit: Optional[Any] = None,
            ip_ban_wait_time_s: float = 320.0,  # 5.3 minutes for IP ban
            recovery_base_delay_s: float = 2.0,  # base delay for exponential backoff
            max_recovery_delay_s: float = 60.0  # max delay for exponential backoff
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
        ip_ban_wait_time_s: float, default 320.0
            Time in seconds to wait if IP ban is detected (HTTP 403).
        recovery_base_delay_s: float, default 2.0
            Base delay in seconds for exponential backoff strategy.
        max_recovery_delay_s: float, default 60.0
            Maximum delay in seconds for exponential backoff strategy.
        """
        super().__init__(
            categories, exchanges, indexes, assets, markets, market_types,
            fields, frequencies, base_url, api_key, max_obs_per_call, rate_limit
        )
        self.exchange = None
        self.exchange_async = None
        self.data_req = None

        self.ip_ban_wait_time_s = ip_ban_wait_time_s
        self.recovery_base_delay_s = recovery_base_delay_s
        self.max_recovery_delay_s = max_recovery_delay_s

        self.data_resp = []
        self.data = pd.DataFrame()

    def get_exchanges_info(self) -> List[str]:
        """
        Get exchanges info.

        Returns
        -------
        exch: list or pd.DataFrame
            List or dataframe with info on supported exchanges.
        """
        if self.exchanges is None:
            self.exchanges = ccxt.exchanges

        return self.exchanges

    def get_indexes_info(self) -> None:
        """
        Get indexes info.
        """
        return None

    def get_assets_info(self, exch: str, as_list: bool = False) -> Union[pd.DataFrame, List[str]]:
        """
        Get assets info.

        Parameters
        ----------
        exch: str
            Name of exchange.
        as_list: bool, default False
            Returns assets info for selected exchanges as list.

        Returns
        -------
        assets: list or pd.DataFrame
            Dataframe with info on available assets or list of assets.
        """
        if self.assets is None:

            # inst exchange
            if exch not in ccxt.exchanges:
                raise ValueError(
                    f"{exch} is not a supported exchange. "
                    f"Use get_exchanges_info() to get a list of supported exchanges.")
            else:
                if self.exchange is None:
                    self.exchange = getattr(ccxt, exch)()

            # get assets on exchange and create df
            self.exchange.load_markets()
            self.assets = pd.DataFrame(self.exchange.currencies).T
            self.assets.index.name = "ticker"

            # as list of assets
            if as_list:
                self.assets = self.assets.index.to_list()

        return self.assets

    def get_markets_info(
            self,
            exch: str,
            quote_ccy: Optional[str] = None,
            mkt_type: Optional[str] = None,
            as_list: bool = False,
    ) -> Union[pd.DataFrame, List[str]]:
        """
        Get markets info.

        Parameters
        ----------
        exch: str
            Name of exchange.
        quote_ccy: str, optional, default None
            Quote currency.
        mkt_type: str,  {'spot', 'future', 'perpetual_future', 'option'}, optional, default None
            Market type.
        as_list: bool, default False
            Returns markets info as list for selected exchange.

        Returns
        -------
        markets: list or pd.DataFrame
            List or dataframe with info on available markets, by exchange.
        """
        if self.markets is None:

            # inst exchange
            if exch not in ccxt.exchanges:
                raise ValueError(
                    f"{exch} is not a supported exchange. "
                    f"Use get_exchanges_info() to get a list of supported exchanges.")
            else:
                if self.exchange is None:
                    self.exchange = getattr(ccxt, exch)()

            # get assets on exchange
            self.markets = pd.DataFrame(self.exchange.load_markets()).T
            self.markets.index.name = "ticker"

            # quote ccy
            if quote_ccy is not None:
                self.markets = self.markets[self.markets.quote == quote_ccy.upper()]

            # mkt type
            if mkt_type == "perpetual_future":
                if self.markets[self.markets.type == "swap"].empty:
                    self.markets = self.markets[self.markets.type == "future"]
                else:
                    self.markets = self.markets[self.markets.type == "swap"]
            elif mkt_type == "spot" or mkt_type == "future" or mkt_type == "option":
                self.markets = self.markets[self.markets.type == mkt_type]

            # dict of assets
            if as_list:
                self.markets = self.markets.index.to_list()

        return self.markets

    def get_fields_info(self) -> List[str]:
        """
        Get fields info.

        Returns
        -------
        fields: list
            List of available fields.
        """
        if self.fields is None:
            self.fields = ["open", "high", "low", "close", "volume", "funding_rate", 'oi']

        return self.fields

    def get_frequencies_info(self, exch: str) -> Dict[str, Union[str, int]]:
        """
        Get frequencies info.

        Parameters
        ----------
        exch: str
            Name of exchange for which to get available assets.

        Returns
        -------
        freq: dictionary
            Dictionary with info on available frequencies.
        """
        if self.frequencies is None:

            # inst exchange
            if exch not in ccxt.exchanges:
                raise ValueError(
                    f"{exch} is not a supported exchange. "
                    f"Use get_exchanges_info() to get a list of supported exchanges.")
            else:
                if self.exchange is None:
                    self.exchange = getattr(ccxt, exch)()

            # freq dict
            self.frequencies = self.exchange.timeframes

        return self.frequencies

    def get_rate_limit_info(self, exch: str) -> Dict[str, Union[str, int]]:
        """
        Get rate limit info.

        Parameters
        ----------
        exch: str
            Name of exchange.

        Returns
        -------
        rate_limit: dictionary
            Dictionary with exchange and required minimal delay between HTTP requests that exchange in milliseconds.

        """
        if self.rate_limit is None:

            # inst exchange
            if exch not in ccxt.exchanges:
                raise ValueError(
                    f"{exch} is not a supported exchange. "
                    f"Use get_exchanges_info() to get a list of supported exchanges.")
            else:
                if self.exchange is None:
                    self.exchange = getattr(ccxt, exch)()

            self.rate_limit = {
                "exchange rate limit":
                    "delay in milliseconds between two consequent HTTP requests to the same exchange",
                exch: self.exchange.rateLimit
            }

        return self.rate_limit

    def get_metadata(self, exch: str) -> None:
        """
        Get CCXT metadata.

        Parameters
        ----------
        exch: str
            Name of exchange.
        """
        # inst exchange
        if exch not in ccxt.exchanges:
            raise ValueError(
                f"{exch} is not a supported exchange. Use get_exchanges_info() to get a list of supported exchanges.")
        else:
            if self.exchange is None:
                self.exchange = getattr(ccxt, exch)()

        # load markets
        self.exchange.load_markets()

        if self.exchanges is None:
            self.exchanges = self.get_exchanges_info()
        if self.market_types is None:
            self.market_types = ["spot", "future", "perpetual_future", "option"]
        if self.assets is None:
            self.assets = list(self.exchange.currencies.keys())
        if self.markets is None:
            self.markets = list(self.exchange.markets.keys())
        if self.fields is None:
            self.fields = ["open", "high", "low", "close", "volume", "funding_rate", 'oi']
        if self.frequencies is None:
            self.frequencies = list(self.exchange.timeframes.keys())
        if self.rate_limit is None:
            self.rate_limit = self.exchange.rateLimit

    # @staticmethod
    # def exponential_backoff_with_jitter(base_delay: float, max_delay: int, attempts: int) -> None:
    #     delay = min(max_delay, base_delay * (2 ** attempts))
    #     delay_with_jitter = delay + random.uniform(0, delay * 0.5)
    #     sleep(delay_with_jitter)
    #
    # @staticmethod
    # async def exponential_backoff_with_jitter_async(base_delay: float, max_delay: int, attempts: int) -> None:
    #     delay = min(max_delay, base_delay * (2 ** attempts))
    #     delay_with_jitter = delay + random.uniform(0, delay * 0.5)
    #     await asyncio.sleep(delay_with_jitter)
    #

    def exponential_backoff_with_jitter(
            self,
            attempts: int,
            status_code: Optional[int] = None
    ) -> float:
        """
        Calculates and applies exponential backoff with full jitter, honoring
        specific error codes (403/429) using configurable instance properties.

        Args:
            attempts: The current retry number (starting at 1 after the first failure).
            status_code: The HTTP status code received (e.g., 403, 429).

        Returns:
            The actual time slept in seconds.
        """

        # 1. CRITICAL: Hard IP ban handling (403)
        if status_code == 403:
            sleep_time = self.ip_ban_wait_time_s
            logging.error(
                f"🚨 CRITICAL: HTTP 403 Forbidden (IP Ban) detected. Pausing for mandatory {sleep_time} seconds.")
            sleep(sleep_time)
            return sleep_time

        # 2. Standard Exponential Backoff for 429 or generic exceptions
        base_delay = self.recovery_base_delay_s
        max_delay = self.max_recovery_delay_s

        # Calculate exponential growth
        delay = min(max_delay, base_delay * (2 ** attempts))

        # Add full jitter (random delay between 0 and the calculated delay)
        sleep_time = random.uniform(0, delay)

        logging.warning(
            f"Applying recovery backoff (Attempt {attempts}, Code {status_code if status_code else 'N/A'}): "
            f"{sleep_time:.2f} seconds."
        )

        sleep(sleep_time)
        return sleep_time

    async def exponential_backoff_with_jitter_async(
            self,
            attempts: int,
            status_code: Optional[int] = None
    ) -> float:
        """
        Async version of exponential_backoff_with_jitter, using configurable instance properties.
        """

        # 1. CRITICAL: Hard IP ban handling (403)
        if status_code == 403:
            sleep_time = self.ip_ban_wait_time_s
            logging.error(
                f"🚨 CRITICAL: HTTP 403 Forbidden (IP Ban) detected. Pausing for mandatory {sleep_time} seconds.")
            await asyncio.sleep(sleep_time)
            return sleep_time

        # 2. Standard Exponential Backoff for 429 or generic exceptions
        base_delay = self.recovery_base_delay_s
        max_delay = self.max_recovery_delay_s

        # Calculate exponential growth
        delay = min(max_delay, base_delay * (2 ** attempts))

        # Add full jitter (random delay between 0 and the calculated delay)
        sleep_time = random.uniform(0, delay)

        logging.warning(
            f"Applying recovery backoff (Attempt {attempts}, Code {status_code if status_code else 'N/A'}): "
            f"{sleep_time:.2f} seconds."
        )

        await asyncio.sleep(sleep_time)
        return sleep_time

    # --- New Exception Helper Methods for Modularity ---

    def _handle_exception_and_backoff(self, e: Exception, attempts: int) -> bool:
        """
        Analyzes a synchronous exception, applies backoff if recoverable, and logs the result.

        Returns:
            True if the error was recoverable (retries should continue).
            False if the error is terminal (retries should stop).
        """
        # 1. CRITICAL: 403 IP Ban Check (Must parse string due to CCXT wrapping)
        error_message = str(e)
        if '403' in error_message or 'Forbidden' in error_message:
            logging.error(f"CCXT Exception: Critical 403 IP Ban detected via parsing.")
            self.exponential_backoff_with_jitter(attempts, status_code=403)
            return True  # Recoverable with mandatory long pause

        # 2. Recoverable CCXT Exceptions (Standard Backoff: proxy 429)
        if isinstance(e, (
                ccxt.RequestTimeout,
                ccxt.RateLimitExceeded,
                ccxt.DDoSProtection,
                ccxt.ExchangeNotAvailable,
                ccxt.OperationFailed,
        )):
            logging.warning(f"CCXT Exception: Recoverable error ({e.__class__.__name__}) on attempt {attempts}.")
            self.exponential_backoff_with_jitter(attempts, status_code=429)
            return True  # Recoverable, continue retries

        # 3. Unrecoverable CCXT Errors (Terminal: e.g., bad symbol)
        if isinstance(e, ccxt.ExchangeError):
            logging.error(f"CCXT Exception: Terminal ExchangeError ({e.__class__.__name__}). Halting retries.")
            return False  # Unrecoverable, stop retries

        # 4. Other/Unknown Exceptions (Terminal)
        logging.error(f"CCXT Exception: Unhandled error ({e.__class__.__name__}). Halting retries: {e}")
        return False

    async def _handle_exception_and_backoff_async(self, e: Exception, attempts: int) -> bool:
        """
        Analyzes an asynchronous exception, applies backoff if recoverable, and logs the result.

        Returns:
            True if the error was recoverable (retries should continue).
            False if the error is terminal (retries should stop).
        """
        # 1. CRITICAL: 403 IP Ban Check (Must parse string due to CCXT wrapping)
        error_message = str(e)
        if '403' in error_message or 'Forbidden' in error_message:
            logging.error(f"CCXT Exception: Critical 403 IP Ban detected via parsing.")
            await self.exponential_backoff_with_jitter_async(attempts, status_code=403)
            return True  # Recoverable with mandatory long pause

        # 2. Recoverable CCXT Exceptions (Standard Backoff: proxy 429)
        if isinstance(e, (
                ccxt.RequestTimeout,
                ccxt.RateLimitExceeded,
                ccxt.DDoSProtection,
                ccxt.ExchangeNotAvailable,
                ccxt.OperationFailed,
        )):
            logging.warning(f"CCXT Exception: Recoverable error ({e.__class__.__name__}) on attempt {attempts}.")
            await self.exponential_backoff_with_jitter_async(attempts, status_code=429)
            return True  # Recoverable, continue retries

        # 3. Unrecoverable CCXT Errors (Terminal: e.g., bad symbol)
        if isinstance(e, ccxt.ExchangeError):
            logging.error(f"CCXT Exception: Terminal ExchangeError ({e.__class__.__name__}). Halting retries.")
            return False  # Unrecoverable, stop retries

        # 4. Other/Unknown Exceptions (Terminal)
        logging.error(f"CCXT Exception: Unhandled error ({e.__class__.__name__}). Halting retries: {e}")
        return False

    async def _fetch_ohlcv_async(self,
                                 ticker: str,
                                 freq: str,
                                 start_date: int,
                                 end_date: int,
                                 exch: str,
                                 trials: int = 3,
                                 ) -> Union[List, None]:
        """
        Fetches OHLCV data for a specific ticker.

        Parameters
        ----------
        ticker: str
            Ticker symbol.
        freq: str
            Frequency of data, e.g. '1m', '5m', '1h', '1d'.
        start_date: str
            Start date in integers in milliseconds since Unix epoch.
        end_date: str
            End date in integers in milliseconds since Unix epoch.
        exch: str
            Name of exchange.
        trials: int, default 3
            Number of attempts to fetch data.

        Returns
        -------
        data_resp: list
            List of timestamps with OHLCV data.
        """
        attempts = 0
        data_resp = []

        # inst exch
        if self.exchange_async is None:
            self.exchange_async = getattr(ccxt_async, exch)()

        # fetch data
        if self.exchange_async.has['fetchOHLCV']:

            # while loop to fetch all data
            while start_date < end_date and attempts < trials:

                try:
                    data = await self.exchange_async.fetch_ohlcv(
                        ticker,
                        freq,
                        since=start_date,
                        limit=self.max_obs_per_call,
                        params={'until': end_date}
                    )

                    # add data to list
                    if data:
                        # noinspection PyUnusedLocal
                        start_date = data[-1][0] + 1
                        data_resp.extend(data)
                    else:
                        break

                except Exception as e:
                    attempts += 1

                    if attempts >= trials:
                        logging.warning(
                            f"Failed to get OHLCV data from {self.exchange_async.id} "
                            f"for {ticker} after {trials} attempts."
                        )
                        break

                    # exception handling
                    if not await self._handle_exception_and_backoff_async(e, attempts):
                        # If the helper returns False, the error is terminal (ExchangeError, etc.)
                        break

                await self.exchange_async.close()
                return data_resp

            else:
                logging.warning(f"OHLCV data is not available for {self.exchange_async.id}.")
                return None

    def _fetch_ohlcv(self,
                     ticker: str,
                     freq: str,
                     start_date: str,
                     end_date: str,
                     exch: str,
                     trials: int = 3,
                     ) -> Union[List, None]:
        """
        Fetches OHLCV data for a specific ticker.

        Parameters
        ----------
        ticker: str
            Ticker symbol.
        freq: str
            Frequency of data, e.g. '1m', '5m', '1h', '1d'.
        start_date: str
            Start date in integers in milliseconds since Unix epoch.
        end_date: str
            End date in integers in milliseconds since Unix epoch.
        exch: str
            Name of exchange.
        trials: int, default 3
            Number of attempts to fetch data.

        Returns
        -------
        data_resp: list
            List of timestamps with OHLCV data.
        """
        attempts = 0
        data_resp = []

        # inst exch
        if self.exchange is None:
            self.exchange = getattr(ccxt, exch)()

        # fetch data
        if self.exchange.has['fetchOHLCV']:

            # while loop to fetch all data
            while start_date < end_date and attempts < trials:

                try:
                    data = self.exchange.fetch_ohlcv(
                        ticker,
                        freq,
                        since=start_date,
                        limit=self.max_obs_per_call,
                        params={
                            'until': end_date,
                            'paginate': True
                        }
                    )

                    # add data to list
                    if data:
                        start_date = data[-1][0] + 1
                        data_resp.extend(data)
                    else:
                        break

                except Exception as e:
                    attempts += 1

                    if attempts >= trials:
                        logging.warning(
                            f"Failed to get OHLCV data from {self.exchange.id} "
                            f"for {ticker} after {trials} attempts."
                        )
                        break

                    # exception handling
                    if not self._handle_exception_and_backoff(e, attempts):
                        # If the helper returns False, the error is terminal (ExchangeError, etc.)
                        break

            return data_resp

        else:
            logging.warning(f"OHLCV data is not available for {self.exchange.id}.")
            return None

    async def _fetch_all_ohlcv_async(self,
                                     tickers,
                                     freq: str,
                                     start_date: int,
                                     end_date: int,
                                     exch: str,
                                     trials: int = 3,
                                     pause: int = 1
                                     ) -> Union[List, None]:
        """
        Fetches OHLCV data for a list of tickers.

        Parameters
        ----------
        tickers: list
            List of ticker symbols.
        freq: str
            Frequency of data, e.g. '1m', '5m', '1h', '1d'.
        start_date: int
            Start date in integers in milliseconds since Unix epoch.
        end_date: int
            End date in integers in milliseconds since Unix epoch.
        exch: str
            Name of exchange.
        trials: int, default 3
            Number of attempts to fetch data.
        pause: int, default 0.5
            Pause in seconds to respect the rate limit.

        Returns
        -------
        data_resp: list
            List of lists of timestamps and OHLCV data for each ticker.
        """
        # inst exch
        if self.exchange_async is None:
            self.exchange_async = getattr(ccxt_async, exch)()

        # create progress bar
        pbar = tqdm(total=len(tickers), desc="Fetching OHLCV data", unit="ticker")

        # loop through tickers
        for ticker in tickers:
            data = await self._fetch_ohlcv_async(ticker, freq, start_date, end_date, trials=trials, exch=exch)
            await asyncio.sleep(pause)
            self.data_resp.append(data)
            pbar.update(1)

        await self.exchange_async.close()

        return self.data_resp

    def _fetch_all_ohlcv(self,
                         tickers,
                         freq: str,
                         start_date: int,
                         end_date: int,
                         exch: str,
                         trials: int = 3,
                         pause: int = 1
                         ) -> Union[List, None]:
        """
        Fetches OHLCV data for a list of tickers.

        Parameters
        ----------
        tickers: list
            List of ticker symbols.
        freq: str
            Frequency of data, e.g. '1m', '5m', '1h', '1d'.
        start_date: int
            Start date in integers in milliseconds since Unix epoch.
        end_date: int
            End date in integers in milliseconds since Unix epoch.
        exch: str
            Name of exchange.
        trials: int, default 3
            Number of attempts to fetch data.
        pause: int, default 0.5
            Pause in seconds to respect the rate limit.

        Returns
        -------
        data_resp: list
            List of lists of timestamps and OHLCV data for each ticker.
        """
        # inst exch
        if self.exchange is None:
            self.exchange = getattr(ccxt, exch)()

        # create progress bar
        pbar = tqdm(total=len(tickers), desc="Fetching OHLCV data", unit="ticker")

        # loop through tickers
        for ticker in tickers:
            data = self._fetch_ohlcv(ticker, freq, start_date, end_date, trials=trials, exch=exch)
            sleep(pause)
            self.data_resp.append(data)
            pbar.update(1)

        return self.data_resp

    async def _fetch_funding_rates_async(self,
                                         ticker: str,
                                         start_date: int,
                                         end_date: int,
                                         exch: str,
                                         trials: int = 3,
                                         ) -> Union[List, None]:
        """
        Fetches funding rates data for a specific ticker.

        Parameters
        ----------
        ticker: str
            Ticker symbol.
        start_date: int
            Start date in integers in milliseconds since Unix epoch.
        end_date: int
            End date in integers in milliseconds since Unix epoch.
        trials: int, default 3
            Number of attempts to fetch data.

        Returns
        -------
        data_resp: list
            List of dictionaries with timestamps and funding rates data.
        """
        attempts = 0
        data_resp = []

        # inst exch
        if self.exchange_async is None:
            self.exchange_async = getattr(ccxt_async, exch)()

        # fetch data
        if self.exchange_async.has['fetchFundingRateHistory']:

            # while loop to get all data
            while start_date < end_date and attempts < trials:

                try:
                    data = await self.exchange_async.fetch_funding_rate_history(
                        ticker,
                        since=start_date,
                        limit=self.max_obs_per_call,
                        params={'until': end_date}
                    )

                    # add data to list
                    if data:
                        start_date = data[-1]['timestamp'] + 1
                        data_resp.extend(data)
                    else:
                        break

                except Exception as e:
                    attempts += 1

                    if attempts >= trials:
                        logging.warning(
                            f"Failed to get funding rates from {self.exchange_async.id} "
                            f"for {ticker} after {trials} attempts."
                        )
                        break

                    # exception handling
                    if not await self._handle_exception_and_backoff_async(e, attempts):
                        break

            await self.exchange_async.close()
            return data_resp

        else:
            logging.warning(f"Funding rates are not available for {self.exchange_async.id}.")
            return None

    def _fetch_funding_rates(self,
                             ticker: str,
                             start_date: int,
                             end_date: int,
                             exch: str,
                             trials: int = 3,
                             ) -> Union[List, None]:
        """
        Fetches funding rates data for a specific ticker.

        Parameters
        ----------
        ticker: str
            Ticker symbol.
        start_date: int
            Start date in integers in milliseconds since Unix epoch.
        end_date: int
            End date in integers in milliseconds since Unix epoch.
        trials: int, default 3
            Number of attempts to fetch data.

        Returns
        -------
        data_resp: list
            List of dictionaries with timestamps and funding rates data.
        """
        attempts = 0
        data_resp = []

        # inst exch
        if self.exchange is None:
            self.exchange = getattr(ccxt, exch)()

        # fetch data
        if self.exchange.has['fetchFundingRateHistory']:

            # while loop to get all data
            while start_date < end_date and attempts < trials:

                try:
                    data = self.exchange.fetch_funding_rate_history(
                        ticker,
                        since=start_date,
                        limit=self.max_obs_per_call,
                        params={'until': end_date}
                    )

                    # add data to list
                    if data:
                        start_date = data[-1]['timestamp'] + 1
                        data_resp.extend(data)
                    else:
                        break

                except Exception as e:
                    attempts += 1

                    if attempts >= trials:
                        logging.warning(
                            f"Failed to get funding rates from {self.exchange.id} "
                            f"for {ticker} after {trials} attempts."
                        )
                        break

                    # exception handling
                    if not self._handle_exception_and_backoff(e, attempts):
                        break

            return data_resp

        else:
            logging.warning(f"Funding rates are not available for {self.exchange.id}.")
            return None

    async def _fetch_all_funding_rates_async(self,
                                             tickers,
                                             start_date: int,
                                             end_date: int,
                                             exch: str,
                                             trials: int = 3,
                                             pause: int = 1
                                             ) -> Union[List, None]:
        """
        Fetches funding rates data for a list of tickers.

        Parameters
        ----------
        tickers: list
            List of ticker symbols.
        start_date: int
            Start date in integers in milliseconds since Unix epoch.
        end_date: int
            End date in integers in milliseconds since Unix epoch.
        exch: str
            Name of exchange.
        trials: int, default 3
            Number of attempts to fetch data.
        pause: int, default 0.5
            Pause in seconds to respect the rate limit.

        Returns
        -------
        data_resp: list
            List of lists of dictionaries with timestamps and funding rates data for each ticker.
        """
        # inst exch
        if self.exchange_async is None:
            self.exchange_async = getattr(ccxt_async, exch)()

        # create progress bar
        pbar = tqdm(total=len(tickers), desc="Fetching funding rates", unit="ticker")

        # loop through tickers
        for ticker in tickers:
            data = await self._fetch_funding_rates_async(ticker, start_date, end_date, trials=trials, exch=exch)
            self.data_resp.append(data)
            pbar.update(1)
            await asyncio.sleep(pause)

        await self.exchange_async.close()

        return self.data_resp

    def _fetch_all_funding_rates(self,
                                 tickers,
                                 start_date: int,
                                 end_date: int,
                                 exch: str,
                                 trials: int = 3,
                                 pause: int = 1
                                 ) -> Union[List, None]:
        """
        Fetches funding rates data for a list of tickers.

        Parameters
        ----------
        tickers: list
            List of ticker symbols.
        start_date: int
            Start date in integers in milliseconds since Unix epoch.
        end_date: int
            End date in integers in milliseconds since Unix epoch.
        exch: str
            Name of exchange.
        trials: int, default 3
            Number of attempts to fetch data.
        pause: int, default 0.5
            Pause in seconds to respect the rate limit.

        Returns
        -------
        data_resp: list
            List of lists of dictionaries with timestamps and funding rates data for each ticker.
        """

        # inst exch
        if self.exchange is None:
            self.exchange = getattr(ccxt, exch)()

        # create progress bar
        pbar = tqdm(total=len(tickers), desc="Fetching funding rates", unit="ticker")

        # loop through tickers
        for ticker in tickers:
            data = self._fetch_funding_rates(ticker, start_date, end_date, trials=trials, exch=exch)
            self.data_resp.append(data)
            pbar.update(1)
            sleep(pause)

        return self.data_resp

    async def _fetch_open_interest_async(self,
                                         ticker: str,
                                         freq: str,
                                         start_date: int,
                                         end_date: int,
                                         exch: str,
                                         trials: int = 3,
                                         ) -> Union[List, None]:
        """
        Fetches open interest data for a specific ticker.

        Parameters
        ----------
        ticker: str
            Ticker symbol.
        freq: str
            Frequency of data, e.g. '1m', '5m', '1h', '1d'.
        start_date: int
            Start date in integers in milliseconds since Unix epoch.
        end_date: int
            End date in integers in milliseconds since Unix epoch.
        exch: str
            Name of exchange.
        trials: int, default 3
            Number of attempts to fetch data.

        Returns
        -------
        data_resp: list
            List of dictionaries with timestamps and open interest data.
        """
        # number of attempts
        attempts = 0
        data_resp = []

        # Maximum historical range for Binance OI is 30 days
        # 30 days in milliseconds: 30 * 24 * 60 * 60 * 1000 = 2,592,000,000 ms
        SAFE_OI_RANGE_MS = 25 * 24 * 60 * 60 * 1000

        # inst exch
        if self.exchange_async is None:
            self.exchange_async = getattr(ccxt_async, exch)()

        # --- Binance 30-day Limit Enforcement ---
        if exch.lower() == 'binanceusdm':  # Binance USDM Futures
            requested_range_ms = end_date - start_date
            if requested_range_ms > SAFE_OI_RANGE_MS:
                # Adjust start_date to fit within the 30-day window ending at end_date
                new_start_date = end_date - SAFE_OI_RANGE_MS
                logging.warning(
                    f"Exchange '{exch}' Open Interest historical data is limited to 30 days. "
                    f"Adjusting start_date for {ticker} from {start_date} to {new_start_date}."
                )
                start_date = new_start_date

        # inst exch
        if self.exchange_async is None:
            self.exchange_async = getattr(ccxt_async, exch)()

        # fetch data
        if self.exchange_async.has['fetchOpenInterestHistory']:

            # while loop to get all data
            while start_date < end_date and attempts < trials:

                try:
                    data = await self.exchange_async.fetch_open_interest_history(
                        ticker,
                        freq,
                        since=start_date,
                        limit=500,
                        params={'until': end_date}
                    )

                    # add data to list
                    if data:
                        start_date = data[-1]['timestamp'] + 1
                        data_resp.extend(data)
                    else:
                        break

                except Exception as e:
                    attempts += 1

                    if attempts >= trials:
                        logging.warning(
                            f"Failed to get funding rates from {self.exchange_async.id} "
                            f"for {ticker} after {trials} attempts."
                        )
                        break

                    # exception handling
                    if not await self._handle_exception_and_backoff_async(e, attempts):
                        break

            await self.exchange_async.close()
            return data_resp

        else:
            logging.warning(f"Funding rates are not available for {self.exchange_async.id}.")
            return None

    def _fetch_open_interest(self,
                             ticker: str,
                             freq: str,
                             start_date: int,
                             end_date: int,
                             exch: str,
                             trials: int = 3,
                             ) -> Union[List, None]:
        """
        Fetches open interest data for a specific ticker.

        Parameters
        ----------
        ticker: str
            Ticker symbol.
        freq: str
            Frequency of data, e.g. '1m', '5m', '1h', '1d'.
        start_date: int
            Start date in integers in milliseconds since Unix epoch.
        end_date: int
            End date in integers in milliseconds since Unix epoch.
        exch: str
            Name of exchange.
        trials: int, default 3
            Number of attempts to fetch data.

        Returns
        -------
        data_resp: list
            List of dictionaries with timestamps and open interest data.
        """
        # number of attempts
        attempts = 0
        data_resp = []

        # Maximum historical range for Binance OI is 30 days
        # 30 days in milliseconds: 30 * 24 * 60 * 60 * 1000 = 2,592,000,000 ms
        SAFE_OI_RANGE_MS = 25 * 24 * 60 * 60 * 1000

        # inst exch
        if self.exchange_async is None:
            self.exchange_async = getattr(ccxt_async, exch)()

        # --- Binance 30-day Limit Enforcement ---
        if exch.lower() == 'binanceusdm':  # Binance USDM Futures
            requested_range_ms = end_date - start_date
            if requested_range_ms > SAFE_OI_RANGE_MS:
                # Adjust start_date to fit within the 30-day window ending at end_date
                new_start_date = end_date - SAFE_OI_RANGE_MS
                logging.warning(
                    f"Exchange '{exch}' open interest historical data is limited to 30 days. "
                    f"Adjusting start_date for {ticker} from {start_date} to {new_start_date}."
                )
                start_date = new_start_date

        # fetch data
        if self.exchange.has['fetchOpenInterestHistory']:

            # while loop to get all data
            while start_date < end_date and attempts < trials:

                try:
                    data = self.exchange.fetch_open_interest_history(
                        ticker,
                        freq,
                        since=start_date,
                        limit=500,
                        params={'until': end_date}
                    )

                    # add data to list
                    if data:
                        start_date = data[-1]['timestamp'] + 1
                        data_resp.extend(data)
                    else:
                        break

                except Exception as e:
                    attempts += 1

                    if attempts >= trials:
                        logging.warning(
                            f"Failed to get funding rates from {self.exchange.id} "
                            f"for {ticker} after {trials} attempts."
                        )
                        break

                    # exception handling
                    if not self._handle_exception_and_backoff(e, attempts):
                        break

            return data_resp

        else:
            logging.warning(f"Funding rates are not available for {self.exchange.id}.")
            return None

    async def _fetch_all_open_interest_async(self,
                                             tickers,
                                             freq: str,
                                             start_date: int,
                                             end_date: int,
                                             exch: str,
                                             trials: int = 3,
                                             pause: int = 1
                                             ) -> Union[List, None]:
        """
        Fetches open interest data for a list of tickers.

        Parameters
        ----------
        tickers: list
            List of ticker symbols.
        freq: str
            Frequency of data, e.g. '1m', '5m', '1h', '1d'.
        start_date: int
            Start date in integers in milliseconds since Unix epoch.
        end_date: int
            End date in integers in milliseconds since Unix epoch.
        exch: str
            Name of exchange.
        trials: int, default 3
            Number of attempts to fetch data.
        pause: int, default 0.5
            Pause in seconds to respect the rate limit.

        Returns
        -------
        data_resp: list
            List of lists of dictionaries with timestamps and open interest data for each ticker.
        """
        # inst exch
        if self.exchange_async is None:
            self.exchange_async = getattr(ccxt_async, exch)()

        # create progress bar
        pbar = tqdm(total=len(tickers), desc="Fetching open interest", unit="ticker")

        # loop through tickers
        for ticker in tickers:
            data = await self._fetch_open_interest_async(ticker, freq, start_date, end_date, trials=trials, exch=exch)
            self.data_resp.extend(data)
            pbar.update(1)
            await asyncio.sleep(pause)

        await self.exchange_async.close()

        return self.data_resp

    def _fetch_all_open_interest(self,
                                 tickers,
                                 freq: str,
                                 start_date: int,
                                 end_date: int,
                                 exch: str,
                                 trials: int = 3,
                                 pause: int = 1
                                 ) -> Union[List, None]:
        """
        Fetches open interest data for a list of tickers.

        Parameters
        ----------
        tickers: list
            List of ticker symbols.
        freq: str
            Frequency of data, e.g. '1m', '5m', '1h', '1d'.
        start_date: int
            Start date in integers in milliseconds since Unix epoch.
        end_date: int
            End date in integers in milliseconds since Unix epoch.
        exch: str
            Name of exchange.
        trials: int, default 3
            Number of attempts to fetch data.
        pause: int, default 0.5
            Pause in seconds to respect the rate limit.

        Returns
        -------
        data_resp: list
            List of lists of dictionaries with timestamps and open interest data for each ticker.
        """
        # inst exch
        if self.exchange is None:
            self.exchange = getattr(ccxt, exch)()

        # create progress bar
        pbar = tqdm(total=len(tickers), desc="Fetching open interest", unit="ticker")

        # loop through tickers
        for ticker in tickers:
            data = self._fetch_open_interest(ticker, freq, start_date, end_date, trials=trials, exch=exch)
            self.data_resp.extend(data)
            pbar.update(1)
            sleep(pause)

        return self.data_resp

    def convert_params(self, data_req: DataRequest) -> DataRequest:
        """
        Converts data request parameters to CCXT format.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        data_req: DataRequest
            Parameters of data request in CCXT format.
        """
        self.data_req = ConvertParams(data_req).to_ccxt()

        # get metadata
        self.get_metadata(self.data_req.exch)

        # check markets
        if not any([market in self.markets for market in self.data_req.source_markets]):
            raise ValueError(
                f"Selected markets are not available. Use the '.markets' attribute to check supported markets."
            )

        # check freq
        if self.data_req.source_freq not in self.frequencies:
            raise ValueError(
                f"{self.data_req.source_freq} frequency is not available. "
                f"Use the '.frequencies' attribute to check available frequencies."
            )

        # check mkt type
        if self.data_req.mkt_type not in self.market_types:
            raise ValueError(
                f"{self.data_req.mkt_type} is not available for {self.data_req.exch}."
            )

        # check start date
        if not isinstance(self.data_req.source_start_date, int):
            raise ValueError(
                f"Start date must be in integers in milliseconds since Unix epoch."
            )

        # check end date
        if not isinstance(self.data_req.source_end_date, int):
            raise ValueError(
                f"End date must be in integers in milliseconds since Unix epoch."
            )

        # check fields
        if not any([field in self.fields for field in self.data_req.fields]):
            raise ValueError(
                f"Selected fields are not available for {self.data_req.exch}. "
                f"Use fields attribute to check available fields."
            )

        # check ohlcv
        if any([field in ['open', 'high', 'low', 'close', 'volume'] for field in self.data_req.fields]) and \
                not self.exchange.has["fetchOHLCV"]:
            raise ValueError(
                f"OHLCV data is not available for {self.data_req.exch}."
                f" Try another exchange or data request."
            )

        # check funding rates
        if any([field == 'funding_rate' for field in self.data_req.fields]) and \
                not self.exchange.has["fetchFundingRateHistory"]:
            raise ValueError(
                f"Funding rates are not available for {self.data_req.exch}."
                f" Try another exchange or data request."
            )

        # check open interest
        if any([field == 'oi' for field in self.data_req.fields]) and \
                not self.exchange.has["fetchOpenInterestHistory"]:
            raise ValueError(
                f"Open interest is not available for {self.data_req.exch}."
                f" Try another exchange or data request."
            )

        # check perp future
        if any([(field == 'funding_rate' or field == 'open_interest') for field in self.data_req.fields]) and \
                self.data_req.mkt_type not in ['perpetual_future', 'future']:
            raise ValueError(
                f"You have requested fields only available for futures markets."
                f" Change mkt_type to 'perpetual_future' or 'future'."
            )

        return self.data_req

    def wrangle_data_resp(self, data_type: str) -> pd.DataFrame:
        """
        Wrangle data response.

        Parameters
        ----------
        data_type: str
            Type of data, e.g. 'ohlcv', 'funding_rate', 'open_interest'.

        Returns
        -------
        df: pd.DataFrame
            Wrangled dataframe with DatetimeIndex and values in tidy format.
        """
        return WrangleData(self.data_req, self.data_resp).ccxt(data_type=data_type)

    async def fetch_tidy_ohlcv_async(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Gets entire OHLCV history and wrangles the data response into tidy data format.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame
            Dataframe with entire OHLCV data history retrieved and wrangled into tidy data format.
        """
        # convert data request parameters to CCXT format
        if self.data_req is None:
            self.convert_params(data_req)

        # data resp
        if self.data_resp:
            self.data_resp = []

        # get entire data history
        await self._fetch_all_ohlcv_async(self.data_req.source_markets,
                                          self.data_req.source_freq,
                                          self.data_req.source_start_date,
                                          self.data_req.source_end_date,
                                          self.data_req.exch,
                                          trials=self.data_req.trials,
                                          pause=self.data_req.pause)

        # wrangle df
        if self.data_resp:
            df = self.wrangle_data_resp(data_type='ohlcv')
            return df
        else:
            logging.warning("Failed to get requested OHLCV data.")

    def fetch_tidy_ohlcv(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Gets entire OHLCV history and wrangles the data response into tidy data format.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame
            Dataframe with entire OHLCV data history retrieved and wrangled into tidy data format.
        """
        # convert data request parameters to CCXT format
        if self.data_req is None:
            self.convert_params(data_req)

        # data resp
        if self.data_resp:
            self.data_resp = []

        # get entire data history
        self._fetch_all_ohlcv(self.data_req.source_markets,
                              self.data_req.source_freq,
                              self.data_req.source_start_date,
                              self.data_req.source_end_date,
                              self.data_req.exch,
                              trials=self.data_req.trials,
                              pause=self.data_req.pause)

        # wrangle df
        if self.data_resp:
            df = self.wrangle_data_resp(data_type='ohlcv')
            return df
        else:
            logging.warning("Failed to get requested OHLCV data.")

    async def fetch_tidy_funding_rates_async(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Gets entire funding rates history and wrangles the data response into tidy data format.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame
            Dataframe with entire data history retrieved and wrangled into tidy data format.
        """
        # convert data request parameters to CCXT format
        if self.data_req is None:
            self.convert_params(data_req)

        # data resp
        if self.data_resp:
            self.data_resp = []

        # get entire data history
        await self._fetch_all_funding_rates_async(self.data_req.source_markets,
                                                  self.data_req.source_start_date,
                                                  self.data_req.source_end_date,
                                                  self.data_req.exch,
                                                  trials=self.data_req.trials,
                                                  pause=self.data_req.pause)

        # wrangle df
        if self.data_resp:
            df = self.wrangle_data_resp(data_type='funding_rates')
            return df
        else:
            logging.warning("Failed to get requested funding rates.")

    def fetch_tidy_funding_rates(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Gets entire funding rates history and wrangles the data response into tidy data format.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame
            Dataframe with entire data history retrieved and wrangled into tidy data format.
        """
        # convert data request parameters to CCXT format
        if self.data_req is None:
            self.convert_params(data_req)

        # data resp
        if self.data_resp:
            self.data_resp = []

        # get entire data history
        self._fetch_all_funding_rates(self.data_req.source_markets,
                                      self.data_req.source_start_date,
                                      self.data_req.source_end_date,
                                      self.data_req.exch,
                                      trials=self.data_req.trials,
                                      pause=self.data_req.pause)

        # wrangle df
        if self.data_resp:
            df = self.wrangle_data_resp(data_type='funding_rates')
            return df
        else:
            logging.warning("Failed to get requested funding rates.")

    async def fetch_tidy_open_interest_async(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Gets entire open interest history and wrangles the data response into tidy data format.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame
            Dataframe with entire data history retrieved and wrangled into tidy data format.
        """
        # convert data request parameters to CCXT format
        if self.data_req is None:
            self.convert_params(data_req)

        # data resp
        if self.data_resp:
            self.data_resp = []

        # get entire data history
        await self._fetch_all_open_interest_async(self.data_req.source_markets,
                                                  self.data_req.source_freq,
                                                  self.data_req.source_start_date,
                                                  self.data_req.source_end_date,
                                                  self.data_req.exch,
                                                  trials=self.data_req.trials,
                                                  pause=self.data_req.pause)

        # wrangle df
        if self.data_resp:
            df = self.wrangle_data_resp(data_type='open_interest')
            return df
        else:
            logging.warning("Failed to get requested open interest.")

    def fetch_tidy_open_interest(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Gets entire open interest history and wrangles the data response into tidy data format.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame
            Dataframe with entire data history retrieved and wrangled into tidy data format.
        """
        # convert data request parameters to CCXT format
        if self.data_req is None:
            self.convert_params(data_req)

        # data resp
        if self.data_resp:
            self.data_resp = []

        # get entire data history
        self._fetch_all_open_interest(self.data_req.source_markets,
                                      self.data_req.source_freq,
                                      self.data_req.source_start_date,
                                      self.data_req.source_end_date,
                                      self.data_req.exch,
                                      trials=self.data_req.trials,
                                      pause=self.data_req.pause)

        # wrangle df
        if self.data_resp:
            df = self.wrangle_data_resp(data_type='open_interest')
            return df
        else:
            logging.warning("Failed to get requested open interest.")

    async def get_data_async(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get data specified by data request.

        Parameters
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and values for selected fields (cols).
        """
        logging.info("Retrieving data request from CCXT...")

        # get OHLCV
        if any([field in ["open", "high", "low", "close", "volume"] for field in data_req.fields]):
            df = await self.fetch_tidy_ohlcv_async(data_req)
            if any(df):
                self.data = pd.concat([self.data, df])

        # get funding rates
        if any([field == "funding_rate" for field in data_req.fields]):
            df = await self.fetch_tidy_funding_rates_async(data_req)
            if any(df):
                self.data = pd.concat([self.data, df], axis=1)

        # get open interest
        if any([field == "oi" for field in data_req.fields]):
            df = await self.fetch_tidy_open_interest_async(data_req)
            if any(df):
                self.data = pd.concat([self.data, df], axis=1)

        # check df
        if self.data.empty:
            raise Exception(
                "No data returned. Check data request parameters and try again."
            )

        # filter df for desired fields and typecast
        fields = [field for field in data_req.fields if field in self.data.columns]
        self.data = self.data.loc[:, fields]

        return self.data.sort_index()

    def get_data(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get data specified by data request.

        Parameters
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and values for selected fields (cols).
        """
        logging.info("Retrieving data request from CCXT...")

        # get OHLCV
        if any([field in ["open", "high", "low", "close", "volume"] for field in data_req.fields]):
            df = self.fetch_tidy_ohlcv(data_req)
            if any(df):
                self.data = pd.concat([self.data, df])

        # get funding rates
        if any([field == "funding_rate" for field in data_req.fields]):
            df = self.fetch_tidy_funding_rates(data_req)
            if any(df):
                self.data = pd.concat([self.data, df], axis=1)

        # get open interest
        if any([field == "oi" for field in data_req.fields]):
            df = self.fetch_tidy_open_interest(data_req)
            if any(df):
                self.data = pd.concat([self.data, df], axis=1)

        # check df
        if self.data.empty:
            raise Exception(
                "No data returned. Check data request parameters and try again."
            )

        # filter df for desired fields and typecast
        fields = [field for field in data_req.fields if field in self.data.columns]
        self.data = self.data.loc[:, fields]

        return self.data.sort_index()
