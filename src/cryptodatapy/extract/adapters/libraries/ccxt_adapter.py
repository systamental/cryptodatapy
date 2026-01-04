import logging
from typing import Any, Dict, List, Optional, Union
import pandas as pd
import asyncio
import random
import ccxt
import ccxt.async_support as ccxt_async
from tqdm.asyncio import tqdm as tqdm_async

from cryptodatapy.extract.adapters.base_adapter import BaseLibraryAdapter
from cryptodatapy.util.datacredentials import DataCredentials
from cryptodatapy.core.data_request import DataRequest
from cryptodatapy.extract.params import CCXTParamConverter
from cryptodatapy.transform.wranglers.ccxt_wrangler import CCXTWrangler


logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(name)s: %(message)s')
logger = logging.getLogger(__name__)


class CCXTAdapter(BaseLibraryAdapter):
    """
    Adapter class for retrieving data from CCTX library-supported exchanges.
    Implements the BaseAdapter contract via BaseLibraryAdapter.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initializes the CCXT adapter with configuration.

        Parameters
        ----------
        config : Optional[Dict[str, Any]], optional
            Configuration dictionary for the adapter
        """

        # Default CCXT configuration
        default_config = {
            'ip_ban_wait_time_s': 300,
            'recovery_base_delay_s': 2,
            'max_recovery_delay_s': 60,
            'max_concurrent_requests': 10,
            'timeout': 30000,
            'enableRateLimit': True,
            'max_obs_per_call': 1000,
            'trials': 3
        }

        # user-provided config (if any) overrides the defaults
        final_config = {**default_config, **(config or {})}

        # initialize BaseAdapter/BaseAPIAdapter with the merged configuration
        super().__init__(final_config)

        # internal cache for metadata
        self._sync_exchanges: Dict[str, ccxt.Exchange] = {}
        self._async_exchanges: Dict[str, ccxt.Exchange] = {}
        self.credentials = DataCredentials()  # for managing API keys

        # initialize properties for caching/metadata
        self.assets: Optional[Union[pd.DataFrame, list]] = None
        self.fields: Optional[Union[pd.DataFrame, list]] = None
        self.markets: Optional[Union[pd.DataFrame, list]] = None
        self.exchanges: Optional[Union[pd.DataFrame, list]] = None
        self.frequencies: Optional[Dict[str, Union[str, int]]] = None
        self.rate_limit: Any = None

    # --------------------------------------------------------------------------
    # --- 0. Private Helpers ---
    # --------------------------------------------------------------------------

    def _init_client(self, exch_name: str = 'binance', is_async: bool = True) -> Any:
        """
        Implementation of the BaseLibraryAdapter abstract method.
        Maps to the specialized exchange instance fetcher.
        """
        return self._get_exchange_instance(exch_name)

    def _get_exchange_instance(self, exch_name: str) -> ccxt_async.Exchange:
        """
        Retrieves or creates an async CCXT exchange instance with correct credentials.
        Improved version: Maintains original credential logic but optimized for our async loop.
        """
        if exch_name not in self._async_exchanges:
            if exch_name not in ccxt_async.exchanges:
                raise ValueError(f"Exchange '{exch_name}' is not supported by CCXT.")

            exch_class = getattr(ccxt_async, exch_name)

            # Re-incorporating your robust credential fetching
            api_key = getattr(self.credentials, f"{exch_name}_api_key", None)
            secret = getattr(self.credentials, f"{exch_name}_secret", None)
            password = getattr(self.credentials, f"{exch_name}_password", None)

            config = {
                'apiKey': api_key,
                'secret': secret,
                'password': password,
                'enableRateLimit': True,  # Mandatory for async concurrency
                'timeout': self._config.get('timeout', 30000),
            }

            # Instantiate and cache
            self._async_exchanges[exch_name] = exch_class(config)

        return self._async_exchanges[exch_name]

    async def close_async_exchanges(self):
        """Closes all async exchange connections."""
        for exch in self._async_exchanges.values():
            await exch.close()
        self._async_exchanges.clear()

    # --------------------------------------------------------------------------
    # --- 1. Helper Methods: Metadata Requests ---
    # --------------------------------------------------------------------------

    def get_exchanges_info(self) -> List[str]:
        """
        Gets list of available exchanges from CCXT.

        Returns
        -------
        exchanges_list: list
            List of available exchange names.
        """
        if self.exchanges is None:
            self.exchanges = ccxt.exchanges
        return self.exchanges

    def get_assets_info(self, exch: str = 'binance', as_list: bool = False, **kwargs) -> Union[pd.DataFrame, List[str]]:
        """
        Get assets/currencies info for a specific exchange.

        Parameters
        ----------
        exch: str, default 'binance'
            Name of exchange.
        as_list: bool, default False
            Returns assets info for selected exchanges as list.
        """
        if self.assets is None:
            if exch not in ccxt.exchanges:
                raise ValueError(
                    f"{exch} is not a supported exchange. "
                    f"Use get_exchanges_info() to get a list of supported exchanges.")
            else:
                exchange = getattr(ccxt, exch)()

                self.assets = CCXTWrangler(
                    data_req=DataRequest(),
                    data_resp=exchange,
                ).wrangle_assets_info(as_list=as_list)

        return self.assets

    def get_markets_info(
            self,
            exch: str = 'binance',
            quote_ccy: Optional[str] = None,
            mkt_type: Optional[str] = None,
            as_list: bool = False,
            **kwargs
    ) -> Union[pd.DataFrame, List[str]]:
        """
        Get markets info for a specific exchange with optional filtering.

        Parameters
        ----------
        exch: str, default 'binance'
            Name of exchange.
        quote_ccy: str, optional, default None
            Quote currency.
        mkt_type: str,  {'spot', 'future', 'perpetual_future', 'option'}, optional, default None
            Market type.
        as_list: bool, default False
            Returns markets info as list for selected exchange.
        """
        if self.markets is None:
            if exch not in ccxt.exchanges:
                raise ValueError(
                    f"{exch} is not a supported exchange. "
                    f"Use get_exchanges_info() to get a list of supported exchanges.")
            else:
                exchange = getattr(ccxt, exch)()

            self.markets = CCXTWrangler(
                data_req=DataRequest(),
                data_resp=exchange,
            ).wrangle_markets_info(quote_ccy=quote_ccy,
                                  mkt_type=mkt_type,
                                  as_list=as_list)

        return self.markets

    def get_fields_info(self, **kwargs) -> List[str]:
        """
        Get list of available fields supported by the CCXT adapter.

        Returns
        -------
        fields: list
            List of available fields.
        """
        return ["open", "high", "low", "close", "volume", "funding_rate", 'oi']

    def get_frequencies_info(self, exch: str = 'binance') -> Dict[str, Union[str, int]]:
        """
        Get available timeframes/frequencies for a specific exchange.

        Parameters
        ----------
        exch: str, default 'binance'
            Name of exchange for which to get available frequencies.

        Returns
        -------
        freq: dictionary
            Dictionary with info on available frequencies.
        """
        if self.frequencies is None:
            if exch not in ccxt.exchanges:
                raise ValueError(
                    f"{exch} is not a supported exchange. "
                    f"Use get_exchanges_info() to get a list of supported exchanges.")
            else:
                exchange = getattr(ccxt, exch)()

            # freq dict
            self.frequencies = exchange.timeframes

        return self.frequencies

    def get_rate_limit_info(self, exch: str = 'binance') -> Dict[str, Any]:
        """
        Get rate limit info (delay in ms) for a specific exchange.

        Parameters
        ----------
        exch: str, default 'binance'
            Name of exchange.
        Returns
        -------
        rate_limit: dictionary
            Dictionary with exchange and required minimal delay between HTTP requests that exchange in milliseconds.
        """
        exchange = getattr(ccxt, exch)()
        return {
            "description": "delay in milliseconds between consequent HTTP requests",
            "exchange": exch,
            "rateLimit": exchange.rateLimit
        }

    # --------------------------------------------------------------------------
    # --- 2. Helper Methods: Exception Handling ---
    # --------------------------------------------------------------------------

    async def _handle_exception_async(self,
                                      e: Exception,
                                      attempt: int,
                                      ticker: str) -> bool:
        """
        Handles exceptions raised during async data fetching.

        Parameters
        ----------
        e: Exception
            The exception that was raised.
        attempt: int
            The current attempt number.
        ticker: str
            The ticker symbol being processed.

        Returns
        -------
        bool
            True if the operation should be retried, False otherwise.
        """
        err_msg = str(e)

        # 1. Critical IP Ban (WAF/Proxy level) - Re-integrated from previous version
        if '403' in err_msg or 'Forbidden' in err_msg:
            wait_time = self._config['ip_ban_wait_time_s']
            logger.error(f"🚨 IP Ban/403 detected for {ticker}. Waiting {wait_time}s...")
            await asyncio.sleep(wait_time)  # Wait longer for IP-level blocks
            return True

        # 2. Recoverable CCXT Errors
        if isinstance(e, (ccxt.NetworkError, ccxt.RateLimitExceeded, ccxt.RequestTimeout, ccxt.DDoSProtection)):
            base_delay = self._config['recovery_base_delay_s']
            max_delay = self._config['max_recovery_delay_s']

            # exponential delay: base * 2^(attempt-1) - Same as previous, but uses 'attempt' index
            exp_delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
            # add jitter: prevents 'thundering herd' effect where all tasks restart at once
            jitter_delay = random.uniform(0.1, exp_delay)

            logger.warning(f"⚠️ {type(e).__name__} for {ticker}. Attempt {attempt}. Retrying in {jitter_delay:.2f}s...")
            await asyncio.sleep(jitter_delay)
            return True

        # 3. Terminal Errors - No point in retrying
        if isinstance(e, (ccxt.AuthenticationError, ccxt.PermissionDenied, ccxt.BadSymbol, ccxt.BadRequest)):
            logger.error(f"🛑 Terminal Error for {ticker}: {e}")
            return False

        # 4. Default for unknown
        logger.error(f"❌ Uncategorized error for {ticker}: {type(e).__name__}: {e}")
        return False

    # --------------------------------------------------------------------------
    # --- 3. Helper Methods: Data Fetching ---
    # --------------------------------------------------------------------------

    async def _fetch_ticker_data_async(
            self,
            exch: ccxt_async.Exchange,  # NEW: Pass pre-instantiated exch object
            req_params: Dict[str, Any],
            ticker: str
    ) -> List[Any]:
        """
        Fetches data for a request dictionary and a specific ticker.
        Handles internal CCXT pagination logic.

        Parameters
        ----------
        req_params: Dict[str, Any]
            Request parameters dictionary.
        ticker: str
            Ticker symbol to fetch data for.
        Returns
        -------
        all_results: list
            List of all fetched data points.
        """

        # unpacking local variables inside the method
        method = req_params['method']
        freq = req_params['source_freq']
        since = req_params['source_start_date']
        end_ts = req_params['source_end_date']

        # using pre-instantiated exch object
        fetch_fn = getattr(exch, method)
        all_results = []
        attempts = 0

        while since < end_ts:
            try:
                # method specific calls
                if method == 'fetchOHLCV':
                    data = await fetch_fn(ticker,
                                          timeframe=freq,
                                          since=since,
                                          limit=self._config['max_obs_per_call'],
                                          params={'until': end_ts}
                                          )
                elif method == 'fetchOpenInterestHistory':
                    data = await fetch_fn(ticker,
                                          freq,
                                          since=since,
                                          limit=500,
                                          params={'until': end_ts}
                                          )
                elif method == 'fetchFundingRateHistory':
                    data = await fetch_fn(ticker,
                                          since=since,
                                          limit=self._config['max_obs_per_call'],
                                          params={'until': end_ts}
                                          )
                else:
                    # non-OHLCV methods usually don't take a 'timeframe' argument
                    data = await fetch_fn(ticker,
                                          since=since,
                                          limit=self._config['max_obs_per_call'],
                                          params={'until': end_ts}
                                          )

                if not data:
                    break

                all_results.extend(data)

                # timestamp extraction
                last_ts = data[-1][0] if isinstance(data[-1], list) else data[-1].get('timestamp')

                if not last_ts or last_ts <= since:
                    break
                since = last_ts + 1
                attempts = 0

                # fixed sleep or fallback
                await asyncio.sleep(exch.rateLimit / 1000 if hasattr(exch, 'rateLimit') else 0.1)

            except Exception as e:
                # basic exception handling
                attempts += 1
                if (not await self._handle_exception_async(e, attempts, ticker) or
                        attempts >= self._config['trials']):
                    logger.error(f"Failed...")
                    break

        return all_results

    async def _fetch_all_tickers_async(
            self,
            req_params: Dict[str, Any]
    ) -> Dict[str, List[Any]]:
        """
        Fetches data for multiple tickers concurrently with throttling and error handling.
        """
        # unpacking local variables inside the method
        exch_name = req_params['exch']

        # create instance inside the ticker loop
        exch = self._get_exchange_instance(exch_name)

        semaphore = asyncio.Semaphore(self._config.get('max_concurrent_requests', 10))

        async def throttled_fetch(symbol: str):

            async with semaphore:
                try:
                    # pre-instantiated exch object down
                    data = await self._fetch_ticker_data_async(exch, req_params, symbol)
                    return symbol, data
                except Exception as e:
                    logger.warning(f"Skipping {symbol} due to unhandled error: {e}")
                    return symbol, []

        # task creation
        tasks = [throttled_fetch(s) for s in req_params['source_markets']]

        # execution with progress bar
        responses = await tqdm_async.gather(*tasks,
                                            desc=f"🚀 {req_params['exch']} | {req_params['method']}",
                                            unit="ticker",
                                            leave=True
                                            )

        return {symbol: data for symbol, data in responses}

    # --------------------------------------------------------------------------
    # --- 4. ETL Pipeline Implementation ---
    # --------------------------------------------------------------------------

    def _convert_params_to_vendor(self, data_req: DataRequest) -> Dict[str, Any]:
        """
        CONVERT: Uses CCXTParamConverter to get standard CCXT parameters.
        """
        converter = CCXTParamConverter(data_req)
        return converter.convert()

    async def _fetch_raw_data(self, vendor_params: Dict[str, Any]) -> Dict[str, List[Any]]:
        """
        EXTRACT: Orchestrates the fetching process by bridging the sync DataRequest
        to the async multi-ticker fetcher.
        """
        data = {}
        for req_dict in vendor_params.get('requests', []):
            method = req_dict['method']
            # Fetch and store results
            data[method] = await self._fetch_all_tickers_async(req_dict)

        await self.close_async_exchanges()

        return data

    def _transform_raw_response(self, data_req: DataRequest, raw_data: List[Any]) -> pd.DataFrame:
        """
        TRANSFORM: Uses CCXTWrangler to clean the data.
        """
        tidy_data = CCXTWrangler(
            data_req=data_req,
            data_resp=raw_data
        ).wrangle()

        return tidy_data

    async def get_data(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Main entry point for the adapter.
        """
        vendor_params = self._convert_params_to_vendor(data_req)
        raw_data = await self._fetch_raw_data(vendor_params)
        return self._transform_raw_response(data_req, raw_data)
