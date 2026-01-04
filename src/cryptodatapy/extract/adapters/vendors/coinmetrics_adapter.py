from typing import Dict, Optional, Union, Any, List
import pandas as pd
import logging
from time import sleep
from tqdm import tqdm

from coinmetrics.api_client import CoinMetricsClient

from cryptodatapy.extract.adapters.base_adapter import BaseAPIAdapter
from cryptodatapy.extract.params.vendors.coinmetrics_param_converter import CoinMetricsParamConverter
from cryptodatapy.util.datacredentials import DataCredentials
from cryptodatapy.core.data_request import DataRequest
from cryptodatapy.util.api_requester import APIRequester
from cryptodatapy.transform.wranglers.coinmetrics_wrangler import CoinMetricsWrangler
from cryptodatapy.extract.config.coinmetrics_config import COINMETRICS_ENDPOINTS

logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)


class CoinMetricsAdapter(BaseAPIAdapter):
    """
    Adapter class for retrieving data from CoinMetrics API.
    Implements the BaseAdapter contract via BaseAPIAdapter.
    """

    # Mapping of data type to CoinMetrics API endpoint
    ENDPOINT_MAP = COINMETRICS_ENDPOINTS

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initializes vendor-specific connection configuration and the CoinMetrics client.

        Parameters
        ----------
        config : Optional[Dict[str, Any]], optional
            Configuration dictionary for the adapter (guaranteed to be CoinMetrics specific).
        """

        data_cred = DataCredentials()

        # hardcoded defaults
        default_config = {
            'api_key': data_cred.coinmetrics_api_key,
            'base_url': data_cred.coinmetrics_base_url
        }

        # user-provided config (if any) overrides the defaults
        final_config = {**default_config, **(config or {})}

        # initialize BaseAdapter/BaseAPIAdapter with the merged configuration
        super().__init__(final_config)

        # initialize and store the CoinMetrics SDK client
        self.client = CoinMetricsClient(api_key=final_config.get('api_key'))

        # initialize properties for caching/metadata
        self.assets: Optional[Union[pd.DataFrame, list]] = None
        self.fields: Optional[pd.DataFrame] = None
        self.markets: Optional[Union[pd.DataFrame, list]] = None
        self.exchanges: Optional[Union[pd.DataFrame, list]] = None
        self.indexes: Optional[Union[pd.DataFrame, list]] = None

    # --------------------------------------------------------------------------
    # --- 1. Helper Methods: Metadata Requests ---
    # --------------------------------------------------------------------------

    def _fetch_raw_meta(self, info_type: str, **kwargs) -> Dict[str, Any]:
        """
        Helper method to fetch raw metadata (assets, markets, fields, exchanges, etc.)

        Parameters
        ----------
        info_type : str
            The type of metadata to fetch (e.g., 'assets', 'markets', 'fields', 'exchanges', 'indexes', etc).
        kwwargs : additional keyword arguments to pass to the CoinMetrics client method.

        Returns
        -------
        Dict[str, Any]
            The raw metadata response from CoinMetrics API.
        """
        try:
            raw_data = getattr(self.client, info_type)(**kwargs)
        except AttributeError:
            raise ValueError(f"Unknown CoinMetrics info_type: {info_type}")

        return raw_data

    def get_exchanges_info(self, as_list: bool = False) -> Union[pd.DataFrame, list]:
        """
        Gets info for available exchanges and returns a standardized DataFrame or list.

        Parameters
        ----------
        as_list : bool, optional
            If True, returns a list of exchange names instead of a DataFrame. Defaults to False.

        Returns
        -------
        Union[pd.DataFrame, list]
            DataFrame or list containing exchange metadata.
        """
        raw_resp = self._fetch_raw_meta('reference_data_exchanges')
        self.exchanges = CoinMetricsWrangler(
            data_req=DataRequest(),
            data_resp=raw_resp
        ).wrangle_exchanges_info(as_list=as_list)
        return self.exchanges

    def get_indexes_info(self, as_list: bool = False) -> Union[pd.DataFrame, list]:
        """
        Gets info for available indexes and returns a standardized DataFrame or list.

        Parameters
        ----------
        as_list : bool, optional
            If True, returns a list of index names instead of a DataFrame. Defaults to False.

        Returns
        -------
        Union[pd.DataFrame, list]
            DataFrame or list containing index metadata.
        """
        raw_resp = self._fetch_raw_meta('reference_data_indexes')
        self.indexes = CoinMetricsWrangler(
            data_req=DataRequest(),
            data_resp=raw_resp
        ).wrangle_indexes_info(as_list=as_list)
        return self.indexes

    def get_assets_info(self, as_list: bool = False) -> Union[pd.DataFrame, list]:
        """
        Gets info for available assets and returns a standardized DataFrame or list.

        Parameters
        ----------
        as_list : bool, optional
            If True, returns a list of asset tickers instead of a DataFrame. Defaults to False.

        Returns
        -------
        Union[pd.DataFrame, list]
            DataFrame or list containing asset metadata.
        """
        raw_resp = self._fetch_raw_meta('reference_data_assets')
        self.assets = CoinMetricsWrangler(
            data_req=DataRequest(),
            data_resp=raw_resp
        ).wrangle_assets_info(as_list=as_list)
        return self.assets

    def get_markets_info(self, exchange: str, as_list: bool = False) -> Union[pd.DataFrame, list]:
        """
        Gets info for available markets and returns a standardized DataFrame or list.

        Parameters
        ----------
        exchange : str
            The exchange for which to fetch market information.
        as_list : bool, optional
            If True, returns a list of market names instead of a DataFrame. Defaults to False.

        Returns
        -------
        Union[pd.DataFrame, list]
            DataFrame or list containing market metadata.
        """
        raw_resp = self._fetch_raw_meta('reference_data_markets', exchange=exchange)
        self.markets = CoinMetricsWrangler(
            data_req=DataRequest(),
            data_resp=raw_resp
        ).wrangle_markets_info(as_list=as_list)
        return self.markets

    def get_fields_info(self, as_list: bool = False) -> Union[pd.DataFrame, list]:
        """
        Gets info for available on-chain fields and returns a standardized DataFrame.

        Returns
        -------
        pd.DataFrame
            DataFrame containing on-chain field metadata.
        """
        raw_resp = self._fetch_raw_meta('reference_data_asset_metrics')
        fields = CoinMetricsWrangler(
            data_req=DataRequest(),
            data_resp=raw_resp
        ).wrangle_fields_info(as_list=as_list)
        return fields

    def get_available_fields(self, as_list: bool = False) -> Union[pd.DataFrame, list]:
        """
        Gets a list of available asset tickers from CoinMetrics.

        Returns
        -------
        List[str]
            List of available asset tickers.
        """
        raw_resp = self._fetch_raw_meta('catalog_asset_metrics_v2')
        self.fields = CoinMetricsWrangler(
            data_req=DataRequest(),
            data_resp=raw_resp
        ).wrangle_available_fields(as_list=as_list)

        return self.fields

    def get_rate_limit_info(self) -> Optional[Any]:
        """
        Gets and updates the number of API calls made and remaining.

        Returns
        -------
        Optional[Any]
            Rate limit information if available.
        """
        # CoinMetrics community API does not provide rate limit info
        return None

    # --------------------------------------------------------------------------
    # --- 2. Internal Data Fetcher  ---
    # --------------------------------------------------------------------------

    def _fetch_all_raw_data(self,
                            pause: float,
                            endpoint: str,
                            params: Dict[str, Union[str, int, float]]) -> pd.DataFrame:
        """
        Internal method to handle the API request, including fetching all pages (pagination),
        with an indeterminate progress bar.

        Parameters
        ----------
        pause: float
            Time to pause between API requests to respect rate limits.
        endpoint: str
            API endpoint to target (e.g., '/timeseries/market-candles').
        params: Dict[str, Union[str, int, float]]
            Converted query parameters for the initial API request.

        Returns
        -------
        df: pd.DataFrame
            DataFrame with all raw time series data combined.
        """
        url = self._config.get('base_url', '') + endpoint
        all_data: List[Dict[str, Any]] = []
        next_page_url: Optional[str] = url

        # initial request uses params, subsequent requests use the next_page_url
        current_params = params

        # Use tqdm to show progress (indeterminate/iterator mode)
        with tqdm(unit='page', desc='Fetching data pages from CoinMetrics') as pbar:
            while next_page_url:
                if all_data:  # Only pause after the first request
                    sleep(pause)

                try:
                    # use the next_page_url if available, otherwise use the base URL + params
                    request_url = next_page_url if next_page_url != url else url

                    # if using the initial URL, pass params. Otherwise, params are included in next_page_url
                    if next_page_url == url:
                        data_resp = APIRequester.get_request(url=request_url, params=current_params)
                    else:
                        # when using the next_page_url, the params are usually part of the URL itself
                        # We pass None for params to avoid re-adding them.
                        data_resp = APIRequester.get_request(url=request_url, params=None)

                except Exception as e:
                    logger.error(f"API request failed for {request_url}: {e}")
                    # return partial data if failure occurs during pagination
                    break

                # check if data_resp is None (indicating a permanent failure)
                if data_resp is None:
                    logger.error(f"API request failed permanently for {request_url} "
                                 f"(Likely 401/403/404 after retries). Stopping pagination.")
                    break

                # the response structure needs to be checked against CoinMetrics format
                page_data = data_resp.get('data', [])
                next_page_url = data_resp.get('next_page_url')

                if not page_data and not next_page_url and not all_data:
                    raise Exception("No data returned from CoinMetrics API for the given request parameters.")

                all_data.extend(page_data)

                # update progress bar with the number of records in this page
                pbar.set_postfix_str(f"Records: {len(all_data):,}")
                pbar.update(1)  # increment page count

        # convert to df
        return pd.DataFrame(all_data)

    # --------------------------------------------------------------------------
    # --- 3. ETL Pipeline Contract Implementation (The Template Method Steps) ---
    # --------------------------------------------------------------------------

    def _convert_params_to_vendor(self, data_req: DataRequest) -> Dict[str, Any]:
        """
        CONVERT: Converts the DataRequest object into the vendor's specific API parameters (URL/payload)
        and determines the endpoint based on the request's data type.

        Parameters
        ----------
        data_req : DataRequest
            The standardized data request object.

        Returns
        -------
        Dict[str, Any]
            Vendor-specific parameters for the API request, including the 'endpoint' key.
        """
        # indexes
        self.indexes = self.get_indexes_info(as_list=True)
        index_fields = ['price_open', 'price_close', 'price_high', 'price_low', 'vwap', 'volume']
        sleep(data_req.pause)

        # markets
        exch = 'binance' if data_req.exch is None else data_req.exch.lower()
        self.markets = self.get_markets_info(exchange=exch, as_list=True)
        market_fields = ['price_open', 'price_close', 'price_high', 'price_low', 'vwap', 'volume',
                      'candle_usd_volume', 'candle_trades_count']
        sleep(data_req.pause)

        # assets
        self.assets = self.get_assets_info(as_list=True)
        asset_fields = self.get_available_fields(as_list=True)
        sleep(data_req.pause)

        # oi fields
        oi_fields = ['contract_count']

        # funding rate fields
        fr_fields = ['rate']

        # trades fields
        trades_fields = ['price', 'amount', 'side']

        # quotes fields
        quotes_fields = ['bid_price', 'ask_price', 'bid_size', 'ask_size']

        # initialize converter
        converter = CoinMetricsParamConverter(data_req)

        # convert parameters based on data type
        vendor_params = converter.convert(self.indexes, index_fields, self.markets, market_fields,
                                          self.assets, asset_fields, oi_fields, fr_fields,
                                          trades_fields, quotes_fields)

        return vendor_params

    def _fetch_raw_data(self, data_req: DataRequest, vendor_params: Dict[str, Any]) -> pd.DataFrame:
        """
        EXTRACT: Submits the vendor-specific parameters to the API and returns the raw response.

        Parameters
        ----------
        data_req : DataRequest
            The standardized data request object (needed for 'pause' value).
        vendor_params : Dict[str, Any]
            Vendor-specific parameters, *including* the 'endpoint' key retrieved from the conversion step.

        Returns
        -------
        pd.DataFrame
            Raw data response from CoinMetrics API.
        """
        raw_data = pd.DataFrame()
        requests = vendor_params['requests']

        for request in requests:

            endpoint = request.pop('endpoint')

            try:
                data_resp = self._fetch_all_raw_data(
                    pause=data_req.pause,
                    endpoint=endpoint,
                    params=request  # Only the URL parameters remain
                )
            except Exception as e:
                logger.error(f"Error fetching data from endpoint {endpoint}: {e}")
                continue

            raw_data = pd.concat([raw_data, data_resp], ignore_index=True)

        return raw_data

    def _transform_raw_response(self, data_req: DataRequest, raw_data: pd.DataFrame) -> pd.DataFrame:
        """
        TRANSFORM: Processes the raw data response into the package's standardized tidy DataFrame format.

        Parameters
        ----------
        data_req : DataRequest
            The standardized data request object.
        raw_data : pd.DataFrame
            Raw data response from CoinMetrics API.

        Returns
        -------
        pd.DataFrame
            Tidy DataFrame containing the requested time series data.
        """
        if raw_data.empty:
            return pd.DataFrame()

        tidy_data = CoinMetricsWrangler(
            data_req=data_req,
            data_resp=raw_data
        ).wrangle_time_series()

        return tidy_data

    # ----------------------------------------------------------------------
    # --- 4. Public Interface (The Template Method) ---
    # ----------------------------------------------------------------------

    def get_data(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Executes the full ETL cycle: convert parameters, fetch raw data, and transform into tidy DataFrame.
        This method acts as the Template Method for the data retrieval process inherited from the BaseAPIAdapter.

        Parameters
        ----------
        data_req : DataRequest
            The standardized data request object.

        Returns
        -------
        pd.DataFrame
            A DataFrame containing the requested time series data.
        """
        # Step 1: CONVERT parameters and determine endpoint
        vendor_params = self._convert_params_to_vendor(data_req)

        # Step 2: EXTRACT raw data (pass data_req for pause/rate limiting)
        raw_data = self._fetch_raw_data(data_req, vendor_params)

        # Step 3: TRANSFORM raw response
        tidy_data = self._transform_raw_response(data_req, raw_data)

        return tidy_data
