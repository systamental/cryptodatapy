from typing import Dict, Optional, Union, Any, List
import pandas as pd
import numpy as np
import time
import logging
from tqdm import tqdm # Import tqdm for the progress bar

from cryptodatapy.extract.adapters.vendors.base_api_vendor import BaseAPIVendorAdapter
from cryptodatapy.util.datacredentials import DataCredentials
from cryptodatapy.core.data_request import DataRequest
from cryptodatapy.extract.params.vendors.defillama_param_converter import DefiLlamaParamConverter
from cryptodatapy.util.api_requester import APIRequester
from cryptodatapy.transform.wranglers.defillama_wrangler import DefiLlamaWrangler

# Set up logging for clarity
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)

data_cred = DataCredentials()


class DefiLlamaAdapter(BaseAPIVendorAdapter):
    """
    Adapter class for retrieving data from DefiLlama API.
    Implements the BaseAPIVendorAdapter contract.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initializes vendor-specific connection configuration."""

        # Consolidate configuration using defaults from DataCredentials
        default_config = {
            'api_key': data_cred.defillama_api_key,
            'base_url': data_cred.defillama_base_url,
            'api_endpoints': data_cred.defillama_endpoints,
            'max_obs_per_call': 2000,
            'rate_limit_rpm': 10
        }

        # Merge provided config with defaults
        final_config = {**default_config, **(config or {})}

        # Call the BaseAPIVendorAdapter __init__ with simplified config
        super().__init__(final_config)

        self._config = final_config
        self.assets = None
        self.fields = None
        self.stablecoins = None
        self.yields = None

    # --------------------------------------------------------------------------
    # --- 1. Helper Methods: Metadata Requests ---
    # --------------------------------------------------------------------------

    def _fetch_raw_meta(self, info_type: str) -> Dict[str, Any]:
        """
        Helper method to fetch raw metadata (chains, protocols, fees, etc.)
        Refactored from req_meta and moved logic to be adapter-local.

        Parameters
        ----------
        info_type : str
            The type of metadata to fetch ('chains', 'protocols', 'fees', 'stablecoins', 'yields').

        Returns
        -------
        Dict[str, Any]
            The raw metadata response from DefiLlama.
        """
        if info_type == 'stablecoins':
            # DefiLlama stablecoins endpoint is outside the main base_url
            url = 'https://stablecoins.llama.fi/stablecoins'
        elif info_type == 'yields':
            url = 'https://yields.llama.fi/pools'
        else:
            endpoint = self._api_endpoints.get(info_type)
            if not endpoint:
                raise ValueError(f"Unknown DefiLlama info_type: {info_type}")
            url = self._base_url + endpoint

        raw_data = APIRequester.get_request(url=url, params={'api_key': self._api_key})

        return raw_data

    def get_chains_info(
            self,
            as_list: bool = False,
            remove_missing: Optional[list] = None
    ) -> Union[pd.DataFrame, list]:
        """
        Get DefiLlama chains information.

        Parameters
        ----------
        as_list : bool, default False
            If True, returns the chains information as a list.
        remove_missing : Optional[list], optional
            List of fields (column names) to check for missing values to filter out.
            Default is None.

        Returns
        -------
        Union[pd.DataFrame, list]
            The requested chains information.
        """
        raw_resp = self._fetch_raw_meta('chains')

        chains = DefiLlamaWrangler(
            data_req=DataRequest(),
            data_resp=raw_resp
        ).wrangle_chains_info(
            as_list=as_list,
            remove_missing=remove_missing
        )

        return chains

    def get_protocols_info(
            self,
            as_list: bool = False,
            remove_missing: Optional[list] = None
    ) -> Union[pd.DataFrame, list]:
        """
        Get DefiLlama protocols information.

        Parameters
        ----------
        as_list : bool, default False
            If True, returns the chains information as a list.
        remove_missing : Optional[list], optional
            List of fields (column names) to check for missing values to filter out.
            Default is None.

        Returns
        -------
        Union[pd.DataFrame, list]
            The requested protocols information.
        """
        raw_resp = self._fetch_raw_meta('protocols')

        protocols = DefiLlamaWrangler(
            data_req=DataRequest(),
            data_resp=raw_resp
        ).wrangle_protocols_info(
            as_list=as_list,
            remove_missing=remove_missing
        )

        return protocols

    def get_fees_info(
            self,
            as_list: bool = False,
            remove_missing: Optional[list] = None
    ) -> Union[pd.DataFrame, list]:
        """
        Get DefiLlama fees information.

        Parameters
        ----------
        as_list : bool, default False
            If True, returns the fees information as a list.
        remove_missing : Optional[list], optional
            List of fields (column names) to check for missing values to filter out.
            Default is None.

        Returns
        -------
        Union[pd.DataFrame, list]
            The requested fees information.
        """
        raw_resp = self._fetch_raw_meta('fees')

        fees = DefiLlamaWrangler(
            data_req=DataRequest(),
            data_resp=raw_resp
        ).wrangle_fees_info(
            as_list=as_list,
            remove_missing=remove_missing
        )

        return fees

    def get_stablecoins_info(
            self,
            as_list: bool = False,
            remove_missing: Optional[list] = None
    ) -> Union[pd.DataFrame, list]:
        """
        Get DefiLlama stablecoins information.

        Parameters
        ----------
        as_list : bool, default False
            If True, returns the stablecoins information as a list.
        remove_missing : Optional[list], optional
            List of fields (column names) to check for missing values to filter out.
            Default is None.

        Returns
        -------
        Union[pd.DataFrame, list]
            The requested stablecoins information.
        """
        raw_resp = self._fetch_raw_meta('stablecoins')

        self.stablecoins = DefiLlamaWrangler(
            data_req=DataRequest(),
            data_resp=raw_resp
        ).wrangle_stablecoins_info(
            as_list=as_list,
            remove_missing=remove_missing
        )

        return self.stablecoins

    def get_yields_info(self) -> Union[pd.DataFrame, list]:
        """
        Get DefiLlama yields information.

        Returns
        -------
        Union[pd.DataFrame, list]
            The requested yields information.
        """
        raw_resp = self._fetch_raw_meta('yields')

        yields = DefiLlamaWrangler(
            data_req=DataRequest(),
            data_resp=raw_resp
        ).wrangle_yields_info()

        return yields

    @staticmethod
    def _normalize_protocols(protocols_df: pd.DataFrame) -> pd.DataFrame:
        """
        1. Processes raw protocols endpoint data, selecting only high-value (Rating 4+) fields
        for identification, classification, and cross-referencing.
        """
        # reset index
        df_protocols = protocols_df.reset_index()

        # rename cols
        df_protocols = df_protocols.rename(columns={
            'name': 'protocolName',
            'ticker': 'ticker',
            'slug': 'protocolSlug',
            'category': 'protocolCategory',
            'tvl': 'tvlUsd',
            'gecko_id': 'geckoId',
            'cmcId': 'cmcId',
            'parentProtocolSlug': 'parentSlug',
            'chains': 'chainsSupported',
            'address': 'tokenAddress',
        })

        # create new cols
        df_protocols['dataEndpoint'] = 'protocol_app'
        df_protocols['isTokenTicker'] = df_protocols['ticker'].notna()
        df_protocols['type'] = 'protocol'

        # reorder cols
        protocol_cols = [
            'ticker', 'protocolName',
            'type', 'protocolCategory',
            'parentSlug', 'protocolSlug',
            'tokenAddress', 'geckoId',
            'cmcId', 'tvlUsd', 'chainsSupported',
            'dataEndpoint', 'isTokenTicker'
        ]

        # Filter to ensure we only keep protocols that are tokenized assets.
        df_protocols = df_protocols.dropna(subset=['ticker'])[protocol_cols]

        return df_protocols

    @staticmethod
    def _normalize_chains(chains_df: pd.DataFrame) -> pd.DataFrame:
        """
        2. Normalizes the raw chain data, adds the 'dataEndpoint' tag,
           and applies manual ticker corrections (e.g., TRON -> TRX).
        """
        # reset index
        df = chains_df.reset_index()

        # rename cols
        df = df.rename(columns={
            'name': 'chainName',
            'gecko_id': 'geckoId',
            'cmcId': 'cmcId',
            'ticker': 'ticker',
            'chainId': 'eip155ChainId',
            'tvl': 'tvlUsd',
        })

        # fix incorrect tickers
        canonical_ticker_map = {
            'TRON': 'TRX',
            # add other incorrect tickers
        }
        df['ticker'] = df['ticker'].replace(canonical_ticker_map)

        # create new cols
        df['chainSlug'] = df['chainName'].str.lower().str.replace(' ', '-').str.replace('[^a-z0-9-]', '', regex=True)
        df['dataEndpoint'] = 'chain'  # Source Tag for Chain Data
        df['isTokenTicker'] = True
        df['type'] = 'chain'

        # reorder cols
        columns_to_keep = [
            'ticker', 'chainName', 'type',
            'chainSlug', 'eip155ChainId',
            'geckoId', 'cmcId',
            'tvlUsd', 'dataEndpoint',
            'isTokenTicker'
        ]
        df_normalized_chains = df.reindex(columns=columns_to_keep)

        return df_normalized_chains

    @staticmethod
    def _unify_assets(df_protocols: pd.DataFrame, df_chains: pd.DataFrame) -> pd.DataFrame:
        """
        4. Merges and concatenates protocols and chains data, applying
           the slug hierarchy, resolving collisions, and establishing
           a canonical Ticker precedence based on Type, Category, and TVL.
        """
        # keep protocol slug
        # if protocol, use parentSlug if not None, else use protocolSlug
        df_protocols['slug'] = np.where(
            df_protocols['parentSlug'].notna(),
            df_protocols['parentSlug'],
            df_protocols['protocolSlug']
        )

        # chain slug
        df_chains['slug'] = df_chains['chainSlug']

        # standardize cols names for concat
        df_protocols_prepared = df_protocols.rename(columns={'protocolName': 'name', 'protocolCategory': 'category'})
        df_chains_prepared = df_chains.rename(columns={'chainName': 'name', 'eip155ChainId': 'chainId'})

        # list all cols across both dfs and align
        common_cols = list(set(df_protocols_prepared.columns) | set(df_chains_prepared.columns))
        df_protocols_aligned = df_protocols_prepared.reindex(columns=common_cols)
        df_chains_aligned = df_chains_prepared.reindex(columns=common_cols)

        # concat protocols & chains
        df_unified = pd.concat([df_protocols_aligned, df_chains_aligned], ignore_index=True)

        # filter tickers
        df_unified.ticker = df_unified.ticker.str.upper()  # ticker to upper
        df_has_ticker = df_unified[
            (df_unified['ticker'].notna()) & (df_unified['ticker'] != '-')].copy()  # remove missing rows

        # IsCanonicalBridge L2s penalty
        df_has_ticker['is_canonical_bridge'] = df_has_ticker['category'].str.contains('Canonical Bridge', na=False)
        # prioritize chains
        type_priority = {'chain': 0, 'protocol': 1}
        df_has_ticker['type_priority'] = df_has_ticker['type'].map(type_priority)
        # sort tickers
        df_has_ticker = df_has_ticker.sort_values(
            by=['is_canonical_bridge', 'tvlUsd', 'type_priority'],
            ascending=[True, False, True]
        )

        # keep the top ticker
        df_canonical_tickers = df_has_ticker.drop_duplicates(
            subset=['ticker'],
            keep='first'
        ).drop(columns=['is_canonical_bridge', 'type_priority'])

        # new df
        df_unified = df_canonical_tickers.copy()
        df_unified = df_unified.sort_values(by='tvlUsd', ascending=False)  # sort by tvl
        df_unified.category = df_unified.category.fillna(df_unified.type)  # fill missing category with type
        df_unified = df_unified.set_index('ticker')  # set ticker as index

        # reorder cols
        columns_to_keep = [
            'name', 'type',
            'category', 'slug',
            'geckoId', 'cmcId',
            'tokenAddress', 'tvlUsd', 'chainsSupported'
        ]
        df_unified = df_unified.reindex(columns=columns_to_keep)

        return df_unified

    # --------------------------------------------------------------------------
    # --- 2. Adapter Contract: Metadata Getters ---
    # --------------------------------------------------------------------------

    def get_assets_info(self) -> pd.DataFrame:
        """
        Get DefiLlama assets information.

        Note that DefiLlama does not have a direct "assets" endpoint. This method synthesizes the assets list by
        combining and normalizing data from the protocols and chains endpoints.
        It applies a hierarchy to resolve slugs and ticker collisions, establishing a canonical ticker based on
        asset type, category, and TVL.

        Returns
        -------
        pd.DataFrame
            The requested assets information.
        """
        if self.assets is not None:
            return self.assets

        # fetch raw data for protocols and chains
        protocols_df = self.get_protocols_info()
        chains_df = self.get_chains_info()

        # normalize protocols and chains
        df_protocols_normalized = self._normalize_protocols(protocols_df)
        df_chains_normalized = self._normalize_chains(chains_df)

        # unify assets
        self.assets = self._unify_assets(df_protocols_normalized, df_chains_normalized)

        return self.assets

    def get_fields_info(self) -> pd.DataFrame:
        """
        Gets DefiLlama fields information.

        Returns
        -------
        pd.DataFrame
            The requested fields information.
        """
        if self.fields is not None:
            return self.fields

        # fields mapping (add more as needed)
        fields = {
            'tvl_usd': {
                'chain': 'v2/historicalChainTvl/',
                'protocol': 'protocol/',
                'params': {}
            },
            'fees_usd': {
                'chain': 'summary/fees/',
                'protocol': 'summary/fees/',
                'params': {'dataType': 'dailyFees'}
            },
            'rev_usd': {
                'chain': 'summary/fees/',
                'protocol': 'summary/fees/',
                'params': {'dataType': 'dailyRevenue'}
            },
            'rev_holders_usd': {
                'chain': None,
                'protocol': 'summary/fees/',
                'params': {'dataType': 'dailyHoldersRevenue'}
            }
        }

        self.fields = pd.DataFrame(fields).T

        return self.fields

    def get_rate_limit_info(self) -> Optional[Any]:
        """DefiLlama credits left in the api key, these reset on the 1st of each month."""
        url = 'pro-api.llama.fi/usage/APIKEY'

        try:
            raw_data = APIRequester.get_request(url=url, params={'api_key': self._api_key})
            return raw_data
        except Exception as e:
            logging.error(f"Error fetching rate limit info: {e}")
            return None

    # --------------------------------------------------------------------------
    # --- 3. --- Helper Methods: Data Requests --- URL Builders & Fetchers ---
    # --------------------------------------------------------------------------

    def _build_single_request_params(self, single_request_dict: Dict[str, Any]) -> tuple[str, Dict[str, Any]]:
        """
        Builds the URL and parameters for a single request object generated by the converter.
        This resolves the full path and query parameters for one API call.

        Parameters
        ----------
        single_request_dict : Dict[str, Any]
            A dictionary representing a single request with keys:
            - 'endpoint': The API endpoint path.
            - 'slug': The slug to append to the endpoint.
            - 'query_params': A dictionary of query parameters specific to this request.

        Returns
        -------
        tuple[str, Dict[str, Any]]
            A tuple containing the full URL and the parameters dictionary for the request.
        """
        base_url = self._base_url
        api_key = self._api_key

        # url: base_url + endpoint + slug
        url = base_url + single_request_dict['endpoint'] + single_request_dict.get('slug', '')

        # query parameters: api_key + query_params
        params = {'api_key': api_key, **single_request_dict['query_params']}

        return url, params

    def _fetch_all_raw_data(self, requests_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Fetches raw data for all requests in the list, implementing rate limiting.
        """
        all_data = []
        num_requests = len(requests_list)

        # calculate the required delay between requests based on the rate limit (RPS)
        rate_limit_rpm = self._config.get('rate_limit_rpm', 10)
        min_delay_seconds = 60 / rate_limit_rpm

        backoff_delay = 0.0  # Exponential backoff factor

        logger.info(f"Starting batch fetch of {num_requests} requests. Conservative delay: {min_delay_seconds:.2f}s.")

        # Wrap the iterable with tqdm for progress visualization
        pbar_desc = f"Fetching DefiLlama Data (Delay: {min_delay_seconds:.2f}s)"

        for i, request_dict in enumerate(tqdm(requests_list, desc=pbar_desc, unit="req", position=0, leave=True)):
            start_time = time.time()

            ticker = request_dict.get('ticker')
            field = request_dict.get('field')

            # url and params
            url, params = self._build_single_request_params(request_dict)

            # fetch data
            data_resp = None
            try:
                logger.debug(f"[{i + 1}/{num_requests}] Fetching {ticker}/{field} from: {url}")
                # APIRequester should handle retries and return None on final failure
                data_resp = APIRequester.get_request(url=url, params=params)

            except Exception as e:
                # Catch unexpected exceptions during the request process
                logger.error(f"FATAL REQUEST ERROR for {ticker}/{field} (URL: {url}): {type(e).__name__} - {e}")
                # Continue to the next iteration as raw_data is None

            # process raw data
            if data_resp is None:
                # apply additional backoff to increase the inter-request delay
                backoff_delay = min(backoff_delay * 2 + 1, 30)  # Increase backoff, limit to 30s

                logger.warning(
                    f"Request failed or returned None for {ticker}/{field}. "
                    f"Applying additional backoff of {backoff_delay:.2f}s before next request. URL: {url}"
                )
                continue

            else:
                # if request succeeded, gradually reduce backoff delay
                if backoff_delay > 0:
                    backoff_delay = max(0, backoff_delay - 1)

                # attach metadata to the data response
                raw_data = {
                    'metadata': {
                        'ticker': ticker,
                        'field': field,
                        'type': request_dict.get('type'),
                        'category': request_dict.get('category')
                    },
                    'data': data_resp
                }

                all_data.append(raw_data)

            # implement rate limiting delay
            end_time = time.time()
            elapsed_time = end_time - start_time

            total_required_sleep = min_delay_seconds + backoff_delay
            sleep_duration = total_required_sleep - elapsed_time

            if sleep_duration > 0 and i < num_requests - 1:  # Don't sleep after the last request
                logger.debug(f"Sleeping for {sleep_duration:.2f}s "
                             f"(Base: {min_delay_seconds:.2f}s + Backoff: {backoff_delay:.2f}s)")
                time.sleep(sleep_duration)

        return all_data

    # --------------------------------------------------------------------------
    # --- 4. Adapter Contract: ETL Pipeline Steps (Implementations) ---
    # --------------------------------------------------------------------------
    def _convert_params_to_vendor(self, data_req: DataRequest) -> Dict[str, Any]:
        """
        CONVERT: Converts the DataRequest object into the vendor's specific API parameters.
        DefiLlama URLs rely heavily on asset name being part of the path.
        """
        vendor_params = {}

        self.get_assets_info()
        self.get_fields_info()

        # Use the DefiLlamaParamConverter to handle the conversion logic
        converter = DefiLlamaParamConverter(data_req, self.assets, self.fields)
        requests_list = converter.convert()

        vendor_params['base_url'] = self._base_url
        vendor_params['api_key'] = self._api_key
        vendor_params['requests'] = requests_list

        return vendor_params

    def _fetch_raw_data(self, params: Dict[str, Any]) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """
        EXTRACT: Submits the vendor-specific parameters to the API and returns the raw response.
        """
        # TODO : Implement batch fetching with rate limiting
        return  self._fetch_all_raw_data(requests_list=params['requests'])

    def _transform_raw_response(self, data_req: DataRequest, raw_data: Any) -> pd.DataFrame:
        """
        TRANSFORM: Processes the raw data response into the package's standardized tidy DataFrame format
        using the dedicated DefiLlamaWrangler.
        """
        # Note: The wrangling logic is now delegated entirely to the separate Wrangler class
        wrangler = DefiLlamaWrangler(data_req=data_req, data_resp=raw_data)

        df = wrangler.wrangle()

        return df

    # --- tvl ---

    # chains
    def _fetch_agg_chains_tvl(self) -> List[Dict[str, Any]]:
        """
        Helper method to fetch TVL data for all chains.
        """
        url = self._base_url + self._api_endpoints.get('all_chains_tvl', 'v2/historicalChainTvl')
        raw_data = APIRequester.get_request(url=url, params={'api_key': self._api_key})
        return raw_data

    def _fetch_chain_tvl(self, chain: str) -> List[Dict[str, Any]]:
        """
        Helper method to fetch TVL data for a specific chain.

        Parameters
        ----------
        chain : str
            The chain identifier (e.g., 'ethereum', 'bitcoin', 'solana').
        """
        path = self._api_endpoints.get('chain_tvl', 'v2/historicalChainTvl/')
        url = self._base_url + path + chain
        raw_data = APIRequester.get_request(url=url, params={'api_key': self._api_key})
        return raw_data

    # protocols
    def _fetch_protocol_tvl(self, protocol: str) -> Dict[str, Any]:
        """
        Helper method to fetch TVL data for a specific protocol.

        Parameters
        ----------
        protocol : str
            The protocol identifier (e.g., 'aave', 'uniswap', 'compound').
        """
        path = self._api_endpoints.get('protocol_tvl', 'protocol/')
        url = self._base_url + path + protocol
        raw_data = APIRequester.get_request(url=url, params={'api_key': self._api_key})
        return raw_data

    # --- fees ---

    def _fetch_agg_fees(self) -> Dict[str, Any]:
        """
        Helper method to fetch aggregated fees data.
        """
        path = self._api_endpoints.get('agg_fees', 'overview/fees')
        url = self._base_url + path
        raw_data = APIRequester.get_request(url=url, params={'api_key': self._api_key})
        return raw_data

    def _fetch_chain_fees(self, chain: str) -> Dict[str, Any]:
        """
        Helper method to fetch fees data for a specific chain.

        Parameters
        ----------
        chain : str
            The chain identifier (e.g., 'ethereum', 'bitcoin', 'solana').
        """
        path = self._api_endpoints.get('chain_fees', 'summary/fees/')
        url = self._base_url + path + chain
        raw_data = APIRequester.get_request(url=url, params={'api_key': self._api_key})
        return raw_data

    def _fetch_app_fees(self, app: str) -> Dict[str, Any]:
        """
        Helper method to fetch fees data for a specific application.

        Parameters
        ----------
        app : str
            The application identifier (e.g., 'uniswap', 'aave', 'compound').
        """
        path = self._api_endpoints.get('app_fees', 'overview/fees/')
        url = self._base_url + path + app
        raw_data = APIRequester.get_request(url=url, params={'api_key': self._api_key})
        return raw_data

    # --- revenue ---
    def _fetch_agg_revenue(self) -> Dict[str, Any]:
        """
        Helper method to fetch aggregated revenue data.
        """
        path = self._api_endpoints.get('agg_revenue', 'overview/fees')
        url = self._base_url + path
        params = {'api_key': self._api_key, 'dataType': 'dailyRevenue'}

        raw_data = APIRequester.get_request(url=url, params=params)
        return raw_data

    def _fetch_chain_revenue(self, chain: str) -> Dict[str, Any]:
        """
        Helper method to fetch revenue data for a specific chain.

        Parameters
        ----------
        chain : str
            The chain identifier (e.g., 'ethereum', 'bitcoin', 'solana').
        """
        path = self._api_endpoints.get('chain_revenue', 'summary/fees/')
        url = self._base_url + path + chain
        params = {'api_key': self._api_key, 'dataType': 'dailyRevenue'}

        raw_data = APIRequester.get_request(url=url, params=params)
        return raw_data

    def _fetch_app_revenue(self, app: str) -> Dict[str, Any]:
        """
        Helper method to fetch revenue data for a specific application.

        Parameters
        ----------
        app : str
            The application identifier (e.g., 'uniswap', 'aave', 'compound').
        """
        path = self._api_endpoints.get('app_revenue', 'overview/fees/')
        url = self._base_url + path + app
        params = {'api_key': self._api_key, 'dataType': 'dailyRevenue'}

        raw_data = APIRequester.get_request(url=url, params=params)
        return raw_data

    # --- revenue holders ---

    def _fetch_agg_holders_revenue(self) -> Dict[str, Any]:
        """
        Helper method to fetch aggregated revenue holders data.
        """
        path = self._api_endpoints.get('holders_revenue', 'overview/fees')
        url = self._base_url + path
        params = {'api_key': self._api_key, 'dataType': 'dailyHoldersRevenue'}

        raw_data = APIRequester.get_request(url=url, params=params)
        return raw_data

    def _fetch_chain_holders_revenue(self, chain: str) -> Dict[str, Any]:
        """
        Helper method to fetch revenue holders data for a specific chain.

        Parameters
        ----------
        chain : str
            The chain identifier (e.g., 'ethereum', 'bitcoin', 'solana').
        """
        path = self._api_endpoints.get('chain_holders_revenue', 'summary/fees/')
        url = self._base_url + path + chain
        params = {'api_key': self._api_key, 'dataType': 'dailyHoldersRevenue'}

        raw_data = APIRequester.get_request(url=url, params=params)
        return raw_data

    def _fetch_app_holders_revenue(self, app: str) -> Dict[str, Any]:
        """
        Helper method to fetch revenue holders data for a specific application.

        Parameters
        ----------
        app : str
            The application identifier (e.g., 'uniswap', 'aave', 'compound').
        """
        path = self._api_endpoints.get('app_holders_revenue', 'overview/fees/')
        url = self._base_url + path + app
        params = {'api_key': self._api_key, 'dataType': 'dailyHoldersRevenue'}

        raw_data = APIRequester.get_request(url=url, params=params)
        return raw_data

    def _fetch_agg_stablecoin_mktcap(self) -> Dict[str, Any]:
        """
        Helper method to fetch aggregated stablecoins data.
        """
        url = 'https://stablecoins.llama.fi/stablecoincharts/all'

        raw_data = APIRequester.get_request(url=url, params={'api_key': self._api_key})
        return raw_data

    def _fetch_stablecoin_mktcap(self, stablecoin_id: str) -> Dict[str, Any]:
        """
        Helper method to fetch stablecoin market cap/supply data for a specific stablecoin.

        Parameters
        ----------
        stablecoin_id : str
            The stablecoin identifier (e.g., 'tether', 'usd-coin', 'dai').
        """
        url = f'https://stablecoins.llama.fi/stablecoin//{stablecoin_id}'

        raw_data = APIRequester.get_request(url=url, params={'api_key': self._api_key})
        return raw_data

    def _convert_ticker_to_slug(self, ticker: str) -> str:
        """
        Helper method to convert a ticker symbol to a DefiLlama protocol slug.
        This would typically involve looking up the ticker in the protocols info DataFrame.
        """
        assets_df = self.get_assets_info()

        # if not matching_protocols.empty:
        #     return matching_protocols.iloc[0]['slug']
        # else:
        #     raise ValueError(f"Ticker '{ticker}' not found in DefiLlama protocols.")

    # 1) convert ticker to slug
    # 2) convert field to endpoint/path
    # 3) build url
    # 4) fetch data

    # tickers
    # protocols

    # fields
    # tvl, chain_fees, app_fees, chain_rev, app_rev, chain_hold_rev app_hold_rev
    # mkt_cap:

    # stablecoins
    # list, done
    # agg stablecoins market cap, done
    # individual stablecoin market cap
    # convert ticker to stablecoin id

    # yields
    # list, done
    # individual yield pool data
    # convert ticker to pool id

    # dexs
    # ToDO

    # perps
    # ToDO

    # active users
    # pro api

    # unlocks
    # pro api

    # etfs
    # pro api
    # list metrics, historical flows

