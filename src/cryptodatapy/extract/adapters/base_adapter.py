from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Union, List
import pandas as pd

from cryptodatapy.core.data_request import DataRequest


class BaseAdapter(ABC):
    """
    The Universal Adapter Interface (Abstract Base Class).
    All concrete adapter types must inherit from this and implement the
    abstract methods (e.g., get_data, metadata methods).
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initializes the adapter with configuration."""
        self._config = config if config is not None else {}

    @abstractmethod
    def get_data(self, request: Any) -> pd.DataFrame:
        """Fetch primary time series data based on the request."""
        pass

    @abstractmethod
    def get_assets_info(self, as_list: bool = False, **kwargs) -> Union[pd.DataFrame, list]:
        """Fetch canonical asset metadata (e.g., protocols, chains, tickers)."""
        pass

    @abstractmethod
    def get_fields_info(self, as_list: bool = False, **kwargs) -> Union[pd.DataFrame, list]:
        """Fetch available field/metric definitions."""
        pass

    # add other common metadata methods here (e.g., get_protocols_info)


class BaseAPIAdapter(BaseAdapter):
    """
    Specialized base class for vendors using external HTTP APIs (e.g., DefiLlama).
    Implements common API-specific logic (e.g., base URL, API key handling).
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)

        self._base_url = self._config.get('base_url')
        self._api_key = self._config.get('api_key')
        self._api_endpoints = config.get('api_endpoints')

        # Note: source-specific metadata lists (assets, fields, etc.) should be
        # defined as class attributes or properties in the concrete subclasses.

    # --- Abstract Methods ---
    @abstractmethod
    def get_rate_limit_info(self) -> Optional[Any]:
        """Gets and updates the number of API calls made and remaining."""
        pass

    # --- Core Pipeline Steps ---
    @abstractmethod
    def _convert_params_to_vendor(self, data_req: DataRequest) -> Dict[str, Any]:
        """CONVERT: Converts the DataRequest object into the vendor's specific API parameters (URL/payload)."""
        pass

    @abstractmethod
    def _fetch_raw_data(self, vendor_params: Dict[str, Any]) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """EXTRACT: Submits the vendor-specific parameters to the API and returns the raw response."""
        pass

    @abstractmethod
    def _transform_raw_response(self, data_req: DataRequest, raw_data: Any) -> pd.DataFrame:
        """
        TRANSFORM: Processes the raw data response into the package's standardized tidy DataFrame format.
        """
        pass

    # --- The Template Method (Public Interface) ---

    def get_data(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Executes the full ETL cycle: convert parameters, fetch raw data, and transform into tidy DataFrame.
        This method acts as the Template Method for the data retrieval process.
        """
        vendor_params = self._convert_params_to_vendor(data_req)
        raw_data = self._fetch_raw_data(vendor_params)
        tidy_data = self._transform_raw_response(data_req, raw_data)

        return tidy_data


class BaseLibraryAdapter(BaseAdapter):
    """
    Refactored Base for Library Wrappers.
    Focuses on client management and bridging sync/async execution.
    """
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self._client = None  # Storage for the library instance

    @abstractmethod
    def _init_client(self, **kwargs) -> Any:
        """Logic to instantiate the library's client object."""
        pass

    @abstractmethod
    def get_rate_limit_info(self) -> Optional[Any]:
        pass

    @abstractmethod
    def get_markets_info(self, as_list: bool = False, **kwargs) -> Union[pd.DataFrame, list]:
        pass

    # --- ETL Steps remain abstract for implementation in concrete classes ---
    @abstractmethod
    def _convert_params_to_vendor(self, data_req: DataRequest) -> Dict[str, Any]:
        pass

    @abstractmethod
    def _fetch_raw_data(self, vendor_params: Dict[str, Any]) -> Any:
        pass

    @abstractmethod
    def _transform_raw_response(self, data_req: DataRequest, raw_data: Any) -> pd.DataFrame:
        pass

    def get_data(self, data_req: DataRequest) -> pd.DataFrame:
        """The Template Method for library-based data retrieval."""
        # Library adapters might need to init client on the fly if params change
        vendor_params = self._convert_params_to_vendor(data_req)
        raw_data = self._fetch_raw_data(vendor_params)
        return self._transform_raw_response(data_req, raw_data)


class BaseWebAdapter(BaseAdapter):
    """
    Specialized base class for vendors that scrape web data (e.g., HTML, XML).
    Implements common logic for session management and anti-bot measures.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        # Placeholder for web scraping logic
