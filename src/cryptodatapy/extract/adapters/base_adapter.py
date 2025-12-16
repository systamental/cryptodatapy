from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Union, List
import pandas as pd

from cryptodatapy.core.data_request import DataRequest


# Any adapter (vendor, library, exchange, web) must adhere to this interface.

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
    def get_assets_info(self, as_list: bool = False) -> Union[pd.DataFrame, list]:
        """Fetch canonical asset metadata (e.g., protocols, chains, tickers)."""
        pass

    @abstractmethod
    def get_fields_info(self, as_list: bool = False) -> Union[pd.DataFrame, list]:
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
    Specialized base class for vendors that wrap Python libraries (e.g., CCXT, DbNomics).
    Handles common logic for library client initialization and exception mapping.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        # Placeholder for library client instance (e.g., self._client = self._init_library_client())


class BaseWebAdapter(BaseAdapter):
    """
    Specialized base class for vendors that scrape web data (e.g., HTML, XML).
    Implements common logic for session management and anti-bot measures.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        # Placeholder for web scraping logic
