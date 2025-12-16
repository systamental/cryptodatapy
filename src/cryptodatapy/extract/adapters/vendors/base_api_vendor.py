from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from cryptodatapy.core.data_request import DataRequest


class BaseAPIVendorAdapter(ABC):
    """
    BaseAPIVendorAdapter is the Abstract Base Class (Target Interface) for proprietary
    REST API data vendors. It enforces the contract for data fetching and standardization.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initializes vendor-specific connection configuration."""
        config = config or {}
        # initialize essential, vendor-specific connection details here
        self._api_key: Optional[str] = config.get('api_key')
        self._base_url: Optional[str] = config.get('base_url')
        self._api_endpoints: Optional[Dict[str, str]] = config.get('api_endpoints')
        self._rate_limit: Optional[Any] = None

        # Note: Vendor-specific metadata lists (assets, fields, etc.) should be
        # defined as class attributes or properties in the concrete subclasses.

    # --- Abstract Methods (The Contract) ---

    @abstractmethod
    def get_assets_info(self) -> pd.DataFrame:
        """Gets info for available assets and returns a standardized DataFrame."""
        pass

    @abstractmethod
    def get_fields_info(self, data_type: Optional[str] = None) -> pd.DataFrame:
        """Gets info for available fields and returns a standardized DataFrame."""
        pass

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
