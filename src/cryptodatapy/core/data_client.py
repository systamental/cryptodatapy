import pandas as pd
from typing import Dict, Type, Any, Optional

from cryptodatapy.core.data_request import DataRequest
from cryptodatapy.extract.adapters.base_adapter import BaseAdapter
from cryptodatapy.extract.adapters.vendors.defillama_adapter import DefiLlamaAdapter


class DataClient:
    """
    The main public interface (Facade) for fetching data and metadata.
    It selects, delegates, and caches the appropriate vendor adapter.
    """

    # The type hint refers to the universal BaseAdapter interface.
    ADAPTER_MAPPING: Dict[str, Type[BaseAdapter]] = {
        'defillama': DefiLlamaAdapter,
        # 'ccxt': CCXTAdapter,  # Example of a Library Adapter
    }

    def __init__(self, source_config: Optional[Dict[str, Dict[str, Any]]] = None):
        """
        Initializes the client with configuration for all vendors.
        Configuration should be structured as {'vendor_name': {'setting': value}}.
        """
        self._source_config = source_config if source_config is not None else {}

        # Adapter cache: stores instantiated adapter objects for reuse
        self._adapters: Dict[str, BaseAdapter] = {}

    def _get_adapter(self, source_name: str) -> BaseAdapter:
        """
        Retrieves an instantiated adapter from the cache, or instantiates and caches it if missing.
        This handles lazy loading and passing the correct, vendor-specific configuration.
        """
        if source_name not in self._adapters:
            if source_name not in self.ADAPTER_MAPPING:
                raise ValueError(
                    f"Unknown data vendor: {source_name}. Available vendors are: {list(self.ADAPTER_MAPPING.keys())}")

            # adapter class
            AdapterClass = self.ADAPTER_MAPPING[source_name]

            # config dict for data source
            adapter_config = self._source_config.get(source_name)

            # 3. Instantiate and Cache the adapter
            self._adapters[source_name] = AdapterClass(config=adapter_config)

        return self._adapters[source_name]

    def get_data(self, request: DataRequest) -> pd.DataFrame:
        """
        Routes the standardized DataRequest to the correct vendor adapter and returns the result.

        Parameters
        ----------
        request : DataRequest
            The standardized data request object containing all necessary parameters.

        Returns
        -------
        pd.DataFrame
            A DataFrame containing the requested time series data.
        """
        adapter = self._get_adapter(request.source)
        return adapter.get_data(request)

    # ----------------------------------------------------------------------
    # --- Metadata ---
    # ----------------------------------------------------------------------
    # These methods delegate to the *instantiated* adapter, allowing access
    # to all methods defined in the BaseAdapter contract.

    def get_assets_info(self, vendor: str = 'defillama') -> pd.DataFrame:
        """
        Delegates the request for canonical asset metadata to the specified vendor adapter.

        Parameters
        ----------
        vendor : str
            The data vendor to use for fetching asset info. Default is 'defillama'.

        Returns
        -------
        pd.DataFrame
            A DataFrame containing asset metadata.
        """
        adapter = self._get_adapter(vendor)
        return adapter.get_assets_info()

    def get_fields_info(self, vendor: str = 'defillama') -> pd.DataFrame:
        """
        Delegates the request for field (metric) definitions to the specified vendor adapter.

        Parameters
        ----------
        vendor : str
            The data vendor to use for fetching field info. Default is 'defillama'.

        Returns
        -------
        pd.DataFrame
            A DataFrame containing field/metric definitions.
        """
        adapter = self._get_adapter(vendor)
        return adapter.get_fields_info()
