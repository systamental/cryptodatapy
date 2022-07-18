import pandas as pd
from abc import ABC, abstractmethod
from typing import Optional, Union, Any


class DataVendor(ABC):
    """
    DataVendor is an abstract base class which provides a blueprint for properties and methods for the
    data vendor subclass.
    """
    def __init__(
            self,
            source_type,
            categories,
            assets,
            indexes,
            markets,
            market_types,
            fields,
            frequencies,
            exchanges,
            base_url,
            api_key,
            max_obs_per_call,
            rate_limit
    ):
        self.source_type = source_type
        self.categories = categories
        self.assets = assets
        self.indexes = indexes
        self.markets = markets
        self.market_types = market_types
        self.fields = fields
        self.frequencies = frequencies
        self.exchanges = exchanges
        self.base_url = base_url
        self.api_key = api_key
        self.max_obs_per_call = max_obs_per_call
        self.rate_limit = rate_limit

    @property
    def source_type(self):
        """
        Returns the type of data source.
        """
        return self._source_type

    @source_type.setter
    def source_type(self, source_type: str):
        """
        Sets the type of data source.
        """
        if source_type in ['data_vendor', 'library', 'exchange', 'on-chain', 'web']:
            self._source_type = source_type
        else:
            raise ValueError("Invalid source type. Source types include: 'data_vendor', 'exchange', 'library',"
                             " 'on-chain' and 'web'.")

    @property
    def categories(self):
        """
        Returns a list of available categories for the data vendor.
        """
        return self._categories

    @categories.setter
    def categories(self, categories: list[str]):
        """
        Sets a list of available categories for the data vendor.
        """
        cat = []
        if isinstance(categories, str):
            categories = [categories]
        for category in categories:
            if category in ['crypto', 'fx', 'rates', 'eqty', 'cmdty', 'credit', 'macro', 'alt']:
                cat.append(category)
            else:
                raise ValueError(
                    f"{category} is an invalid category. Valid categories are: 'crypto', 'fx', 'rates', 'equities', "
                    f"'commodities', 'credit', 'macro', 'alt'.")
        self._categories = cat

    @property
    def assets(self):
        """
        Returns a list of available assets for the data vendor.
        """
        return self._assets

    @assets.setter
    def assets(self, assets: Union[str, list[str], dict[str, list[str]]]):
        """
        Sets a list of available assets for the data vendor.
        """
        if assets is None:
            self._assets = assets
        elif isinstance(assets, str):
            self._assets = [assets]
        elif isinstance(assets, list):
            self._assets = assets
        elif isinstance(assets, dict):
            self._assets = assets
        else:
            raise ValueError("Assets must be a list of strings (tickers) or"
                             " dict with {cat: list[str]} key-value pairs.")

    @abstractmethod
    def get_assets_info(self):
        """
        Gets info for available assets from the data vendor.
        """
        # to be implemented by subclasses

    @property
    def indexes(self):
        """
        Returns a list of available indices for the data vendor.
        """
        return self._indexes

    @indexes.setter
    def indexes(self, indexes: Union[str, list[str], dict[str, list[str]]]):
        """
        Sets a list of available indexes for the data vendor.
        """
        if indexes is None:
            self._indexes = indexes
        elif isinstance(indexes, str):
            self._indexes = [indexes]
        elif isinstance(indexes, list):
            self._indexes = indexes
        elif isinstance(indexes, dict):
            self._indexes = indexes
        else:
            raise ValueError("Indexes must be a list of strings (tickers) or"
                             " dict with {cat: list[str]} key-value pairs.")

    @abstractmethod
    def get_indexes_info(self):
        """
        Gets info for available indexes from the data vendor.
        """
        # to be implemented by subclasses

    @property
    def markets(self):
        """
        Returns a list of available markets for the data vendor.
        """
        return self._markets

    @markets.setter
    def markets(self, markets: Union[str, list[str], dict[str, list[str]]]):
        """
        Sets a list of available markets for the data vendor.
        """
        if markets is None:
            self._markets = markets
        elif isinstance(markets, str):
            self._markets = [markets]
        elif isinstance(markets, list):
            self._markets = markets
        elif isinstance(markets, dict):
            self._markets = markets
        else:
            raise ValueError("Markets must be a list of strings (pairs) or"
                             " dict with {cat: list[str]} key-value pairs.")

    @abstractmethod
    def get_markets_info(self):
        """
        Gets info for available markets from the data vendor.
        """
        # to be implemented by subclasses

    @property
    def market_types(self):
        """
        Returns a list of available market types for the data vendor.
        """
        return self._market_types

    @market_types.setter
    def market_types(self, markets: list[str]):
        """
        Sets a list of available market types for the data vendor.
        """
        mkt_types = []
        if isinstance(markets, str):
            markets = [markets]
        for mkt in markets:
            if mkt in ['spot', 'perpetual_future', 'future', 'option']:
                mkt_types.append(mkt)
            else:
                raise ValueError(f"{mkt} is an invalid market type. Valid market types are: 'spot', 'future', "
                                 f"'perpetual_future' and 'option'")
        self._market_types = mkt_types

    @property
    def fields(self):
        """
        Returns a list of available fields for the data vendor.
        """
        return self._fields

    @fields.setter
    def fields(self, fields: Union[str, list[str], dict[str, list[str]]]):
        """
        Sets a list of available fields for the data vendor.
        """
        if fields is None:
            self._fields = fields
        elif isinstance(fields, str):
            self._fields = [fields]
        elif isinstance(fields, list):
            self._fields = fields
        elif isinstance(fields, dict):
            self._fields = fields
        else:
            raise ValueError("Fields must be a list of strings (fields) or"
                             " dict with {cat: list[str]} key-value pairs.")

    @abstractmethod
    def get_fields_info(self, data_type: Optional[str]):
        """
        Gets info for available fields from the data vendor.
        """
        # to be implemented by subclasses

    @property
    def frequencies(self):
        """
        Returns a list of available data frequencies for the data vendor.
        """
        return self._frequencies

    @frequencies.setter
    def frequencies(self, frequencies: list[str]):
        """
        Sets a list of available data frequencies for the data vendor.
        """
        valid_freq = ['tick', 'block', '1s', '1min', '5min', '10min', '15min', '30min', '1h', '2h', '4h', '8h',
                      'b', 'd', 'w', 'm', 'q', 'y']
        freq = []
        if isinstance(frequencies, str):
            frequencies = [frequencies]
        for frequency in frequencies:
            if frequency in valid_freq:
                freq.append(frequency)
            else:
                raise ValueError(f"{frequency} is an invalid data frequency. Valid frequencies are: {valid_freq}.")
        self._frequencies = freq

    @property
    def exchanges(self):
        """
        Returns a list of available exchanges for the data vendor.
        """
        return self._exchanges

    @exchanges.setter
    def exchanges(self, exchanges: Union[str, list[str], dict[str, list[str]]]):
        """
        Sets a list of available exchanges for the data vendor.
        """
        if exchanges is None:
            self._exchanges = exchanges
        elif isinstance(exchanges, str):
            self._exchanges = [exchanges]
        elif isinstance(exchanges, list):
            self._exchanges = exchanges
        elif isinstance(exchanges, dict):
            self._exchanges = exchanges
        else:
            raise ValueError("Exchanges must be a list of strings (exchanges) or"
                             " dict with {cat: list[str]} key-value pairs.")

    @abstractmethod
    def get_exchanges_info(self):
        """
        Gets info for available exchanges from the data vendor.
        """
        # to be implemented by subclasses

    @property
    def base_url(self):
        """
        Returns the base url for the data vendor.
        """
        return self._base_url

    @base_url.setter
    def base_url(self, url: str):
        """
        Sets the base url for the data vendor.
        """
        if url is None:
            self._base_url = url
        elif isinstance(url, str):
            self._base_url = url
        else:
            raise TypeError("Base url must be a string containing the data vendor's base URL to which endpoint paths"
                            " are appended.")

    @property
    def api_key(self):
        """
        Returns the api key for the data vendor.
        """
        return self._api_key

    @api_key.setter
    def api_key(self, api_key: str):
        """
        Sets the api key for the data vendor.
        """
        if api_key is None:
            self._api_key = api_key
        elif isinstance(api_key, str):
            self._api_key = api_key
        else:
            raise TypeError('Api key must be a string.')

    @property
    def max_obs_per_call(self):
        """
        Returns the maximum observations per API call for the data vendor.
        """
        return self._max_obs_per_call

    @max_obs_per_call.setter
    def max_obs_per_call(self, limit: Union[int, str]):
        """
        Sets the maximum number of observations per API call for the data vendor.
        """
        if limit is None:
            self._max_obs_per_call = limit
        elif isinstance(limit, int) or isinstance(limit, str):
            self._max_obs_per_call = int(limit)
        else:
            raise TypeError('Maximum number of observations per API call must be an integer or string.')

    @property
    def rate_limit(self):
        """
        Returns the number of API calls made and remaining.
        """
        return self._rate_limit

    @rate_limit.setter
    def rate_limit(self, limit: Any):
        """
        Sets the number of API calls made and remaining.
        """
        self._rate_limit = limit

    @abstractmethod
    def get_rate_limit_info(self):
        """
        Gets the number of API calls made and remaining.
        """
        # to be implemented by subclasses

    @abstractmethod
    def get_data(self, data_req) -> pd.DataFrame:
        """
        Submits data request to API.
        """
        # to be implemented by subclasses

    @abstractmethod
    def wrangle_data_resp(self, data_resp: pd.DataFrame) -> pd.DataFrame:
        """
        Wrangles data response from data vendor API into tidy format.
        """
        # to be implemented by subclasses
