# import libraries
import pandas as pd
from abc import ABC, abstractmethod
from typing import Optional


class DataVendor(ABC):
    """
    DataVendor is an abstract base class (ABS) which provides a blueprint for properties and methods for the
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
        self._source_type = source_type
        self._categories = categories
        self._assets = assets
        self._indexes = indexes
        self._markets = markets
        self._market_types = market_types
        self._fields = fields
        self._frequencies = frequencies
        self._exchanges = exchanges
        self._base_url = base_url
        self._api_key = api_key
        self._max_obs_per_call = max_obs_per_call
        self._rate_limit = rate_limit

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
        if source_type in ['exchange', 'data_vendor', 'library', 'on-chain', 'web']:
            self._source_type = source_type
        else:
            raise ValueError("Invalid source type. Source types include: 'exchange', 'data_vendor', 'library',"
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
        # convert str to list
        if isinstance(categories, str):
            categories = [categories]
        # check if valid category
        for category in categories:
            if category in ['crypto', 'fx', 'rates', 'equities', 'commodities', 'credit', 'macro', 'alt']:
                cat.append(category)
            else:
                raise ValueError(
                    f"{category} is an invalid category. Valid categories are: 'crypto', 'fx', 'rates', 'equities', "
                    f"'commodities', 'credit', 'macro', 'alt'.")
        # set categories
        self._categories = cat

    @property
    def assets(self):
        """
        Returns a list of available assets for the data vendor.
        """
        return self._assets

    @assets.setter
    def assets(self, assets):
        """
        Sets a list of available assets for the data vendor.
        """
        raise AttributeError('Assets cannot be set. Use the method get_assets() to retrieve available assets.')

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
    def indexes(self, indexes):
        """
        Sets a list of available indexes for the data vendor.
        """
        raise AttributeError('Indexes cannot be set. Use the method get_indexes() to retrieve available indexes.')

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
    def markets(self, markets):
        """
        Sets a list of available markets for the data vendor.
        """
        raise AttributeError('Markets cannot be set. Use the method get_markets() to retrieve available markets.')

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
        # convert str to list
        if isinstance(markets, str):
            markets = [markets]
        # check if valid market types
        for mkt in markets:
            if mkt in ['spot', 'perpetual_futures', 'futures', 'options']:
                mkt_types.append(mkt)
            else:
                raise ValueError(f"{mkt} is an invalid market type. Valid market types are: 'spot', 'futures', "
                                 f"'perpetual_futures' and 'options'")
                # set categories
        self._market_types = mkt_types

    @property
    def fields(self):
        """
        Returns a list of available fields for the data vendor.
        """
        return self._fields

    @fields.setter
    def fields(self, fields):
        """
        Sets a list of available fields for the data vendor.
        """
        raise AttributeError('Fields cannot be set. Use the method get_fields() to retrieve available fields.')

    @abstractmethod
    def get_fields(self, data_type: Optional[str]):
        """
        Gets a list of available fields from the data vendor.
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
        freq = []
        # convert str to list
        if isinstance(frequencies, str):
            frequencies = [frequencies]
        # check if valid data frequency
        for frequency in frequencies:
            if frequency in ['tick', '1min', '5min', '10min', '20min', '30min', '1h', '2h', '4h', '8h', 'd', 'w', 'm']:
                freq.append(frequency)
            else:
                raise ValueError(f"{frequency} is an invalid data frequency. Valid frequencies are: 'tick', '1min', "
                                 f"'5min', '10min', '15min', '30min', '1h', '2h', '4h', '8h', 'd', 'w', 'm'.")
        # set categories
        self._frequencies = freq

    @property
    def exchanges(self):
        """
        Returns a list of available exchanges for the data vendor.
        """
        return self._exchanges

    @exchanges.setter
    def exchanges(self, exchanges):
        """
        Sets a list of available exchanges for the data vendor.
        """
        raise AttributeError('Exchanges cannot be set. Use the method get_exchanges() to retrieve available exchanges.')

    @abstractmethod
    def get_exchanges_info(self):
        """
        Gets info for available exchanges from the data vendor.
        """
        # to be implemented by subclasses

    @property
    def base_url(self):
        """
        Returns the base url for the data source.
        """
        return self._base_url

    @base_url.setter
    def base_url(self, url: str):
        """
        Sets the base url for the data vendor.
        """
        if not isinstance(url, str):
            raise TypeError('Base url must be a string containing the data vendor web address.')
        else:
            self._base_url = url

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
        if not isinstance(api_key, str):
            raise TypeError('Api key must be a string.')
        else:
            self._api_key = api_key

    @property
    def max_obs_per_call(self):
        """
        Returns the maximum observations per API call for the data vendor.
        """
        return self._max_obs_per_call

    @max_obs_per_call.setter
    def max_obs_per_call(self, limit: int):
        """
        Sets the maximum number of observations per API call for the data vendor.
        """
        if not isinstance(limit, int):
            raise TypeError('Maximum number of observations per API call must be an integer.')
        else:
            self._max_obs_per_call = limit

    @property
    def rate_limit(self):
        """
        Returns the number of API calls made and remaining.
        """
        return self._rate_limit

    @rate_limit.setter
    def rate_limit(self, limit):
        """
        Sets the number of API calls made and remaining.
        """
        raise AttributeError('Rate limit cannot be set. Use the method get_rate_limit() to retrieve API call info.')

    @abstractmethod
    def get_rate_limit_info(self):
        """
        Gets the number of API calls made and remaining.
        """
        # to be implemented by subclasses

    @abstractmethod
    def convert_data_req_params(self, data_req) -> dict:
        """
        Converts CryptoDataPy data request parameters to data vendor parameters.
        """
        # to be implemented by subclasses

    @abstractmethod
    def fetch_data(self, data_req) -> pd.DataFrame:
        """
        Submits data request to API.
        """
        # to be implemented by subclasses
