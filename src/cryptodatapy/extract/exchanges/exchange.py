from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from cryptodatapy.extract.datarequest import DataRequest


class Exchange(ABC):
    """
    Abstract base class for crypto exchanges (CEX or DEX).

    This class provides a blueprint for interacting with crypto exchanges,
    including authentication, data retrieval, and trading functionality.
    """
    def __init__(
            self,
            name,
            exch_type,
            is_active,
            categories,
            assets,
            markets,
            market_types,
            fields,
            frequencies,
            fees,
            base_url,
            api_key,
            max_obs_per_call,
            rate_limit
    ):
        self.name = name
        self.exch_type = exch_type
        self.is_active = is_active
        self.categories = categories
        self.assets = assets
        self.markets = markets
        self.market_types = market_types
        self.fields = fields
        self.frequencies = frequencies
        self.fees = fees
        self.base_url = base_url
        self.api_key = api_key
        self.max_obs_per_call = max_obs_per_call
        self.rate_limit = rate_limit

    @property
    def name(self):
        """
        Returns the type of exchange.
        """
        return self._name

    @name.setter
    def name(self, name: str):
        """
        Sets the name of the exchange.
        """
        if name is None:
            self._name = name
        elif not isinstance(name, str):
            raise TypeError("Exchange name must be a string.")
        else:
            self._name = name

    @property
    def exch_type(self):
        """
        Returns the type of exchange.
        """
        return self._exch_type

    @exch_type.setter
    def exch_type(self, exch_type: str):
        """
        Sets the type of exchange.
        """
        valid_exch_types = ['cex', 'dex']

        if exch_type is None:
            self._exch_type = exch_type
        elif not isinstance(exch_type, str):
            raise TypeError("Exchange type must be a string.")
        elif exch_type not in valid_exch_types:
            raise ValueError(f"{exch_type} is invalid. Valid exchange types are: {valid_exch_types}")
        else:
            self._exch_type = exch_type

    @property
    def is_active(self):
        """
        Returns whether the exchange is active.
        """
        return self._is_active

    @is_active.setter
    def is_active(self, is_active: bool):
        """
        Sets whether the exchange is active.
        """
        if is_active is None:
            self._is_active = is_active
        elif not isinstance(is_active, bool):
            raise TypeError("is_active must be a boolean.")
        else:
            self._is_active = is_active

    @property
    def categories(self):
        """
        Returns a list of available categories for the data vendor.
        """
        return self._categories

    @categories.setter
    def categories(self, categories: Union[str, List[str]]):
        """
        Sets a list of available categories for the data vendor.
        """
        valid_categories = [
            "crypto",
            "fx",
            "eqty",
            "cmdty",
            "index",
            "rates",
            "bonds",
            "credit",
            "macro",
            "alt",
        ]
        cat = []

        if categories is None:
            self._categories = categories
        else:
            if not isinstance(categories, str) and not isinstance(categories, list):
                raise TypeError("Categories must be a string or list of strings.")
            if isinstance(categories, str):
                categories = [categories]
            for category in categories:
                if category in valid_categories:
                    cat.append(category)
                else:
                    raise ValueError(
                        f"{category} is invalid. Valid categories are: {valid_categories}"
                    )
            self._categories = cat

    @property
    def assets(self):
        """
        Returns a list of available assets for the data vendor.
        """
        return self._assets

    @assets.setter
    def assets(self, assets: Optional[Union[str, List[str], Dict[str, List[str]]]]):
        """
        Sets a list of available assets for the data vendor.
        """
        if (
            assets is None
            or isinstance(assets, list)
            or isinstance(assets, dict)
            or isinstance(assets, pd.DataFrame)
        ):
            self._assets = assets
        elif isinstance(assets, str):
            self._assets = [assets]
        else:
            raise TypeError(
                "Assets must be a string (ticker), list of strings (tickers),"
                " a dict with {cat: List[str]} key-value pairs or dataframe."
            )

    @abstractmethod
    def get_assets_info(self):
        """
        Gets info for available assets from the data vendor.
        """
        # to be implemented by subclasses

    @property
    def markets(self):
        """
        Returns a list of available markets for the data vendor.
        """
        return self._markets

    @markets.setter
    def markets(self, markets: Optional[Union[str, List[str], Dict[str, List[str]]]]):
        """
        Sets a list of available markets for the data vendor.
        """
        if (
            markets is None
            or isinstance(markets, list)
            or isinstance(markets, dict)
            or isinstance(markets, pd.DataFrame)
        ):
            self._markets = markets
        elif isinstance(markets, str):
            self._markets = [markets]
        else:
            raise TypeError(
                "Markets must be a string (ticker), list of strings (tickers),"
                " a dict with {cat: List[str]} key-value pairs or dataframe."
            )

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
    def market_types(self, market_types: Optional[Union[str, List[str]]]):
        """
        Sets a list of available market types for the data vendor.
        """
        valid_mkt_types, mkt_types_list = [
            None,
            "spot",
            "etf",
            "perpetual_future",
            "future",
            "swap",
            "option",
        ], []

        if market_types is None:
            self._market_types = market_types
        elif isinstance(market_types, str) and market_types in valid_mkt_types:
            self._market_types = [market_types]
        elif isinstance(market_types, list):
            for mkt in market_types:
                if mkt in valid_mkt_types:
                    mkt_types_list.append(mkt)
                else:
                    raise ValueError(
                        f"{mkt} is invalid. Valid market types are: {valid_mkt_types}"
                    )
            self._market_types = mkt_types_list
        else:
            raise TypeError("Market types must be a string or list of strings.")

    @property
    def fields(self):
        """
        Returns a list of available fields for the data vendor.
        """
        return self._fields

    @fields.setter
    def fields(self, fields: Optional[Union[str, List[str], Dict[str, List[str]]]]):
        """
        Sets a list of available fields for the data vendor.
        """
        if fields is None or isinstance(fields, list) or isinstance(fields, dict):
            self._fields = fields
        elif isinstance(fields, str):
            self._fields = [fields]
        else:
            raise TypeError(
                "Fields must be a string (field), list of strings (fields) or"
                " a dict with {cat: List[str]} key-value pairs."
            )

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
    def frequencies(
        self, frequencies: Optional[Union[str, List[str], Dict[str, List[str]]]]
    ):
        """
        Sets a list of available data frequencies for the data vendor.
        """
        if (
            frequencies is None
            or isinstance(frequencies, list)
            or isinstance(frequencies, dict)
        ):
            self._frequencies = frequencies
        elif isinstance(frequencies, str):
            self._frequencies = [frequencies]
        else:
            raise TypeError(
                "Frequencies must be a string (frequency), list of strings (frequencies) or"
                " a dict with {cat: List[str]} key-value pairs."
            )

    @abstractmethod
    def get_frequencies_info(self, data_type: Optional[str]):
        """
        Gets info for available frequencies from the exchange.
        """
        # to be implemented by subclasses

    @property
    def fees(self):
        """
        Returns a list of fees for the data vendor.
        """
        return self._fees

    @fees.setter
    def fees(self, fees: Optional[Union[float, Dict[str, float]]]):
        """
        Sets a list of fees for the data vendor.
        """
        if fees is None or isinstance(fees, float) or isinstance(fees, dict):
            self._fees = fees
        else:
            raise TypeError("Fees must be a float or dict with {cat: float} key-value pairs.")

    @property
    def base_url(self):
        """
        Returns the base url for the data vendor.
        """
        return self._base_url

    @base_url.setter
    def base_url(self, url: Optional[str]):
        """
        Sets the base url for the data vendor.
        """
        if url is None or isinstance(url, str):
            self._base_url = url
        else:
            raise TypeError(
                "Base url must be a string containing the data vendor's base URL to which endpoint paths"
                " are appended."
            )

    @property
    def api_key(self):
        """
        Returns the api key for the data vendor.
        """
        return self._api_key

    @api_key.setter
    def api_key(self, api_key: Optional[str]):
        """
        Sets the api key for the data vendor.
        """
        if api_key is None or isinstance(api_key, str) or isinstance(api_key, dict):
            self._api_key = api_key
        else:
            raise TypeError(
                "Api key must be a string or dict with data source-api key key-value pairs."
            )

    @property
    def max_obs_per_call(self):
        """
        Returns the maximum observations per API call for the data vendor.
        """
        return self._max_obs_per_call

    @max_obs_per_call.setter
    def max_obs_per_call(self, limit: Optional[Union[int, str]]):
        """
        Sets the maximum number of observations per API call for the data vendor.
        """
        if limit is None:
            self._max_obs_per_call = limit
        elif isinstance(limit, int) or isinstance(limit, str):
            self._max_obs_per_call = int(limit)
        else:
            raise TypeError(
                "Maximum number of observations per API call must be an integer or string."
            )

    @property
    def rate_limit(self):
        """
        Returns the number of API calls made and remaining.
        """
        return self._rate_limit

    @rate_limit.setter
    def rate_limit(self, limit: Optional[Any]):
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
    def get_metadata(self) -> None:
        """
        Get exchange metadata.
        """
        # to be implemented by subclasses

    @abstractmethod
    def get_data(self, data_req) -> pd.DataFrame:
        """
        Submits get data request to API.
        """
        # to be implemented by subclasses

    @staticmethod
    @abstractmethod
    def _wrangle_data_resp(data_req: DataRequest, data_resp: Union[Dict[str, Any], pd.DataFrame]) -> pd.DataFrame:
        """
        Wrangles data response from data vendor API into tidy format.
        """
        # to be implemented by subclasses
