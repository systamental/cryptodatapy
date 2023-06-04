from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from cryptodatapy.extract.datarequest import DataRequest


class Web(ABC):
    """
    Web is an abstract base class which provides a blueprint for properties and methods for the
    web subclass.
    """

    def __init__(
        self,
        categories,
        assets,
        indexes,
        markets,
        market_types,
        fields,
        frequencies,
        base_url,
        file_formats

    ):
        self.categories = categories
        self.assets = assets
        self.indexes = indexes
        self.markets = markets
        self.market_types = market_types
        self.fields = fields
        self.frequencies = frequencies
        self.base_url = base_url
        self.file_formats = file_formats

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
            None,
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
    def indexes(self):
        """
        Returns a list of available indices from the web page.
        """
        return self._indexes

    @indexes.setter
    def indexes(self, indexes: Optional[Union[str, List[str], Dict[str, List[str]]]]):
        """
        Sets a list of available indexes for the web page.
        """
        if indexes is None or isinstance(indexes, list) or isinstance(indexes, dict):
            self._indexes = indexes
        elif isinstance(indexes, str):
            self._indexes = [indexes]
        else:
            raise TypeError(
                "Indexes must be a string (ticker), list of strings (tickers) or"
                " a dict with {cat: List[str]} key-value pairs."
            )

    @abstractmethod
    def get_indexes_info(self):
        """
        Gets info for available indexes from the data vendor.
        """
        # to be implemented by subclasses

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
        if assets is None or isinstance(assets, list) or isinstance(assets, dict):
            self._assets = assets
        elif isinstance(assets, str):
            self._assets = [assets]
        else:
            raise TypeError(
                "Assets must be a string (ticker), list of strings (tickers) or"
                " a dict with {cat: List[str]} key-value pairs."
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
        if markets is None or isinstance(markets, list) or isinstance(markets, dict):
            self._markets = markets
        elif isinstance(markets, str):
            self._markets = [markets]
        else:
            raise TypeError(
                "Markets must be a string (ticker), list of strings (tickers) or"
                " a dict with {cat: List[str]} key-value pairs."
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
    def file_formats(self):
        """
        Returns the file formats for the files on the web page.
        """
        return self._file_formats

    @file_formats.setter
    def file_formats(self, formats: Optional[Union[str, List[str]]]):
        """
        Sets the file formats for the files on the web page.
        """
        if formats is None or isinstance(formats, list):
            self._file_formats = formats
        elif isinstance(formats, str):
            self._file_formats = [formats]
        else:
            raise TypeError(
                "File format must be a string or list containing the file formats for the web page's data files"
            )

    @abstractmethod
    def get_data(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Submits get data request to API.
        """
        # to be implemented by subclasses

    @staticmethod
    @abstractmethod
    def wrangle_data_resp(data_req: DataRequest, data_resp: Union[Dict[str, Any], pd.DataFrame]) -> pd.DataFrame:
        """
        Wrangles data response from data vendor API into tidy format.
        """
        # to be implemented by subclasses
