import logging
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import asyncio

from cryptodatapy.extract.datarequest import DataRequest
from cryptodatapy.extract.exchanges.exchange import Exchange
from cryptodatapy.transform.convertparams import ConvertParams
from cryptodatapy.transform.wrangle import WrangleData
from cryptodatapy.util.datacredentials import DataCredentials

# data credentials
data_cred = DataCredentials()


class Dydx(Exchange):
    """
    Retrieves data from dydx exchange.
    """
    def __init__(
            self,
            name: str = "dydx",
            exch_type: str = "dex",
            is_active: bool = True,
            categories: Union[str, List[str]] = "crypto",
            assets: Optional[Dict[str, List[str]]] = None,
            markets: Optional[Dict[str, List[str]]] = None,
            market_types: List[str] = ["spot", "future", "perpetual_future", "option"],
            fields: Optional[List[str]] = ["open", "high", "low", "close", "volume", "funding_rate", 'oi'],
            frequencies: Optional[Dict[str, Union[str, int]]] = None,
            fees: Optional[Dict[str, float]] = {'spot': {'maker': 0.0, 'taker': 0.0},
                                                'perpetual_future': {'maker': 0.0, 'taker': 0.0}
                                                },
            base_url: Optional[str] = None,
            api_key: Optional[str] = None,
            max_obs_per_call: Optional[int] = None,
            rate_limit: Optional[Any] = None
    ):
        """
        Initializes the Dydx class.

        Parameters:
        -----------
        name: str
            The name of the exchange.
        exch_type: str
            The type of the exchange.
        is_active: bool
            Whether the exchange is active.
        categories: Union[str, List[str]]
            The categories of the exchange.
        assets: Optional[Dict[str, List[str]]]
            The assets traded on the exchange.
        markets: Optional[Dict[str, List[str]]]
            The markets traded on the exchange.
        market_types: List[str]
            The types of markets traded on the exchange.
        fields: Optional[List[str]]
            The fields to retrieve from the exchange.
        frequencies: Optional[Dict[str, Union[str, int]]]
            The frequencies of the data to retrieve.
        fees: Optional[Dict[str, float]]
            The fees for the exchange.
        base_url: Optional[str]
            The base url of the exchange.
        api_key: Optional[str]
            The api key for the exchange.
        max_obs_per_call: Optional[int]
            The maximum number of observations per call.
        rate_limit: Optional[Any]
            The rate limit for the exchange.
        """
        super().__init__(
            name=name,
            exch_type=exch_type,
            is_active=is_active,
            categories=categories,
            assets=assets,
            markets=markets,
            market_types=market_types,
            fields=fields,
            frequencies=frequencies,
            fees=fees,
            base_url=base_url,
            api_key=api_key,
            max_obs_per_call=max_obs_per_call,
            rate_limit=rate_limit
        )
        self.data_req = None
        self.data = pd.DataFrame()

    def get_assets_info(self):
        pass

    def get_markets_info(self):
        pass

    def get_fields_info(self, data_type: Optional[str]):
        pass

    def get_frequencies_info(self):
        pass

    def get_rate_limit_info(self):
        pass

    def get_metadata(self):
        pass

    def _fetch_ohlcv(self):
        pass

    def _fetch_funding_rates(self):
        pass

    def _fetch_open_interest(self):
        pass

    def _convert_params(self):
        pass

    @staticmethod
    def _wrangle_data_resp(data_req: DataRequest, data_resp: Union[Dict[str, Any], pd.DataFrame]) -> pd.DataFrame:
        pass

    def _fetch_tidy_ohlcv(self):
        pass

    def _fetch_tidy_funding_rates(self):
        pass

    def _fetch_tidy_open_interest(self):
        pass

    def get_data(self, data_req) -> pd.DataFrame:
        pass
