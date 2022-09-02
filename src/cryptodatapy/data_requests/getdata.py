import pandas as pd
import pytz
from typing import Union, Optional
from datetime import datetime
from cryptodatapy.data_requests.datarequest import DataRequest
from cryptodatapy.data_vendors.cryptocompare_api import CryptoCompare
from cryptodatapy.data_vendors.coinmetrics_api import CoinMetrics
from cryptodatapy.data_vendors.ccxt_api import CCXT
from cryptodatapy.data_vendors.glassnode_api import Glassnode
from cryptodatapy.data_vendors.tiingo_api import Tiingo
from cryptodatapy.data_vendors.investpy_api import InvestPy
from cryptodatapy.data_vendors.pandasdr_api import PandasDataReader
from cryptodatapy.data_vendors.dbnomics_api import DBnomics


class GetData():
    """
    Retrieves data from any data source.
    """

    def __init__(self, data_req: DataRequest):
        """
        Constructor

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.
        """
        self.data_req = data_req

    def get_meta(self, attr: str = 'assets') -> pd.DataFrame:
        """
        Get metadata.

        Parameters
        ----------
        attr: str, {'source_type', 'categories', 'exchanges', 'indexes', 'assets', 'markets', 'market_types',
                    'fields', 'frequencies', 'base_url', 'api_key', 'max_obs_per_call', 'rate_limit',
                    'get_exchanges_info', 'get_indexes_info', 'get_assets_info', 'get_markets_info',
                    'get_fields_info', 'get_frequencies_info', 'get_rate_limit_info'}, default 'assets'
            Gets the specified property or method from the data source object.
        """
        # data source objects
        data_source_dict = {'cryptocompare': CryptoCompare(), 'coinmetrics': CoinMetrics(), 'ccxt': CCXT(),
                            'glassnode': Glassnode(), 'tiingo': Tiingo(), 'investpy': InvestPy(),
                            'dbnomics': DBnomics(), 'yahoo': PandasDataReader(), 'fred': PandasDataReader(),
                            'av-daily': PandasDataReader(), 'av-forex-daily': PandasDataReader()}

        # available properties and methods
        valid_properties = ['source_type', 'categories', 'exchanges', 'indexes', 'assets', 'markets', 'market_types',
                            'fields', 'frequencies', 'base_url', 'api_key', 'max_obs_per_call', 'rate_limit']
        valid_methods = ['get_exchanges_info', 'get_indexes_info', 'get_assets_info', 'get_markets_info',
                         'get_fields_info', 'get_frequencies_info', 'get_rate_limit_info']

        # instantiate data source obj
        ds = data_source_dict[self.data_req.data_source]
        # get property or method from data source obj
        if attr in valid_properties:
            meta = getattr(ds, attr)
        elif attr in valid_methods:
            meta = getattr(ds, attr)()
        else:
            raise AttributeError(f"Select a valid property or method. Valid properties include: {properties}."
                                 f" Valid methods include: {methods}.")

        return meta

    def get_data(self) -> pd.DataFrame:
        """
        Get requested data.

        """
        # data source objects
        data_source_dict = {'cryptocompare': CryptoCompare(), 'coinmetrics': CoinMetrics(), 'ccxt': CCXT(),
                            'glassnode': Glassnode(), 'tiingo': Tiingo(), 'investpy': InvestPy(),
                            'dbnomics': DBnomics(), 'yahoo': PandasDataReader(), 'fred': PandasDataReader(),
                            'av-daily': PandasDataReader(), 'av-forex-daily': PandasDataReader()}

        # instantiate data source obj
        ds = data_source_dict[self.data_req.data_source]
        # get data
        df = ds.get_data(self.data_req)

        return df
