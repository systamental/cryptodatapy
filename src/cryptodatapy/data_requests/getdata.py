import numpy as np
import pandas as pd
# import pytz
from cryptodatapy.data_requests.datarequest import DataRequest
from cryptodatapy.data_vendors.ccxt_api import CCXT
from cryptodatapy.data_vendors.coinmetrics_api import CoinMetrics
from cryptodatapy.data_vendors.cryptocompare_api import CryptoCompare
from cryptodatapy.data_vendors.dbnomics_api import DBnomics
from cryptodatapy.data_vendors.glassnode_api import Glassnode
from cryptodatapy.data_vendors.investpy_api import InvestPy
from cryptodatapy.data_vendors.pandasdr_api import PandasDataReader
from cryptodatapy.data_vendors.tiingo_api import Tiingo
from cryptodatapy.util.datacredentials import DataCredentials
# from datetime import datetime
# from typing import Union, Optional, Any


class GetData():
    """
    Retrieves data from data source.
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

    def get_meta(self, attr: str = None, method: str = None, **kwargs) -> pd.DataFrame:
        """
        Get metadata.

        Parameters
        ----------
        attr: str, {'source_type', 'categories', 'exchanges', 'indexes', 'assets', 'markets', 'market_types',
                    'fields', 'frequencies', 'base_url', 'api_key', 'max_obs_per_call', 'rate_limit'}, default assets
            Gets the specified attribute or method from the data source object.
        method: str, {'get_exchanges_info', 'get_indexes_info', 'get_assets_info', 'get_markets_info',
                    'get_fields_info', 'get_frequencies_info', 'get_rate_limit_info'}, default 'assets'}
            Gets the specified method from the data source object.

        Other Parameters
        ----------------
        cat: str, optional, default None
            Category for which to return metadata, e.g. 'crypto', 'fx', 'rates', 'macro', etc.
        exch: str, default 'binance'
            Name of exchange for which to return metdata, e.g. 'binance', 'ftx', 'kraken', etc.
        data_type: str, {'market', 'on-chain', 'off-chain'}, default None
            Type of data for which to return metadata, e.g. 'on-chain', 'market' or 'off-chain'.
        as_dict: bool, default False
            Returns attribute metadata as dictionary.
        as_list: bool, default False
            Returns attribute metdata as list.

        Returns
        -------
        meta: Any
            Returns metadata for selected attribute or method.
        """
        # data source objects
        data_source_dict = {'cryptocompare': CryptoCompare, 'coinmetrics': CoinMetrics, 'ccxt': CCXT,
                            'glassnode': Glassnode, 'tiingo': Tiingo, 'investpy': InvestPy,
                            'dbnomics': DBnomics, 'yahoo': PandasDataReader, 'fred': PandasDataReader,
                            'av-daily': PandasDataReader, 'av-forex-daily': PandasDataReader}

        # available properties and methods
        valid_attr = ['source_type', 'categories', 'exchanges', 'indexes', 'assets', 'markets', 'market_types',
                      'fields', 'frequencies', 'base_url', 'api_key', 'max_obs_per_call', 'rate_limit']
        valid_meth = ['get_vendors_info', 'get_exchanges_info', 'get_indexes_info', 'get_assets_info',
                      'get_markets_info', 'get_fields_info', 'get_frequencies_info', 'get_rate_limit_info',
                      'get_news_sources', 'get_news', 'get_top_market_cap_info', 'get_avail_assets_info']

        # instantiate data source obj
        ds = data_source_dict[self.data_req.data_source]
        # get property or method from data source obj
        if attr in valid_attr:
            meta = getattr(ds(), attr)
        elif method in valid_meth:
            meta = getattr(ds(), method)(**kwargs)
        else:
            raise AttributeError(f"Select a valid attribute or method. Valid attributes: {valid_attr}."
                                 f" Valid methods include: {valid_meth}.")

        return meta

    def get_series(self, method: str = 'get_data') -> pd.DataFrame:
        """
        Get requested data.

        Parameters
        ----------
        method: str, {'get_data', 'get_ohlcv', 'get_indexes', 'get_onchain', 'get_social', 'get_trades', 'get_quotes',
                      'get_funding_rates', 'get_open_interest', 'get_eqty', 'get_eqty_iex', 'get_etfs', 'get_stocks',
                      'get_fx', 'get_rates', 'get_cmdty', 'get_crypto', 'get_macro_series'}, default 'get_data'
            Gets the specified method from the data source object.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and field (cols) values.
        """
        # data source objects
        data_source_dict = {'cryptocompare': CryptoCompare, 'coinmetrics': CoinMetrics, 'ccxt': CCXT,
                            'glassnode': Glassnode, 'tiingo': Tiingo, 'investpy': InvestPy,
                            'dbnomics': DBnomics, 'yahoo': PandasDataReader, 'fred': PandasDataReader,
                            'av-daily': PandasDataReader, 'av-forex-daily': PandasDataReader}

        # instantiate data source obj
        ds = data_source_dict[self.data_req.data_source]
        # get data
        df = getattr(ds(), method)(self.data_req)

        return df
