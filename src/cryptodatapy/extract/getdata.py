from typing import Optional
import pandas as pd

from cryptodatapy.extract.data_vendors.coinmetrics_api import CoinMetrics
from cryptodatapy.extract.data_vendors.cryptocompare_api import CryptoCompare
from cryptodatapy.extract.data_vendors.glassnode_api import Glassnode
from cryptodatapy.extract.data_vendors.tiingo_api import Tiingo
from cryptodatapy.extract.datarequest import DataRequest
from cryptodatapy.extract.libraries.ccxt_api import CCXT
from cryptodatapy.extract.libraries.dbnomics_api import DBnomics
from cryptodatapy.extract.libraries.investpy_api import InvestPy
from cryptodatapy.extract.libraries.pandasdr_api import PandasDataReader
from cryptodatapy.extract.web.aqr import AQR


class GetData:
    """
    Retrieves data from selected data source.
    """

    def __init__(self, data_req: DataRequest, api_key: Optional[str] = None):
        """
        Constructor

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.
        api_key: str
            Api key for data source if required.
        """
        self.data_req = data_req
        self.api_key = api_key

    def get_meta(self, attr: str = None, method: str = None, **kwargs) -> pd.DataFrame:
        """
        Get metadata.

        Parameters
        ----------
        attr: str, {'categories', 'exchanges', 'indexes', 'assets', 'markets', 'market_types', 'fields', 'frequencies',
                    'base_url', 'api_key', 'max_obs_per_call', 'rate_limit'}, default assets
            Gets the specified attribute or method from the data source object.
        method: str, {'get_exchanges_info', 'get_indexes_info', 'get_assets_info', 'get_markets_info',
                    'get_fields_info', 'get_frequencies_info', 'get_rate_limit_info', 'get_onchain_tickers_list',
                    'get_top_mkt_cap_info'}
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
            Metadata for selected attribute or method.

        Examples
        --------
        >>> data_req = DataRequest(source='ccxt')
        >>> GetData(data_req).get_meta(attr='exchanges')
        '['aax', 'alpaca', 'ascendex', 'bequant', 'bibox', 'bigone', 'binance', 'binancecoinm', 'binanceus', ...]'

        >>> data_req = DataRequest(source='investpy')
        >>> GetData(data_req).get_meta(attr='categories')
        '['fx', 'rates', 'eqty', 'cmdty', 'macro']'

        >>> data_req = DataRequest(source='ccxt')
        >>> GetData(data_req).get_meta(method='get_assets_info')
                        id          numericId       code        precision
        ticker
        1INCH	        1INCH	       None	        1INCH	        8
        1INCHDOWN	    1INCHDOWN	   None	        1INCHDOWN	    8
        1INCHUP	        1INCHUP	       None	        1INCHUP	        8
        AAVE	        AAVE	       None	        AAVE	        8
        """
        # data source objects
        data_source_dict = {
            "cryptocompare": CryptoCompare,
            "coinmetrics": CoinMetrics,
            "ccxt": CCXT,
            "glassnode": Glassnode,
            "tiingo": Tiingo,
            "investpy": InvestPy,
            "dbnomics": DBnomics,
            "yahoo": PandasDataReader,
            "fred": PandasDataReader,
            "av-daily": PandasDataReader,
            "av-forex-daily": PandasDataReader,
            "famafrench": PandasDataReader,
            "aqr": AQR
        }

        # available attr and methods
        valid_attr = [
            "source_type",
            "categories",
            "exchanges",
            "indexes",
            "assets",
            "markets",
            "market_types",
            "fields",
            "frequencies",
            "base_url",
            "api_key",
            "max_obs_per_call",
            "rate_limit",
        ]
        valid_meth = [
            "get_vendors_info",
            "get_exchanges_info",
            "get_indexes_info",
            "get_assets_info",
            "get_markets_info",
            "get_fields_info",
            "get_frequencies_info",
            "get_rate_limit_info",
            "get_news_sources",
            "get_news",
            "get_top_mkt_cap_info",
            "get_onchain_tickers_list",
        ]

        # data source
        ds = data_source_dict[self.data_req.source]
        # instantiate ds obj
        if self.api_key is not None:
            ds = ds(api_key=self.api_key)
        else:
            ds = ds()
        # get property or method from data source obj
        if attr in valid_attr:
            meta = getattr(ds, attr)
        elif method in valid_meth:
            meta = getattr(ds, method)(**kwargs)
        else:
            raise AttributeError(
                f"Select a valid attribute or method. Valid attributes: {valid_attr}."
                f" Valid methods include: {valid_meth}."
            )

        return meta

    def get_series(self, method: str = "get_data") -> pd.DataFrame:
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

        Examples
        --------
        >>> data_req = DataRequest(source='ccxt', tickers=['btc', 'eth'], fields=['open', 'high', 'low', 'close',
                                   'volume'], freq='d', exch='ftx', start_date='2017-01-01')
        >>> GetData(data_req).get_series()
                                open        high        low         close       volume
        date	    ticker
        2020-03-28	BTC	        6243.25	    6298.5	    6028.0	    6237.5	    3888.9424
                    ETH	        128.995	    133.0	    125.11	    131.04	    1751.65972
        2020-03-29	BTC	        6233.5	    6262.5	    5869.5	    5876.5	    114076.5831
                    ETH	        130.98	    131.84	    123.81	    124.33	    138449.60906
        2020-03-30	BTC	        5876.0	    6609.0	    5856.0	    6396.5	    224231.1718


        >>> data_req = DataRequest(source='glassnode', tickers=['btc', 'eth'],
                                   fields=['add_act', 'tx_count', 'issuance'], freq='d', start_date='2016-01-01')
        >>> GetData(data_req).get_series()
                                add_act     tx_count    issuance
        date        ticker
        2016-01-01	BTC	        316489	    123957	    0.085386
                    ETH	        2350	    8232	    0.133048
        2016-01-02	BTC	        419389	    148893	    0.09197
                    ETH	        2410	    9164	    0.140147
        2016-01-03	BTC	        394047	    142463	    0.091947
        """
        # data source objects
        data_source_dict = {
            "cryptocompare": CryptoCompare,
            "coinmetrics": CoinMetrics,
            "ccxt": CCXT,
            "glassnode": Glassnode,
            "tiingo": Tiingo,
            "investpy": InvestPy,
            "dbnomics": DBnomics,
            "yahoo": PandasDataReader,
            "fred": PandasDataReader,
            "av-daily": PandasDataReader,
            "av-forex-daily": PandasDataReader,
            "famafrench": PandasDataReader,
            "aqr": AQR
        }

        # data source
        ds = data_source_dict[self.data_req.source]
        # instantiate ds obj
        if self.api_key is not None:
            ds = ds(api_key=self.api_key)
        else:
            ds = ds()
        # get data
        df = getattr(ds, method)(self.data_req)

        return df
