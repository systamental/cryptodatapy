import logging
from typing import Dict, Any, Tuple
from cryptodatapy.core.data_request import DataRequest
from cryptodatapy.extract.params.base_param_converter import BaseParamConverter

logger = logging.getLogger(__name__)


class CCXTParamConverter(BaseParamConverter):
    """
    Converts a standard DataRequest object into the specific set of parameters
    required by a CCXT-supported exchange (e.g., Binance, KuCoin).

    The implementation is stateless, with conversion logic returning data
    instead of relying on a mutable class attribute (self.params).
    """

    def __init__(self, data_req: DataRequest):
        """
        Initializes the converter with the data request object.
        """
        super().__init__(data_req)

    def _convert_exchange(self, mkt_type: str, exch: str) -> str:
        """
        Converts the standard exchange name to the CCXT-specific ID
        based on the market type (spot vs. futures).
        """
        # Note: Using a dictionary lookup for complex mappings like this
        # can sometimes be cleaner than long if/elif chains.
        if mkt_type == "perpetual_future":
            futures_map = {
                "binance": "binanceusdm",
                "kucoin": "kucoinfutures",
                "huobi": "huobipro",
                "bitfinex": "bitfinex2",
                "mexc": "mexc3",
            }
            # Use lowercased exchange name for lookup, fallback to original if not found
            return futures_map.get(exch.lower() if exch else "", exch.lower() if exch else "binanceusdm")

        # Default for spot or other market types
        return exch.lower() if exch else "binance"

    def _convert_dates(self) -> Tuple[int, int]:
        """
        Converts start and end dates to Unix timestamps in milliseconds (CCXT standard).

        Leverages the base class utility (super()._convert_dates) to handle
        normalization, defaults, and formatting based on the specified parameters.

        Returns
        -------
        Tuple[int, int]
            (source_start_date_ms, source_end_date_ms)
        """
        # The base class returns Union[str, int, datetime]. Since we specified 'ts_ms',
        # it returns an int (timestamp). The explicit casting is removed for conciseness,
        # relying on the base class guarantee.
        start_ts_ms, end_ts_ms = super()._convert_dates(
            format_type='ts_ms',
            default_start_str='2010-01-01'
        )

        # We rely on the base class returning int types for 'ts_ms' format.
        return start_ts_ms, end_ts_ms

    def convert(self) -> Dict[str, Any]:
        """
        Orchestrates all conversions and compiles the final dictionary of CCXT parameters.

        Returns
        -------
        Dict[str, Any]
            A dictionary containing vendor-specific parameters.
        """
        req = self.data_req

        # Independent conversions
        source_tickers = self._convert_case_tickers(case='upper')
        quote_ccy = self._convert_quote_ccy(default_ccy='USDT')
        source_freq = self._convert_freq()
        exch = self._convert_exchange(req.mkt_type, req.exch)
        start_ts, end_ts = self._convert_dates()  # Calls the configured helper

        # Dependent conversions (rely on fields in data_req which might be processed
        # in the independent step, but generally safe to run now)
        source_markets = self._convert_markets()
        source_fields = self.convert_fields(data_source='ccxt')

        # return params
        return {
            'source_tickers': source_tickers,
            'source_fields': source_fields,
            'source_freq': source_freq,
            'source_markets': source_markets,
            'source_start_date': start_ts,
            'source_end_date': end_ts,
            'quote_ccy': quote_ccy,
            'exch': exch,
            'tz': req.tz if req.tz else "UTC",
        }
