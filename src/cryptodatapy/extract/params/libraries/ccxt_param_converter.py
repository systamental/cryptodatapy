import logging
from typing import Dict, Any, Tuple, List
from cryptodatapy.core.data_request import DataRequest
from cryptodatapy.extract.params.base_param_converter import BaseParamConverter

logger = logging.getLogger(__name__)


class CCXTParamConverter(BaseParamConverter):
    """
    Converts a standard DataRequest object into the specific set of parameters
    required by a CCXT-supported exchange (e.g., Binance, KuCoin).

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
        if mkt_type == "perpetual_future":
            futures_map = {
                "binance": "binanceusdm",
                "kucoin": "kucoinfutures",
                "huobi": "huobipro",
                "bitfinex": "bitfinex2",
                "mexc": "mexc3",
            }
            # use lower cased exchange name for lookup, fallback to original if not found
            return futures_map.get(exch.lower() if exch else "", exch.lower() if exch else "binanceusdm")

        # default for spot or other market types
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
        # convert dates to int types using 'ts_ms' format in base class method
        start_ts_ms, end_ts_ms = super()._convert_dates(
            format_type='ts_ms',
            default_start_str='2010-01-01'
        )
        return start_ts_ms, end_ts_ms

    def _get_required_methods(self) -> List[str]:
        """
        Identifies which CCXT methods are needed based on requested fields.
        """
        methods = []
        fields = self.data_req.fields if self.data_req.fields else []

        # Check for OHLCV fields
        ohlcv_fields = {'open', 'high', 'low', 'close', 'volume'}
        if any(f in ohlcv_fields for f in fields) or not fields:
            methods.append('fetchOHLCV')

        # Check for Funding Rate
        if 'funding_rate' in fields:
            methods.append('fetchFundingRateHistory')

        # Check for Open Interest
        if any(f in {'oi', 'oi_value', 'base_volume', 'quote_volume'} for f in fields):
            methods.append('fetchOpenInterestHistory')

        return methods

    def convert(self) -> Dict[str, Any]:
        """
        Orchestrates all conversions and compiles the final dictionary of CCXT parameters.

        Returns
        -------
        Dict[str, Any]
            A dictionary containing vendor-specific parameters.
        """
        req = self.data_req

        # generate shared parameters
        source_tickers = self._convert_case_tickers(case='upper')
        quote_ccy = self._convert_quote_ccy(default_ccy='USDT')
        source_freq = self._convert_freq()
        exch = self._convert_exchange(req.mkt_type, req.exch)
        start_ts, end_ts = self._convert_dates()
        source_markets = self._convert_markets()

        # build the list of specific requests
        requests = []
        for method in self._get_required_methods():
            requests.append({
                'method': method,
                'source_tickers': source_tickers,
                'source_markets': source_markets,
                'source_freq': source_freq,
                'source_start_date': start_ts,
                'source_end_date': end_ts,
                'exch': exch,
                'quote_ccy': quote_ccy,
                'tz': req.tz if req.tz else "UTC",
            })

        return {
            'exch': exch,
            'requests': requests
        }