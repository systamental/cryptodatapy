import logging
from typing import Dict, Any, List, Optional, Tuple
from cryptodatapy.core.data_request import DataRequest
from cryptodatapy.extract.params.base_param_converter import BaseParamConverter
from cryptodatapy.extract.config.coinmetrics_config import COINMETRICS_ENDPOINTS

# logging setup
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)


class CoinMetricsParamConverter(BaseParamConverter):
    """
    Converts a standard DataRequest object into the specific set of parameters
    required by CoinMetrics API.
    """

    # Mapping of data type to CoinMetrics API endpoint
    ENDPOINT_MAP = COINMETRICS_ENDPOINTS

    def __init__(self, data_req: DataRequest):
        """
        Initializes the converter with the data request object.
        """
        super().__init__(data_req)

        self.base_params = self._get_base_params()

    # --------------------------------------------------------------------------
    # --- Helper Conversions ---
    # --------------------------------------------------------------------------

    def _convert_tickers(self) -> List[str]:
        """
        Converts the standard ticker to CoinMetrics-specific asset ID(s).

        Returns
        -------
        List[str]
            List of CoinMetrics asset IDs (lowercase).
        """
        if self.data_req.source_tickers is None:
            return [ticker.lower() for ticker in self.data_req.tickers]
        else:
            return [ticker.lower() for ticker in self.data_req.source_tickers]

    def _convert_freq(self) -> str:
        """
        Converts the standard frequency to CoinMetrics-specific frequency string.
        Corrected some imprecise mappings to better reflect CoinMetrics granularity.

        Returns
        -------
        str
            CoinMetrics frequency string.
        """
        # CoinMetrics uses 'm' for minutes, 'h' for hours, 'd' for days, etc.
        freq_map = {
            None: "1d",
            "block": "1b",
            "tick": "raw",
            "1s": "1s",
            "1min": "1m",
            "5min": "5m",
            "10min": "10m",
            "15min": "15m",
            "30min": "30m",
            "1h": "1h",
            "2h": "2h",
            "4h": "4h",
            "8h": "8h",
            "d": "1d",
            "w": "1d",
            "m": "1d"
        }
        return freq_map.get(self.data_req.freq, "1d")

    def _convert_exchange(self) -> str:
        """
        Converts the standard exchange to CoinMetrics-specific exchange ID.

        Returns
        -------
        str
            CoinMetrics exchange ID.
        """
        if self.data_req.exch is None:
            return "binance"
        else:
            return self.data_req.exch.lower()

    def _convert_markets(self) -> List[str]:
        """
        Converts the standard market types to CoinMetrics-specific market IDs.
        Note: The complex logic for perpetual futures is retained but simplified slightly.

        Returns
        -------
        List[str]
            List of CoinMetrics market IDs.
        """
        mkts_list = []
        quote_ccy = self._convert_quote_ccy(default_ccy='USDT', case='lower')
        exch = self._convert_exchange()
        mkt_type = self.data_req.mkt_type.lower() if self.data_req.mkt_type else 'spot'

        if self.data_req.source_markets:
            # If source_markets is explicitly provided, use it directly
            return self.data_req.source_markets

        # --- Automated Market Construction ---
        for ticker in self.data_req.tickers:
            t = ticker.lower()

            if mkt_type == "spot":
                # Example: binance-btc-usdt-spot
                mkts_list.append(f"{exch}-{t}-{quote_ccy}-{mkt_type}")

            elif mkt_type == "perpetual_future":
                # This logic is complex and vendor-specific, retained for functionality
                if exch in ["binance", "bybit", "bitmex"]:
                    # Example: binance-BTCUSDT-future (CM style for perp futures)
                    mkts_list.append(f"{exch}-{ticker.upper()}{quote_ccy.upper()}-future")
                elif exch == "okex":
                    # Example: okex-BTC-USDT-SWAP-future
                    mkts_list.append(f"{exch}-{ticker.upper()}-{quote_ccy.upper()}-SWAP-future")
                elif exch == "huobi":
                    # Example: huobi-BTC-USDT_SWAP-future
                    mkts_list.append(f"{exch}-{ticker.upper()}-{quote_ccy.upper()}_SWAP-future")
                elif exch == "hitbtc":
                    # Example: hitbtc-BTCUSDT_PERP-future
                    mkts_list.append(f"{exch}-{ticker.upper()}{quote_ccy.upper()}_PERP-future")
                else:
                    logger.warning(
                        f"Unsupported perpetual future exchange logic for: {exch}. Skipping market generation.")

        return mkts_list

    def _convert_dates(self) -> Tuple[Optional[str], Optional[str]]:
        """
        Converts start and end dates to CoinMetrics-specific date strings.

        Returns
        -------
        Tuple[Optional[str], Optional[str]]
            (source_start_date, source_end_date) in 'YYYY-MM-DD' format or None.
        """
        start_date, end_date = super()._convert_dates(
            format_type='str_ymd',
            default_start_str='2009-01-01'
        )

        return start_date, end_date

    # --------------------------------------------------------------------------
    # --- Specific Conversion Methods (Helper methods for the public convert()) ---
    # --------------------------------------------------------------------------

    def _get_base_params(self) -> Dict[str, Any]:
        """
        Returns common parameters for all CoinMetrics requests.

        Returns
        -------
        Dict[str, Any]
            A dictionary containing common vendor-specific parameters.
        """
        start_date, end_date = self._convert_dates()
        return {
            'start_time': start_date,
            'end_time': end_date,
            'pretty': True,
            'page_size': 10000,
        }

    def _convert_index(self, index_tickers: List[str], index_fields: List[str]) -> Dict[str, Any]:
        """
        Converts the standard index request to CoinMetrics-specific parameters.

        Parameters
        ----------
        index_tickers: List[str]
            List of valid index tickers for CoinMetrics.
        index_fields: List[str]
            List of valid index fields for CoinMetrics.

        Returns
        -------
        Dict[str, Any]
            A dictionary containing vendor-specific parameters for index data.
        """
        cm_tickers = self._convert_tickers()
        cm_fields = self.convert_fields(data_source='coinmetrics')
        cm_freq = self._convert_freq()

        # check tickers
        if not any(ticker.upper() in index_tickers for ticker in cm_tickers):
            cm_tickers = []
        else:
            cm_tickers = [ticker for ticker in cm_tickers if ticker.upper() in index_tickers]

            # check fields
            if not any(field in index_fields for field in cm_fields):
                cm_fields = []
            else:
                cm_fields = [field for field in cm_fields if field in index_fields]

            # check freq
            if cm_freq not in ["1s", "15s", "1h", "1d"]:
                cm_freq = ""

        if cm_tickers and cm_fields and cm_freq:
            return {
                'indexes': ",".join(cm_tickers),
                'frequency': cm_freq,
                **self.base_params,
                'endpoint': self.ENDPOINT_MAP['index'],
            }
        else:
            return {}

    def _convert_ohlcv(self, market_tickers: List[str], market_fields: List[str]) -> Dict[str, Any]:
        """
        Converts the standard OHLCV request to CoinMetrics-specific parameters.

        Parameters
        ----------
        market_tickers: List[str]
            List of valid market tickers for CoinMetrics.
        market_fields: List[str]
            List of valid market fields for CoinMetrics.

        Returns
        -------
        Dict[str, Any]
            A dictionary containing vendor-specific parameters for OHLCV data.
        """
        cm_markets = self._convert_markets()
        cm_freq = self._convert_freq()
        cm_fields = self.convert_fields(data_source='coinmetrics')

        # check tickers
        if not any(mkt in market_tickers for mkt in cm_markets):
            cm_markets = []
        else:
            cm_markets = [mkt for mkt in cm_markets if mkt in market_tickers]

            # check fields
            if not any(field in market_fields for field in cm_fields):
                cm_fields = []
            else:
                cm_fields = [field for field in cm_fields if field in market_fields]

            # check freq
            if cm_freq not in ["1m", "1h", "1d"]:
                cm_freq = ""

        if cm_markets and cm_fields and cm_freq:
            return {
                'markets': ','.join(cm_markets),
                'frequency': cm_freq,
                **self.base_params,
                'endpoint': self.ENDPOINT_MAP['ohlcv'],
            }
        else:
            return {}

    def _convert_onchain(self, asset_tickers: List[str], asset_fields: List[str]) -> Dict[str, Any]:
        """
        Converts the standard on-chain data request to CoinMetrics-specific parameters.

        Parameters
        ----------
        asset_tickers: List[str]
            List of valid asset tickers for CoinMetrics.
        asset_fields: List[str]
            List of valid asset fields for CoinMetrics.

        Returns
        -------
        Dict[str, Any]
            A dictionary containing vendor-specific parameters for on-chain data.
        """
        cm_tickers = self._convert_tickers()
        cm_freq = self._convert_freq()
        cm_fields = self.convert_fields(data_source='coinmetrics')

        # check tickers
        if not any(ticker in asset_tickers for ticker in cm_tickers):
            cm_tickers = []
        else:
            cm_tickers = [ticker for ticker in cm_tickers if ticker in asset_tickers]

            # check fields
            if not any(field in asset_fields for field in cm_fields):
                cm_fields = []
            else:
                cm_fields = [field for field in cm_fields if field in asset_fields]

            # check freq
            if cm_freq not in ["1b", "1d"]:
                cm_freq = ""

        if cm_tickers and cm_fields and cm_freq:
            return {
                'assets': ",".join(cm_tickers),
                'metrics': ",".join(cm_fields),
                'frequency': cm_freq,
                **self.base_params,
                'ignore_forbidden_errors': True,
                'ignore_unsupported_errors': True,
                'endpoint': self.ENDPOINT_MAP['onchain'],
            }
        else:
            return {}

    def _convert_open_interest(self,
                               market_tickers: List[str],
                               oi_fields: List[str] = ['contract_count']
                               ) -> Dict[str, Any]:
        """
        Converts the standard open interest data request to CoinMetrics-specific parameters.

        Returns
        -------
        Dict[str, Any]
            A dictionary containing vendor-specific parameters for open interest data.
        """
        cm_markets = self._convert_markets()
        cm_fields = self.convert_fields(data_source='coinmetrics')
        cm_mkt_type = self.data_req.mkt_type

        # check tickers
        if not any(mkt in cm_markets for mkt in market_tickers):
            cm_markets = []
        else:
            cm_markets = [mkt for mkt in cm_markets if mkt in market_tickers]

            # check fields
            if not any(field in cm_fields for field in oi_fields):
                cm_fields = []
            else:
                cm_fields = [field for field in cm_fields if field in oi_fields]

            # check mkt type
            if self.data_req.mkt_type not in ["perpetual_future", "future", "option"]:
                cm_mkt_type = ""

        if cm_markets and cm_fields and cm_mkt_type:
            return {
                'markets': ",".join(cm_markets),
                **self.base_params,
                'endpoint': self.ENDPOINT_MAP['open_interest'],
            }
        else:
            return {}

    def _convert_funding_rates(self,
                               market_tickers: List[str],
                               rate_fields: List[str] = ['rate'],
                               ) -> Dict[str, Any]:
        """
        Converts the standard funding rates data request to CoinMetrics-specific parameters.

        Parameters
        ----------
        market_tickers: List[str]
            List of valid market tickers for CoinMetrics.
        rate_fields: List[str]
            List of valid funding rate fields for CoinMetrics.

        Returns
        -------
        Dict[str, Any]
            A dictionary containing vendor-specific parameters for funding rates data.
        """
        cm_markets = self._convert_markets()
        cm_fields = self.convert_fields(data_source='coinmetrics')
        cm_mkt_type = self.data_req.mkt_type

        # check tickers
        if not any(mkt in cm_markets for mkt in market_tickers):
            cm_markets = []
        else:
            cm_markets = [mkt for mkt in cm_markets if mkt in market_tickers]

            # check fields
            if not any(field in cm_fields for field in rate_fields):
                cm_fields = []
            else:
                cm_fields = [field for field in cm_fields if field in rate_fields]

            # check mkt type
            if self.data_req.mkt_type not in ["perpetual_future", "future", "option"]:
                cm_mkt_type = ""

        if cm_markets and cm_fields and cm_mkt_type:
            return {
                'markets': ",".join(cm_markets),
                **self.base_params,
                'endpoint': self.ENDPOINT_MAP['funding_rates'],
            }
        else:
            return {}

    def _convert_trades(self,
                        market_tickers: List[str],
                        trade_fields: List[str] = ['amount', 'price', 'side']
                        ) -> Dict[str, Any]:
        """
        Converts the standard trades data request to CoinMetrics-specific parameters.

        Parameters
        ----------
        market_tickers: List[str]
            List of valid market tickers for CoinMetrics.
        trade_fields: List[str]
            List of valid trade fields for CoinMetrics.

        Returns
        -------
        Dict[str, Any]
            A dictionary containing vendor-specific parameters for trades data.
        """
        cm_markets = self._convert_markets()
        cm_fields = self.convert_fields(data_source='coinmetrics')
        cm_freq = self._convert_freq()

        # check tickers
        if not any(mkt in cm_markets for mkt in market_tickers):
            cm_markets = []
        else:
            cm_markets = [mkt for mkt in cm_markets if mkt in market_tickers]

            # check fields
            if not any(field in cm_fields for field in trade_fields):
                cm_fields = []
            else:
                cm_fields = [field for field in cm_fields if field in trade_fields]

            # check freq
            if cm_freq != "raw":
                cm_freq = ""

        if cm_markets and cm_fields and cm_freq:
            return {
                'markets': ",".join(cm_markets),
                **self.base_params,
                'endpoint': self.ENDPOINT_MAP['trades'],
            }
        else:
            return {}

    def _convert_quotes(self,
                        market_tickers: List[str],
                        quote_fields: List[str] = ['bid_price', 'ask_price', 'bid_size', 'ask_size']
                        ) -> Dict[str, Any]:
        """
        Converts the standard quotes data request to CoinMetrics-specific parameters.

        Parameters
        ----------
        market_tickers: List[str]
            List of valid market tickers for CoinMetrics.
        quote_fields: List[str]
            List of valid quote fields for CoinMetrics.

        Returns
        -------
        Dict[str, Any]
            A dictionary containing vendor-specific parameters for quotes data.
        """
        cm_markets = self._convert_markets()
        cm_freq = self._convert_freq()
        cm_fields = self.convert_fields(data_source='coinmetrics')

        # check tickers
        if not any(mkt in cm_markets for mkt in market_tickers):
            cm_markets = []
        else:
            cm_markets = [mkt for mkt in cm_markets if mkt in market_tickers]

            # check fields
            if not any(field in quote_fields for field in cm_fields):
                cm_fields = []
            else:
                cm_fields = [field for field in cm_fields if field in quote_fields]

            # check freq
            if cm_freq not in ["raw", "1s", "1m", "1h", "1d"]:
                cm_freq = ""

        if cm_markets and cm_fields and cm_freq:
            return {
                'markets': ",".join(cm_markets),
                'granularity': cm_freq,
                **self.base_params,
                'endpoint': self.ENDPOINT_MAP['quotes'],
            }
        else:
            return {}

    # --------------------------------------------------------------------------
    # --- Public Abstract Method Implementation ---
    # --------------------------------------------------------------------------

    def convert(self,
                index_tickers: Optional[List[str]] = None,
                index_fields: Optional[List[str]] = None,
                market_tickers: Optional[List[str]] = None,
                market_fields: Optional[List[str]] = None,
                asset_tickers: Optional[List[str]] = None,
                asset_fields: Optional[List[str]] = None,
                oi_fields: Optional[List[str]] = None,
                rate_fields: Optional[List[str]] = None,
                trade_fields: Optional[List[str]] = None,
                quote_fields: Optional[List[str]] = None,
                ) -> Dict[str, Any]:
        """
        Converts the DataRequest object into a dictionary of CoinMetrics parameters.
        This method dispatches to the correct specific conversion method based on ticker and field availability.

        Parameters
        ----------

        Returns
        -------
        Dict[str, Any]
            A dictionary containing vendor-specific parameters.
        """
        # initialize request list
        request_list: List[Dict[str, Any]] = []

        # indexes
        index_params = self._convert_index(
            index_tickers=index_tickers if index_tickers else [],
            index_fields=index_fields if index_fields else []
        )
        if index_params:
            request_list.append(index_params)

        # ohlcv
        ohlcv_params = self._convert_ohlcv(
            market_tickers=market_tickers if market_tickers else [],
            market_fields=market_fields if market_fields else []
        )
        if ohlcv_params:
            request_list.append(ohlcv_params)

        # onchain
        onchain_params = self._convert_onchain(
            asset_tickers=asset_tickers if asset_tickers else [],
            asset_fields=asset_fields if asset_fields else []
        )
        if onchain_params:
            request_list.append(onchain_params)

        # open interest
        oi_params = self._convert_open_interest(
            market_tickers=market_tickers if market_tickers else []
        )
        if oi_params:
            request_list.append(oi_params)

        # funding rates
        fr_params = self._convert_funding_rates(
            market_tickers=market_tickers if market_tickers else []
        )
        if fr_params:
            request_list.append(fr_params)

        # trades
        trades_params = self._convert_trades(
            market_tickers=market_tickers if market_tickers else []
        )
        if trades_params:
            request_list.append(trades_params)

        # quotes
        quotes_params = self._convert_quotes(
            market_tickers=market_tickers if market_tickers else []
        )
        if quotes_params:
            request_list.append(quotes_params)

        return {'requests': request_list}
