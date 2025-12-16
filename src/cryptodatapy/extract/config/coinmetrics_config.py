from typing import Dict, Any


# Mapping of data type to CoinMetrics API endpoint
COINMETRICS_ENDPOINTS: Dict[str, str] = {
    'index': '/timeseries/index-levels',
    'ohlcv': '/timeseries/market-candles',
    'onchain': '/timeseries/asset-metrics',
    'open_interest': '/timeseries/market-openinterest',
    'funding_rates': '/timeseries/market-funding-rates',
    'trades': '/timeseries/market-trades',
    'quotes': '/timeseries/market-quotes',
}