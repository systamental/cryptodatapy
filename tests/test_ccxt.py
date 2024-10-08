import pandas as pd
import pytest
from unittest.mock import AsyncMock, Mock
import ccxt
import ccxt.async_support as ccxt_async

from cryptodatapy.transform import ConvertParams
from cryptodatapy.extract.datarequest import DataRequest
from cryptodatapy.extract.libraries.ccxt_api import CCXT


class TestCCXT:
    """
    Test CCXT class.
    """
    @pytest.fixture(autouse=True)
    def setup(self):
        self.ccxt_instance = CCXT(
            exchanges=["binance", "bitmex", "okx", "coinbase", "kraken"],
        )
        self.ccxt_instance.exchange = Mock()
        self.ccxt_instance.exchange_async = AsyncMock()
        self.ccxt_instance.exchange_async.id = "binance"
        self.ccxt_instance.exchange_async.has = {
            "fetchOHLCV": True,
            "fetchFundingRateHistory": True,
            "fetchOpenInterestHistory": True
        }
        self.ccxt_instance.exchange_async.rateLimit = 1000
        self.data_req = DataRequest(
            source="ccxt",
            exch='binance',
            tickers=['btc'],
            fields=['open', 'high', 'low', 'close', 'volume', 'funding_rate', 'oi'],
            freq='1h',
            mkt_type='perpetual_future',
            start_date=pd.Timestamp.utcnow() - pd.Timedelta(days=1)
        )
        self.data_req = ConvertParams(self.data_req).to_ccxt()
        self.ccxt_instance.exchange_async.fetchOHLCV.return_value = [
            [1727600400000, 65559.91, 65665.18, 65559.9, 65579.91, 181.75826],
            [1727604000000, 65579.91, 65582.0, 65470.67, 65571.99, 189.6351],
            [1727607600000, 65572.0, 65720.0, 65571.99, 65691.92, 287.04811],
            [1727611200000, 65691.92, 65759.5, 65691.91, 65704.51, 88.31542]
        ]
        self.ccxt_instance.exchange_async.fetchFundingRateHistory.return_value = [
            {'info': {'symbol': 'BTCUSDT',
                      'fundingTime': '1698854400000',
                      'fundingRate': '0.00010000',
                      'markPrice': '34297.00000000'},
             'symbol': 'BTC/USDT:USDT',
             'fundingRate': 0.0001,
             'timestamp': 1698854400000,
             'datetime': '2023-11-01T16:00:00.000Z'},
            {'info': {'symbol': 'BTCUSDT',
                      'fundingTime': '1698883200001',
                      'fundingRate': '0.00010000',
                      'markPrice': '35437.50000000'},
             'symbol': 'BTC/USDT:USDT',
             'fundingRate': 0.0001,
             'timestamp': 1698883200001,
             'datetime': '2023-11-02T00:00:00.001Z'},
            {'info': {'symbol': 'BTCUSDT',
                      'fundingTime': '1698912000000',
                      'fundingRate': '0.00010000',
                      'markPrice': '35280.70000000'},
             'symbol': 'BTC/USDT:USDT',
             'fundingRate': 0.0001,
             'timestamp': 1698912000000,
             'datetime': '2023-11-02T08:00:00.000Z'}
        ]
        self.ccxt_instance.exchange_async.fetchOpenInterestHistory.return_value = [
            {'symbol': 'BTC/USDT:USDT',
                'baseVolume': 80994.915,
                'quoteVolume': 4774763632.131,
                'openInterestAmount': 80994.915,
                'openInterestValue': 4774763632.131,
                'timestamp': 1725149100000,
                'datetime': '2024-09-01T00:05:00.000Z',
                'info': {'symbol': 'BTCUSDT',
                        'sumOpenInterest': '80994.91500000',
                        'sumOpenInterestValue': '4774763632.13100000',
                        'timestamp': '1725149100000'}},
                {'symbol': 'BTC/USDT:USDT',
                'baseVolume': 80985.684,
                'quoteVolume': 4778836605.276244,
                'openInterestAmount': 80985.684,
                'openInterestValue': 4778836605.276244,
                'timestamp': 1725149400000,
                'datetime': '2024-09-01T00:10:00.000Z',
                'info': {'symbol': 'BTCUSDT',
                        'sumOpenInterest': '80985.68400000',
                        'sumOpenInterestValue': '4778836605.27624400',
                        'timestamp': '1725149400000'}},
                {'symbol': 'BTC/USDT:USDT',
                'baseVolume': 80952.295,
                'quoteVolume': 4776952310.872535,
                'openInterestAmount': 80952.295,
                'openInterestValue': 4776952310.872535,
                'timestamp': 1725149700000,
                'datetime': '2024-09-01T00:15:00.000Z',
                'info': {'symbol': 'BTCUSDT',
                        'sumOpenInterest': '80952.29500000',
                        'sumOpenInterestValue': '4776952310.87253500',
                        'timestamp': '1725149700000'}}
        ]

    def test_initialization(self):
        """
        Test initialization of CCXT class.
        """
        # types
        assert isinstance(self.ccxt_instance, CCXT)
        assert isinstance(self.ccxt_instance.categories, list)
        assert isinstance(self.ccxt_instance.market_types, list)
        assert isinstance(self.ccxt_instance.max_obs_per_call, int)
        # values
        assert self.ccxt_instance.categories == ['crypto']
        assert self.ccxt_instance.market_types == ['spot', 'future', 'perpetual_future', 'option']
        assert self.ccxt_instance.max_obs_per_call == 1000

    def test_get_exchanges_info(self):
        """
        Test get exchanges method.
        """
        exchanges = self.ccxt_instance.get_exchanges_info()
        # types
        assert isinstance(exchanges, list), "Exchanges should be a list."
        # values
        assert 'binance' in exchanges

    @pytest.mark.asyncio
    async def test_fetch_ohlcv(self):

        data = await self.ccxt_instance._fetch_ohlcv(
            ticker="BTC/USDT",
            freq="1m",
            start_date=1625097600000,
            end_date=1625097660000,
            exch='binance'
        )

        assert len(data) == 4
        assert data[0][0] == 1727600400000
        assert data[1][4] == 65571.99

    @pytest.mark.asyncio
    async def test_fetch_funding_rate(self):

        data = await self.ccxt_instance._fetch_funding_rates(
            ticker="BTC/USDT",
            start_date=1625097600000,
            end_date=1625097660000,
            exch='binance'
        )

        assert len(data) == 3
        assert data[0]['symbol'] == 'BTC/USDT:USDT'
        assert data[1]['fundingRate'] == 0.0001
        assert data[2]['timestamp'] == 1698912000000
        assert data[0]['datetime'] == '2023-11-01T16:00:00.000Z'

    @pytest.mark.asyncio
    async def test_fetch_open_interest(self):

        data = await self.ccxt_instance._fetch_open_interest(
            ticker="BTC/USDT",
            freq='1h',
            start_date=1625097600000,
            end_date=1625097660000,
            exch='binance'
        )

        assert len(data) == 3
        assert data[0]['symbol'] == 'BTC/USDT:USDT'
        assert data[1]['openInterestValue'] == 4778836605.276244
        assert data[1]['openInterestAmount'] == 80985.684
        assert data[2]['timestamp'] == 1725149700000
        assert data[0]['datetime'] == '2024-09-01T00:05:00.000Z'

    @pytest.mark.asyncio
    async def test_fetch_tidy_ohlcv(self, exch='binance'):
        """
        Test fetch_tidy_ohlcv method.
        """
        self.ccxt_instance.exchange = getattr(ccxt, exch)()
        self.ccxt_instance.exchange_async = getattr(ccxt_async, exch)()

        df = await self.ccxt_instance.fetch_tidy_ohlcv(self.data_req)

        assert not df.empty, "Dataframe was returned empty."
        assert isinstance(df.index.get_level_values(0), pd.DatetimeIndex), "Index is not DatetimeIndex."
        assert list(df.columns) == ["open", "high", "low", "close", "volume"], "Fields are missing from dataframe."
        assert (df.dtypes == 'Float64').all(), "Data types are not float64."

    @pytest.mark.asyncio
    async def test_fetch_tidy_funding_rates(self, exch='binance'):
        """
        Test fetch_tidy_funding_rates method.
        """
        self.ccxt_instance.exchange = getattr(ccxt, exch)()
        self.ccxt_instance.exchange_async = getattr(ccxt_async, exch)()

        df = await self.ccxt_instance.fetch_tidy_funding_rates(self.data_req)

        assert not df.empty, "Dataframe was returned empty."
        assert isinstance(df.index.get_level_values(0), pd.DatetimeIndex), "Index is not DatetimeIndex."
        assert list(df.columns) == ["funding_rate"], "Fields are missing from dataframe."
        assert (df.dtypes == 'Float64').all(), "Data types are not float64."

    @pytest.mark.asyncio
    async def test_fetch_tidy_open_interest(self, exch='binance'):
        """
        Test fetch_tidy_open_interest method.
        """
        self.ccxt_instance.exchange = getattr(ccxt, exch)()
        self.ccxt_instance.exchange_async = getattr(ccxt_async, exch)()

        df = await self.ccxt_instance.fetch_tidy_open_interest(self.data_req)

        assert not df.empty, "Dataframe was returned empty."
        assert isinstance(df.index.get_level_values(0), pd.DatetimeIndex), "Index is not DatetimeIndex."
        assert list(df.columns) == ["oi"], "Fields are missing from dataframe."
        assert (df.dtypes == 'Float64').all(), "Data types are not float64."

    @pytest.mark.asyncio
    async def test_get_data(self, exch='binance'):
        """
        Test get data method.
        """
        self.ccxt_instance.exchange = getattr(ccxt, exch)()
        self.ccxt_instance.exchange_async = getattr(ccxt_async, exch)()

        df = await self.ccxt_instance.get_data(self.data_req)

        assert not df.empty, "Dataframe was returned empty."
        assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."
        assert isinstance(df.index.get_level_values(0), pd.DatetimeIndex), "Index is not DatetimeIndex."
        assert list(df.index.get_level_values(1).unique()) == ["BTC/USDT:USDT"], "Tickers are missing from dataframe."
        assert list(df.columns) == ["open", "high", "low", "close", "volume", "funding_rate", "oi"], \
            "Fields are missing from dataframe."
        assert (df.dtypes == 'Float64').all(), "Data types are not float64."


if __name__ == "__main__":
    pytest.main()
