from datetime import datetime
import pandas as pd
import pytest
from time import sleep

from cryptodatapy.extract.data_vendors.coinmetrics_api import CoinMetrics
from cryptodatapy.extract.datarequest import DataRequest


@pytest.fixture
def data_req():
    return DataRequest(tickers=['btc', 'eth', 'sol'], fields=["add_act", "tx_count", "close"])


class TestCoinMetrics:
    """
    Test class for CoinMetrics.
    """

    @pytest.fixture(autouse=True)
    def cm_default(self):
        self.cm = CoinMetrics()

    def test_get_exchanges_info(self):
        """
        Test get_exchanges_info method.
        """
        exch_info = self.cm.get_exchanges_info()
        sleep(0.6)

        # data type
        assert isinstance(exch_info, pd.DataFrame), "Exchanges info should be a dataframe."
        # shape
        assert exch_info.shape[1] == 3, "Exchanges info should have 3 columns."
        # columns
        assert all(exch_info.columns == ['markets', 'min_time', 'max_time']), \
            "Exchanges info should have 'markets', 'min_time', 'max_time' columns."
        # index
        assert exch_info.index.name == 'exchange', "Exchanges info should have 'exchange' as index name."
        assert 'binance' in exch_info.index, "Exchanges info should have 'binance' as index."

    def test_get_indexes_info(self):
        """
        Test get_indexes_info method.
        """
        idx_info = self.cm.get_indexes_info()
        sleep(0.6)

        # data type
        assert isinstance(idx_info, pd.DataFrame), "Indexes info should be a dataframe."
        # shape
        assert idx_info.shape[1] == 4, "Indexes info should have 4 columns."
        # columns
        assert 'description' in idx_info.columns, "Indexes info should have 'description' column."
        # index
        assert idx_info.index.name == 'ticker', "Indexes info should have 'ticker' as index name."
        assert 'CMBI10' in idx_info.index, "Indexes info should have 'CMBI10' as index."

    def test_get_assets_info(self):
        """
        Test get_assets_info method.
        """
        assets_info = self.cm.get_assets_info()
        sleep(0.6)

        # data type
        assert isinstance(assets_info, pd.DataFrame), "Assets info should be a dataframe."
        # shape
        assert assets_info.shape[1] == 6, "Assets info should have 6 columns."
        # columns
        assert 'metrics' in assets_info.columns, "Assets info should have 'metrics' column."
        # index
        assert assets_info.index.name == 'ticker', "Assets info should have 'ticker' as index name."
        assert 'btc' in assets_info.index, "Assets info should have 'btc' as index."

    def test_get_markets_info(self):
        """
        Test get_markets_info method.
        """
        markets_info = self.cm.get_markets_info()
        sleep(0.6)

        # data type
        assert isinstance(markets_info, pd.DataFrame), "Markets info should be a dataframe."
        # shape
        assert markets_info.shape[1] == 33, "Markets info should have 33 columns."
        # columns
        assert 'exchange' in markets_info.columns, "Markets info should have 'exchange' column."
        # index
        assert markets_info.index.name == 'market', "Markets info should have 'market' as index name."
        assert 'binance-btc-usdt-spot' in markets_info.index, "Markets info should have 'binance-btc-usdt-spot'."

    def test_get_fields_info(self):
        """
        Test get_fields_info method.
        """
        fields_info = self.cm.get_fields_info()
        sleep(0.6)

        # data type
        assert isinstance(fields_info, pd.DataFrame), "Fields info should be a dataframe."
        # shape
        assert fields_info.shape[1] == 10, "Fields info should have 10 columns."
        # columns
        assert 'category' in fields_info.columns, "Fields info should have 'category' column."
        # index
        assert fields_info.index.name == 'fields', "Fields info should have 'fields' as index name."
        assert 'AdrActCnt' in fields_info.index, "Fields info should have 'AdrActCnt' as index."

    def test_get_inst_info(self):
        """
        Test get_inst_info method.
        """
        inst_info = self.cm.get_inst_info()
        sleep(0.6)

        # data type
        assert isinstance(inst_info, pd.DataFrame), "Institutions info should be a dataframe."
        # shape
        assert inst_info.shape[1] == 2, "Institutions info should have 2 columns."
        # columns
        assert 'metrics' in inst_info.columns, "Institutions info should have 'metrics' column."
        assert inst_info.institution.iloc[0] == 'grayscale', "Institutions info should have 'grayscale'."

    def test_get_onchain_fields_info(self):
        """
        Test get_onchain_fields_info method.
        """
        onchain_fields_info = self.cm.get_onchain_fields_info()
        sleep(0.6)

        # data type
        assert isinstance(onchain_fields_info, pd.DataFrame), "Onchain fields info should be a dataframe."
        # shape
        assert onchain_fields_info.shape[1] == 10, "Onchain fields info should have 10 columns."
        # columns
        assert 'category' in onchain_fields_info.columns, "Onchain fields info should have 'category' column."
        # index
        assert onchain_fields_info.index.name == 'fields', "Onchain fields info should have 'fields' as index name."
        assert 'AdrActCnt' in onchain_fields_info.index, "Onchain fields info should have 'AdrActCnt' as index."

    def test_get_onchain_tickers_list(self, data_req):
        """
        Test get_onchain_tickers_list method.
        """
        onchain_tickers_list = self.cm.get_onchain_tickers_list(data_req)
        sleep(0.6)

        # data type
        assert isinstance(onchain_tickers_list, list), "Onchain tickers list should be a list."
        # elements
        assert 'btc' in onchain_tickers_list, "Onchain tickers list should have 'btc'."

    def test_get_metadata(self):
        """
        Test get_metadata method.
        """
        self.cm.get_metadata()
        sleep(6)

        # data type
        assert isinstance(self.cm.exchanges, list), "Exchanges should be a list."
        assert isinstance(self.cm.indexes, list), "Indexes should be a list."
        assert isinstance(self.cm.assets, list), "Assets should be a list."
        assert isinstance(self.cm.markets, list), "Markets should be a list."
        assert isinstance(self.cm.fields, list), "Fields should be a list."
        assert isinstance(self.cm.frequencies, list), "Frequencies should be a list."
        assert isinstance(self.cm.market_types, list), "Market types should be a list."

    def test_req_data(self):
        """
        Test req_data method.
        """
        df = self.cm.req_data(data_type='/timeseries/market-candles',
                              params={'markets': ['binance-btc-usdt-spot']})
        sleep(0.6)

        # data type
        assert isinstance(df, pd.DataFrame), "Dataframe should be returned."
        # columns
        assert list(df.columns) == ['market', 'time', 'price_open', 'price_close', 'price_high', 'price_low', 'vwap',
                                    'volume', 'candle_usd_volume', 'candle_trades_count'], \
            "Columns should be 'market', 'time', 'price_open', 'price_close', 'price_high', 'price_low', 'vwap', " \
            "'volume', 'candle_usd_volume', 'candle_trades_count'."
        # vals
        assert 'binance-btc-usdt-spot' in df.market.unique(), "Market is missing from dataframe."
        # index
        assert isinstance(df.index, pd.RangeIndex), "Index should be RangeIndex."
        # shape
        assert df.shape[1] == 10, "Dataframe should have 10 columns."

    def test_wrangle_data_resp(self, data_req):
        """
        Test wrangle_data_resp method.
        """
        df = self.cm.req_data(data_type='/timeseries/market-candles',
                              params={'markets': ['binance-btc-usdt-spot']})
        df = self.cm.wrangle_data_resp(data_req, df)
        sleep(0.6)

        # data type
        assert isinstance(df, pd.DataFrame), "Dataframe should be returned."
        # columns
        assert list(df.columns) == ["open", "high", "low", "close", "volume", "vwap"], \
            "Columns should be 'open', 'high', 'low', 'close', 'volume', 'vwap'."
        # index
        assert isinstance(df.index, pd.MultiIndex), "Index should be MultiIndex."
        assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index should be DatetimeIndex."
        # vals
        assert list(df.index.droplevel(0).unique()) == ['BTC'], "Tickers are missing from dataframe."
        # shape
        assert df.shape[1] == 6, "Dataframe should have 6 columns."

    def test_get_tidy_data(self, data_req):
        """
        Test get_tidy_data method.
        """
        df = self.cm.get_tidy_data(data_req, data_type='/timeseries/market-candles',
                                   params={'markets': ['binance-btc-usdt-spot']})
        sleep(0.6)

        # data type
        assert isinstance(df, pd.DataFrame), "Dataframe should be returned."
        # columns
        assert list(df.columns) == ["open", "high", "low", "close", "volume", "vwap"], \
            "Columns should be 'open', 'high', 'low', 'close', 'volume', 'vwap'."
        # index
        assert isinstance(df.index, pd.MultiIndex), "Index should be MultiIndex."
        assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index should be DatetimeIndex."
        assert pd.Timestamp.utcnow().date() - df.index.droplevel(1)[-1].date() < pd.Timedelta(days=3), \
            "End date should be less than 72h ago."
        # vals
        assert list(df.index.droplevel(0).unique()) == ['BTC'], "Tickers are missing from dataframe."
        # shape
        assert df.shape[1] == 6, "Dataframe should have 6 columns."

    def test_check_tickers(self, data_req):
        """
        Test check_tickers method.
        """
        tickers = self.cm.check_tickers(data_req, data_type='asset_metrics')
        sleep(0.6)

        # data type
        assert isinstance(tickers, list), "Tickers should be a list."
        # elements
        assert tickers == ['btc', 'eth', 'sol'], "Tickers should be 'btc', 'eth', 'sol'."

    def test_check_fields(self, data_req):
        """
        Test check_fields method.
        """
        fields = self.cm.check_fields(data_req, data_type='asset_metrics')
        sleep(0.6)

        # data type
        assert isinstance(fields, list), "Fields should be a list."
        # elements
        assert fields == ['AdrActCnt', 'TxCnt'], "Fields should be ['AdrActCnt', 'TxCnt']."

    def test_check_params(self):
        """
        Test check_params method.
        """
        dr = DataRequest(tickers=['btc'], fields=["add_act"], freq='1min')
        # asset metrics
        with pytest.raises(ValueError):
            self.cm.check_params(dr, data_type='asset_metrics')
        # funding rates
        dr = DataRequest(mkt_type='spot')
        with pytest.raises(ValueError):
            self.cm.check_params(dr, data_type='funding_rates')

    def test_indexes(self):
        """
        Test indexes method.
        """
        dr = DataRequest(tickers=['btc', 'eth', 'cmbi10', 'cmbi10m'])
        df = self.cm.get_indexes(dr)
        sleep(0.6)

        # data type
        assert isinstance(df, pd.DataFrame), "Dataframe should be returned."
        # columns
        assert list(df.columns) == ["close"], "Columns should be 'close'."
        # index
        assert isinstance(df.index, pd.MultiIndex), "Index should be MultiIndex."
        assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index should be DatetimeIndex."
        assert pd.Timestamp.utcnow().date() - df.index.droplevel(1)[-1].date() < pd.Timedelta(days=3), \
            "End date should be less than 72h ago."
        # vals
        assert list(df.index.droplevel(0).unique()) == ['CMBI10', 'CMBI10M'], "Tickers are missing from dataframe."
        # shape
        assert df.shape[1] == 1, "Dataframe should have 1 column."

    def test_get_institutions(self):
        """
        Test get_institutions method.
        """
        dr = DataRequest(inst="grayscale", source_fields=["btc_shares_outstanding"])
        df = self.cm.get_institutions(dr)
        sleep(0.6)

        # data type
        assert isinstance(df, pd.DataFrame), "Dataframe should be returned."
        assert (df.dtypes == 'Int64').all(), "Dataframe should have Int64 dtype."
        # columns
        assert list(df.columns) == ["btc_shares_outstanding"], "Columns should be 'btc_shares_outstanding'."
        # index
        assert isinstance(df.index, pd.MultiIndex), "Index should be MultiIndex."
        assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index should be DatetimeIndex."
        assert pd.Timestamp.utcnow().date() - df.index.droplevel(1)[-1].date() < pd.Timedelta(days=7), \
            "End date should be less than 7 days ago."
        # vals
        assert list(df.index.droplevel(0).unique()) == ['grayscale'], "Institutions are missing from dataframe."
        # shape
        assert df.shape[1] == 1, "Dataframe should have 1 column."

    def test_get_ohlcv(self, data_req):
        """
        Test get_ohlcv method.
        """
        df = self.cm.get_ohlcv(data_req)
        sleep(0.6)

        # data type
        assert isinstance(df, pd.DataFrame), "Dataframe should be returned."
        # columns
        assert list(df.columns) == ['open', 'high', 'low', 'close', 'volume', 'vwap'], "Columns should be 'open', " \
                                                                                       "'high', 'low', 'close', " \
                                                                                       "'volume', 'vwap'."
        # index
        assert isinstance(df.index, pd.MultiIndex), "Index should be MultiIndex."
        assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index should be DatetimeIndex."
        assert pd.Timestamp.utcnow().date() - df.index.droplevel(1)[-1].date() < pd.Timedelta(days=3), \
            "End date should be less than 72h ago."
        # vals
        assert list(df.index.droplevel(0).unique()) == ['BTC', 'ETH', 'SOL'], "Tickers are missing from dataframe."
        # shape
        assert df.shape[1] == 6, "Dataframe should have 1 column."

    def test_get_onchain(self, data_req):
        """
        Test get_onchain method.
        """
        df = self.cm.get_onchain(data_req)
        sleep(0.6)

        # data type
        assert isinstance(df, pd.DataFrame), "Dataframe should be returned."
        # columns
        assert list(df.columns) == ['add_act', 'tx_count'], "Columns should be 'add_act', 'tx_count'."
        # index
        assert isinstance(df.index, pd.MultiIndex), "Index should be MultiIndex."
        assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index should be DatetimeIndex."
        assert pd.Timestamp.utcnow().date() - df.index.droplevel(1)[-1].date() < pd.Timedelta(days=3), \
            "End date should be less than 72h ago."
        # vals
        assert list(df.index.droplevel(0).unique()) == ['BTC', 'ETH'], "Tickers are missing from dataframe."
        # shape
        assert df.shape[1] == 2, "Dataframe should have 2 columns."

    def test_get_open_interest(self, data_req):
        """
        Test get_open_interest method.
        """
        dr = DataRequest(tickers=['btc', 'eth'], mkt_type="perpetual_future")
        df = self.cm.get_open_interest(dr)
        sleep(0.6)

        # data type
        assert isinstance(df, pd.DataFrame), "Dataframe should be returned."
        # columns
        assert list(df.columns) == ['oi'], "Columns should be 'oi'."
        # index
        assert isinstance(df.index, pd.MultiIndex), "Index should be MultiIndex."
        assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index should be DatetimeIndex."
        assert pd.Timestamp.utcnow().date() - df.index.droplevel(1)[-1].date() < pd.Timedelta(days=5), \
            "End date should be less than 5 days ago."
        # vals
        assert list(df.index.droplevel(0).unique()) == ['BTC', 'ETH'], "Tickers are missing from dataframe."
        # shape
        assert df.shape[1] == 1, "Dataframe should have 1 column."

    def test_get_funding_rates(self, data_req):
        """
        Test get_funding_rates method.
        """
        dr = DataRequest(tickers=['btc', 'eth'], mkt_type="perpetual_future")
        df = self.cm.get_funding_rates(dr)
        sleep(0.6)

        # data type
        assert isinstance(df, pd.DataFrame), "Dataframe should be returned."
        # columns
        assert list(df.columns) == ['funding_rate'], "Columns should be 'funding_rate'."
        # index
        assert isinstance(df.index, pd.MultiIndex), "Index should be MultiIndex."
        assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index should be DatetimeIndex."
        assert pd.Timestamp.utcnow().date() - df.index.droplevel(1)[-1].date() < pd.Timedelta(days=1), \
            "End date should be less than 1 day ago."
        # vals
        assert list(df.index.droplevel(0).unique()) == ['BTC', 'ETH'], "Tickers are missing from dataframe."
        # shape
        assert df.shape[1] == 1, "Dataframe should have 1 column."

    def test_get_trades(self):
        """
        Test get_trades method.
        """
        dr = DataRequest(tickers=['btc', 'eth'], freq='tick', start_date=datetime.utcnow() - pd.Timedelta(seconds=30))
        df = self.cm.get_trades(dr)
        sleep(0.6)

        # data type
        assert isinstance(df, pd.DataFrame), "Dataframe should be returned."
        # shape
        assert df.shape[1] == 3, "Dataframe should have 3 columns."
        # columns
        assert list(df.columns) == ['trade_size', 'trade_price', 'trade_side'], \
            "Columns should be 'trade_size', 'trade_price', 'trade_side'."
        # index
        assert isinstance(df.index, pd.MultiIndex), "Index should be MultiIndex."
        assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index should be DatetimeIndex."
        assert pd.Timestamp.utcnow().date() - df.index.droplevel(1)[-1].date() < pd.Timedelta(days=3), \
            "End date should be less than 3 days ago."
        # tickers
        assert list(df.index.droplevel(0).unique()) == ['BTC', 'ETH'], "Tickers are missing from dataframe."

    def test_get_quotes(self):
        """
        Test get_quotes method.
        """
        dr = DataRequest(tickers=['btc', 'eth'], freq='tick', start_date=datetime.utcnow() - pd.Timedelta(seconds=30))
        df = self.cm.get_quotes(dr)
        sleep(0.6)

        # data type
        assert isinstance(df, pd.DataFrame), "Dataframe should be returned."
        # shape
        assert df.shape[1] == 4, "Dataframe should have 4 columns."
        # columns
        assert list(df.columns) == ['ask', 'ask_size', 'bid', 'bid_size'], \
            "Columns should be 'ask', 'ask_size', 'bid', 'bid_size'."
        # index
        assert isinstance(df.index, pd.MultiIndex), "Index should be MultiIndex."
        assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index should be DatetimeIndex."
        assert pd.Timestamp.utcnow().date() - df.index.droplevel(1)[-1].date() < pd.Timedelta(days=3), \
            "End date should be less than 3 days ago."
        # tickers
        assert list(df.index.droplevel(0).unique()) == ['BTC', 'ETH'], "Tickers are missing from dataframe."

    def test_get_data(self, data_req):
        """
        Test get_data method.
        """
        df = self.cm.get_data(data_req)
        sleep(0.6)

        # data types
        assert isinstance(df, pd.DataFrame), "Dataframe should be returned."
        assert df.add_act.dtypes == 'Int64'
        assert df.tx_count.dtypes == 'Int64'
        assert df.close.dtypes == 'Float64'
        # shape
        assert df.shape[1] == 3, "Dataframe should have 3 columns."
        # columns
        assert list(df.columns) == ['add_act', 'tx_count', 'close'], "Columns should be 'add_act', 'tx_count', 'close'."
        # index
        assert isinstance(df.index, pd.MultiIndex), "Index should be MultiIndex."
        assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index should be DatetimeIndex."
        assert pd.Timestamp.utcnow().date() - df.index.droplevel(1)[-1].date() < pd.Timedelta(days=3), \
            "End date should be less than 3 days ago."
        # tickers
        assert list(df.index.droplevel(0).unique()) == ['BTC', 'ETH', 'SOL'], "Tickers are missing from dataframe."


if __name__ == "__main__":
    pytest.main()
