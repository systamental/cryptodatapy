from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from cryptodatapy.extract.data_vendors.coinmetrics_api import CoinMetrics
from cryptodatapy.extract.datarequest import DataRequest
from cryptodatapy.transform.wrangle import WrangleData


@pytest.fixture
def data_req():
    return DataRequest()


@pytest.fixture
def cm():
    return CoinMetrics()


def test_integration_get_fields_info(cm):
    """
    Test integration of get_fields_info and get_inst_info for all fields info.
    """
    fields_list = cm.get_fields_info(as_list=True)
    assert "AdrActCnt" in fields_list, "Fields list is missing 'AdrActCnt'."
    assert "price_close" in fields_list, "Fields list is missing 'price_close'."
    assert 'btc_net_asset_value' in fields_list, "Fields list is missing 'btc_net_asset_value'."


def test_integration_get_onchain_tickers_list(cm) -> None:
    """
    Test integration of ConvertParams and get_fields_info to get assets with available fields method
    """
    onchain_tickers = ['busd', 'steth_lido', 'dcr', 'xem', 'bch', 'uma', 'swrv', 'gno', 'doge', 'renbtc', 'algo']
    data_req = DataRequest(fields=["add_act", "tx_count"])
    assets_list = cm.get_onchain_tickers_list(data_req)
    assert len(assets_list) != 0, "Assets list was returned empty."  # non empty
    assert isinstance(assets_list, list), "Should be a list."  # list
    assert all([ticker in assets_list for ticker in onchain_tickers]), \
        "Tickers are missing from assets list."  # tickers


def test_req_data(cm) -> None:
    """
    Test data request to CoinMetrics client.
    """
    df = cm.req_data(data_type='get_market_candles', markets=['binance-btc-usdt-spot', 'binance-eth-usdt-spot'])
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert list(df.market.unique()) == ['binance-btc-usdt-spot', 'binance-eth-usdt-spot'], \
        "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == ['market', 'time', 'price_open', 'price_close', 'price_high', 'price_low', 'vwap',
                                'volume', 'candle_usd_volume', 'candle_trades_count'], \
        "Fields are missing from dataframe."  # fields
    assert pd.Timestamp.utcnow() - df.time.iloc[-1] < pd.Timedelta(days=3), "End date is more than 72h ago."  # end date


@pytest.fixture
def cm_req_data_mkt_candles():
    return pd.read_csv('data/cm_ohlcv_df.csv')


def test_wrangle_data_resp(data_req, cm_req_data_mkt_candles):
    """
    Test wrangling of data response.
    """
    # wrangle data resp
    df = WrangleData(data_req, cm_req_data_mkt_candles).coinmetrics()
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert list(df.index.droplevel(0).unique()) == ['BTC', 'ETH'], "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == ["open", "high", "low", "close", "volume", "vwap"], \
        "Fields are missing from dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp("2017-08-17 00:00:00"), "Wrong start date."  # start date
    assert isinstance(df.close.dropna().iloc[-1], np.float64), "Close is not a numpy float."  # dtype


def test_integration_tidy_data(data_req, cm) -> None:
    """
    Test integration of req_data and wrangle_data_resp into tidy data format.
    """
    df = cm.req_data(data_type='get_market_candles', markets=['binance-btc-usdt-spot', 'binance-eth-usdt-spot'])
    df = WrangleData(data_req, df).coinmetrics()
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert list(df.index.droplevel(0).unique()) == ['BTC', 'ETH'], "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == ["open", "high", "low", "close", "volume", "vwap"], \
        "Fields are missing from dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp("2017-08-17 00:00:00"), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < pd.Timedelta(days=3),\
        "End date is more than 72h ago."  # end date
    assert isinstance(df.close.dropna().iloc[-1], np.float64), "Close is not a numpy float."  # dtype


def test_filter_tickers(cm) -> None:
    """
    Test ticker filtering to avoid calling API when requested tickers are not available.
    """
    data_req = DataRequest(tickers=['btc', 'eth', 'cmbi10', 'cmbi10m'])
    tickers = cm.filter_tickers(data_req, data_type='indexes')
    assert tickers == ['cmbi10', 'cmbi10m'], "Indexes tickers were not filtered."
    data_req = DataRequest(tickers=['btc', 'eth', 'cmbi10', 'cmbi10m'])
    tickers = cm.filter_tickers(data_req, data_type='market_candles')
    assert tickers == ['binance-btc-usdt-spot', 'binance-eth-usdt-spot'], "Market tickers were not filtered."
    data_req = DataRequest(tickers=['btc', 'eth', 'cmbi10', 'cmbi10m'])
    tickers = cm.filter_tickers(data_req, data_type='asset_metrics')
    assert tickers == ['btc', 'eth'], "Asset tickers were not filtered."


def test_filter_fields(cm) -> None:
    """
    Test fields filtering to avoid calling API when requested fields are not available.
    """
    data_req = DataRequest(fields=['close', 'add_act', 'tx_count', 'issuance'])
    fields = cm.filter_fields(data_req, data_type='asset_metrics')
    assert fields == ['AdrActCnt', 'TxCnt', 'IssTotNtv'], "Fields were not properly filtered."
    data_req = DataRequest(source_fields=['lpt_shares_outstanding', 'zec_net_asset_value', 'tx_count', 'issuance'])
    fields = cm.filter_fields(data_req, data_type='institutions')
    assert fields == ['lpt_shares_outstanding', 'zec_net_asset_value'], "Fields were not properly filtered."


def test_check_freq_params(cm) -> None:
    """
    Test frequency check when requested frequency is not available.
    """
    data_req = DataRequest(freq='5min')
    with pytest.raises(ValueError):
        cm.check_params(data_req, data_type='indexes')


def test_check_mkt_type_params(cm) -> None:
    """
    Test market type check when requested market type is not available.
    """
    data_req = DataRequest(mkt_type='spot')
    with pytest.raises(ValueError):
        cm.check_params(data_req, data_type='funding_rates')


def test_integration_get_institutions(cm) -> None:
    """
    Test integration of get institutions method.
    """
    data_req = DataRequest(inst="grayscale", source_fields=["btc_shares_outstanding"])
    df = cm.get_institutions(data_req)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(
        df.index, pd.MultiIndex
    ), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(
        df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert list(df.index.droplevel(0).unique()) == [
        "grayscale"
    ], "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == [
        "btc_shares_outstanding"
    ], "Fields are missing from dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp(
        "2019-01-02"
    ), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < pd.Timedelta(
        days=7
    ), "End date is more than 7 days ago."  # end date
    assert isinstance(
        df.btc_shares_outstanding.dropna().iloc[-1], np.int64
    ), "Shares outstanding is not a numpy int."  # dtypes


def test_integration_get_open_interest(cm) -> None:
    """
    Test integration of get open interest method.
    """
    data_req = DataRequest(mkt_type="perpetual_future")
    df = cm.get_open_interest(data_req)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(
        df.index, pd.MultiIndex
    ), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(
        df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert list(df.index.droplevel(0).unique()) == [
        "BTC"
    ], "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == ["oi"], "Fields are missing from dataframe."  # fields
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[0][0] < pd.Timedelta(
        days=5
    ), "Start date is more than 5 days ago."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < pd.Timedelta(
        days=1
    ), "End date is more than 24h ago."  # end date
    assert isinstance(
        df.oi.dropna().iloc[-1], np.float64
    ), "Open interest is not a numpy float."  # dtype


def test_integration_get_funding_rates(cm) -> None:
    """
    Test integration of get funding rates method.
    """
    data_req = DataRequest(mkt_type="perpetual_future")
    df = cm.get_funding_rates(data_req)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(
        df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert list(df.index.droplevel(0).unique()) == [
        "BTC"
    ], "Tickers are missing from dataframe."  # tickers
    assert isinstance(
        df.index, pd.MultiIndex
    ), "Dataframe should be MultiIndex."  # multiindex
    assert list(df.columns) == [
        "funding_rate"
    ], "Fields are missing from dataframe."  # fields
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < pd.Timedelta(
        days=1
    ), "End date is more than 24h ago."  # end date
    assert isinstance(df.funding_rate.dropna().iloc[-1], np.float64)  # dtypes


def test_integration_get_trades(cm) -> None:
    """
    Test integration of get trades method.
    """
    data_req = DataRequest(
        freq="tick", start_date=datetime.utcnow() - pd.Timedelta(seconds=30)
    )
    df = cm.get_trades(data_req)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(
        df.index, pd.MultiIndex
    ), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(
        df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert list(df.index.droplevel(0).unique()) == [
        "BTC"
    ], "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == [
        "trade_size",
        "trade_price",
        "trade_side",
    ], "Fields are missing from dataframe."
    assert pd.Timestamp.utcnow() - df.index[-1][0] < pd.Timedelta(
        days=1
    ), "End date is more than 24h ago."  # end date
    assert isinstance(df.trade_price.dropna().iloc[-1], np.float64)  # dtypes


def test_integration_get_quotes(cm) -> None:
    """
    Test integration of get quotes method.
    """
    data_req = DataRequest(
        freq="tick", start_date=datetime.utcnow() - pd.Timedelta(seconds=30)
    )
    df = cm.get_quotes(data_req)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(
        df.index, pd.MultiIndex
    ), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(
        df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert list(df.index.droplevel(0).unique()) == [
        "BTC"
    ], "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == [
        "ask",
        "ask_size",
        "bid",
        "bid_size",
    ], "Fields are missing from dataframe."  # fields
    assert pd.Timestamp.utcnow() - df.index[-1][0] < pd.Timedelta(
        days=1
    ), "End date is more than 24h ago."  # end date
    assert isinstance(df.ask.dropna().iloc[-1], np.float64)  # dtypes


def test_integration_get_data(cm) -> None:
    """
    Test integration of get data method.
    """
    data_req = DataRequest(tickers=["btc", "eth", "ada"], fields=["close", "add_act", "issuance"])
    df = cm.get_data(data_req)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(
        df.index, pd.MultiIndex
    ), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(
        df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert list(df.index.droplevel(0).unique()) == [
        "BTC",
        "ETH",
        "ADA"
    ], "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == [
        "close",
        "add_act",
        "issuance"
    ], "Fields are missing from dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp(
        "2009-01-09"
    ), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < pd.Timedelta(
        days=3
    ), "End date is more than 72h ago."  # end date
    assert isinstance(
        df.close.dropna().iloc[-1], np.float64
    ), "Close is not a numpy float."  # dtypes
    assert isinstance(
        df.add_act.dropna().iloc[-1], np.int64
    ), "Active addresses is not a numpy int."  # dtypes


if __name__ == "__main__":
    pytest.main()
