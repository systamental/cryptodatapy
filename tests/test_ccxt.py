import numpy as np
import pandas as pd
import pytest
from cryptodatapy.extract.datarequest import DataRequest
from cryptodatapy.extract.libraries.ccxt_api import CCXT


@pytest.fixture
def data_req():
    return DataRequest()


@pytest.fixture
def ccxt():
    return CCXT()


def test_get_exchanges_info(ccxt) -> None:
    """
    Test get exchanges info method.
    """
    df = ccxt.get_exchanges_info(exch='binance')
    assert df.loc['binance', 'rateLimit'] == '50'


def test_get_assets_info(ccxt) -> None:
    """
    Test get assets info method.
    """
    df = ccxt.get_assets_info(exch='binance')
    assert df.loc['BTC', 'id'] == 'BTC'


def test_get_markets_info(ccxt) -> None:
    """
    Test get assets info method.
    """
    df = ccxt.get_assets_info(exch='binance')
    assert df.loc['BTC', 'id'] == 'BTC'


def test_get_frequencies_info(ccxt) -> None:
    """
    Test get frequencies info method.
    """
    freq = ccxt.get_frequencies_info(exch='binance')
    assert freq == {'1s': '1s', '1m': '1m', '3m': '3m', '5m': '5m', '15m': '15m',
                    '30m': '30m', '1h': '1h', '2h': '2h', '4h': '4h', '6h': '6h',
                    '8h': '8h', '12h': '12h', '1d': '1d', '3d': '3d', '1w': '1w',
                    '1M': '1M'}, "Wrong data frequencies for binance."


def test_get_rate_limit_info(ccxt) -> None:
    """
    Test get rate limit info method.
    """
    rate_limit = ccxt.get_rate_limit_info(exch='binance')['binance']
    assert rate_limit == 50, "Rate limit should be 50 for binance"


def test_integration_get_all_ohlcv_hist(ccxt, data_req) -> None:
    """
    Test integration of req_data in a loop until the full data history has been retrieved.
    """
    df = ccxt.get_all_ohlcv_hist(data_req, ticker='BTC/USDT')
    assert not df.empty
    assert pd.to_datetime(df.datetime.min(), unit='ms') <= pd.Timestamp('2019-09-08 00:00:00'), \
        "Wrong index start date."  # start date


def test_integration_get_all_funding_hist(ccxt) -> None:
    """
    Test integration of req_data in a loop until the full data history has been retrieved.
    """

    data_req = DataRequest(mkt_type='perpetual_future')
    df = ccxt.get_all_funding_hist(data_req, ticker='BTC/USDT')
    assert not df.empty
    assert df.datetime.min() <= '2019-09-10T08:00:00.000Z', "Wrong funding rates start date."  # start date


@pytest.fixture
def ccxt_data_resp():
    return pd.read_csv('tests/data/ccxt_ohlcv_df.csv', index_col=0)


def test_wrangle_data_resp(ccxt, data_req, ccxt_data_resp) -> None:
    """
    Test wrangling of data response from get_all_data_hist into tidy data format.
    """
    df = ccxt.wrangle_data_resp(data_req, ccxt_data_resp)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert (df == 0).sum().sum() == 0, "Found zero values."  # 0s
    assert isinstance(df.index, pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert list(df.columns) == ["open", "high", "low", "close", "volume"], \
        "Fields are missing from dataframe."  # fields
    assert df.index[0] == pd.Timestamp('2017-08-17 00:00:00'), "Wrong start date."  # start date
    assert isinstance(
        df.close.dropna().iloc[-1], np.float64
    ), "Close is not a numpy float."  # dtypes


def test_check_params(ccxt) -> None:
    """
    Test parameter values before calling API.
    """
    data_req = DataRequest(fields='funding_rate')  # check mkt type
    with pytest.raises(ValueError):
        ccxt.check_params(data_req)
    data_req = DataRequest(tickers='notaticker')  # check tickers
    with pytest.raises(ValueError):
        ccxt.check_params(data_req)
    data_req = DataRequest(fields='notafield')  # check fields
    with pytest.raises(ValueError):
        ccxt.check_params(data_req)
    data_req = DataRequest(freq='tick')
    with pytest.raises(ValueError):
        ccxt.check_params(data_req)


def test_integration_get_ohlcv(ccxt, data_req) -> None:
    """
    Test integration of get_ohlcv method.
    """

    df = ccxt.get_ohlcv(data_req)
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
        "open",
        "high",
        "low",
        "close",
        "volume",
    ], "Fields are missing from dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp(
        "2017-08-17"
    ), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < pd.Timedelta(
        days=3
    ), "End date is more than 72h ago."  # end date
    assert isinstance(
        df.close.dropna().iloc[-1], np.float64
    ), "Close is not a numpy float."  # dtypes


def test_get_funding_rates(ccxt) -> None:
    """
    Test integration of get_funding_rates method.
    """

    data_req = DataRequest(mkt_type="perpetual_future")
    df = ccxt.get_funding_rates(data_req)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(
        df.index, pd.MultiIndex
    ), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(
        df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert df.index.droplevel(0).unique() == [
        "BTC"
    ], "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == [
        "funding_rate"
    ], "Fields are missing from dataframe."  # fields
    assert df.index[0][0] < pd.Timestamp(
        "2019-10-10"
    ), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < pd.Timedelta(
        days=1
    ), "End date is more than 24h ago."  # end date
    assert isinstance(
        df.funding_rate.dropna().iloc[-1], np.float64
    ), "Funding rate is not a numpy float."  # dtypes


def test_integration_get_data(ccxt) -> None:
    """
    Test get data methods integration.
    """
    data_req = DataRequest(
        tickers=["btc", "eth"],
        fields=["close", "funding_rate"],
        mkt_type="perpetual_future",
    )
    df = ccxt.get_data(data_req)
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
    ], "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == [
        "close",
        "funding_rate",
    ], "Fields are missing from dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp(
        "2019-09-08 00:00:00"
    ), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < pd.Timedelta(
        days=3
    ), "End date is more than 72h ago."  # end date
    assert isinstance(
        df.close.dropna().iloc[-1], np.float64
    ), "Close is not a numpy float."  # dtypes
    assert isinstance(
        df.funding_rate.dropna().iloc[-1], np.float64
    ), "Funding rate is not a numpy float."  # dtypes


if __name__ == "__main__":
    pytest.main()
