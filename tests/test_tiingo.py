import numpy as np
import pandas as pd
import pytest
import responses
import json

from cryptodatapy.extract.data_vendors.tiingo_api import Tiingo
from cryptodatapy.extract.datarequest import DataRequest


@pytest.fixture
def tg():
    return Tiingo()


@pytest.fixture
def tg_req_crypto_info():
    with open('tests/data/tg_crypto_req.json') as f:
        return json.load(f)


@responses.activate
def test_req_crypto_info(tg_req_crypto_info, tg):
    """
    Test get request for crypto info.
    """
    url = tg.base_url + 'crypto'
    responses.add(responses.GET, url, json=tg_req_crypto_info, status=200)

    crypto_req = tg.req_crypto()
    assert crypto_req == tg_req_crypto_info


@pytest.fixture
def tg_req_iex():
    with open('tests/data/tg_iex_data_req.json') as f:
        return json.load(f)


@responses.activate
def test_req_iex(tg_req_iex, tg):
    """
    Test get request for iex data.
    """
    url = 'https://api.tiingo.com/iex/spy/prices?startDate=2022-01-01+00%3A00%3A00' + \
          '&endDate=2022-09-15&resampleFreq=1hour'
    responses.add(responses.GET, url, json=tg_req_iex, status=200)

    data_req = DataRequest(freq='8h', start_date='2022-01-01', end_date='2022-09-15')
    iex_req = tg.req_data(data_req, data_type='iex', ticker='spy')
    assert iex_req == tg_req_iex


@pytest.fixture
def tg_req_eqty():
    with open('tests/data/tg_eqty_data_req.json') as f:
        return json.load(f)


@responses.activate
def test_req_eqty(tg_req_eqty, tg):
    """
    Test get request for eqty data.
    """
    url = 'https://api.tiingo.com/tiingo/daily/prices?tickers=spy&startDate=2000-01-01+00%3A00%3A00' + \
          '&endDate=2021-12-31'
    responses.add(responses.GET, url, json=tg_req_eqty, status=200)

    data_req = DataRequest(start_date='2000-01-01', end_date='2021-12-31')
    eqty_req = tg.req_data(data_req, data_type='eqty', ticker='spy')
    assert eqty_req == tg_req_eqty


@pytest.fixture
def tg_req_crypto():
    with open('tests/data/tg_crypto_data_req.json') as f:
        return json.load(f)


@responses.activate
def test_req_crypto(tg_req_crypto, tg):
    """
    Test get request for crypto data.
    """
    url = 'https://api.tiingo.com/tiingo/crypto/prices?tickers=btcusd&startDate=2015-01-01+00%3A00%3A00&' + \
          'endDate=2021-12-31&resampleFreq=1day'
    responses.add(responses.GET, url, json=tg_req_crypto, status=200)

    data_req = DataRequest(start_date='2015-01-01', end_date='2021-12-31')
    crypto_req = tg.req_data(data_req, data_type='crypto', ticker='btcusd')
    assert crypto_req == tg_req_crypto


@pytest.fixture
def tg_req_fx():
    with open('tests/data/tg_fx_data_req.json') as f:
        return json.load(f)


@responses.activate
def test_req_fx(tg_req_fx, tg):
    """
    Test get request for fx data.
    """
    url = 'https://api.tiingo.com/tiingo/fx/prices?tickers=eurusd&startDate=2015-01-01+00%3A00%3A00' + \
          '&endDate=2021-12-31&resampleFreq=1day'
    responses.add(responses.GET, url, json=tg_req_fx, status=200)

    data_req = DataRequest(start_date='2015-01-01', end_date='2021-12-31')
    fx_req = tg.req_data(data_req, data_type='fx', ticker='eurusd')
    assert fx_req == tg_req_fx


def test_wrangle_eqty_data_resp(tg, tg_req_eqty) -> None:
    """
    Test wrangling of data response into tidy data format.
    """
    data_req = DataRequest(start_date='2000-01-01', end_date='2021-12-31')
    df = tg.wrangle_data_resp(data_req, tg_req_eqty, data_type='eqty')
    assert not df.empty, "Dataframe was returned empty."  # non-empty
    assert isinstance(df.index, pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert (df.loc[:, df.columns != 'dividend'] == 0).sum().sum() == 0, "Dataframe has missing values."
    assert list(df.columns) == ['open', 'high', 'low', 'close', 'volume', 'open_adj', 'high_adj',
                                'low_adj', 'close_adj', 'volume_adj', 'dividend', 'split'], "Missing columns."  # fields
    assert isinstance(df.close.iloc[-1], np.float64), "Close price should be a numpy float."  # dtypes
    assert isinstance(df.volume.iloc[-1], np.int64), "Close price should be a numpy int."  # dtypes


def test_wrangle_iex_data_resp(tg, tg_req_iex) -> None:
    """
    Test wrangling of data response into tidy data format.
    """
    data_req = DataRequest(freq='8h', start_date='2022-01-01', end_date='2022-09-15')
    df = tg.wrangle_data_resp(data_req, tg_req_iex, data_type='iex')
    assert not df.empty, "Dataframe was returned empty."  # non-empty
    assert (df == 0).sum().sum() == 0, "Dataframe has missing values."
    assert isinstance(df.index, pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert list(df.columns) == ['close', 'high', 'low', 'open'], "Missing columns."  # fields
    assert isinstance(df.close.iloc[-1], np.float64), "Close price should be a numpy float."  # dtypes


def test_wrangle_crypto_data_resp(tg, tg_req_crypto) -> None:
    """
    Test wrangling of data response into tidy data format.
    """

    data_req = DataRequest(start_date='2015-01-01', end_date='2021-12-31')
    df = tg.wrangle_data_resp(data_req, tg_req_crypto, data_type='crypto')
    assert not df.empty, "Dataframe was returned empty."  # non-empty
    assert (df.loc[:, df.columns != 'trades'] == 0).sum().sum() == 0, "Dataframe has missing values."
    assert isinstance(df.index, pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert list(df.columns) == ['open', 'high', 'low', 'close', 'volume', 'volume_quote_ccy', 'trades'], \
        "Column names are incorrect."  # fields
    assert isinstance(df.close.iloc[-1], np.float64), "Close prices should be numpy float."  # dtypes
    assert isinstance(df.trades.iloc[-1], np.int64), "Trades should be numpy int."  # dtypes


def test_wrangle_fx_data_resp(tg, tg_req_fx) -> None:
    """
    Test wrangling of data response into tidy data format.
    """

    data_req = DataRequest(start_date='2015-01-01', end_date='2021-12-31')
    df = tg.wrangle_data_resp(data_req, tg_req_fx, data_type='fx')
    assert not df.empty, "Dataframe was returned empty."  # non-empty
    assert (df == 0).sum().sum() == 0, "Dataframe has missing values."
    assert isinstance(df.index, pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert list(df.columns) == ['open', 'high', 'low', 'close'], "Column names are incorrect."  # fields
    assert isinstance(df.close.iloc[-1], np.float64), "Close price should be a numpy float."  # dtypes


def test_integration_get_all_tickers(tg) -> None:
    """
    Test integration of req_data, wrangle_data_resp and get_tidy_data to retrieve data for all tickers
    by looping through tickers list and adding them to a multinindex dataframe.
    """
    data_req = DataRequest(tickers=['btc', 'eth', 'ada'], cat='crypto')
    df = tg.get_all_tickers(data_req, data_type='crypto')
    assert set(df.index.droplevel(0).unique()) == {'BTC', 'ETH', 'ADA'}


def test_check_params(tg) -> None:
    """
    Test parameter values before calling API.
    """
    data_req = DataRequest(tickers='spy')
    with pytest.raises(ValueError):
        tg.check_params(data_req)
    data_req = DataRequest(tickers='notaticker', cat='eqty')
    with pytest.raises(ValueError):
        tg.check_params(data_req)
    data_req = DataRequest(tickers=["btc"], fields=["notafield"], cat='crypto')
    with pytest.raises(ValueError):
        tg.check_params(data_req)


def test_integration_get_eqty(tg) -> None:
    """
    Test get equity daily data integration method with req_data, wrangle_data_resp and get_all_tickers.
    """
    data_req = DataRequest(
        tickers=["spy", "tlt", "gsp"], cat="eqty", start_date="2020-01-01"
    )
    df = tg.get_eqty(data_req)
    assert not df.empty, "Indexes dataframe was returned empty."  # non-empty
    assert isinstance(
        df.index, pd.MultiIndex
    ), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(
        df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert df.index.droplevel(0).unique().to_list() == [
        "SPY",
        "TLT",
        "GSP",
    ], "Tickers are missing from dataframe"  # tickers
    assert set(df.columns) == {
        "close",
        "high",
        "low",
        "open",
        "volume",
        "close_adj",
        "high_adj",
        "low_adj",
        "open_adj",
        "volume_adj",
        "dividend",
        "split",
    }, "Fields are missing from indexes dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp(
        "2020-01-02 00:00:00"
    ), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < pd.Timedelta(
        days=5
    ), "End date is more than 5 days ago."  # end date
    assert isinstance(
        df.close.dropna().iloc[-1], np.float64
    ), "Close is not a numpy float."  # dtypes


def test_integration_get_eqty_iex(tg) -> None:
    """
    Test get equity iex data integration method with req_data, wrangle_data_resp and get_all_tickers.
    """
    data_req = DataRequest(
        tickers=["meta", "aapl", "amzn", "nflx", "goog"], cat="eqty", freq="5min"
    )
    df = tg.get_eqty_iex(data_req)
    assert not df.empty, "Indexes dataframe was returned empty."  # non-empty
    assert isinstance(
        df.index, pd.MultiIndex
    ), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(
        df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert df.index.droplevel(0).unique().to_list() == [
        "META",
        "AAPL",
        "AMZN",
        "NFLX",
        "GOOG",
    ], "Tickers are missing from dataframe"  # tickers
    assert set(df.columns) == {
        "close",
        "high",
        "low",
        "open",
    }, "Fields are missing from indexes dataframe."  # fields
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < pd.Timedelta(
        days=4
    ), "End date is more than 4 days ago."  # end date
    assert isinstance(
        df.close.dropna().iloc[-1], np.float64
    ), "Close is not a numpy float."  # dtypes


def test_integration_get_crypto(tg) -> None:
    """
    Test get crypto data integration method with req_data, wrangle_data_resp and get_all_tickers.
    """
    data_req = DataRequest(tickers=["btc", "eth", "sol"], cat="crypto")
    df = tg.get_crypto(data_req)
    assert not df.empty, "Indexes dataframe was returned empty."  # non-empty
    assert isinstance(
        df.index, pd.MultiIndex
    ), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(
        df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert df.index.droplevel(0).unique().to_list() == [
        "BTC",
        "ETH",
        "SOL",
    ], "Tickers are missing from dataframe"  # tickers
    assert set(df.columns) == {
        "open",
        "high",
        "low",
        "close",
        "volume",
        "volume_quote_ccy",
        "trades",
    }, "Fields are missing from indexes dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp(
        "2011-08-19 00:00:00"
    ), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < pd.Timedelta(
        days=4
    ), "End date is more than 4 days ago."  # end date
    assert isinstance(
        df.close.dropna().iloc[-1], np.float64
    ), "Close is not a numpy float."  # dtypes


def test_get_fx(tg) -> None:
    """
    Test get fx data integration method with req_data, wrangle_data_resp and get_all_tickers.
    """
    data_req = DataRequest(
        tickers=["eur", "gbp", "jpy", "cad", "try", "brl"],
        cat="fx",
        start_date="1990-01-01",
    )
    df = tg.get_fx(data_req)
    assert not df.empty, "Indexes dataframe was returned empty."  # non-empty
    assert isinstance(
        df.index, pd.MultiIndex
    ), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(
        df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert df.index.droplevel(0).unique().to_list() == [
        "EURUSD",
        "GBPUSD",
        "USDJPY",
        "USDCAD",
        "USDTRY",
        "USDBRL",
    ], "Tickers are missing from dataframe"  # tickers
    assert set(df.columns) == {
        "open",
        "high",
        "low",
        "close",
    }, "Fields are missing from indexes dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp(
        "1990-01-02 00:00:00"
    ), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.unstack().index[
        -1
    ] < pd.Timedelta(
        days=4
    ), "End date is more than 4 days ago."  # end date
    assert isinstance(
        df.close.dropna().iloc[-1], np.float64
    ), "Close is not a numpy float."  # dtypes


def test_get_data(tg) -> None:
    """
    Test get data integration method with get_eqty, get_eqty_iex, get_crypto and get_fx.
    """
    data_req = DataRequest(tickers=["meta", "aapl", "amzn", "nflx", "goog"], cat="eqty")
    df = tg.get_data(data_req)
    assert not df.empty, "Indexes dataframe was returned empty."  # non-empty
    assert isinstance(
        df.index, pd.MultiIndex
    ), "Dataframe should be multiIndex."  # multiindex
    assert isinstance(
        df.index.droplevel(1), pd.DatetimeIndex
    ), "Index is not DatetimeIndex."  # datetimeindex
    assert df.index.droplevel(0).unique().to_list() == [
        "AAPL",
        "AMZN",
        "NFLX",
        "META",
        "GOOG",
    ], "Tickers are missing from dataframe"  # tickers
    assert set(df.columns) == {
        "close"
    }, "Fields are missing from indexes dataframe."  # fields
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < pd.Timedelta(
        days=5
    ), "End date is more than 5 days ago."  # end date
    assert isinstance(
        df.close.dropna().iloc[-1], np.float64
    ), "Close is not a numpy float."  # dtypes


if __name__ == "__main__":
    pytest.main()
