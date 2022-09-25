import numpy as np
import pandas as pd
import pytest
import responses
import json

from cryptodatapy.extract.data_vendors.glassnode_api import Glassnode
from cryptodatapy.extract.datarequest import DataRequest
from cryptodatapy.util.datacredentials import DataCredentials

# url endpoints
base_url = DataCredentials().glassnode_base_url
urls = {'assets_info': 'assets', 'fields_info': 'endpoints'}


@pytest.fixture
def data_req():
    return DataRequest()


@pytest.fixture
def gn():
    return Glassnode()


@pytest.fixture
def gn_req_assets():
    with open('tests/data/gn_assets_req.json') as f:
        return json.load(f)


@responses.activate
def test_req_assets(gn_req_assets, gn):
    """
    Test get request for assets info.
    """
    url = base_url + urls['assets_info'] + '?api_key=' + gn.api_key
    responses.add(responses.GET, url, json=gn_req_assets, status=200)

    req_assets = gn.req_assets()
    assert req_assets == gn_req_assets


@pytest.fixture
def gn_req_fields():
    with open('tests/data/gn_fields_req.json') as f:
        return json.load(f)


@responses.activate
def test_req_fields(gn_req_fields, gn):
    """
    Test get request for fields info.
    """
    url = base_url.replace('v1', 'v2') + urls['fields_info'] + '?api_key=' + gn.api_key
    responses.add(responses.GET, url, json=gn_req_fields, status=200)

    req_fields = gn.req_fields()
    assert req_fields == gn_req_fields


@pytest.fixture
def gn_req_data():
    with open('tests/data/gn_data_req.json') as f:
        return json.load(f)


@responses.activate
def test_req_data(gn_req_data, data_req, gn):
    """
    Test get request for data.
    """
    url = base_url + 'addresses/count' + '?api_key=' + gn.api_key + '&a=btc&s=1230940800&i=24h'
    responses.add(responses.GET, url, json=gn_req_data, status=200)

    req_data = gn.req_data(data_req, ticker='btc', field='addresses/count')
    assert req_data == gn_req_data


def test_wrangle_data_resp(data_req, gn, gn_req_data) -> None:
    """
    Test wrangling of data response into tidy data format.
    """
    df = gn.wrangle_data_resp(data_req, gn_req_data, field='addresses/count')
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert df.shape[1] == 1, "Dataframe should only have 1 column."  # shape
    assert (df == 0).sum().sum() == 0, "Dataframe has missing values."
    assert isinstance(df.index, pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert list(df.columns) == ["add_tot"], "Column name is incorrect."  # fields
    assert df.index[0] == pd.Timestamp('2009-01-03 00:00:00'), "Wrong start date."  # start date
    assert isinstance(df.add_tot.iloc[-1], np.int64), "Total addresses should be a numpy int."  # dtypes


def test_integration_get_all_fields(gn) -> None:
    """
    Test integration of req_data, wrangle_data_resp (get_tidy_data) to retrieve data for all fields
    by looping through fields and adding them to a dataframe.
    """
    data_req = DataRequest(fields=['open', 'high', 'low', 'close', 'add_act', 'tx_count', 'issuance'])
    df = gn.get_all_fields(data_req, ticker='btc')
    assert set(df.columns) == {'open', 'high', 'low', 'close', 'add_act', 'tx_count', 'issuance'}


def test_check_params(gn) -> None:
    """
    Test parameter values before calling API.
    """
    data_req = DataRequest(tickers=['ADA', 'BTC'])
    with pytest.raises(ValueError):
        gn.check_params(data_req)
    data_req = DataRequest(fields=['close', 'volume'])
    with pytest.raises(ValueError):
        gn.check_params(data_req)
    data_req = DataRequest(freq='tick')
    with pytest.raises(ValueError):
        gn.check_params(data_req)


def test_get_data_integration(gn) -> None:
    """
    Test integration of data retrieval methods.
    """
    data_req = DataRequest(tickers=["btc", "eth"], freq="d", fields=["close", "add_act", "tx_count"])
    df = gn.get_data(data_req)
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
        "add_act",
        "tx_count",
    ], "Fields are missing from dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp(
        "2009-01-12"
    ), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < pd.Timedelta(
        days=2
    ), "End date is more than 48h ago."  # end date
    assert isinstance(
        df.close.dropna().iloc[-1], np.float64
    ), "Close is not a numpy float."  # dtypes
    assert isinstance(
        df.add_act.dropna().iloc[-1], np.int64
    ), "Active addresses is not a numpy int."  # dtypes
    assert isinstance(
        df.tx_count.dropna().iloc[-1], np.int64
    ), "Transactions count is not a numpy int."  # dtypes


if __name__ == "__main__":
    pytest.main()
