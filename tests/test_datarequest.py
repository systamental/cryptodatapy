# import pandas as pd
# from datetime import datetime, timedelta
from cryptodatapy.data_requests.datarequest import DataRequest
import pytest


@pytest.fixture
def datarequest():
    return DataRequest()


def test_data_source_error(datarequest) -> None:
    """
    Test data source for data request.
    """
    dr = datarequest
    with pytest.raises(ValueError):
        dr.data_source = ['wrong source type']


def test_tickers_error(datarequest) -> None:
    """
    Test tickers for data request.
    """
    dr = datarequest
    with pytest.raises(TypeError):
        dr.tickers = None


def test_freq_error(datarequest) -> None:
    """
    Test frequency for data request.
    """
    dr = datarequest
    with pytest.raises(ValueError):
        dr.freq = '5s'


def test_quote_ccy_error(datarequest) -> None:
    """
    Test quote currency for data request.
    """
    dr = datarequest
    with pytest.raises(TypeError):
        dr.quote_ccy = ['usd']


def test_exch_error(datarequest) -> None:
    """
    Test exchange for data request.
    """
    dr = datarequest
    with pytest.raises(TypeError):
        dr.exch = {'crypto': 'binance'}


def test_mkt_type_error(datarequest) -> None:
    """
    Test market type for data request.
    """
    dr = datarequest
    with pytest.raises(ValueError):
        dr.mkt_type = 'swaption'


def test_start_date_error(datarequest) -> None:
    """
    Test start date for data request.
    """
    dr = datarequest
    with pytest.raises(ValueError):
        dr.start_date = '01/01/2015'


def test_end_date_error(datarequest) -> None:
    """
    Test end date for data request.
    """
    dr = datarequest
    with pytest.raises(ValueError):
        dr.end_date = '12-31-2015'


def test_fields_error(datarequest) -> None:
    """
    Test fields for data request.
    """
    dr = datarequest
    with pytest.raises(TypeError):
        dr.fields = None


def test_inst_error(datarequest) -> None:
    """
    Test institution for data request.
    """
    dr = datarequest
    with pytest.raises(TypeError):
        dr.inst = ['grayscale']


def test_tz_error(datarequest) -> None:
    """
    Test timezone for data request.
    """
    dr = datarequest
    with pytest.raises(ValueError):
        dr.tz = 'NYC'


def test_cat_error(datarequest) -> None:
    """
    Test category for data request.
    """
    dr = datarequest
    with pytest.raises(ValueError):
        dr.cat = 'on-chain'


def test_trials_error(datarequest) -> None:
    """
    Test trials for data request.
    """
    dr = datarequest
    with pytest.raises(TypeError):
        dr.trials = ['3']


def test_pause_error(datarequest) -> None:
    """
    Test pause for data request.
    """
    dr = datarequest
    with pytest.raises(TypeError):
        dr.pause = ['15s']


def test_source_tickers_error(datarequest) -> None:
    """
    Test source tickers for data request.
    """
    dr = datarequest
    with pytest.raises(TypeError):
        dr.source_tickers = {'crypto': ['BTC', 'ETH']}


def test_source_freq_error(datarequest) -> None:
    """
    Test source frequency for data request.
    """
    dr = datarequest
    with pytest.raises(TypeError):
        dr.source_freq = {'crypto': '15s'}


def test_source_fields_error(datarequest) -> None:
    """
    Test source fields for data request.
    """
    dr = datarequest
    with pytest.raises(TypeError):
        dr.source_fields = {'crypto': ['close_price']}


if __name__ == "__main__":
    pytest.main()

