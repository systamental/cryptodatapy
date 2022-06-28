# import libraries
import pandas as pd
from datetime import datetime, timedelta
from cryptodatapy.data_requests.datarequest import DataRequest
from cryptodatapy.data_vendors.cryptocompare import CryptoCompare
from cryptodatapy.util.convertparams import *
import pytest

@pytest.fixture
def cryptocompare():
    return CryptoCompare()

@pytest.fixture
def datarequest():
    return DataRequest()

# source type
def test_source_type(cryptocompare) -> None:
    """
    Test source type for CryptoCompare.
    """
    cc = cryptocompare
    assert cc.source_type == 'data_vendor', "Source type should be 'data_vendor'."

def test_source_type_error(cryptocompare) -> None:
    """
    Test source type errors for CryptoCompare.
    """
    cc = cryptocompare
    with pytest.raises(ValueError):
        cc.source_type = 'anecdotal'

# categories
def test_categories(cryptocompare) -> None:
    """
    Test categories for CryptoCompare.
    """
    cc = cryptocompare
    assert cc.categories == ['crypto'], "Category should be 'crypto'."

def test_categories_error(cryptocompare) -> None:
    """
    Test categories errors for CryptoCompare.
    """
    cc = cryptocompare
    with pytest.raises(ValueError):
        cc.categories = ['real_estate', 'art']

# assets
def test_assets(cryptocompare) -> None:
    """
    Test assets list for CryptoCompare.
    """
    cc = cryptocompare
    assert 'BTC' in cc.assets, "Assets list is missing 'BTC'."

def test_assets_error(cryptocompare) -> None:
    """
    Test assets errors for CryptoCompare.
    """
    cc = cryptocompare
    with pytest.raises(AttributeError):
        cc.assets = ['BTC']

# get assets
def test_get_assets_info(cryptocompare) -> None:
    """
    Test get assets info for CryptoCompare.
    """
    cc = cryptocompare
    assert cc.get_assets_info().loc['BTC', 'CoinName'] == 'Bitcoin', "Asset info is missing 'Bitcoin'."

# get top market cap assets
def test_get_top_market_cap_assets(cryptocompare) -> None:
    """
    Test get top market cap assets for CryptoCompare.
    """
    cc = cryptocompare
    assert 'BTC' in cc.get_top_market_cap_assets(), "'BTC' is not in top market cap list."

def test_top_market_cap_error(cryptocompare) -> None:
    """
    Test get top market cap assets errors for CryptoCompare.
    """
    cc = cryptocompare
    with pytest.raises(ValueError):
        cc.get_top_market_cap_assets(n=101)

# indexes
def test_indexes(cryptocompare) -> None:
    """
    Test indexes for CryptoCompare.
    """
    cc = cryptocompare
    assert 'MVDA' in cc.indexes, "Index list is missing 'MVDA'."

def test_indexes_error(cryptocompare) -> None:
    """
    Test indexes errors for CryptoCompare.
    """
    cc = cryptocompare
    with pytest.raises(AttributeError):
        cc.indexes = ['BGCI']

# indexes info
def test_get_indexes_info(cryptocompare) -> None:
    """
    Test get indexes info for CryptoCompare.
    """
    cc = cryptocompare
    assert cc.get_indexes_info().loc['MVDA', 'name'] == 'MVIS CryptoCompare Digital Assets 100', \
        "Index info is missing 'MVDA'."

# markets info
def test_markets(cryptocompare) -> None:
    """
    Test markets info for CryptoCompare.
    """
    cc = cryptocompare
    assert 'BTCUSDT' in cc.markets, "Assets list is missing 'BTC'."

# markets
def test_markets_error(cryptocompare) -> None:
    """
    Test markets errors for CryptoCompare.
    """
    cc = cryptocompare
    with pytest.raises(AttributeError):
        cc.markets = ['BTCUSDC', 'ETHUSD']

# market types
def test_market_types(cryptocompare) -> None:
    """
    Test market types for CryptoCompare.
    """
    cc = cryptocompare
    assert cc.market_types == ['spot'], "Market types should be 'spot'."

# market types
def test_market_types_error(cryptocompare) -> None:
    """
    Test market types errors for CryptoCompare.
    """
    cc = cryptocompare
    with pytest.raises(ValueError):
        cc.market_types = ['swaps']

# fields
def test_fields_close(cryptocompare) -> None:
    """
    Test close field for CryptoCompare.
    """
    cc = cryptocompare
    assert 'close' in cc.fields, "Fields list is missing 'close'."

def test_fields_active_addresses(cryptocompare) -> None:
    """
    Test active addresses field for CryptoCompare.
    """
    cc = cryptocompare
    assert 'active_addresses' in cc.fields, "Fields list is missing 'active_addresses'."

def test_fields_followers(cryptocompare) -> None:
    """
    Test followers field for CryptoCompare.
    """
    cc = cryptocompare
    assert 'followers' in cc.fields, "Fields list is missing 'followers'."

def test_fields_error(cryptocompare) -> None:
    """
    Test fields error for CryptoCompare.
    """
    cc = cryptocompare
    with pytest.raises(AttributeError):
        cc.fields = ['vwap']

# frequencies
def test_frequencies(cryptocompare) -> None:
    """
    Test frequencies for CryptoCompare.
    """
    cc = cryptocompare
    assert 'd' in cc.frequencies, "Frequencies list is missing 'd'."

def test_frequencies_error(cryptocompare) -> None:
    """
    Test frequencies error for CryptoCompare.
    """
    cc = cryptocompare
    with pytest.raises(ValueError):
        cc.frequencies = '1hour'

# exchanges
def test_exchanges(cryptocompare) -> None:
    """
    Test exchanges for CryptoCompare.
    """
    cc = cryptocompare
    assert 'Binance' in cc.exchanges, "Exchanges list is missing 'Binance'."

# exchanges
def test_exchanges_error(cryptocompare) -> None:
    """
    Test exchanges error for CryptoCompare.
    """
    cc = cryptocompare
    with pytest.raises(AttributeError):
        cc.exchanges = ['Binance']

# exchanges info
def test_get_exchanges_info(cryptocompare) -> None:
    """
    Test exchanges info for CryptoCompare.
    """
    cc = cryptocompare
    assert cc.get_exchanges_info().loc['Binance', 'InternalName'] == 'Binance', \
        "Exchanges info is missing 'Binance'."

# news
def test_get_news(cryptocompare) -> None:
    """
    Test get news for CryptoCompare.
    """
    cc = cryptocompare
    print(cc.get_news())
    assert 'title' in cc.get_news().columns, "News is missing 'title'."

# news sources
def test_get_news_sources(cryptocompare) -> None:
    """
    Test get news sources for CryptoCompare.
    """
    cc = cryptocompare
    assert cc.get_news_sources().loc['coindesk', 'name'] == 'CoinDesk', "News sources is missing 'CoinDesk'."

# base url
def test_base_url(cryptocompare) -> None:
    """
    Test base url for CryptoCompare.
    """
    cc = cryptocompare
    assert cc.base_url == 'https://min-api.cryptocompare.com/data/', "Base url is incorrect."

# base url
def test_base_url_error(cryptocompare) -> None:
    """
    Test base url errors for CryptoCompare.
    """
    cc = cryptocompare
    with pytest.raises(TypeError):
        cc.base_url = 2225

# api key
def test_api_key_error(cryptocompare) -> None:
    """
    Test api key errors for CryptoCompare.
    """
    cc = cryptocompare
    with pytest.raises(TypeError):
        cc.api_key = float(0.5)

# max obs per call
def test_max_obs_per_call(cryptocompare) -> None:
    """
    Test max obs per call for CryptoCompare.
    """
    cc = cryptocompare
    assert cc.max_obs_per_call == int(2000), "Max observations per call should be int(2000)."

def test_max_obs_per_call_error(cryptocompare) -> None:
    """
    Test max obs per call errors for CryptoCompare.
    """
    cc = cryptocompare
    with pytest.raises(TypeError):
        cc.max_obs_per_call = '2000'

# rate limit info
def test_get_rate_limit_info(cryptocompare) -> None:
    """
    Test get rate limit info for CryptoCompare.
    """
    cc = cryptocompare
    assert cc.get_rate_limit_info().loc['month', 'calls_left'] > 0, "Monthly rate limit has been reached."

def test_rate_limit_error(cryptocompare) -> None:
    """
    Test rate limit errors for CryptoCompare.
    """
    cc = cryptocompare
    with pytest.raises(AttributeError):
        cc.rate_limit = '2000'

# convert params
@pytest.mark.parametrize(
    "dr_tickers, cc_tickers",
    [
        ('btc', ['BTC']),
        ('eth', ['ETH']),
        (None, ['BTC']),
        (['btc', 'eth', 'sol'], ['BTC', 'ETH', 'SOL'])
    ]
)
def test_convert_data_req_params_tickers(dr_tickers, cc_tickers, cryptocompare) -> None:
    """
    Test tickers parameter conversion to CryptoCompare format.
    """
    cc = cryptocompare
    data_req = DataRequest(tickers=dr_tickers)
    cc_params = cc.convert_data_req_params(data_req)
    assert cc_params['tickers'] == cc_tickers, "Tickers parameter conversion failed."

@pytest.mark.parametrize(
    "dr_freq, cc_freq",
    [
        ('5min', 'histominute'),
        ('30min', 'histominute'),
        ('1h', 'histohour'),
        ('d', 'histoday'),
        (None, 'histoday'),
        ('w', 'histoday')
    ]
)
def test_convert_data_req_params_freq(dr_freq, cc_freq, cryptocompare) -> None:
    """
    Test frequency parameter conversion to CryptoCompare format.
    """
    cc = cryptocompare
    data_req = DataRequest(freq=dr_freq)
    cc_params = cc.convert_data_req_params(data_req)
    assert cc_params['frequency'] == cc_freq, "Frequency parameter conversion failed."

@pytest.mark.parametrize(
    "dr_ccy, cc_ccy",
    [
        (None, 'USD'),
        ('usdt', 'USDT'),
        ('btc', 'BTC'),
    ]
)
def test_convert_data_req_params_quote_ccy(dr_ccy, cc_ccy, cryptocompare) -> None:
    """
    Test quote currency parameter conversion to CryptoCompare format.
    """
    cc = cryptocompare
    data_req = DataRequest(quote_ccy=dr_ccy)
    cc_params = cc.convert_data_req_params(data_req)
    assert cc_params['currency'] == cc_ccy, "Quote currency parameter conversion failed."

@pytest.mark.parametrize(
    "dr_exch, cc_exch",
    [
        (None, 'CCCAGG'),
        ('binance', 'binance'),
    ]
)
def test_convert_data_req_params_exchange(dr_exch, cc_exch, cryptocompare) -> None:
    """
    Test exchange parameter conversion to CryptoCompare format.
    """
    cc = cryptocompare
    data_req = DataRequest(exch=dr_exch)
    cc_params = cc.convert_data_req_params(data_req)
    assert cc_params['exchange'] == cc_exch, "Quote currency parameter conversion failed."

@pytest.mark.parametrize(
    "dr_sd, cc_sd",
    [
        (None, convert_datetime_to_unix_tmsp('2009-01-03 00:00:00')),
        ('2015-01-01', convert_datetime_to_unix_tmsp('2015-01-01')),
        (datetime(2015,1,1), convert_datetime_to_unix_tmsp(datetime(2015,1,1))),
        (pd.Timestamp('2015-01-01 00:00:00'), convert_datetime_to_unix_tmsp(pd.Timestamp('2015-01-01 00:00:00'))),
    ]
)
def test_convert_data_req_params_start_date(dr_sd, cc_sd, cryptocompare) -> None:
    """
    Test start date parameter conversion to CryptoCompare format.
    """
    cc = cryptocompare
    data_req = DataRequest(start_date=dr_sd)
    cc_params = cc.convert_data_req_params(data_req)
    assert cc_params['start_date'] == cc_sd, "Start date parameter conversion failed."

@pytest.mark.parametrize(
    "dr_ed, cc_ed",
    [
        (None, convert_datetime_to_unix_tmsp(datetime.utcnow())),
        ('2020-12-31', convert_datetime_to_unix_tmsp('2020-12-31')),
        (datetime(2020,12,31), convert_datetime_to_unix_tmsp(datetime(2020,12,31))),
        (pd.Timestamp('2020-12-31 00:00:00'), convert_datetime_to_unix_tmsp(pd.Timestamp('2020-12-31 00:00:00'))),
    ]
)
def test_convert_data_req_params_end_date(dr_ed, cc_ed, cryptocompare, datarequest) -> None:
    """
    Test end date parameter conversion to CryptoCompare format.
    """
    cc = cryptocompare
    data_req = DataRequest(end_date=dr_ed)
    cc_params = cc.convert_data_req_params(data_req)
    if dr_ed is None:
        assert pd.to_datetime(cc_params['end_date'], unit='s').date() == datetime.utcnow().date()
    else:
        assert cc_params['end_date'] == cc_ed, "End date parameter conversion failed."

# fetch indexes
def test_fetch_indexes(cryptocompare) -> None:
    """
    Test indexes data retrieval from CryptoCompare API.
    """
    cc = cryptocompare
    data_req = DataRequest(tickers=['mvda', 'bvin'])
    df = cc.fetch_indexes(data_req)
    assert not df.empty, "Indexes dataframe was returned empty."
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be multiIndex."
    assert list(df.columns) == ['open', 'high', 'low', 'close'], "Fields are missing from indexes dataframe."
    assert df.index[0][0].date() == datetime(2017, 7, 20).date(), "Wrong start date."
    assert df.index[-1][0].date() >= (datetime.utcnow().date() - timedelta(days=1)), "End date is two days ago."

# fetch ohlcv
def test_fetch_ohlcv(cryptocompare, datarequest) -> None:
    """
    Test OHLCV data retrieval from CryptoCompare API.
    """
    cc = cryptocompare
    data_req = datarequest
    df = cc.fetch_ohlcv(data_req)
    assert not df.empty, "OHLCV dataframe was returned empty."
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be multiIndex."
    assert list(df.columns) == ['open', 'high', 'low', 'close', 'volume'], "Fields are missing from OHLCV dataframe."
    assert df.index[0] == (pd.Timestamp('2010-07-17 00:00:00'), 'BTC'), "Wrong start date."
    assert df.index[-1][0].date() == datetime.utcnow().date(), "End date is not today's date."

# fetch on-chain
def test_fetch_onchain(cryptocompare, datarequest) -> None:
    """
    Test on-chain data retrieval from CryptoCompare API.
    """
    cc = cryptocompare
    data_req = datarequest
    df = cc.fetch_onchain(data_req)
    assert not df.empty, "On-chain dataframe was returned empty."
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be multiIndex."
    assert 'active_addresses' in list(df.columns), "Fields are missing from on-chain dataframe."
    assert df.index[0] == (pd.Timestamp('2009-01-03 00:00:00'), 'BTC'), "Wrong start date."
    assert df.index[-1][0].date() >= (datetime.utcnow().date() - timedelta(days=2)), "End date is 3 days ago."

# fetch social stats
def test_fetch_social(cryptocompare, datarequest) -> None:
    """
    Test social stats data retrieval from CryptoCompare API.
    """
    cc = cryptocompare
    data_req = datarequest
    df = cc.fetch_social(data_req)
    assert not df.empty, "Social media dataframe was returned empty."
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be multiIndex."
    assert 'followers' in list(df.columns), "Fields are missing from social stats dataframe."
    assert df.index[0] == (pd.Timestamp('2017-05-26 00:00:00'), 'BTC') , "Wrong start date."
    assert (df.index[-1][0].date() >= (datetime.utcnow().date() - timedelta(days=1))), "End date is over 3 days ago."

# fetch data
def test_fetch_data_integration(cryptocompare) -> None:
    """
    Test integration of data retrieval from CryptoCompare API.
    """
    cc = cryptocompare
    data_req = DataRequest(tickers=['btc', 'eth', 'sol'], fields=['close', 'active_addresses', 'followers'])
    df = cc.fetch_data(data_req)
    assert not df.empty, "Dataframe was returned empty."
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be multiIndex."
    assert list(df.columns) == ['close', 'active_addresses', 'followers'], "Fields are missing from dataframe."
    assert df.index[0] == (pd.Timestamp('2009-01-03 00:00:00'), 'BTC'), "Wrong start date."
    assert (df.index[-1][0].date() > (datetime.utcnow().date() - timedelta(days=1))), "End date is over 3 days ago."


if __name__ == "__main__":
    pytest.main()
