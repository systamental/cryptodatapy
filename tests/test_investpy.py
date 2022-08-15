import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from cryptodatapy.data_requests.datarequest import DataRequest
from cryptodatapy.data_vendors.investpy_api import InvestPy
import pytest


@pytest.fixture
def investpy():
    return InvestPy()


def test_source_type(investpy) -> None:
    """
    Test source type property.
    """
    ip = investpy
    assert ip.source_type == 'library', "Source type should be 'library'."


def test_source_type_error(investpy) -> None:
    """
    Test source type errors.
    """
    ip = investpy
    with pytest.raises(ValueError):
        ip.source_type = 'anecdotal'


def test_categories(investpy) -> None:
    """
    Test categories property.
    """
    ip = investpy
    assert ip.categories == ['fx', 'rates', 'eqty', 'cmdty', 'macro'], "Incorrect categories."


def test_categories_error(investpy) -> None:
    """
    Test categories errors.
    """
    ip = investpy
    with pytest.raises(ValueError):
        ip.categories = ['real_estate', 'art']


def test_assets(investpy) -> None:
    """
    Test assets property.
    """
    ip = investpy
    assert 'U.S. 10Y' in ip.assets['rates'], "Assets list is missing 'U.S. 10Y rates'."


def test_get_assets_info(investpy) -> None:
    """
    Test get assets info method.
    """
    ip = investpy
    assert ip.get_assets_info()['rates'].loc['U.S. 10Y', 'country'] == 'united states', \
        "Asset info is missing 'U.S. 10Y rates'."


def test_indexes(investpy) -> None:
    """
    Test indexes property.
    """
    ip = investpy
    assert 'SPX' in ip.indexes, "Index list is missing 'S&P 500 VIX'."


def test_get_indexes_info(investpy) -> None:
    """
    Test get indexes info method.
    """
    ip = investpy
    assert ip.get_indexes_info().loc['SPX', 'name'] == 'S&P 500', "Index info is missing 'SPX'."


def test_market_types(investpy) -> None:
    """
    Test market types.
    """
    ip = investpy
    assert ip.market_types == ['spot', 'future'], "Some market types are missing'."


def test_market_types_error(investpy) -> None:
    """
    Test market types errors.
    """
    ip = investpy
    with pytest.raises(ValueError):
        ip.market_types = ['swaps']


def test_fields(investpy) -> None:
    """
    Test fields property.
    """
    ip = investpy
    assert ip.fields['macro'] == ['actual', 'previous', 'expected', 'surprise'], "Fields for macro cat are incorrect."


def test_frequencies(investpy) -> None:
    """
    Test frequencies property.
    """
    ip = investpy
    assert ip.frequencies['rates'] == ['d', 'w', 'm', 'q', 'y'], "Frequencies are incorrect."


def test_frequencies_error(investpy) -> None:
    """
    Test frequencies error.
    """
    ip = investpy
    with pytest.raises(TypeError):
        ip.frequencies = 5


def test_get_indexes(investpy) -> None:
    """
    Test get indexes data method.
    """
    ip = investpy
    data_req = DataRequest(tickers=['US_Eqty_Idx', 'JP_Eqty_Idx', 'EZ_Eqty_Idx'], cat='eqty')
    df = ip.get_indexes(data_req)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert list(df.index.droplevel(0).unique()) == ['US_Eqty_Idx', 'JP_Eqty_Idx', 'EZ_Eqty_Idx'], \
        "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == ['open', 'high', 'low', 'close', 'volume'], \
        "Fields are missing from dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp('1979-12-26 00:00:00'), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < timedelta(days=4), \
        "End date is more than 4 days ago."  # end date
    assert isinstance(df.close.dropna().iloc[-1], np.float64), "Close is not a numpy float."  # dtypes


def test_get_etfs(investpy) -> None:
    """
    Test get ETF data method.
    """
    ip = investpy
    data_req = DataRequest(tickers=['AR_Eqty_MSCI_ETF', 'ARE_Eqty_MSCI_ETF', 'AU_Eqty_MSCI_ETF', 'BE_Eqty_MSCI_ETF'],
                           cat='eqty')
    df = ip.get_etfs(data_req)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert list(df.index.droplevel(0).unique()) == ['AR_Eqty_MSCI_ETF', 'ARE_Eqty_MSCI_ETF', 'AU_Eqty_MSCI_ETF',
                                                    'BE_Eqty_MSCI_ETF'], \
        "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == ['open', 'high', 'low', 'close', 'volume'], \
        "Fields are missing from dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp('2011-03-04 00:00:00'), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < timedelta(days=4), \
        "End date is more than 4 days ago."  # end date
    assert isinstance(df.close.dropna().iloc[-1], np.float64), "Close is not a numpy float."  # dtypes


def test_get_stocks(investpy) -> None:
    """
    Test get stocks data method.
    """
    ip = investpy
    data_req = DataRequest(tickers=['FB', 'AAPL', 'AMZN', 'NFLX', 'GOOG'], cat='eqty')
    df = ip.get_stocks(data_req)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert list(df.index.droplevel(0).unique()) == ['FB', 'AAPL', 'AMZN', 'NFLX', 'GOOG'], \
        "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == ['open', 'high', 'low', 'close', 'volume'], \
        "Fields are missing from dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp('2012-05-18 00:00:00'), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < timedelta(days=4), \
        "End date is more than 4 days ago."  # end date
    assert isinstance(df.close.dropna().iloc[-1], np.float64), "Close is not a numpy float."  # dtypes


def test_get_eqty(investpy) -> None:
    """
    Test get Eqty data method.
    """
    ip = investpy
    data_req = DataRequest(tickers=['US_Eqty_Idx', 'US_Eqty_MSCI', 'SPY', 'US_Rates_Long_ETF', 'TLT'], cat='eqty')
    df = ip.get_eqty(data_req)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert list(df.index.droplevel(0).unique()) == ['US_Eqty_Idx', 'US_Eqty_MSCI', 'SPY', 'US_Rates_Long_ETF', 'TLT'], \
        "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == ['open', 'high', 'low', 'close', 'volume'], \
        "Fields are missing from dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp('1979-12-26 00:00:00'), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < timedelta(days=4), \
        "End date is more than 4 days ago."  # end date
    assert isinstance(df.close.dropna().iloc[-1], np.float64), "Close is not a numpy float."  # dtypes


def test_get_fx(investpy) -> None:
    """
    Test get FX data method.
    """
    ip = investpy
    data_req = DataRequest(tickers=['EUR', 'GBP', 'CAD', 'JPY', 'BRL', 'TRY'], cat='fx')
    df = ip.get_fx(data_req)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert list(df.index.droplevel(0).unique()) == ['EURUSD', 'GBPUSD', 'CADUSD', 'JPYUSD', 'BRLUSD', 'TRYUSD'], \
        "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == ['open', 'high', 'low', 'close'], \
        "Fields are missing from dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp('1979-12-27 00:00:00'), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < timedelta(days=4), \
        "End date is more than 4 days ago."  # end date
    assert isinstance(df.close.dropna().iloc[-1], np.float64), "Close is not a numpy float."  # dtypes


def test_get_rates(investpy) -> None:
    """
    Test get rates data method.
    """
    ip = investpy
    data_req = DataRequest(tickers=['US_Rates_3M', 'DE_Rates_2Y', 'JP_Rates_5Y', 'CA_Rates_10Y', 'GB_Rates_30Y'],
                           cat='rates')
    df = ip.get_rates(data_req)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert list(df.index.droplevel(0).unique()) == ['US_Rates_3M', 'DE_Rates_2Y', 'JP_Rates_5Y', 'CA_Rates_10Y',
                                                    'GB_Rates_30Y'], "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == ['open', 'high', 'low', 'close'], \
        "Fields are missing from dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp('1990-01-09 00:00:00'), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < timedelta(days=4), \
        "End date is more than 4 days ago."  # end date
    assert isinstance(df.close.dropna().iloc[-1], np.float64), "Close is not a numpy float."  # dtypes


def test_get_cmdty(investpy) -> None:
    """
    Test get commodity data method.
    """
    ip = investpy
    data_req = DataRequest(tickers=['Gold', 'Crude_Oil_Brent', 'Copper', 'Uranium'], cat='cmdty')
    df = ip.get_cmdty(data_req)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert list(df.index.droplevel(0).unique()) == ['Gold', 'Crude_Oil_Brent', 'Copper', 'Uranium'], \
        "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == ['open', 'high', 'low', 'close', 'volume'], \
        "Fields are missing from dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp('1975-01-03 00:00:00'), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < timedelta(days=4), \
        "End date is more than 4 days ago."  # end date
    assert isinstance(df.close.dropna().iloc[-1], np.float64), "Close is not a numpy float."  # dtypes


def test_get_macro(investpy) -> None:
    """
    Test get macro series method.
    """
    ip = investpy
    data_req = DataRequest(tickers=['US_Manuf_PMI', 'CN_Manuf_PMI', 'EZ_M3'], cat='macro', start_date='2019-01-01')
    df = ip.get_macro_series(data_req)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert list(df.index.droplevel(0).unique()) == ['US_Manuf_PMI', 'CN_Manuf_PMI', 'EZ_M3'], \
        "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == ['actual', 'expected', 'previous', 'surprise'], \
        "Fields are missing from dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp('2019-01-03'), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < timedelta(days=30), \
        "End date is more than a month ago."  # end date
    assert isinstance(df.actual.dropna().iloc[-1], np.float64), "Actual is not a numpy float."  # dtypes


def test_get_data(investpy) -> None:
    """
    Test get data method.
    """
    ip = investpy
    data_req = DataRequest(tickers=['US_Eqty_Idx', 'US_Eqty_MSCI', 'SPY', 'US_Rates_Long_ETF', 'TLT'], cat='eqty')
    df = ip.get_data(data_req)
    assert not df.empty, "Dataframe was returned empty."  # non empty
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."  # multiindex
    assert isinstance(df.index.droplevel(1), pd.DatetimeIndex), "Index is not DatetimeIndex."  # datetimeindex
    assert list(df.index.droplevel(0).unique()) == ['US_Eqty_Idx', 'US_Eqty_MSCI', 'SPY', 'US_Rates_Long_ETF', 'TLT'], \
        "Tickers are missing from dataframe."  # tickers
    assert list(df.columns) == ['close'], "Fields are missing from dataframe."  # fields
    assert df.index[0][0] == pd.Timestamp('1979-12-26 00:00:00'), "Wrong start date."  # start date
    assert pd.Timestamp.utcnow().tz_localize(None) - df.index[-1][0] < timedelta(days=4), \
        "End date is more than 4 days ago."  # end date
    assert isinstance(df.close.dropna().iloc[-1], np.float64), "Close is not a numpy float."  # dtypes


if __name__ == "__main__":
    pytest.main()
