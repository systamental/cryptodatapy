from cryptodatapy.util.datacatalog import DataCatalog
import pytest


@pytest.fixture
def datacatalog():
    return DataCatalog()


# data sources
def test_data_sources(datacatalog) -> None:
    """
    Test source type for CryptoCompare.
    """
    dc = datacatalog
    assert dc.data_sources['cryptocompare'] == 'https://min-api.cryptocompare.com/documentation', \
        "Invalid url for data source."
    assert dc.data_sources['coinmetrics'] == 'https://docs.coinmetrics.io/info/markets', "Invalid url for data source."
    assert dc.data_sources['glassnode'] == 'https://glassnode.com/', "Invalid url for data source."
    assert dc.data_sources['investpy'] == 'https://investpy.readthedocs.io/', "Invalid url for data source."


def test_get_tickers_meta(datacatalog) -> None:
    """
    Test tickers metadata retrieval.
    """
    dc = datacatalog
    df = dc.get_tickers_metadata(cat='crypto', subcat='stablecoin')
    assert not df.empty, "Tickers dataframe was returned empty."
    assert df.index.name == "ticker", "Dataframe index name should be 'ticker'."
    assert 'category' in list(df.columns), "Category is missing from tickers dataframe."
    assert 'USDC' in df.index, "Missing USDC stablecoin ticker."
    assert df.loc['USDT', 'subcategory'] == 'stablecoin', "Subcategory should be inflation."
    tickers_list = dc.get_tickers_metadata(cat='crypto', subcat='stablecoin', as_list=True)
    assert isinstance(tickers_list, list), "'as_list' fails to return list."


def test_search_tickers(datacatalog) -> None:
    """
    Test tickers metadata search.
    """
    dc = datacatalog
    df = dc.search_tickers(by_col='name', keyword='USD Coin')
    assert not df.empty, "Tickers dataframe was returned empty."
    assert df.index.name == "ticker", "Dataframe index name should be 'ticker'."
    assert 'name' in list(df.columns), "Name is missing from tickers dataframe."
    assert 'USDC' in df.index, "Missing USDC ticker."
    assert df.loc['USDC', 'subcategory'] == 'stablecoin', "Subcategory should be stablecoin."


def test_get_fields_meta(datacatalog) -> None:
    """
    Test fields metadata retrieval.
    """
    dc = datacatalog
    df = dc.get_fields_metadata(cat='on-chain')
    assert not df.empty, "Fields dataframe was returned empty."
    assert df.index.name == "id", "Dataframe index name should be 'id'."
    assert 'category' in list(df.columns), "Category is missing from fields dataframe."
    assert 'add_act' in df.index, "Missing active addresses field."
    assert df.loc['add_act', 'coinmetrics_id'] == 'AdrActCnt', "Wrong CM id for active addresses."
    fields_list = dc.get_fields_metadata(cat='on-chain', as_list=True)
    assert isinstance(fields_list, list), "'as_list' fails to return list."


def test_search_fields(datacatalog) -> None:
    """
    Test fields metadata search.
    """
    dc = datacatalog
    df = dc.search_fields(by_col='subcategory', keyword='addresses')
    assert not df.empty, "Fields dataframe was returned empty."
    assert df.index.name == "id", "Dataframe index name should be 'id'."
    assert 'name' in list(df.columns), "Name is missing from fields dataframe."
    assert 'add_act' in df.index, "Missing USDC ticker."
    assert df.loc['add_act', 'cryptocompare_id'] == 'active_addresses', "Wrong CC id for active addresses."


def test_scrape_stablecoins(datacatalog) -> None:
    """
    Test fields metadata search.
    """
    dc = datacatalog
    df = dc.scrape_stablecoins(source='coingecko')
    assert not df.empty, "Stablecoins dataframe was returned empty."
    assert df.index.name == "ticker", "Dataframe index name should be 'ticker'."
    assert 'mkt_cap' in list(df.columns), "Market cap info is missing from stablecoins dataframe."
    assert 'USDC' in df.index, "Missing USDC ticker."
    assert df.loc['BUSD', 'name'] == 'Binance USD', "Wrong name for BUSD."


if __name__ == "__main__":
    pytest.main()
