from dataclasses import dataclass, field
from importlib import resources
from typing import Dict, List, Optional, Union

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


@dataclass
class DataCatalog:
    """
    Data catalog which makes it easy to find, extract, use, and share CryptoDataPy datasets.

    Parameters
    ----------
    data_sources: str
        Name and url of available data sources.

    """

    data_sources: Dict = field(
        default_factory=lambda: {
            "ccxt": "https://github.com/ccxt/ccxt",
            "cryptocompare": "https://min-api.cryptocompare.com/documentation",
            "coinmetrics": "https://docs.coinmetrics.io/info/markets",
            "glassnode": "https://glassnode.com/",
            "tiingo": "https://api.tiingo.com/products/crypto-api",
            "yahoo finance": "https://finance.yahoo.com/",
            "investpy": "https://investpy.readthedocs.io/",
            "dbnomics": "https://db.nomics.world/providers",
            "fred": "https://fred.stlouisfed.org/",
        }
    )

    @staticmethod
    def get_tickers_metadata(
        tickers: Optional[Union[str, List[str]]] = None,
        country_id_2: Optional[str] = None,
        country_id_3: Optional[str] = None,
        country_name: Optional[str] = None,
        agg: Optional[str] = None,
        cat: Optional[str] = None,
        subcat: Optional[str] = None,
        mkt_type: Optional[str] = None,
        quote_ccy: Optional[str] = None,
        as_list=False,
    ) -> pd.DataFrame():
        """
        Gets ticker metadata. Excludes individual equity and cryptoasset tickers.

        Parameters
        ----------
        tickers: str or list, optional, default None
            Tickers for which to get metadata.
        country_id_2: str, optional, default None
            Country code for which to filter tickers, ISO 3166-1 alpha-2 format.
        country_id_3: str, optional, default None
            Country code for which to filter tickers, ISO 3166-1 alpha-3 format.
        country_name: str, optional, default None
            Country name for which to filer tickers.
        agg: str, {'DM', 'EM', 'WL', 'Euro zone'}, optional, default None
            Country aggregate.
        cat: str, {'crypto', 'fx', 'cmdty', 'eqty', 'rates', 'bonds', 'credit', 'macro', 'alt'}
            Tickers category.
        subcat: str, { 'spot rate', 'index', 'effective exchange rate', 'yield', 'swap rate', 'real yield',
                       'inflation', 'spread', 'etf', 'vol', 'reit', 'industrial metals', 'grains', 'energy', 'softs',
                       'livestock', 'precious metals', 'growth', 'unemployment', 'money', 'credit', 'property'},
                       optional, default None
            Tickers subcategory.
        mkt_type: str, {'spot', 'etf', 'perpetual_future', 'future', 'swap', 'option'}, optional, default None
            Market type, e.g. 'spot ', 'future', 'perpetual_future', 'option'.
        quote_ccy: str,  optional, default None
            Quote currency for base asset, e.g. 'GBP' for EURGBP, 'USD' for BTCUSD (bitcoin in dollars), etc.
        as_list: bool, default False
            Returns requested tickers as list.

        Returns
        -------
        tickers_df: pd.DataFrame
            DataFrame with requested tickers metadata.
        """
        # get tickers csv file
        with resources.path("cryptodatapy.conf", "tickers.csv") as f:
            tickers_path = f
        tickers_df = pd.read_csv(tickers_path, index_col=0, encoding="latin1")

        # filter by tickers
        if tickers is not None:
            tickers_df = tickers_df.loc[tickers, :]
        # filter by country id 2
        if country_id_2 is not None:
            tickers_df = tickers_df[tickers_df.country_id_2 == country_id_2.upper()]
        # filter by country id 3
        if country_id_3 is not None:
            tickers_df = tickers_df[tickers_df.country_id_3 == country_id_3.upper()]
        # filter by country name
        if country_name is not None:
            tickers_df = tickers_df[tickers_df.country_name == country_name.title()]
        # filter by aggregate
        if agg is not None:
            tickers_df = tickers_df[tickers_df["agg"] == agg]
        # filter by category
        if cat is not None:
            tickers_df = tickers_df[tickers_df.category == cat.lower()]
        # filter by sub category
        if subcat is not None:
            tickers_df = tickers_df[tickers_df.subcategory == subcat.lower()]
        # filter by mkt type
        if mkt_type is not None:
            tickers_df = tickers_df[tickers_df.mkt_type == mkt_type.lower()]
        # filter by quote ccy
        if quote_ccy is not None:
            tickers_df = tickers_df[tickers_df.quote_ccy == quote_ccy.upper()]
        # as list
        if as_list:
            tickers_df = list(set(tickers_df.index.to_list()))

        return tickers_df

    @staticmethod
    def search_tickers(
        by_col: Optional[str] = None, keyword: Optional[str] = None
    ) -> pd.DataFrame():
        """
        Searches for tickers metadata.

        Parameters
        ----------
        by_col: str, optional, default None
            Column by which to search.
        keyword: str, optional, default None
            Keyword for which to search.

        Returns
        -------
        tickers_df: pd.DataFrame
            DataFrame with requested tickers metadata.
        """
        # get tickers csv file
        with resources.path("cryptodatapy.conf", "tickers.csv") as f:
            tickers_path = f
        tickers_df = pd.read_csv(tickers_path, index_col=0, encoding="latin1")

        if by_col is None or keyword is None:
            raise ValueError("Provide values to search for 'by_col' and 'keyword' parameters.")
        else:
            tickers_df = tickers_df[tickers_df[by_col].str.contains(keyword, na=False)]

        return tickers_df

    @staticmethod
    def get_fields_metadata(
        fields: Optional[Union[str, List[str]]] = None,
        name: Optional[str] = None,
        cat: Optional[str] = None,
        subcat: Optional[str] = None,
        as_list=False,
    ) -> pd.DataFrame():
        """
        Gets fields metadata.

        Parameters
        ----------
        fields: str or list, optional, default None
            Field ids for which to get metadata.
        name: str, optional, default None
            Name of fields for which to get metadata.
        cat: str, {'all', 'market', 'on-chain', 'off-chain'}, optional, default None
            Fields category, i.e. type of data.
        subcat: str, optional, default None
            Fields subcategory.
        as_list: bool, False
            Returns requested fields as list.

        Returns
        -------
        fields_df: pd.DataFrame
            DataFrame with requested fields metadata.
        """
        # get fields csv file
        with resources.path("cryptodatapy.conf", "fields.csv") as f:
            fields_path = f
        fields_df = pd.read_csv(fields_path, index_col=0, encoding="latin1")

        # filter by field ids
        if fields is not None:
            fields_df = fields_df.loc[fields, :]
        # filter by field name
        if name is not None:
            fields_df = fields_df[fields_df.name == name.lower()]
        # filter by category
        if cat is not None:
            fields_df = fields_df[fields_df.category == cat.lower()]
        # filter by sub category
        if subcat is not None:
            fields_df = fields_df[fields_df.subcategory == subcat.lower()]
        # as list
        if as_list:
            fields_df = list(set(fields_df.index.to_list()))

        return fields_df

    @staticmethod
    def search_fields(
        by_col: Optional[str] = None, keyword: Optional[str] = None
    ) -> pd.DataFrame():
        """
        Searches for fields metadata.

        Parameters
        ----------
        by_col: str, optional, default None
            Column by which to search.
        keyword: str, optional, default None
            Keyword for which to search.

        Returns
        -------
        fields_df: pd.DataFrame
            DataFrame with fields metadata.
        """
        # get fields csv file
        with resources.path("cryptodatapy.conf", "fields.csv") as f:
            fields_path = f
        fields_df = pd.read_csv(fields_path, index_col=0, encoding="latin1")

        if by_col is None or keyword is None:
            raise ValueError("Provide values to search for 'by_col' and 'keyword' parameters.")
        else:
            fields_df = fields_df[fields_df[by_col].str.contains(keyword, na=False)]

        return fields_df

    @staticmethod
    def scrape_stablecoins(
        source: str = "coingecko", rank_by: str = "mkt_cap", as_list=False
    ) -> Union[pd.DataFrame, list]:
        """
        Web scrapes stablecoin information from websites.

        Parameters
        ----------
        source: str, {'coinmarketcap', 'coingecko'}, default 'coingecko'
            Website from which to scrape list of stablecoins.
        rank_by: str, default 'mkt_cap'
            Metrics by which to rank stablecoins.
        as_list: bool, default False
            Return stablecoins tickers as list.

        Returns
        -------
        sc: Pd.Dataframe or list
            DataFrame with stablecoin info or list of stablecoin tickers.
        """
        # chrome driver
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
        # urls
        sources = {
            "coinmarketcap": "http://coinmarketcap.com/view/stablecoin/",
            "coingecko": "https://www.coingecko.com/en/categories/stablecoins",
        }
        # get url
        driver.get(sources[source])
        # manage wait time
        driver.implicitly_wait(60)
        # page source
        html = driver.page_source
        # get tables
        tables = pd.read_html(html)
        df = tables[0]
        # close driver
        driver.close()

        # wrangle table
        if source == "coinmarketcap":
            df["name"] = df.Name.str.split("(\\d+)", 1, expand=True)[0]
            df["ticker"] = df.Name.str.split("(\\d+)", 1, expand=True)[2]
            df["price"] = df.Price.str.split("$", 1, expand=True)[1].astype(float)
            df["24h_%_chg"] = (
                df["24h %"].str.split("%", 1, expand=True)[0].astype(float) / 100
            )
            df["7d_%_chg"] = (
                df["7d %"].str.split("%", 1, expand=True)[0].astype(float) / 100
            )
            df["mkt_cap"] = (
                df["Market Cap"]
                .str.split("$", 2, expand=True)[2]
                .str.replace(",", "", regex=False)
                .astype(float)
            )
            df["volume_24h"] = (
                df["Volume(24h)"]
                .str.split(" ", 1, expand=True)[0]
                .str.replace("$", "", regex=False)
                .str.replace(",", "", regex=False)
                .astype(float)
            )
            df["circ_suppkly"] = (
                df["Circulating Supply"]
                .str.split(" ", 1, expand=True)[0]
                .str.replace(",", "", regex=False)
                .astype(float)
            )
            # reorder cols
            df = df.loc[:, "name":]
            # set index
            df.set_index("ticker", inplace=True)

        elif source == "coingecko":

            df["name"] = (
                df.Coin.str.split(" ", 4, expand=True)[0]
                + " "
                + df.Coin.str.split(" ", 4, expand=True)[1]
            )
            tickers_list = []
            for row in df.Coin.str.split(" ", 4, expand=True).iterrows():
                tickers_list.append(row[1].dropna().iloc[-1])
            df["ticker"] = tickers_list
            df["price"] = (
                df.Price.str.split("$", 0, expand=True)[1]
                .str.replace(",", "")
                .astype(float)
            )
            df["mkt_cap"] = (
                df["Market Capitalization"]
                .str.split("$", 1, expand=True)[1]
                .str.replace(",", "")
                .astype(float)
            )
            df["24h_volume"] = (
                df["24h Volume"]
                .str.split("$", 1, expand=True)[1]
                .str.replace(",", "")
                .astype(float)
            )
            # reorder cols
            df = df.loc[:, "name":]
            # set index
            df.set_index("ticker", inplace=True)

        # rank by
        if rank_by is not None:
            sc = df.sort_values(by=rank_by, ascending=False)
        else:
            sc = df

        # tickers list
        if as_list:
            # remove duplicated tickers
            sc = df.loc[df.index.drop_duplicates().to_list()].index.to_list()

        return sc
