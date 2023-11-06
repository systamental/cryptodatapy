import logging
from importlib import resources
from typing import Any, Dict, List, Optional, Union

import investpy
import pandas as pd

from cryptodatapy.extract.datarequest import DataRequest
from cryptodatapy.extract.libraries.library import Library
from cryptodatapy.transform.convertparams import ConvertParams
from cryptodatapy.transform.wrangle import WrangleData, WrangleInfo
from cryptodatapy.util.datacredentials import DataCredentials

# data credentials
data_cred = DataCredentials()


class InvestPy(Library):
    """
    Retrieves data from InvestPy API.
    """

    def __init__(
            self,
            categories=None,
            exchanges: Optional[List[str]] = None,
            indexes: Optional[Dict[str, List[str]]] = None,
            assets: Optional[Dict[str, List[str]]] = None,
            markets: Optional[Dict[str, List[str]]] = None,
            market_types=None,
            fields: Optional[Dict[str, List[str]]] = None,
            frequencies=None,
            base_url: Optional[str] = None,
            api_key: Optional[str] = None,
            max_obs_per_call: Optional[int] = None,
            rate_limit: Optional[Any] = None,
    ):
        """
        Constructor

        Parameters
        ----------
        categories: list or str, {'crypto', 'fx', 'rates', 'eqty', 'cmdty', 'credit', 'macro', 'alt'}
            List or string of available categories, e.g. ['crypto', 'fx', 'alt'].
        exchanges: list, optional, default None
            List of available exchanges, e.g. ['Binance', 'Coinbase', 'Kraken', 'FTX', ...].
        indexes: dictionary, optional, default None
            Dictionary of available indexes, by cat-indexes key-value pairs,  e.g. [{'eqty': ['SPX', 'N225'],
            'rates': [.... , ...}.
        assets: dictionary, optional, default None
            Dictionary of available assets, by cat-assets key-value pairs,  e.g. {'rates': ['Germany 2Y', 'Japan 10Y',
            ...], 'eqty: ['SPY', 'TLT', ...], ...}.
        markets: dictionary, optional, default None
            Dictionary of available markets, by cat-markets key-value pairs,  e.g. [{'fx': ['EUR/USD', 'USD/JPY', ...],
            'crypto': ['BTC/ETH', 'ETH/USDT', ...}.
        market_types: list
            List of available market types e.g. [spot', 'perpetual_future', 'future', 'option'].
        fields: dictionary, optional, default None
            Dictionary of available fields, by cat-fields key-value pairs,  e.g. {'cmdty': ['date', 'open', 'high',
            'low', 'close', 'volume'], 'macro': ['actual', 'previous', 'expected', 'surprise']}
        frequencies: dictionary
            Dictionary of available frequencies, by cat-frequencies key-value pairs, e.g. {'fx':
            ['d', 'w', 'm', 'q', 'y'], 'rates': ['d', 'w', 'm', 'q', 'y'], 'eqty': ['d', 'w', 'm', 'q', 'y'], ...}.
        base_url: str, optional, default None
            Base url used for GET requests. If not provided, default is set to base_url stored in DataCredentials.
        api_key: str, optional, default None
            Api key, e.g. 'dcf13983adf7dfa79a0dfa35adf'. If not provided, default is set to
            api_key stored in DataCredentials.
        max_obs_per_call: int, optional, default None
            Maximum number of observations returned per API call. If not provided, default is set to
            api_limit stored in DataCredentials.
        rate_limit: Any, optional, default None
            Number of API calls made and left, by time frequency.
        """
        Library.__init__(
            self,
            categories,
            exchanges,
            indexes,
            assets,
            markets,
            market_types,
            fields,
            frequencies,
            base_url,
            api_key,
            max_obs_per_call,
            rate_limit,
        )

        if frequencies is None:
            self.frequencies = {
                "macro": [
                    "1min",
                    "5min",
                    "10min",
                    "15min",
                    "30min",
                    "1h",
                    "2h",
                    "4h",
                    "8h",
                    "d",
                    "w",
                    "m",
                    "q",
                    "y",
                ],
            }
        if categories is None:
            self.categories = ["macro"]
        if indexes is None:
            self.indexes = self.get_indexes_info(as_list=True)
        if assets is None:
            self.assets = self.get_assets_info(cat=None, as_dict=True)
        if fields is None:
            self.fields = self.get_fields_info()

    def get_exchanges_info(self) -> None:
        """
        Get exchanges info.
        """
        return None

    @staticmethod
    def req_meta(data_type: str) -> Any:
        """
        Request metadata.

        Parameters
        ----------
        data_type: str, {'indexes', 'fx', 'rates', 'etfs', 'eqty', 'cmdty'}
            Type of data to request metadata for.

        Returns
        -------
        meta: Any
         Object with metadata.
        """
        data_types = {'indexes': 'indices', 'fx': 'currency_crosses', 'rates': 'bonds', 'etfs': 'etfs',
                      'eqty': 'stocks', 'cmdty': 'commodities'}
        try:
            meta = getattr(getattr(investpy, data_types[data_type]), 'get_' + data_types[data_type])()

        except AssertionError as e:
            logging.warning(e)
            logging.warning(f"Failed to get metadata for {data_type}.")

        else:
            return meta

    def get_indexes_info(self, as_list: bool = False) -> Union[List[str], pd.DataFrame]:
        """
        Get available indexes info.

        Parameters
        ----------
        as_list: bool, default False
            Returns available indexes as list.

        Returns
        -------
        indexes: list or pd.DataFrame
            List or dataframe with info on available indexes.
        """
        # req data
        indexes = self.req_meta(data_type='indexes')
        # wrangle data resp
        indexes = WrangleInfo(indexes).ip_meta_resp(data_type='indexes', as_list=as_list)

        return indexes

    def get_fx_info(self, as_list: bool = False) -> Union[List[str], pd.DataFrame]:
        """
        Get fx info.

        Parameters
        ----------
        as_list: bool, default False
            Returns available fx pairs as list.

        Returns
        -------
        fx: list or pd.DataFrame
            List or dataframe with info on available fx pairs.
        """
        # req data
        fx = self.req_meta(data_type='fx')
        # wrangle data resp
        fx = WrangleInfo(fx).ip_meta_resp(data_type='fx', as_list=as_list)

        return fx

    def get_rates_info(self, as_list: bool = False) -> Union[List[str], pd.DataFrame]:
        """
        Get rates info.

        Parameters
        ----------
        as_list: bool, default False
            Returns available rates as list.

        Returns
        -------
        rates: list or pd.DataFrame
            List or dataframe with info on available rates.
        """
        # req data
        rates = self.req_meta(data_type='rates')
        # wrangle data resp
        rates = WrangleInfo(rates).ip_meta_resp(data_type='rates', as_list=as_list)

        return rates

    def get_etfs_info(self, as_list: bool = False) -> Union[List[str], pd.DataFrame]:
        """
        Get etfs info.

        Parameters
        ----------
        as_list: bool, default False
            Returns available etfs as list.

        Returns
        -------
        etfs: list or pd.DataFrame
            List or dataframe with info on available etfs.
        """
        # req data
        etfs = self.req_meta(data_type='etfs')
        # wrangle data resp
        etfs = WrangleInfo(etfs).ip_meta_resp(data_type='etfs', as_list=as_list)

        return etfs

    def get_eqty_info(self, as_list: bool = False) -> Union[List[str], pd.DataFrame]:
        """
        Get equities/stocks info.

        Parameters
        ----------
        as_list: bool, default False
            Returns available equities as list.

        Returns
        -------
        eqty: list or pd.DataFrame
            List or dataframe with info on available equities.
        """
        # req data
        eqty = self.req_meta(data_type='eqty')
        # wrangle data resp
        eqty = WrangleInfo(eqty).ip_meta_resp(data_type='eqty', as_list=as_list)

        return eqty

    def get_cmdty_info(self, as_list: bool = False) -> Union[List[str], pd.DataFrame]:
        """
        Get commodities info.

        Parameters
        ----------
        as_list: bool, default False
            Returns available commodities as list.

        Returns
        -------
        cmdty: list or pd.DataFrame
            List or dataframe with info on available commodities.
        """
        # req data
        cmdty = self.req_meta(data_type='cmdty')
        # wrangle data resp
        cmdty = WrangleInfo(cmdty).ip_meta_resp(data_type='cmdty', as_list=as_list)

        return cmdty

    def get_assets_info(self, cat: Optional[str] = None, as_dict: bool = False) -> \
            Union[Dict[str, List[str]], pd.DataFrame]:
        """
        Get assets info.

        Parameters
        ----------
        cat: str, {'crypto', 'fx', 'rates', 'cmdty', 'eqty'}, optional, default None
            Asset class or time series category.
        as_dict: bool, default False
            Returns available assets as dictionary, with cat-assets key-values pairs.

        Returns
        -------
        assets: dictionary or pd.DataFrame
            Dictionary or dataframe with info on available assets, by category.
        """
        # store asset info in dict
        assets_info = {'fx': self.get_fx_info()}

        # rates
        rates = self.get_rates_info()
        idx = self.get_indexes_info()
        etfs = self.get_etfs_info()
        assets_info['rates'] = pd.concat([rates,
                                          idx[idx['class'] == 'bonds'],
                                          etfs[etfs.asset_class == 'bond']]).loc[:, :'currency']

        # cmdty
        cmdty = self.get_cmdty_info()
        assets_info['cmdty'] = pd.concat([cmdty,
                                          idx[idx['class'] == 'commodities'],
                                          etfs[etfs.asset_class == 'commodity']]).loc[:, : 'currency'].drop(
            columns='title')
        # eqty
        eqty = self.get_eqty_info()

        assets_info['eqty'] = pd.concat([eqty,
                                         idx[(idx['class'] != 'bonds') & (idx['class'] != 'commodities')],
                                         etfs[etfs.asset_class == 'equity']]).loc[:, : 'currency'].drop(columns='isin')

        # macro
        if cat == "macro":
            raise ValueError(f"Asset info not available for macro data.")

        # not valid cat
        if cat not in self.categories and cat is not None:
            raise ValueError(
                f"Asset info is only available for cat: {self.categories}."
            )

        # asset dict
        if as_dict:
            assets_dict = {}
            for asset in assets_info.keys():
                assets_dict[asset] = assets_info[asset].index.to_list()
            assets_info = assets_dict

        # asset info cat
        if cat is not None:
            assets_info = assets_info[cat]

        return assets_info

    def get_markets_info(self) -> None:
        """
        Get markets info.
        """
        return None

    def get_fields_info(self, cat: Optional[str] = None) -> Dict[str, List[str]]:
        """
        Get fields info.

        Parameters
        ----------
        cat: str, {'crypto', 'eqty', 'fx', 'rates', 'cmdty', 'macro'}, optional, default None
            Asset class or time series category.

        Returns
        -------
        fields_list: dictionary
            Dictionary with info on available fields, by category.
        """
        # list of fields
        macro_fields_list = ["actual", "previous", "expected", "surprise"]

        # fields dict
        fields = {
            "macro": macro_fields_list,
        }
        # fields info cat
        if cat is not None:
            fields = fields[cat]

        return fields

    def get_rate_limit_info(self) -> None:
        """
        Get rate limit info.
        """
        return None

    @staticmethod
    def get_econ_calendar(cty: str) -> pd.DataFrame:
        """
        Get economic calendar from start date.

        Parameters
        ----------
        cty: str
            Country to retrieve econ calendar for.

        Returns
        -------
        df: pd.DataFrame
            Dataframe with econ calendar data.
        """
        # country dictionary
        ctys_dict = {'united states': 'us', 'euro zone': 'ez', 'china': 'cn', 'india': 'in', 'japan': 'jp',
                     'germany': 'de', 'russia': 'ru', 'indonesia': 'id', 'brazil': 'br', 'united kingdom': 'gb',
                     'france': 'fr', 'turkey': 'tr', 'italy': 'it', 'mexico': 'mx', 'south korea': 'kr',
                     'canada': 'ca'}

        # get econ calendar
        with resources.path('cryptodatapy.datasets', ctys_dict[cty] + '_econ_calendar.csv') as f:
            fields_dict_path = f
        # get fields and data resp
        ec_df = pd.read_csv(fields_dict_path, index_col=0)

        return ec_df

    def get_all_ctys_eco_cals(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get economic calendar for all requested countries.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame
            Dataframe with updated econ calendars for all countries.
        """
        # convert data req params to InvestPy format
        ip_data_req = ConvertParams(data_req).to_investpy()
        # remove dup ctys
        ctys = list(set(ip_data_req['ctys']))
        # ctys df
        df = pd.DataFrame()

        for cty in ctys:
            df0 = self.get_econ_calendar(cty)
            df = pd.concat([df, df0])

        return df

    def get_macro(self, data_req: DataRequest, econ_cal: pd.DataFrame) -> pd.DataFrame:
        """
        Gets and wrangles macro/econ release series from econ calendar.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.
        econ_cal: pd.DataFrame
            Dataframe with econ calendar data response.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1) and values for fields (cols).
        """
        # convert data req params to InvestPy format
        ip_data_req = ConvertParams(data_req).to_investpy()

        # emtpy df
        df = pd.DataFrame()

        # loop through tickers, countries
        for dr_ticker, ip_ticker, cty in zip(data_req.tickers, ip_data_req["tickers"], ip_data_req["ctys"]):
            # filter data calendar for ticker, country
            df0 = econ_cal[(econ_cal.event.str.startswith(ip_ticker))
                           & (econ_cal.zone.str.match(cty.lower()))].copy()
            # wrangle data resp
            df1 = self.wrangle_data_resp(data_req, df0)
            # add ticker to index
            df1["ticker"] = dr_ticker
            df1.set_index(["ticker"], append=True, inplace=True)
            # stack ticker dfs
            df = pd.concat([df, df1])

        return df.sort_index()

    def check_params(self, data_req: DataRequest) -> None:
        """
        Checks the parameters of the data request before requesting data to reduce API calls
        and improve efficiency.

        """
        # check cat
        if data_req.cat != 'macro':
            raise ValueError(
                f"Invalid category. Only historical macro data is available."
            )

        # check freq
        if data_req.freq not in self.frequencies[data_req.cat]:
            raise ValueError(
                f"Invalid data frequency. Valid data frequencies are: {self.frequencies}."
            )

        # check fields
        if not all([field in self.fields[data_req.cat] for field in data_req.fields]):
            raise ValueError(f"Invalid fields. Valid fields are: {self.fields}.")

    def get_data(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get market (eqty, fx, rates, cmdty) and/or off-chain (macro) data.

        Parameters
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and values for market or macro series
            for selected fields (cols), in tidy format.
        """
        # econ cal
        econ_cal = self.get_all_ctys_eco_cals(data_req)
        # emppty df to store data
        df = pd.DataFrame()

        # get data
        try:
            if data_req.cat == "macro":
                df = self.get_macro(data_req, econ_cal)

        except Exception as e:
            logging.warning(e)
            raise Exception(
                f"No data returned. InvestPy is deprecated. "
                f"Only metadata and historical macroeconomic data are available. "
                f"Check data request parameters and try again."
            )

        return df

    @staticmethod
    def wrangle_data_resp(
            data_req: DataRequest, data_resp: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Wrangle data response.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.
        data_resp: pd.DataFrame
            Data response from data request.

        Returns
        -------
        df: pd.DataFrame
            Wrangled dataframe with DatetimeIndex (level 0), ticker (level 1), and values for market or macro series
            for selected fields (cols), in tidy format.
        """
        # wrangle data resp
        df = WrangleData(data_req, data_resp).investpy()

        return df
