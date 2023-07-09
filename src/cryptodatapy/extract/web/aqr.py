import logging
from typing import Dict, List, Optional, Union

import pandas as pd

from cryptodatapy.extract.datarequest import DataRequest
from cryptodatapy.extract.web.web import Web
from cryptodatapy.transform.convertparams import ConvertParams
from cryptodatapy.transform.wrangle import WrangleData
from cryptodatapy.util.datacredentials import DataCredentials

# data credentials
data_cred = DataCredentials()


class AQR(Web):
    """
    Retrieves data from AQR data sets.
    """

    def __init__(
            self,
            categories=None,
            indexes: Optional[Dict[str, List[str]]] = None,
            assets: Optional[Dict[str, List[str]]] = None,
            markets: Optional[Dict[str, List[str]]] = None,
            market_types=None,
            fields: Optional[Dict[str, List[str]]] = None,
            frequencies=None,
            base_url: str = data_cred.aqr_base_url,
            file_formats: Optional[Union[str, List[str]]] = 'xlsx'
    ):
        """
        Constructor

        Parameters
        ----------
        categories: list or str, {'crypto', 'fx', 'rates', 'eqty', 'cmdty', 'credit', 'macro', 'alt'}
            List or string of available categories, e.g. ['crypto', 'fx', 'alt'].
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
        file_formats: list or str, {'xlsx', 'xls'}, default 'xlsx'
            List of available file formats.
        """
        Web.__init__(
            self,
            categories,
            indexes,
            assets,
            markets,
            market_types,
            fields,
            frequencies,
            base_url,
            file_formats
        )

        if categories is None:
            self.categories = ["fx", "rates", "eqty", "cmdty", "credit"]
        if frequencies is None:
            self.frequencies = {
                "fx": ["m", "q", "y"],
                "rates": ["m", "q", "y"],
                "cmdty": ["m", "q", "y"],
                "eqty": ["d", "w", "m", "q", "y"],
                "credit": ["m", "q", "y"],
            }
        if market_types is None:
            self.market_types = ["spot", "future"]
        if fields is None:
            self.fields = self.get_fields_info()

    def get_indexes_info(self) -> None:
        """
        Get indexes info.
        """
        eqty_idxs = ['AUS', 'AUT', 'BEL', 'CAN', 'CHE', 'DEU', 'DNK', 'ESP', 'FIN', 'FRA', 'GBR', 'GRC', 'HKG', 'IRL',
                     'ISR', 'ITA', 'JPN', 'NLD', 'NOR', 'NZL', 'PRT', 'SGP', 'SWE', 'USA', 'WLD']
        print(
            f"AQR publishes excess returns data for the following equity market indexes: {eqty_idxs}"
        )

    def get_assets_info(self) -> None:
        """
        Get assets info.
        """
        print(
            f"AQR does not publish data for individual assets."
        )

    def get_markets_info(self) -> None:
        """
        Get markets info.
        """
        print(
            f"AQR does not publish data for individual markets."
        )

    @staticmethod
    def get_fields_info(
            data_type: Optional[str] = "market", cat: Optional[str] = None
    ) -> Dict[str, List[str]]:
        """
        Get fields info.

        Parameters
        ----------
        data_type: str, {'market', 'on-chain', 'off-chain'}, default 'market'
            Type of data.
        cat: str, {'crypto', 'eqty', 'fx', 'rates', 'cmdty', 'macro'}, optional, default None
            Asset class or time series category.

        Returns
        -------
        fields: dictionary
            Dictionary with info on available fields, by category.
        """
        if data_type == "on-chain" or data_type == 'off-chain':
            raise ValueError(
                "AQR only publishes total and excess return series used in their research papers."
            )

        # list of fields
        market_fields_list = ['ret', 'tr', 'er']

        # fields dict
        fields = {
            "fx": market_fields_list,
            "rates": market_fields_list,
            "eqty": market_fields_list,
            "cmdty": market_fields_list,
            "credit": market_fields_list,
        }

        # fields obj
        if cat is not None:
            fields = fields[cat]

        return fields

    def set_excel_params(self, data_req: DataRequest, ticker: str) -> Dict[str, Union[str, int]]:
        """
        Sets excel parameters for reading excel files.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.
        ticker: str
            Ticker symbol.

        Returns
        -------
        dict: dictionary
            Dictionary with params to read excel file.

        """
        # convert data request parameters to aqr format
        conv_data_req = ConvertParams(data_req).to_aqr()

        # param dict
        params = {
            'file': conv_data_req['tickers'][ticker][0],  # file name
            'freq': conv_data_req['freq'],  # freq
            'format': self.file_formats[0],  # file format
            'sheet': conv_data_req['tickers'][ticker][1],  # sheet name
            'url': None,  # url
            'parse_dates': True,  # parsing dates
            'index_col': None,      # index col
            'header': None      # header row
        }
        # set index url, col and header
        params['url'] = self.base_url + params['file'] + params['freq'] + "." + params['format']
        if params['file'] == 'Century-of-Factor-Premia-':
            params['index_col'] = 'Unnamed: 0'
            params['header'] = 18
        elif params['file'] == 'Time-Series-Momentum-Factors-':
            params['index_col'] = 'Unnamed: 0'
            params['header'] = 17
        elif params['file'] == 'Commodities-for-the-Long-Run-Index-Level-Data-':
            params['index_col'] = 'Unnamed: 0'
            params['header'] = 10
        elif params['file'] == 'Credit-Risk-Premium-Preliminary-Paper-Data':
            params['url'] = self.base_url + params['file'] + "." + params['format']
            params['index_col'] = 'Date'
            params['header'] = 10
        else:
            params['index_col'] = 'DATE'
            params['header'] = 18

        return params

    def get_series(self, data_req: DataRequest) -> Dict[str, pd.DataFrame]:
        """
        Gets series from AQR data file.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        dfs_dict: dictionary
            Dictionary with ticker-dataframe key-value pairs.

        """
        # convert data request parameters to aqr format
        conv_data_req = ConvertParams(data_req).to_aqr()

        try:
            # fetch data
            df_dicts = {}

            for ticker in conv_data_req['tickers']:
                # set excel params
                params = self.set_excel_params(data_req, ticker)
                # fetch excel file
                df1 = pd.read_excel(params['url'], sheet_name=params['sheet'], index_col=params['index_col'],
                                    parse_dates=params['parse_dates'], header=params['header'])
                # add df to dicts
                df_dicts[ticker] = df1

        except Exception as e:
            logging.warning(e)
            logging.warning(f"Failed to get data for: {conv_data_req['tickers']}.")

        else:
            return df_dicts

    @staticmethod
    def wrangle_data_resp(data_req: DataRequest, data_resp: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Wrangle data response.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.
        data_resp: Dictionary
            Dictionary with ticker-dataframe key-value pairs.

        Returns
        -------
        df: pd.DataFrame
            Wrangled dataframe with DatetimeIndex (level 0), ticker (level 1), and values for market or macro series
            for selected fields (cols), in tidy format.
        """
        # wrangle data resp
        df = WrangleData(data_req, data_resp).aqr()

        return df

    def get_tidy_data(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Gets data from FRED and wrangles the data response into tidy data format.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            Dataframe with DatetimeIndex (level 0), tickers (level 1) and actual values (cols),
            in tidy data format.
        """
        # change to get series
        data_resp = self.get_series(data_req)
        # wrangle data resp
        df = self.wrangle_data_resp(data_req, data_resp)

        return df

    def check_params(self, data_req: DataRequest) -> None:
        """
        Checks the data request parameters before requesting data to reduce API calls
        and improve efficiency.

        """
        # check cat
        if data_req.cat not in self.categories:
            raise ValueError(
                f"Select a valid category. Valid categories are: {self.categories}."
            )
        # check freq
        if data_req.freq not in self.frequencies[data_req.cat]:
            raise ValueError(
                f"Invalid data frequency. Valid data frequencies are: {self.frequencies}."
            )
        # check fields
        if not any(field in self.fields[data_req.cat] for field in data_req.fields):
            raise ValueError(f"Invalid fields. Valid data fields are: {self.fields}.")

    def get_data(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get data.

        Parameters
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and values for selected fields (cols),
            in tidy format.
        """
        # check params
        self.check_params(data_req)

        # get tidy data
        df = self.get_tidy_data(data_req)

        # check if df empty
        if df.empty:
            raise Exception(
                "No data returned. Check data request parameters and try again."
            )

        return df.sort_index()
