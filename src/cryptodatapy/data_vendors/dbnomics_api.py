import pandas as pd
import numpy as np
import logging
import requests
from datetime import datetime, timedelta
from time import sleep
from importlib import resources
from typing import Optional, Union, Any
from cryptodatapy.util.datacredentials import DataCredentials
from cryptodatapy.data_requests.datarequest import DataRequest
from cryptodatapy.util.convertparams import ConvertParams
from cryptodatapy.data_vendors.datavendor import DataVendor
import dbnomics


# data credentials
data_cred = DataCredentials()


class DBnomics(DataVendor):
    """
    Retrieves data from DBNomics API.
    """

    def __init__(
            self,
            source_type: str = 'library',
            categories: list[str] = ['macro'],
            exchanges: list[str] = None,
            assets: list[str] = None,
            indexes: list[str] = None,
            markets: list[str] = None,
            market_types: list[str] = None,
            fields: dict[str, list[str]] = None,
            frequencies: dict[str, list[str]] = {'macro': ['d', 'w', 'm', 'q', 'y']},
            base_url: str = None,
            api_key: str = None,
            max_obs_per_call: int = None,
            rate_limit: str = None
    ):
        """
        Constructor

        Parameters
        ----------
        source_type: string, {'data_vendor', 'exchange', 'library', 'on-chain', 'web'}
            Type of data source, e.g. 'data_vendor', 'exchange', etc.
        categories: list, {'crypto', 'fx', 'rates', 'eqty', 'commodities', 'credit', 'macro', 'alt'}
            List of available categories, e.g. ['crypto', 'fx', 'alt']
        exchanges: list
            List of available exchanges, e.g. ['Binance', 'Coinbase', 'Kraken', 'FTX']
        vendors: list
            List of available data vendors, e.g. ['IMF Cross Country Macroeconomic Statistics']
        assets: list
            List of available assets, e.g. ['btc', 'eth']
        indexes: list
            List of available indexes, e.g. ['mvda', 'bvin']
        markets: list
            List of available markets as asset/quote currency pairs, e.g. ['btcusdt', 'ethbtc']
        market_types: list
            List of available market types/contracts, e.g. [spot', 'perpetual_future', 'future', 'option']
        fields: list
            List of available fields, e.g. ['open', 'high', 'low', 'close', 'volume']
        frequencies: list
            List of available frequencies, e.g. ['tick', '1min', '5min', '10min', '20min', '30min', '1h', '2h', '4h',
            '8h', 'd', 'w', 'm']
        base_url: str
            Base url used in GET requests. If not provided, default is set to base url stored in DataCredentials.
        api_key: str
            Api key. If not provided, default is set to cryptocompare_api_key stored in DataCredentials.
        max_obs_per_call: int
            Maximum number of observations returns per API call.
        rate_limit: pd.DataFrame or dict
            Number of API calls made and left by frequency.
        """

        DataVendor.__init__(self, source_type, categories, exchanges, assets, indexes, markets, market_types, fields,
                            frequencies, base_url, api_key, max_obs_per_call, rate_limit)

        # set fields
        if fields is None:
            self.fields = self.get_fields_info()

    @staticmethod
    def get_vendors_info():
        """
        Gets available vendors info.
        """
        print(f"See providers page to find available vendors: {data_cred.dbnomics_vendors_url} ")

    @staticmethod
    def get_assets_info():
        """
        Gets available assets info.
        """
        print(f"See search page for available assets: {data_cred.dbnomics_search_url} ")

    @staticmethod
    def get_indexes_info():
        """
        Gets available indexes info.
        """
        return None

    @staticmethod
    def get_markets_info():
        """
        Gets market pairs info.
        """
        return None

    def get_fields_info(self, data_type: Optional[str] = 'off-chain', cat=None) -> dict[str, list[str]]:
        """
        Gets fields info.

        Parameters
        ----------
        data_type: str, {'market', 'on-chain', 'off-chain'}, default 'market'
            Type of data.
        cat: str, {'crypto', 'eqty', 'fx', 'rates', 'cmdty', 'macro'}, default None
            Asset class or time series category.

        Returns
        -------
        fields_list: dict
            Info on available fields, by category.
        """
        if data_type == 'market' or data_type == 'on-chain':
            raise ValueError("Market and on-chain data are not available."
                             " DBnomics aggregates economic data provided by national and international statistical institutions,"
                             " researchers and private companies.")

        # list of fields
        macro_fields_list = ['actual']

        # fields dict
        fields = {
            'macro': macro_fields_list,
        }

        # fields obj
        if cat is not None:
            fields = fields[cat]

        return fields

    @staticmethod
    def get_exchanges_info():
        """
        Gets exchanges info.
        """
        return None

    def get_rate_limit_info(self):
        """
        Gets rate limit info.
        """
        return None

    def get_data(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Submits data request to DBnomics web API.
        """
        # convert data request parameters to InvestPy format
        dbn_data_req = ConvertParams(data_source='dbnomics').convert_to_source(data_req)
        with resources.path('cryptodatapy.conf', 'tickers.csv') as f:
            tickers_path = f
        tickers = pd.read_csv(tickers_path, index_col=0, encoding='latin1').index.to_list()

        # check cat
        if data_req.cat not in self.categories:
            raise ValueError(f"Invalid category. Valid categories are: {self.categories}.")

        # check freq
        if data_req.freq not in self.frequencies[data_req.cat]:
            raise ValueError(f"Invalid data frequency. Valid data frequencies are: {self.frequencies}.")

        # check fields
        if not any(field in self.fields[data_req.cat] for field in data_req.fields):
            raise ValueError("Invalid fields. See '.fields' property for available fields.")

        # emtpy df
        df = pd.DataFrame()

        # get data from dbnomics
        for dbn_ticker, dr_ticker in zip(dbn_data_req['tickers'], data_req.tickers):
            # loop through tickers
            df0 = dbnomics.fetch_series(dbn_ticker)
            # wrangle data resp
            if not df0.empty:
                # wrangle data resp
                df1 = self.wrangle_data_resp(data_req, df0)
                # add ticker to index
                if data_req.source_tickers is None:
                    df1['ticker'] = dr_ticker
                else:
                    df1['ticker'] = dbn_ticker
                df1.set_index(['ticker'], append=True, inplace=True)
                # stack ticker dfs
                df = pd.concat([df, df1])

                # check if df empty
        if df.empty:
            raise Exception('No data returned. Check data request parameters and try again.')

        # filter df for desired fields and typecast
        fields = [field for field in data_req.fields if field in df.columns]
        df = df.loc[:, fields]

        return df

    @staticmethod
    def wrangle_data_resp(data_req: DataRequest, data_resp: pd.DataFrame) -> pd.DataFrame:
        """
        Wrangles data response.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.
        data_resp: pd.DataFrame
            Data response from GET request.

        Returns
        -------
        df: pd.DataFrame
            Wrangled dataframe in tidy format.
        """
        # convert cols to cryptodatapy format
        df = ConvertParams(data_source='dbnomics').convert_fields_to_lib(data_req, data_resp)

        # set index
        df.set_index('date', inplace=True)
        # filter for desired start to end date
        if data_req.start_date is not None:
            df = df[(df.index >= data_req.start_date)]
        if data_req.end_date is not None:
            df = df[(df.index <= data_req.end_date)]
        # resample freq
        df = df.resample(data_req.freq).last().ffill()

        # filter bad data
        df = df[df != 0]  # 0 values
        df = df[~df.index.duplicated()]  # duplicate rows
        df = df.dropna(how='all').dropna(how='all', axis=1)  # entire row or col NaNs

        # type conversion
        df = ConvertParams().convert_dtypes(df)

        return df