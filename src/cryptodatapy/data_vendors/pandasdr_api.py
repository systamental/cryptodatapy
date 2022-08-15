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
import pandas_datareader.data as web


# data credentials
data_cred = DataCredentials()


class PandasDataReader(DataVendor):
    """
    Retrieves data from Pandas Data Reader.
    """

    def __init__(
            self,
            source_type: str = 'library',
            categories: list[str] = ['fx', 'rates', 'eqty', 'credit', 'macro'],
            exchanges: list[str] = None,
            assets: list[str] = None,
            indexes: list[str] = None,
            markets: list[str] = None,
            market_types: list[str] = ['spot'],
            fields: dict[str, list[str]] = None,
            frequencies: dict[str, list[str]] = {
                'crypto': ['d', 'w', 'm', 'q', 'y'],
                'fx': ['d', 'w', 'm', 'q', 'y'],
                'rates': ['d', 'w', 'm', 'q', 'y'],
                'eqty': ['d', 'w', 'm', 'q', 'y'],
                'credit': ['d', 'w', 'm', 'q', 'y'],
                'macro': ['d', 'w', 'm', 'q', 'y']
            },
            base_url: str = None,
            api_key: dict = {'fred': None, 'yahoo': None,
                             'av-daily': data_cred.av_api_key, 'av-forex-daily': data_cred.av_api_key},
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
        print(f"See providers page to find available vendors: {data_cred.pdr_vendors_url} ")

    @staticmethod
    def get_exchanges_info():
        """
        Gets exchanges info.
        """
        print(f"See specific data vendor for available exchanges: {data_cred.pdr_vendors_url}")

    @staticmethod
    def get_assets_info():
        """
        Gets available assets info.
        """
        print(f"See specific data vendor for available assets: {data_cred.pdr_vendors_url} ")

    @staticmethod
    def get_indexes_info():
        """
        Gets available indexes info.
        """
        print(f"See specific data vendor for available indexes: {data_cred.pdr_vendors_url}")

    @staticmethod
    def get_markets_info():
        """
        Gets market pairs info.
        """
        print(f"See specific data vendor for available markets: {data_cred.pdr_vendors_url}")

    @staticmethod
    def get_fields_info(data_type: Optional[str] = 'market', cat=None) -> dict[str, list[str]]:
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
        if data_type == 'on-chain':
            raise ValueError("Pandas Data Reader is a market data aggregator of market and off-chain data.")

        # list of fields
        market_fields_list = ['open', 'high', 'low', 'close', 'volume']
        macro_fields_list = ['actual']

        # fields dict
        fields = {
            'fx': market_fields_list,
            'rates': market_fields_list,
            'eqty': market_fields_list,
            'credit': market_fields_list,
            'macro': macro_fields_list
        }

        # fields obj
        if cat is not None:
            fields = fields[cat]

        return fields

    @staticmethod
    def get_rate_limit_info():
        """
        Gets rate limit info.
        """
        print(f"See specific data vendor for rate limits: {data_cred.pdr_vendors_url}")

    def get_data(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Submits data request to pandas reader web API.
        """
        if data_req.data_source is None:
            raise ValueError("Data source cannot be None. Specify data source for data request.")

        # convert data request parameters to InvestPy format
        pdr_data_req = ConvertParams(data_source=data_req.data_source).convert_to_source(data_req)

        # check cat
        if data_req.cat not in self.categories:
            raise ValueError(f"Invalid category. Valid categories are: {self.categories}.")
        # check freq
        if data_req.freq not in self.frequencies[data_req.cat]:
            raise ValueError(f"Invalid data frequency. Valid data frequencies are: {self.frequencies}.")
        # check fields
        if not any(field in self.fields[data_req.cat] for field in data_req.fields):
            raise ValueError(f"Invalid fields. Valide data fields are: {self.fields}.")

        # get fred data
        if data_req.data_source == 'fred' or data_req.data_source == 'yahoo':
            df0 = web.DataReader(pdr_data_req['tickers'], data_req.data_source, pdr_data_req['start_date'],
                                 pdr_data_req['end_date'], api_key=self.api_key[data_req.data_source])
            # wrangle data resp
            df1 = self.wrangle_data_resp(data_req, df0)

        # get alpha vantage data
        elif data_req.data_source == 'av-daily' or data_req.data_source == 'av-forex-daily':

            # emtpy df
            df1 = pd.DataFrame()
            # get data from dbnomics
            for pdr_ticker, dr_ticker in zip(pdr_data_req['tickers'], data_req.tickers):

                # loop through tickers
                df0 = web.DataReader(pdr_ticker, data_req.data_source, pdr_data_req['start_date'],
                                     pdr_data_req['end_date'], api_key=self.api_key[data_req.data_source])
                # wrangle data resp
                if not df0.empty:
                    # wrangle data resp
                    df0 = self.wrangle_data_resp(data_req, df0)
                    # add ticker to index
                    if data_req.source_tickers is None:
                        df0['ticker'] = dr_ticker
                    else:
                        df0['ticker'] = pdr_ticker
                    df0.set_index(['ticker'], append=True, inplace=True)
                    # stack ticker dfs
                    df1 = pd.concat([df1, df0])

                    # check if df empty
        if df1.empty:
            raise Exception('No data returned. Check data request parameters and try again.')

        # filter df for desired fields and typecast
        fields = [field for field in data_req.fields if field in df1.columns]
        df = df1.loc[:, fields].copy()
        # type conversion
        df = ConvertParams().convert_dtypes(df)

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
        df = data_resp.copy()  # copy data resp

        # wrangle fred data resp
        if data_req.data_source == 'fred':

            # convert tickers to cryptodatapy format
            df.columns = data_req.tickers
            # resample to match end of reporting period, not beginning
            df = df.resample('d').last().ffill().resample(data_req.freq).last().stack().to_frame().reset_index()
            if data_req.cat == 'macro':
                df.columns = ['DATE', 'symbol', 'actual']  # rename cols
            else:
                df.columns = ['DATE', 'symbol', 'close']  # rename cols
            # convert fields to cryptodatapy format
            df = ConvertParams(data_source=data_req.data_source).convert_fields_to_lib(data_req, df)
            # set index
            df.set_index(['date', 'ticker'], inplace=True)

        elif data_req.data_source == 'av-daily' or data_req.data_source == 'av-forex-daily':

            # reset index
            df.reset_index(inplace=True)
            # convert fields to cryptodatapy format
            df = ConvertParams(data_source=data_req.data_source).convert_fields_to_lib(data_req, df)
            # set datetimeindex
            df['date'] = pd.to_datetime(df.date)
            df.set_index(['date'], inplace=True)

        elif data_req.data_source == 'yahoo':

            # reset index and drop cols name
            df = df.stack().reset_index()
            df.columns.name = None
            # convert fields to cryptodatapy format
            df = ConvertParams(data_source=data_req.data_source).convert_fields_to_lib(data_req, df)
            # set index
            df.set_index(['date', 'ticker'], inplace=True)

            # TODO: wrangle data responses from other data sources
        else:
            pass

        # filter bad data
        df = df[df != 0]  # 0 values
        df = df[~df.index.duplicated()]  # duplicate rows
        df = df.dropna(how='all').dropna(how='all', axis=1)  # entire row or col NaNs

        return df
