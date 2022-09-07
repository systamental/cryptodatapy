import pandas as pd
import logging
from typing import Optional, Any
from cryptodatapy.util.datacredentials import DataCredentials
from cryptodatapy.extract.datarequest import DataRequest
from cryptodatapy.extract.libraries.library import Library
from cryptodatapy.transform.convertparams import ConvertParams
from cryptodatapy.transform.wrangle import WrangleData
import pandas_datareader.data as web


# data credentials
data_cred = DataCredentials()


class PandasDataReader(Library):
    """
    Retrieves data from Pandas Data Reader API.
    """

    def __init__(
            self,
            categories: list[str] = ['fx', 'rates', 'eqty', 'credit', 'macro'],
            exchanges: Optional[list[str]] = None,
            indexes: Optional[dict[str, list[str]]] = None,
            assets: Optional[dict[str, list[str]]] = None,
            markets: Optional[dict[str, list[str]]] = None,
            market_types: list[str] = ['spot'],
            fields: Optional[dict[str, list[str]]] = None,
            frequencies: dict[str, list[str]] = {
                'crypto': ['d', 'w', 'm', 'q', 'y'],
                'fx': ['d', 'w', 'm', 'q', 'y'],
                'rates': ['d', 'w', 'm', 'q', 'y'],
                'eqty': ['d', 'w', 'm', 'q', 'y'],
                'credit': ['d', 'w', 'm', 'q', 'y'],
                'macro': ['d', 'w', 'm', 'q', 'y']
            },
            base_url: Optional[str] = None,
            api_key: dict = {'fred': None, 'yahoo': None, 'av-daily': data_cred.av_api_key,
                             'av-forex-daily': data_cred.av_api_key},
            max_obs_per_call: Optional[int] = None,
            rate_limit: Optional[Any] = None
    ):
        """
        Constructor

        Parameters
        ----------
        categories: list or str, {'crypto', 'fx', 'rates', 'eqty', 'commodities', 'credit', 'macro', 'alt'}
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
        api_key: dictionary
            Api keys for data source by source-api key key-value pairs, e.g. {'av-daily' :'dcf13983adf7dfa79a0df',
            'fred' : dcf13983adf7dfa79a0df', ...}. If not provided, default is set to api_key stored in DataCredentials.
        max_obs_per_call: int, optional, default None
            Maximum number of observations returned per API call. If not provided, default is set to
            api_limit stored in DataCredentials.
        rate_limit: Any, optional, default None
            Number of API calls made and left, by time frequency.
        """
        Library.__init__(self, categories, exchanges, indexes, assets, markets, market_types, fields,
                         frequencies, base_url, api_key, max_obs_per_call, rate_limit)
        # set fields
        if fields is None:
            self.fields = self.get_fields_info()

    @staticmethod
    def get_vendors_info():
        """
        Get vendors info.
        """
        print(f"See providers page to find available vendors: {data_cred.pdr_vendors_url} ")

    @staticmethod
    def get_exchanges_info():
        """
        Get exchanges info.
        """
        print(f"See specific data vendor for available exchanges: {data_cred.pdr_vendors_url}")

    @staticmethod
    def get_indexes_info():
        """
        Get indexes info.
        """
        print(f"See specific data vendor for available indexes: {data_cred.pdr_vendors_url}")

    @staticmethod
    def get_assets_info():
        """
        Get assets info.
        """
        print(f"See specific data vendor for available assets: {data_cred.pdr_vendors_url} ")

    @staticmethod
    def get_markets_info():
        """
        Get markets info.
        """
        print(f"See specific data vendor for available markets: {data_cred.pdr_vendors_url}")

    @staticmethod
    def get_fields_info(data_type: Optional[str] = 'market', cat: Optional[str] = None) -> dict[str, list[str]]:
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
        Get rate limit info.
        """
        print(f"See specific data vendor for rate limits: {data_cred.pdr_vendors_url}")

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
        if data_req.data_source is None:
            raise ValueError("Data source cannot be None. Select a data source for the data request.")

        # convert data request parameters to InvestPy format
        pdr_data_req = ConvertParams(data_req, data_source=data_req.data_source).convert_to_source()

        # check cat
        if data_req.cat not in self.categories:
            raise ValueError(f"Invalid category. Valid categories are: {self.categories}.")
        # check freq
        if data_req.freq not in self.frequencies[data_req.cat]:
            raise ValueError(f"Invalid data frequency. Valid data frequencies are: {self.frequencies}.")
        # check fields
        if not any(field in self.fields[data_req.cat] for field in data_req.fields):
            raise ValueError(f"Invalid fields. Valid data fields are: {self.fields}.")

        # emtpy df
        df0, df1 = pd.DataFrame(), pd.DataFrame()

        # get fred data
        if data_req.data_source == 'fred' or data_req.data_source == 'yahoo':

            try:
                df0 = web.DataReader(pdr_data_req['tickers'], data_req.data_source, pdr_data_req['start_date'],
                                     pdr_data_req['end_date'], api_key=self.api_key[data_req.data_source])
            except Exception as e:
                logging.warning(e)
                logging.warning(f"Failed to get data for: {pdr_data_req['tickers']}.")
            else:
                # wrangle data resp
                df1 = self.wrangle_data_resp(data_req, df0)

        # get alpha vantage data
        elif data_req.data_source == 'av-daily':

            # get data from dbnomics, loop through tickers
            for pdr_ticker, dr_ticker in zip(pdr_data_req['tickers'], data_req.tickers):

                try:
                    df0 = web.DataReader(pdr_ticker, data_req.data_source, pdr_data_req['start_date'],
                                         pdr_data_req['end_date'], api_key=self.api_key[data_req.data_source])
                except Exception as e:
                    logging.warning(e)
                    logging.warning(f"Failed to get data for: {dr_ticker}.")

                else:
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

        # get alpha vantage data
        elif data_req.data_source == 'av-forex-daily':

            # get data from dbnomics, loop through tickers
            for pdr_ticker, dr_ticker in zip(pdr_data_req['mkts'], data_req.tickers):

                try:
                    df0 = web.DataReader(pdr_ticker, data_req.data_source, pdr_data_req['start_date'],
                                         pdr_data_req['end_date'], api_key=self.api_key[data_req.data_source])
                except Exception as e:
                    logging.warning(e)
                    logging.warning(f"Failed to get data for: {dr_ticker}.")

                else:
                    # wrangle data resp
                    df0 = self.wrangle_data_resp(data_req, df0)
                    # add ticker to index
                    df0['ticker'] = pdr_ticker.replace('/', '')
                    df0.set_index(['ticker'], append=True, inplace=True)
                    # stack ticker dfs
                    df1 = pd.concat([df1, df0])

        # check if df empty
        if df1.empty:
            raise Exception('No data returned. Check data request parameters and try again.')

        # filter df for desired fields and typecast
        fields = [field for field in data_req.fields if field in df1.columns]
        df = df1.loc[:, fields].copy()

        return df.sort_index()

    @staticmethod
    def wrangle_data_resp(data_req: DataRequest, data_resp: pd.DataFrame) -> pd.DataFrame:
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
        df = WrangleData(data_req, data_resp, data_source=data_req.data_source).tidy_data()

        return df