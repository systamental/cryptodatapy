import pandas as pd
from typing import Optional
from cryptodatapy.util.datacredentials import DataCredentials
from cryptodatapy.extract.datarequest import DataRequest
from cryptodatapy.transform.convertparams import ConvertParams
from cryptodatapy.transform.wrangle import WrangleData
from cryptodatapy.extract.libraries.library import Library
import dbnomics


# data credentials
data_cred = DataCredentials()


class DBnomics(Library):
    """
    Retrieves data from DBnomics API.
    """

    def __init__(
            self,
            categories: list[str] = ['macro'],
            exchanges: Optional[list[str]] = None,
            indexes: Optional[list[str]] = None,
            assets: Optional[list[str]] = None,
            markets: Optional[list[str]] = None,
            market_types: Optional[list[str]] = None,
            fields: Optional[dict[str, list[str]]] = None,
            frequencies: dict[str, list[str]] = {'macro': ['d', 'w', 'm', 'q', 'y']},
            base_url: Optional[str] = None,
            api_key: Optional[str] = None,
            max_obs_per_call: Optional[int] = None,
            rate_limit: Optional[str] = None
    ):
        """
        Constructor

        Parameters
        ----------
        categories: list or str, {'crypto', 'fx', 'rates', 'eqty', 'commodities', 'credit', 'macro', 'alt'}
            List or string of available categories, e.g. ['crypto', 'fx', 'alt'].
        exchanges: list, optional, default None
            List of available exchanges, e.g. ['Binance', 'Coinbase', 'Kraken', 'FTX', ...].
        indexes: list, optional, default None
            List of available indexes, e.g. ['mvda', 'bvin'].
        assets: list, optional, default None
            List of available assets, e.g. ['btc', 'eth'].
        markets: list, optional, default None
            List of available markets as base asset/quote currency pairs, e.g. ['btcusdt', 'ethbtc'].
        market_types: list, optional, default None
            List of available market types, e.g. [spot', 'perpetual_future', 'future', 'option'].
        fields: dictionary, optional, default None
            Dictionary of available fields, by category-fields key-value pairs,
             e.g. {'macro': 'actual', 'expected', 'suprise'}.
        frequencies: dictionary
            Dictionary of available frequencies, by category-freq key-value pairs, e.g. ['tick', '1min', '5min',
            '10min', '20min', '30min', '1h', '2h', '4h', '8h', 'd', 'w', 'm']
        base_url: str, optional, default None
            Base url used in GET requests. If not provided, default is set to base url stored in DataCredentials.
        api_key: str, optional, default None
            Api key. If not provided, default is set to cryptocompare_api_key stored in DataCredentials.
        max_obs_per_call: int, optional, default None
            Maximum number of observations returns per API call.
        rate_limit: str, optional, default None
            Number of API calls made and left by frequency.
        """
        Library.__init__(self, categories, exchanges, indexes, assets, markets, market_types, fields,
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

    @staticmethod
    def get_fields_info(cat: Optional[str] = None) -> dict[str, list[str]]:
        """
        Gets fields info.

        Parameters
        ----------
        cat: str, {'crypto', 'eqty', 'fx', 'rates', 'cmdty', 'macro', 'alt'}, default None
            Asset class or time series category.

        Returns
        -------
        fields: dictionary
            Dictionary with info on available fields, by category.
        """

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

    @staticmethod
    def get_rate_limit_info():
        """
        Gets rate limit info.
        """
        return None

    def get_data(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get data macro data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1) and values macro or off-chain fields (cols).
        """
        # convert data request parameters to InvestPy format
        dbn_data_req = ConvertParams(data_source='dbnomics').convert_to_source(data_req)

        # check cat
        if data_req.cat not in self.categories:
            raise ValueError(f"Invalid category. Valid categories are: {self.categories}.")

        # check freq
        if data_req.freq not in self.frequencies[data_req.cat]:
            raise ValueError(f"Invalid data frequency. Valid data frequencies are: {self.frequencies}.")

        # check fields
        if not any(field in self.fields[data_req.cat] for field in data_req.fields):
            raise ValueError("Invalid fields. See fields property for available fields.")

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
            Data response from GET request.

        Returns
        -------
        df: pd.DataFrame
            Wrangled dataframe with DatetimeIndex (level 0), ticker (level 1), and values for macro data series
            for selected fields (cols), in tidy format.
        """
        # wrangle data resp
        df = WrangleData(data_req, data_resp, data_source='dbnomics').tidy_data()

        return df
