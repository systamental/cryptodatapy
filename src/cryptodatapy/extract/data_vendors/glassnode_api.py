import logging
import pandas as pd
import requests
from cryptodatapy.data_requests.datarequest import DataRequest
from cryptodatapy.data_vendors.datavendor import DataVendor
from cryptodatapy.util.convertparams import ConvertParams
from cryptodatapy.util.datacredentials import DataCredentials
from time import sleep
from typing import Optional, Any, Union

# data credentials
data_cred = DataCredentials()


class Glassnode(DataVendor):
    """
    Retrieves data from Glassnode API.
    """
    def __init__(
            self,
            source_type: str = 'data_vendor',
            categories: list[str] = ['crypto'],
            exchanges: Optional[list[str]] = None,
            indexes: Optional[list[str]] = None,
            assets: Optional[list[str]] = None,
            markets: Optional[list[str]] = None,
            market_types: list[str] = ['spot', 'perpetual_future', 'future', 'option'],
            fields: Optional[list[str]] = None,
            frequencies: list[str] = ['10min', '30min', '1h', '2h', '4h', '8h', 'd', 'w', 'm', 'q', 'y'],
            base_url: str = data_cred.glassnode_base_url,
            api_key: str = data_cred.glassnode_api_key,
            max_obs_per_call: Optional[int] = None,
            rate_limit: Optional[Any] = None
    ):
        """
        Constructor

        Parameters
        ----------
        source_type: str, {'data_vendor', 'exchange', 'library', 'on-chain', 'web'}
            Type of data source, e.g. 'data_vendor', 'exchange', etc.
        categories: list or str, {'crypto', 'fx', 'rates', 'eqty', 'commodities', 'credit', 'macro', 'alt'}
            List or string of available categories, e.g. ['crypto', 'fx', 'alt'].
        exchanges: list, optional, default None
            List of available exchanges, e.g. ['Binance', 'Coinbase', 'Kraken', 'FTX', ...].
        indexes: list, optional, default None
            List of available indexes, e.g. ['mvda', 'bvin'].
        assets: list, optional, default None
            List of available assets, e.g. ['btc', 'eth'].
        markets: list, optional, default None
            List of available markets as asset/quote currency pairs, e.g. ['btcusdt', 'ethbtc'].
        market_types: list
            List of available market types, e.g. [spot', 'perpetual_future', 'future', 'option'].
        fields: list, optional, default None
            List of available fields, e.g. ['open', 'high', 'low', 'close', 'volume'].
        frequencies: list
            List of available frequencies, e.g. ['tick', '1min', '5min', '10min', '20min', '30min', '1h', '2h', '4h',
            '8h', 'd', 'w', 'm']
        base_url: str
            Base url used for GET requests. If not provided, default is set to base_url stored in DataCredentials.
        api_key: str
            Api key, e.g. 'dcf13983adf7dfa79a0dfa35adf'. If not provided, default is set to
            api_key stored in DataCredentials.
        max_obs_per_call: int, optional, default None
            Maximum number of observations returned per API call. If not provided, default is set to
            api_limit stored in DataCredentials.
        rate_limit: Any, optional, Default None
            Number of API calls made and left, by time frequency.
        """
        DataVendor.__init__(self, source_type, categories, exchanges, indexes, assets, markets, market_types, fields,
                            frequencies, base_url, api_key, max_obs_per_call, rate_limit)
        # api key
        if api_key is None:
            raise TypeError(f"Set your api key. Alternatively, you can use the function "
                            f"{set_credential.__name__} which uses keyring to store your "
                            f"api key in {DataCredentials.__name__}.")
        self.assets = self.get_assets_info(as_list=True)
        self.fields = self.get_fields_info(data_type=None, as_list=True)
        self.rate_limit = self.get_rate_limit_info()

    @staticmethod
    def get_exchanges_info():
        """
        Gets exchanges info.
        """
        print(f"See supported exchanges: {data_cred.glassnode_search_url}")

    @staticmethod
    def get_indexes_info():
        """
        Gets indexes info.
        """
        return None

    def get_assets_info(self, as_list: bool = False) -> Union[list[str], pd.DataFrame]:
        """
         Get assets info.

        Parameters
        ----------
        as_list: bool, default False
            Returns assets info as list.

        Returns
        -------
        assets: list or pd.DataFrame
            List or dataframe with info for available assets.
        """
        try:  # try get request
            url = data_cred.glassnode_base_url + 'assets'
            params = {
                'api_key': self.api_key
            }
            r = requests.get(url, params=params)
            r.raise_for_status()

        except Exception as e:
            logging.warning(e)
            logging.warning(f"Failed to get asset info.")

        else:
            # format response
            assets = pd.DataFrame(r.json())
            # rename cols and set index
            assets.rename(columns={'symbol': 'ticker'}, inplace=True)
            assets = assets.set_index('ticker')
            # asset list
            if as_list:
                assets = list(assets.index)

            return assets

    @staticmethod
    def get_markets_info():
        """
        Get markets info.
        """
        return None

    def get_fields_info(self, data_type: Optional[str] = None, as_list: bool = False) -> Union[list[str], pd.DataFrame]:
        """
        Get fields info.

        Parameters
        ----------
        data_type: str, {'market', 'on-chain', 'off-chain'}, default None
            Type of data.
        as_list: bool, default False
            Returns available fields info as list.

        Returns
        -------
        fields: list or pd.DataFrame
            List or dataframe with info on available fields.
        """
        try:  # try get request
            url = 'https://api.glassnode.com/v2/metrics/endpoints'
            params = {
                'api_key': self.api_key
            }
            r = requests.get(url, params=params)
            r.raise_for_status()

        except Exception as e:
            logging.warning(e)
            logging.warning(f"Failed to get asset info.")

        else:
            # format response
            fields = pd.DataFrame(r.json())
            # create fields and cat cols
            fields['fields'] = fields.path.str.split(pat='/', expand=True, n=3)[3]
            fields['categories'] = fields.path.str.split(pat='/', expand=True)[3]
            # rename and reorder cols, and set index
            fields.rename(columns={'resolutions': 'frequencies'}, inplace=True)
            fields = fields.loc[:, ['fields', 'categories', 'tier', 'assets', 'currencies', 'frequencies', 'formats',
                                    'path']]
            fields.set_index('fields', inplace=True)

            # filter fields info
            if data_type == 'market':
                fields = fields[(fields.categories == 'market') | (fields.categories == 'derivatives')]
            elif data_type == 'on-chain':
                fields = fields[(fields.categories != 'market') | (fields.categories != 'derivatives')]
            elif data_type == 'off-chain':
                fields = fields[fields.categories == 'institutions']
            else:
                fields = fields
            # fields list
            if as_list:
                fields = list(fields.index)

            return fields

    @staticmethod
    def get_rate_limit_info():
        """
        Get rate limit info.
        """
        return None

    def get_data(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get market, on-chain or off-chain data.

        Parameters
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and values for market, on-chain and/or
            off-chain fields (cols), in tidy format.
        """
        # convert data request parameters to CryptoCompare format
        gn_data_req = ConvertParams(data_source='glassnode').convert_to_source(data_req)
        # empty df to add data
        df = pd.DataFrame()

        # check tickers
        tickers = self.assets
        if not any(ticker.upper() in tickers for ticker in gn_data_req['tickers']):
            raise ValueError(f"Assets are not available. Available assets include {self.assets}.")

        # check fields
        fields = self.fields
        if not any(i in fields for i in gn_data_req['fields']):
            raise ValueError(f"Fields are not available. Available fields include: {self.fields}.")

        # check freq
        if data_req.freq not in ['10min', '15min', '30min', '1h', '2h', '4h', '8h', 'd', 'w', 'm', 'q']:
            raise ValueError(f"On-chain data is only available for {self.frequencies} frequencies."
                             f" Change data request frequency and try again.")

        # loop through tickers and fields
        for ticker in gn_data_req['tickers']:  # loop tickers

            df0 = pd.DataFrame()  # ticker df

            for gn_field, dr_field in zip(gn_data_req['fields'], data_req.fields):  # loop fields

                # set number of attempts and bool for while loop
                attempts = 0
                # run a while loop to onchain data in case the attempt fails
                while attempts < gn_data_req['trials']:
                    try:  # get request
                        url = 'https://api.glassnode.com/v1/metrics/' + gn_field
                        params = {
                            'api_key': self.api_key,
                            'a': ticker,
                            's': gn_data_req['start_date'],
                            'u': gn_data_req['end_date'],
                            'i': gn_data_req['freq'],
                        }
                        r = requests.get(url, params=params)
                        r.raise_for_status()
                        df1 = pd.read_json(r.text, convert_dates=['t'])
                        assert not df1.empty

                    except Exception as e:
                        logging.warning(e)
                        attempts += 1
                        sleep(gn_data_req['pause'])
                        logging.warning(f"Failed to pull {dr_field} data for {ticker} after attempt #{str(attempts)}.")
                        if attempts == 3:
                            logging.warning(
                                f"Failed to pull {dr_field} data for {ticker} after many attempts.")
                            break

                    else:
                        # rename val col
                        if 'v' in df1.columns:
                            df1.rename(columns={'v': dr_field}, inplace=True)
                        # wrangle data resp
                        df2 = self.wrangle_data_resp(data_req, df1)
                        # add fields to ticker df
                        df0 = pd.concat([df0, df2], axis=1)
                        break

            # add ticker to index
            df0['ticker'] = ticker.upper()
            df0.set_index(['ticker'], append=True, inplace=True)
            # stack ticker dfs
            df = pd.concat([df, df0])

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
            Data response from API.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            Wrangled dataframe with DatetimeIndex (level 0), ticker or institution (level 1), and market, on-chain or
            off-chain values for selected fields (cols), in tidy format.
        """
        df = data_resp.copy()  # make copy

        # format cols
        df.rename(columns={'t': 'date'}, inplace=True)
        if 'o' in df.columns:  # ohlcv data resp
            df = pd.concat([df.date, df['o'].apply(pd.Series)], axis=1)
            df.rename(columns={'o': 'open', 'h': 'high', 'c': 'close', 'l': 'low'}, inplace=True)
            df = df.loc[:, ['date', 'open', 'high', 'low', 'close']]
        elif 'v' in df.columns:  # on-chain and off-chain data resp
            df = df.loc[:, ['date', 'v']]

        # convert date and set datetimeindex
        df = df.set_index('date').sort_index()

        # filter for desired start to end date
        if data_req.start_date is not None:
            df = df[(df.index >= data_req.start_date)]
        if data_req.end_date is not None:
            df = df[(df.index <= data_req.end_date)]

        # resample freq
        df = df.resample(data_req.freq).last()

        # remove bad data
        df = df[df != 0].dropna(how='all')  # 0 values
        df = df[~df.index.duplicated()]  # duplicate rows
        df.dropna(how='all', inplace=True)  # remove entire row NaNs

        # type conversion
        df = df.apply(pd.to_numeric, errors='ignore').convert_dtypes()

        return df