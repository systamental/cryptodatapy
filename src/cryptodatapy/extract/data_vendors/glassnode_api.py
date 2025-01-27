from typing import Optional, Any, Union, Dict

import pandas as pd

from cryptodatapy.extract.data_vendors.datavendor import DataVendor
from cryptodatapy.extract.datarequest import DataRequest
from cryptodatapy.transform.convertparams import ConvertParams
from cryptodatapy.transform.wrangle import WrangleData, WrangleInfo
from cryptodatapy.util.datacredentials import DataCredentials


# data credentials
data_cred = DataCredentials()

# url endpoints
urls = {'assets_info': 'assets', 'fields_info': 'endpoints'}


class Glassnode(DataVendor):
    """
    Retrieves data from Glassnode API.
    """

    def __init__(
            self,
            categories=None,
            exchanges: Optional[list[str]] = None,
            indexes: Optional[list[str]] = None,
            assets: Optional[list[str]] = None,
            markets: Optional[list[str]] = None,
            market_types=None,
            fields: Optional[list[str]] = None,
            frequencies=None,
            base_url: str = data_cred.glassnode_base_url,
            api_key: str = data_cred.glassnode_api_key,
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
        DataVendor.__init__(self, categories, exchanges, indexes, assets, markets, market_types, fields,
                            frequencies, base_url, api_key, max_obs_per_call, rate_limit)

        if frequencies is None:
            self.frequencies = ['10min', '15min', '30min', '1h', '2h', '4h', '8h', 'd', 'w', 'm', 'q', 'y']
        if market_types is None:
            self.market_types = ['spot', 'perpetual_future', 'future', 'option']
        if categories is None:
            self.categories = ['crypto']
        if api_key is None:
            raise TypeError("Set your Glassnode api key in environment variables as 'GLASSNODE_API_KEY' or "
                            "add it as an argument when instantiating the class. To get an api key, visit: "
                            "https://docs.glassnode.com/basic-api/api-key")
        if assets is None:
            self.assets = self.get_assets_info(as_list=True)
        if fields is None:
            self.fields = self.get_fields_info(data_type=None, as_list=True)

    def get_exchanges_info(self) -> None:
        """
        Gets exchanges info.
        """
        return None

    def get_indexes_info(self) -> None:
        """
        Gets indexes info.
        """
        return None

    def req_assets(self) -> Dict[str, Any]:
        """
        Get request for assets info.

        Returns
        -------
        dict: dictionary
            Data response with asset info in json format.
        """
        return DataRequest().get_req(url=self.base_url + urls['assets_info'], params={'api_key': self.api_key})

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
            List or dataframe with info on available assets.
        """
        # data req
        data_resp = self.req_assets()
        # wrangle data resp
        assets = WrangleInfo(data_resp).gn_assets_info(as_list=as_list)

        return assets

    def get_markets_info(self) -> None:
        """
        Get markets info.
        """
        return None

    def req_fields(self) -> Dict[str, Any]:
        """
        Get request for fields info.

        Returns
        -------
        dict: dictionary
            Data response with fields info in json format.
        """
        return DataRequest().get_req(url=self.base_url.replace('v1', 'v2') + urls['fields_info'],
                                     params={'api_key': self.api_key})

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
        # data req
        data_resp = self.req_fields()
        # wrangle data resp
        fields = WrangleInfo(data_resp).gn_fields_info(data_type=data_type, as_list=as_list)

        return fields

    def get_rate_limit_info(self) -> None:
        """
        Get rate limit info.
        """
        return None

    def req_data(self, data_req: DataRequest, ticker: str, field: str) -> Dict[str, Any]:
        """
        Submits data request to API.

        Parameters
        ----------
        data_req: DataRequest
            Data request parameters in CryptoDataPy format.
        ticker: str
            Requested ticker symbol.
        field: str
            Requested field.

        Returns
        -------
        data_resp: dict
            Data response in json format.
        """
        # convert data request parameters to CryptoCompare format
        gn_data_req = ConvertParams(data_req).to_glassnode()

        # set url, params
        url = self.base_url + field
        params = {
            'api_key': self.api_key,
            'a': ticker,
            's': gn_data_req['start_date'],
            'u': gn_data_req['end_date'],
            'i': gn_data_req['freq'],
            'c': gn_data_req['quote_ccy']
        }
        # data req
        data_resp = DataRequest().get_req(url=url, params=params)

        return data_resp

    @staticmethod
    def wrangle_data_resp(data_req: DataRequest, data_resp: Dict[str, Any], field: str) -> pd.DataFrame:
        """
        Wrangle data response into tidy data format.

        Parameters
        ----------
        data_req: DataRequest
            Data request parameters in CryptoDataPy format.
        data_resp: dictionary
            Data response in JSON format.
        field: str
            Requested field.

        Returns
        -------
        df: pd.DataFrame
            Wrangled dataframe with DatetimeIndex and selected field values (cols), in tidy format.
        """
        # wrangle data resp
        if data_resp is None or len(data_resp) == 0:
            df = None
        else:
            df = WrangleData(data_req, data_resp).glassnode(field)

        return df

    def get_tidy_data(self, data_req: DataRequest, ticker: str, field: str) -> pd.DataFrame:
        """
        Submits data request and wrangles the data response into tidy data format.

        Parameters
        ----------
        data_req: DataRequest
            Data request parameters in CryptoDataPy format.
        ticker: str
            Requested ticker symbol.
        field: str
            Requested field.

        Returns
        -------
        df: pd.DataFrame
            Dataframe with DatetimeIndex and field values (col) wrangled into tidy data format.
        """
        # get entire data history
        df = self.req_data(data_req, ticker=ticker, field=field)
        # wrangle df
        df = self.wrangle_data_resp(data_req, df, field)

        return df

    def get_all_fields(self, data_req: DataRequest, ticker: str) -> pd.DataFrame:
        """
        Loops list of tickers, retrieves data in tidy format for each ticker and stores it in a dataframe.

        Parameters
        ----------
        data_req: DataRequest
            Data request parameters in CryptoDataPy format.
        ticker: str
            Requested ticker symbol.

        Returns
        -------
        df: pd.DataFrame
            Dataframe with DatetimeIndex and values for fields (cols), in tidy data format.
        """
        # convert data request parameters to CryptoCompare format
        gn_data_req = ConvertParams(data_req).to_glassnode()

        df = pd.DataFrame()  # empty fields df
        counter = 0  # ohlc counter to avoid requesting OHLC data multiples times

        for field in gn_data_req['fields']:  # loop through fields

            df0 = None

            # get tidy data
            if field == 'market/price_usd_ohlc' and counter == 0:
                df0 = self.get_tidy_data(data_req, ticker, field)
                counter += 1
            elif field != 'market/price_usd_ohlc':
                df0 = self.get_tidy_data(data_req, ticker, field)

            # add field to fields df
            if df0 is not None:
                df = pd.concat([df, df0], axis=1)

        return df

    def check_params(self, data_req: DataRequest) -> None:
        """
        Check data request parameters before calling API to improve efficiency.

        Parameters
        ----------
        data_req: DataRequest
            Data request parameters in CryptoDataPy format.
        """
        # convert data request parameters to CryptoCompare format
        gn_data_req = ConvertParams(data_req).to_glassnode()

        # check tickers
        if not all([ticker.upper() in self.assets for ticker in gn_data_req['tickers']]):
            raise ValueError(f"Some of the selected assets are not available."
                             " See assets attribute for a list of available assets.")

        # check fields
        if not all([field in self.fields for field in gn_data_req['fields']]):
            raise ValueError(f"Some of the selected fields are not available."
                             " See fields attribute for a list of available fields.")

        # check freq
        if data_req.freq not in self.frequencies:
            raise ValueError(f"On-chain data is only available for {self.frequencies} frequencies."
                             f" Change data request frequency and try again.")

        return None

    def get_data(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get market, on-chain or off-chain data.

        Parameters
        ----------
        data_req: DataRequest
            Data request parameters in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and values for market, on-chain and/or
            off-chain fields (cols), in tidy data format.
        """
        # convert data request parameters to CryptoCompare format
        gn_data_req = ConvertParams(data_req).to_glassnode()

        # check params
        self.check_params(data_req)

        # empty df to add data
        df = pd.DataFrame()

        # loop through tickers and fields
        for ticker in gn_data_req['tickers']:  # loop tickers

            # get all fields for ticker
            df0 = self.get_all_fields(data_req, ticker)
            # add ticker to index
            df0['ticker'] = ticker.upper()
            df0.set_index(['ticker'], append=True, inplace=True)
            # stack ticker dfs
            df = pd.concat([df, df0])

        # filter df for desired fields and typecast
        fields = [field for field in data_req.fields if field in df.columns]
        df = df.loc[:, fields]

        return df.sort_index()
