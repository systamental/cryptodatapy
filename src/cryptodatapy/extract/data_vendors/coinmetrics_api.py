import logging
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from coinmetrics.api_client import CoinMetricsClient

from cryptodatapy.extract.data_vendors.datavendor import DataVendor
from cryptodatapy.extract.datarequest import DataRequest
from cryptodatapy.transform.convertparams import ConvertParams
from cryptodatapy.transform.wrangle import WrangleData, WrangleInfo
from cryptodatapy.util.datacredentials import DataCredentials

# data credentials
data_cred = DataCredentials()
# CoinMetrics community API client:
client = CoinMetricsClient()


class CoinMetrics(DataVendor):
    """
    Retrieves data from Coin Metrics Python client API v4.
    """

    def __init__(
            self,
            categories=None,
            exchanges: Optional[List[str]] = None,
            indexes: Optional[List[str]] = None,
            assets: Optional[List[str]] = None,
            markets: Optional[List[str]] = None,
            market_types=None,
            fields: Optional[List[str]] = None,
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
        categories: list or str, {'crypto', 'fx', 'rates', 'eqty', 'commodities', 'credit', 'macro', 'alt'}
            List or string of available categories, e.g. ['crypto', 'fx', 'alt'].
        exchanges: list, optional, default None
            List of available exchanges, e.g. ['Binance', 'Coinbase', 'Kraken', 'FTX', ...].
        indexes: list, optional, default None
            List of available indexes, e.g. ['mvda', 'bvin'].
        assets: list, optional, default None
            List of available assets, e.g. ['ftx': 'btc', 'eth', ...]
        markets: list, optional, default None
            List of available markets as base asset/quote currency pairs, e.g. [btcusdt', 'ethbtc', ...].
        market_types: list
            List of available market types/contracts, e.g. [spot', 'perpetual_futures', 'futures', 'options']
        fields: list, optional, default None
            List of available fields, e.g. ['open', 'high', 'low', 'close', 'volume'].
        frequencies: list
            List of available frequencies, e.g. ['tick', '1min', '5min', '10min', '15min', '30min', '1h', '2h', '4h',
        base_url: str, optional, default None
            Base url used for GET requests. If not provided, default is set to base_url stored in DataCredentials.
        api_key: str, optional, default None
            Api key, e.g. 'dcf13983adf7dfa79a0dfa35adf'. If not provided, default is set to
            api_key stored in DataCredentials.
        max_obs_per_call: int, optional, default None
            Maximum number of observations returned per API call. If not provided, default is set to
            api_limit stored in DataCredentials.
        rate_limit: Any, optional, Default None
            Number of API calls made and left, by time frequency.
        """
        DataVendor.__init__(
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
            self.frequencies = [
                "tick",
                "block",
                "1s",
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
            ]
        if market_types is None:
            self.market_types = ["spot", "perpetual_future", "future", "option"]
        if categories is None:
            self.categories = ["crypto"]
        if exchanges is None:
            self._exchanges = self.get_exchanges_info(as_list=True)
        if indexes is None:
            self._indexes = self.get_indexes_info(as_list=True)
        if assets is None:
            self._assets = self.get_assets_info(as_list=True)
        if markets is None:
            self._markets = self.get_markets_info(as_list=True)
        if fields is None:
            self._fields = self.get_fields_info(data_type=None, as_list=True)

    @staticmethod
    def req_meta(data_type: str) -> Dict[str, Any]:
        """
        Request metadata.

        Parameters
        ----------
        data_type: str, {'catalog_exchanges', 'catalog_indexes', 'catalog_assets', 'catalog_institutions',
                         'catalog_markets', 'catalog_metrics' }
            Type of data to request metadata for.

        Returns
        -------
        meta: Any
            Object with metadata.
        """
        try:
            meta = getattr(client, data_type)()

        except AssertionError as e:
            logging.warning(e)
            logging.warning(f"Failed to get metadata for {data_type}.")

        else:
            return meta

    def get_exchanges_info(self, as_list: bool = False) -> Union[List[str], pd.DataFrame]:
        """
        Get exchanges info.

        Parameters
        ----------
        as_list: bool, default False
            Returns exchanges info as list.

        Returns
        -------
        exch: list or pd.DataFrame
            List or dataframe with info on supported exchanges.
        """
        # req data
        exch = self.req_meta(data_type='catalog_exchanges')
        # wrangle data resp
        exch = WrangleInfo(exch).cm_meta_resp(as_list=as_list, index_name='exchange')

        return exch

    def get_indexes_info(self, as_list: bool = False) -> Union[List[str], pd.DataFrame]:
        """
        Get indexes info.

        Parameters
        ----------
        as_list: bool, default False
            Returns indexes info as list.

        Returns
        -------
        indexes: list or pd.DataFrame
            List or dataframe with info on available indexes.
        """
        # req data
        indexes = self.req_meta(data_type='catalog_indexes')
        # wrangle data resp
        indexes = WrangleInfo(indexes).cm_meta_resp(as_list=as_list, index_name='ticker')

        return indexes

    def get_assets_info(self, as_list: bool = False, ) -> Union[List[str], pd.DataFrame]:
        """
        Get assets info.

        Parameters
        ----------
        as_list: bool, default False
            Returns assets info as a list.

        Returns
        -------
        assets: list or pd.DataFrame
            List or dataframe with info on available assets.
        """
        # req data
        assets = self.req_meta(data_type='catalog_assets')
        # wrangle data resp
        assets = WrangleInfo(assets).cm_meta_resp(as_list=as_list, index_name='ticker')

        return assets

    def get_inst_info(self, as_dict: bool = False) -> Union[Dict[str, List[str]], pd.DataFrame]:
        """
        Get institutions info.

        Parameters
        ----------
        as_dict: bool, default False
            Returns available institutions as dictionary.

        Returns
        -------
        inst: dictionary or pd.DataFrame
            Dictionary or dataframe with info on available institutions.
        """
        # req data
        inst = self.req_meta(data_type='catalog_institutions')
        # wrangle data resp
        inst = WrangleInfo(inst).cm_inst_info(as_dict=as_dict)

        return inst

    def get_markets_info(self, as_list: bool = False) -> Union[List[str], pd.DataFrame]:
        """
        Get markets info.

        Parameters
        ----------
        as_list: bool, default False
            Returns markets info as dict with exchange-markets key-values pair.

        Returns
        -------
        mkts: list or pd.DataFrame
            List or dataframe with info on available markets, by exchange.
        """
        # req data
        mkts = self.req_meta(data_type='catalog_markets')
        # wrangle data resp
        mkts = WrangleInfo(mkts).cm_meta_resp(as_list=as_list)

        return mkts

    def get_onchain_fields_info(self, as_list: bool = False) -> Union[List[str], pd.DataFrame]:
        """
        Get on-chain fields info.

        Parameters
        ----------
        as_list: bool, default False
            Returns on-chain fields as list.

        Returns
        -------
        onchain_fields: list or pd.DataFrame
            List or dataframe of on-chain info.
        """
        # req data
        onchain_fields = self.req_meta(data_type='catalog_metrics')
        # wrangle data resp
        onchain_fields = WrangleInfo(onchain_fields).cm_meta_resp(as_list=as_list, index_name='fields')

        return onchain_fields

    def get_fields_info(self, data_type: Optional[str] = None, as_list: bool = False) -> Union[List[str], pd.DataFrame]:
        """
        Get fields info. Can be filtered by data type.

        Parameters
        ----------
        data_type: str, optional, {'market', 'on-chain', 'off-chain'}, default None
            Type of data.
        as_list: bool, default False
            Returns available fields as list.

        Returns
        -------
        fields: list or pd.DataFrame
            List or dataframe with info on available fields.
        """
        # req data
        ohlcv_fields = ['price_open', 'price_close', 'price_high', 'price_low', 'vwap', 'volume', 'candle_usd_volume',
                        'candle_trades_count']  # get market fields
        inst_fields = list(self.get_inst_info(as_dict=True).values())[0]  # inst fields
        onchain_fields = self.get_onchain_fields_info()  # get onchain fields

        # fields df
        if data_type == "market":
            fields = onchain_fields[onchain_fields.category == "Market"]
        elif data_type == "off-chain":
            fields = inst_fields
        else:
            fields = onchain_fields

        # fields list
        if as_list:
            if data_type == "market":
                fields = ohlcv_fields + list(fields.index)
            elif data_type == "on-chain":
                fields = list(fields.index)
            elif data_type == "off-chain":
                fields = inst_fields
            else:
                fields = ohlcv_fields + list(fields.index) + inst_fields

        return fields

    def get_onchain_tickers_list(self, data_req: DataRequest) -> List[str]:
        """
        Get list of available assets for fields in data request.

        Parameters
        ----------
        data_req: DataRequest
            Data request object with 'fields' parameter.

        Returns
        -------
        asset_list: list
            List of available assets for selected fields.
        """
        # convert data request parameters to Coin Metrics format
        cm_data_req = ConvertParams(data_req).to_coinmetrics()
        # fields param
        fields = cm_data_req["fields"]

        # fields dict
        fields_dict = {}
        for field in fields:
            df = self.get_fields_info().loc[field]  # get fields info
            # add to dict
            fields_dict[field] = df["frequencies"][0]["assets"]

        # asset list
        asset_list = list(set.intersection(*(set(val) for val in fields_dict.values())))

        # return asset list if dict not empty
        if len(fields_dict) != 0:
            return asset_list
        else:
            raise Exception("No fields were found. Check available fields and try again.")

    def get_rate_limit_info(self) -> None:
        """
        Get rate limit info.
        """
        return None

    @staticmethod
    def req_data(data_type: str, **kwargs) -> pd.DataFrame:
        """
        Sends data request to Python client.

        Parameters
        ----------
        data_type: str, {'get_index_levels', 'get_institution_metrics', 'get_market_candles', 'get_asset_metrics',
                         'get_market_open_interest', 'get_market_funding_rates', 'get_market_trades',
                         'get_market_quotes'}
            Data type to retrieve.

        Other Parameters
        ----------------
        indexes: list
            List of indexes.
        assets: list
            List of assets.
        markets: list
            List of markets.
        metrics: list
            List of metrics.
        frequency: str
            Frequency of data observations.
        start_time: str, pd.Timestamp, datetime
            Start datetime.
        end_time: str, pd.Timestamp, datetime
            End datetime.
        timezone: str
            Timezone.

        Returns
        -------
        df: pd.DataFrame
            Dataframe with datetime, ticker/identifier, and field/col values.
        """
        try:
            df = getattr(client, data_type)(**kwargs).to_dataframe()
            assert not df.empty

        except Exception as e:
            logging.warning(f"Failed to {data_type}.")
            logging.warning(e)

        else:
            return df

    @staticmethod
    def wrangle_data_resp(data_req: DataRequest, data_resp: pd.DataFrame()):
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
        # wrangle data resp
        df = WrangleData(data_req, data_resp).coinmetrics()

        return df

    def get_tidy_data(self, data_req: DataRequest, data_type: str, **kwargs) -> pd.DataFrame:
        """
        Gets data and wrangles it into tidy data format.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.
        data_type: str, {'get_index_levels', 'get_institution_metrics', 'get_market_candles', 'get_asset_metrics',
                         'get_market_open_interest', 'get_market_funding_rates', 'get_market_trades',
                         'get_market_quotes'}
            Data type to retrieve.
        **kwargs: other parameters

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            Dataframe with DatetimeIndex (level 0), ticker (level 1) and values for fields/col, in tidy data format.
        """
        # get entire data history
        df = self.req_data(data_type, **kwargs)
        # wrangle df
        df = self.wrangle_data_resp(data_req, df)

        return df

    def filter_tickers(self, data_req: DataRequest, data_type: str) -> List[str]:
        """
        Filters tickers to only those with data available.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.
        data_type: str, {'indexes', 'institutions', market_candles', 'asset_metrics', 'open_interest', 'funding_rates',
                         'trades', quotes'}
            Data type to retrieve.

        Returns
        -------
        tickers: list
            List of filtered tickers.
        """
        # convert params
        cm_data_req = ConvertParams(data_req).to_coinmetrics()
        # tickers
        tickers = []

        # check if tickers indexes
        if data_type == 'indexes':
            idx_list, tickers = self.indexes, []
            for ticker in cm_data_req["tickers"]:
                if ticker.upper() in idx_list:
                    tickers.append(ticker)  # keep only avail tickers

        # check if tickers assets
        elif data_type == 'market_candles' or data_type == 'open_interest' or \
                data_type == 'funding_rates' or data_type == 'trades' or data_type == 'quotes':
            asset_list, tickers = self.assets, []
            for asset, ticker in zip(cm_data_req["tickers"], cm_data_req["mkts"]):
                if asset in asset_list:
                    tickers.append(ticker)  # keep only avail asset tickers

        # check if tickers assets
        elif data_type == 'asset_metrics':
            asset_list, tickers = self.assets, []
            for ticker in cm_data_req["tickers"]:
                if ticker in asset_list:
                    tickers.append(ticker)  # keep only asset tickers

        # raise error if no tickers are indexes
        if len(tickers) == 0:
            raise ValueError(
                f"{data_req.tickers} are not valid tickers for requested data type."
                f" Use attributes to get a list of available indexes and assets."
            )

        return tickers

    def filter_fields(self, data_req: DataRequest, data_type: str) -> List[str]:
        """
        Filters fields to only those with data available.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.
        data_type: str, {'indexes', 'institutions', 'market_candles', 'asset_metrics', 'open_interest',
                        'funding_rates', 'trades', quotes'}
            Data type to retrieve.

        Returns
        -------
        fields: list
            List of filtered fields.
        """
        # convert params
        cm_data_req = ConvertParams(data_req).to_coinmetrics()
        # fields
        fields = []

        # check if fields inst
        if data_type == 'institutions':
            fields, inst_list = ([], list(self.get_inst_info(as_dict=True).values())[0])
            for field in cm_data_req["fields"]:
                if field in inst_list:
                    fields.append(field)  # keep only inst fields

        elif data_type == 'asset_metrics':
            onchain_list, fields = (self.get_fields_info(data_type="on-chain", as_list=True), [])
            for field in cm_data_req["fields"]:
                if field in onchain_list:
                    fields.append(field)  # keep only on-chain fields

        # raise error if fields is empty
        if len(fields) == 0:
            raise ValueError(
                f"{data_req.fields} are not valid institution fields."
                f" Use the fields property to get a list of available source fields."
            )

        return fields

    @staticmethod
    def check_params(data_req: DataRequest, data_type: str) -> None:
        """
        Checks if valid parameters for request.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.
        data_type: str, {'indexes', 'institutions', 'market_candles', 'asset_metrics', 'open_interest', 'funding_rates',
                         'trades', quotes'}
            Data type to retrieve.

        """
        # convert params
        cm_data_req = ConvertParams(data_req).to_coinmetrics()

        # indexes
        if data_type == 'indexes':
            if cm_data_req["freq"] not in ["1h", "1d"]:
                raise ValueError(
                    f"Indexes data is only available for hourly, daily, weekly, monthly and quarterly"
                    f" frequencies. Change data request frequency and try again."
                )

        # institutions
        elif data_type == 'institutions':
            if cm_data_req["freq"] != "1d":
                raise ValueError(
                    f"Institutions data is only available for daily frequency."
                    f" Change data request frequency and try again."
                )

        # ohlcv
        elif data_type == 'market_candles':
            if cm_data_req["freq"] not in ["1m", "1h", "1d"]:
                raise ValueError(
                    f"OHLCV data is only available for minute, hourly, daily, weekly, monthly and quarterly"
                    f" frequencies. Change data request frequency and try again."
                )

        # on-chain
        elif data_type == 'asset_metrics':
            if cm_data_req["freq"] not in ["1b", "1d"]:
                raise ValueError(
                    f"On-chain data is only available for 'block' and 'd' frequencies."
                    f" Change data request frequency and try again."
                )

        # funding rate
        elif data_type == 'funding_rates':
            if data_req.mkt_type not in ["perpetual_future", "future", "option"]:
                raise ValueError(
                    f"Funding rates are only available for 'perpetual_future', 'future' and"
                    f" 'option' market types. Change 'mkt_type' in data request and try again."
                )

        elif data_type == 'open_interest':
            if data_req.mkt_type not in ["perpetual_future", "future", "option"]:
                raise ValueError(
                    f"Open interest is only available for 'perpetual_future', 'future' and"
                    f" 'option' market types. Change 'mkt_type' in data request and try again."
                )

        # trades & quotes
        elif data_type == 'trades' or data_type == 'quotes':
            if cm_data_req["freq"] != "tick":
                raise ValueError(
                    f"{data_type} data is only available at the 'tick' frequency."
                    f" Change data request frequency and try again."
                )

        return None

    def get_indexes(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get indexes data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame
            DataFrame with DatetimeIndex (level 0), tickers (level 1) and index values (cols).
        """
        # convert data request parameters to Coin Metrics format
        cm_data_req = ConvertParams(data_req).to_coinmetrics()

        # check freq
        self.check_params(data_req, data_type='indexes')

        # filter tickers
        tickers = self.filter_tickers(data_req, data_type='indexes')

        # get indexes
        df = self.get_tidy_data(data_req,
                                data_type='get_index_levels',
                                indexes=tickers,
                                frequency=cm_data_req["freq"],
                                start_time=cm_data_req["start_date"],
                                end_time=cm_data_req["end_date"],
                                )

        return df

    def get_institutions(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get institutions data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame
            DataFrame with DatetimeIndex (level 0), tickers (level 1) and institution fields values (cols).
        """
        # convert data request parameters to Coin Metrics format
        cm_data_req = ConvertParams(data_req).to_coinmetrics()

        # check freq
        self.check_params(data_req, data_type='institutions')

        # filter fields
        fields = self.filter_fields(data_req, data_type='institutions')

        # get tidy data
        df = self.get_tidy_data(data_req,
                                data_type='get_institution_metrics',
                                institutions=cm_data_req["inst"],
                                metrics=fields,
                                frequency=cm_data_req["freq"],
                                start_time=cm_data_req["start_date"],
                                end_time=cm_data_req["end_date"],
                                )

        return df

    def get_ohlcv(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get OHLCV (candles) data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and OHLCV values (cols).
        """
        # convert data request parameters to Coin Metrics format
        cm_data_req = ConvertParams(data_req).to_coinmetrics()

        # check freq
        self.check_params(data_req, data_type='market_candles')

        # filter tickers
        tickers = self.filter_tickers(data_req, data_type='market_candles')

        # get tidy data
        df = self.get_tidy_data(data_req,
                                data_type='get_market_candles',
                                markets=tickers,
                                frequency=cm_data_req["freq"],
                                start_time=cm_data_req["start_date"],
                                end_time=cm_data_req["end_date"],
                                )

        return df

    def get_onchain(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get on-chain data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and on-chain values (cols).
        """
        # convert data request parameters to Coin Metrics format
        cm_data_req = ConvertParams(data_req).to_coinmetrics()

        # check freq
        self.check_params(data_req, data_type='asset_metrics')

        # filter tickers
        tickers = self.filter_tickers(data_req, data_type='asset_metrics')
        # filter fields
        fields = self.filter_fields(data_req, data_type='asset_metrics')

        # get tidy data
        df = self.get_tidy_data(data_req,
                                data_type='get_asset_metrics',
                                assets=tickers,
                                metrics=fields,
                                frequency=cm_data_req["freq"],
                                start_time=cm_data_req["start_date"],
                                end_time=cm_data_req["end_date"],
                                )

        return df

    def get_open_interest(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get open interest data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and open interest values (cols).
        """
        # convert data request parameters to Coin Metrics format
        cm_data_req = ConvertParams(data_req).to_coinmetrics()

        # check mkt type
        self.check_params(data_req, data_type='open_interest')

        # filter tickers
        tickers = self.filter_tickers(data_req, data_type='open_interest')

        # get indexes
        df = self.get_tidy_data(data_req,
                                data_type='get_market_open_interest',
                                markets=tickers,
                                start_time=cm_data_req["start_date"],
                                end_time=cm_data_req["end_date"],
                                )

        return df

    def get_funding_rates(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get funding rates data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and funding rates values (cols).
        """
        # convert data request parameters to Coin Metrics format
        cm_data_req = ConvertParams(data_req).to_coinmetrics()

        # check mkt type
        self.check_params(data_req, data_type='funding_rates')

        # filter tickers
        tickers = self.filter_tickers(data_req, data_type='funding_rates')

        # get indexes
        df = self.get_tidy_data(data_req,
                                data_type='get_market_funding_rates',
                                markets=tickers,
                                start_time=cm_data_req["start_date"],
                                end_time=cm_data_req["end_date"],
                                )

        return df

    def get_trades(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get trades (transactions) data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and bid/ask price and size values (cols).
        """
        # convert data request parameters to Coin Metrics format
        cm_data_req = ConvertParams(data_req).to_coinmetrics()

        # check mkt type
        self.check_params(data_req, data_type='trades')

        # filter tickers
        tickers = self.filter_tickers(data_req, data_type='trades')

        # get indexes
        df = self.get_tidy_data(data_req,
                                data_type='get_market_trades',
                                markets=tickers,
                                start_time=cm_data_req["start_date"]
                                )

        return df

    def get_quotes(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get quotes (order book) data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1), and bid/ask price and size values (cols).
        """
        # convert data request parameters to Coin Metrics format
        cm_data_req = ConvertParams(data_req).to_coinmetrics()

        # check mkt type
        self.check_params(data_req, data_type='quotes')

        # filter tickers
        tickers = self.filter_tickers(data_req, data_type='quotes')

        # get indexes
        df = self.get_tidy_data(data_req,
                                data_type='get_market_quotes',
                                markets=tickers,
                                start_time=cm_data_req["start_date"]
                                )

        return df

    def get_data(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get market, on-chain and/or off-chain data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1) and values for market, on-chain and/or off-chain
            fields (cols), in tidy format.
        """
        # convert data request parameters to Coin Metrics format
        cm_data_req = ConvertParams(data_req).to_coinmetrics()

        # check if fields available
        if not all([field in self.fields for field in cm_data_req["fields"]]):
            raise ValueError(
                "Some selected fields are not available. Check available fields with"
                " fields property and try again."
            )

        # fields list
        ohlcv_list = ['price_open', 'price_close', 'price_high', 'price_low', 'vwap', 'volume',
                      'candle_usd_volume', 'candle_trades_count']
        oc_list = [field for field in self.fields if field not in ohlcv_list]
        # empty df
        df = pd.DataFrame()

        # get indexes data
        if any([ticker.upper() in self.indexes for ticker in cm_data_req["tickers"]]) and any(
                [field in ohlcv_list for field in cm_data_req["fields"]]
        ):
            df0 = self.get_indexes(data_req)
            df = pd.concat([df, df0])

        # get OHLCV data
        if any([ticker in self.assets for ticker in cm_data_req["tickers"]]) and any(
                [field in ohlcv_list for field in cm_data_req["fields"]]
        ):
            df1 = self.get_ohlcv(data_req)
            df = pd.concat([df, df1])

        # get on-chain data
        if any([ticker in self.assets for ticker in cm_data_req["tickers"]]) and any(
                [field in oc_list for field in cm_data_req["fields"]]
        ):
            df2 = self.get_onchain(data_req)
            df = pd.concat([df, df2], axis=1)

        # check if df empty
        if df.empty:
            raise Exception("No data returned."
                            " Check data request parameters and try again.")

        # filter df for desired fields and sort index by date
        fields = [field for field in data_req.fields if field in df.columns]
        df = df.loc[:, fields]

        return df.sort_index()
