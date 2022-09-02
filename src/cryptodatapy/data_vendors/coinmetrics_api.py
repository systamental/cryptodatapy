import pandas as pd
import numpy as np
import logging
import requests
from datetime import datetime, timedelta
from typing import Optional, Union, Any
from cryptodatapy.util.datacredentials import DataCredentials
from cryptodatapy.data_requests.datarequest import DataRequest
from cryptodatapy.util.convertparams import ConvertParams
from cryptodatapy.data_vendors.datavendor import DataVendor
from coinmetrics.api_client import CoinMetricsClient


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
            source_type: str = 'data_vendor',
            categories: list[str] = ['crypto'],
            exchanges: Optional[list[str]] = None,
            indexes: Optional[list[str]] = None,
            assets: Optional[list[str]] = None,
            markets: Optional[list[str]] = None,
            market_types: list[str] = ['spot', 'perpetual_future', 'future', 'option'],
            fields: Optional[list[str]] = None,
            frequencies: list[str] = ['tick', 'block', '1s', '1min', '5min', '10min', '15min', '30min', '1h', '2h',
                                      '4h', '8h', 'd', 'w', 'm', 'q'],
            base_url: Optional[str] = None,
            api_key: Optional[str] = None,
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
        DataVendor.__init__(self, source_type, categories, exchanges, indexes, assets, markets, market_types, fields,
                            frequencies, base_url, api_key, max_obs_per_call, rate_limit)

        self._exchanges = self.get_exchanges_info(as_list=True)
        self._indexes = self.get_indexes_info(as_list=True)
        self._assets = self.get_assets_info(as_list=True)
        self._markets = self.get_markets_info(as_list=True)
        self._fields = self.get_fields_info(data_type=None, as_list=True)
        self._rate_limit = self.get_rate_limit_info()

    @staticmethod
    def get_exchanges_info(as_list: bool = False) -> Union[list[str], pd.DataFrame]:
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
        try:
            exch_dict = client.catalog_exchanges()  # get exchanges info

        except AssertionError as e:
            logging.warning(e)
            logging.warning("Failed to get exchanges info.")

        else:
            # format response
            exch = pd.DataFrame(exch_dict)  # store in df
            exch.set_index(exch.columns[0], inplace=True)  # set index
            # markets list
            if as_list:
                exch = list(exch.index)
            return exch

    @staticmethod
    def get_indexes_info(as_list: bool = False) -> Union[list[str], pd.DataFrame]:
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
        try:
            idx_dict = client.catalog_indexes()  # get indexes info

        except AssertionError as e:
            logging.warning(e)
            logging.warning("Failed to get indexes info.")

        else:
            # format response
            indexes = pd.DataFrame(idx_dict)  # store in df
            indexes.set_index(indexes.columns[0], inplace=True)  # set index
            indexes.index.name = 'ticker'  # change index col to ticker
            # indexes list
            if as_list:
                indexes = list(indexes.index)

            return indexes

    @staticmethod
    def get_assets_info(as_list: bool = False) -> Union[list[str], pd.DataFrame]:
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
        try:
            assets_dict = client.catalog_assets()  # get asset info

        except AssertionError as e:
            logging.warning(e)
            logging.warning("Failed to get assets info.")

        else:
            # format response
            assets = pd.DataFrame(assets_dict)  # store dict in df
            assets.set_index(assets.columns[0], inplace=True)  # set index
            assets.index.name = 'ticker'  # change asset col to ticker
            # asset list
            if as_list:
                assets = list(assets.index)

            return assets

    @staticmethod
    def get_institutions_info(as_dict: bool = False) -> Union[dict[str], pd.DataFrame]:
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
        try:
            inst = client.catalog_institutions()  # get institutions info

        except AssertionError as e:
            logging.warning(e)
            logging.warning("Failed to get institutions info.")

        else:
            # format response
            inst = pd.DataFrame(inst)  # store in df
            # indexes list
            if as_dict:
                inst_dict = {}
                for institution in inst.institution:
                    metrics_list = []
                    for metrics in inst.metrics[0]:
                        metrics_list.append(metrics['metric'])
                    inst_dict[institution] = metrics_list
                return inst_dict
            else:
                return inst

    @staticmethod
    def get_markets_info(as_list: bool = False) -> Union[list[str], pd.DataFrame]:
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
        try:
            mkts_dict = client.catalog_markets()  # get indexes info

        except AssertionError as e:
            logging.warning(e)
            logging.warning("Failed to get markets info.")

        else:
            # format response
            mkts = pd.DataFrame(mkts_dict)  # store in df
            mkts.set_index(mkts.columns[0], inplace=True)  # set index
            # markets list
            if as_list:
                mkts = list(mkts.index)

            return mkts

    def get_fields_info(self, data_type: Optional[str] = None, as_list: bool = False) -> Union[list[str], pd.DataFrame]:
        """
        Get fields info.

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
        try:  # gather all fields
            inst_list = list(self.get_institutions_info(as_dict=True).values())[0]  # inst fields
            ohlcv_list = list(client.get_market_candles(markets=['binance-btc-usdt-spot']).to_dataframe().columns)
            metrics_dict = client.catalog_metrics()  # get all metrics info

        except AssertionError as e:
            logging.warning(e)
            logging.warning("Failed to get fields info.")

        else:
            fields = pd.DataFrame(metrics_dict)
            fields.set_index(fields.columns[0], inplace=True)  # set index
            fields.index.name = 'fields'  # change metrics col to fields
            # fields info
            if data_type == 'market':
                fields = fields[fields.category == 'Market']
            elif data_type == 'off-chain':
                fields = inst_list
            else:
                fields = fields

            # fields list
            if as_list:
                if data_type == 'market':
                    fields = ohlcv_list + list(fields.index)
                elif data_type == 'on-chain':
                    fields = list(fields.index)
                elif data_type == 'off-chain':
                    fields = inst_list
                else:
                    fields = ohlcv_list + list(fields.index) + inst_list

            return fields

    def get_avail_assets_info(self, data_req: DataRequest = None) -> list[str]:
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
        # raise error if data req is None
        if data_req is None:
            raise TypeError("Provide a data request with 'fields' parameter.")
        # convert data request parameters to Coin Metrics format
        cm_data_req = ConvertParams(data_source='coinmetrics').convert_to_source(data_req)
        # fields param
        fields = cm_data_req['fields']

        # fields dict
        fields_dict = {}
        for field in fields:
            try:
                df = self.get_fields_info(data_type=None).loc[field]  # get fields info
            except KeyError as e:
                logging.warning(e)
                logging.warning(f"{field} is not an on-chain metric. Check available fields and try again.")
            else:
                # add to dict
                fields_dict[field] = df['frequencies'][0]['assets']

        # asset list
        asset_list = list(set.intersection(*(set(val) for val in fields_dict.values())))
        # return asset list if dict not empty
        if len(fields_dict) != 0:
            return asset_list
        else:
            raise Exception('No fields were found. Check available fields and try again.')

    @staticmethod
    def get_rate_limit_info():
        """
        Get rate limit info.
        """
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
        cm_data_req = ConvertParams(data_source='coinmetrics').convert_to_source(data_req)

        # check if avaliable frequency
        if cm_data_req['freq'] not in ['1h', '1d']:
            raise ValueError(f"Indexes data is only available for hourly, daily, weekly, monthly and quarterly"
                             f" frequencies. Change data request frequency and try again.")

        # check if tickers are indexes
        idx_list, tickers = self.indexes, []
        for ticker in cm_data_req['tickers']:
            if ticker.upper() in idx_list:
                tickers.append(ticker)  # keep only index tickers
            else:
                pass
        # raise error if all tickers are assets
        if len(tickers) == 0:
            raise ValueError(f"{cm_data_req['tickers']} are not valid indexes."
                             f" Use indexes property to get a list of available indexes.")

        try:
            df = client.get_index_levels(indexes=tickers, frequency=cm_data_req['freq'],
                                         start_time=cm_data_req['start_date'], end_time=cm_data_req['end_date'],
                                         timezone=data_req.tz).to_dataframe()

        except Exception as e:
            logging.warning(e)
            logging.warning(f"Failed to pull data for {tickers}.")

        else:
            # wrangle data resp
            df = self.wrangle_data_resp(data_req, df)

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
        cm_data_req = ConvertParams(data_source='coinmetrics').convert_to_source(data_req)

        # check if avaliable frequency
        if cm_data_req['freq'] != '1d':
            raise ValueError(f"Institutions data is only available for daily frequency."
                             f" Change data request frequency and try again.")

        # check if fields are valid fields
        fields_list, inst_list = [], list(self.get_institutions_info(as_dict=True).values())[0]
        for field in cm_data_req['fields']:
            if field in inst_list:
                fields_list.append(field)  # keep only on-chain fields
            else:
                pass
        # raise error if fields is empty
        if len(fields_list) == 0:
            raise ValueError(f"{cm_data_req['fields']} are not valid institution fields."
                             f" Use the fields property to get a list of available source fields.")

        try:
            df = client.get_institution_metrics(institutions=cm_data_req['inst'], metrics=cm_data_req['fields'],
                                                frequency=cm_data_req['freq'], start_time=cm_data_req['start_date'],
                                                end_time=cm_data_req['end_date'], timezone=data_req.tz).to_dataframe()

        except Exception as e:
            logging.warning(e)
            logging.warning(f"Failed to pull data for {tickers}.")

        else:
            # wrangle data resp
            df = self.wrangle_data_resp(data_req, df)

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
        cm_data_req = ConvertParams(data_source='coinmetrics').convert_to_source(data_req)

        # check if available frequency
        if cm_data_req['freq'] not in ['1m', '1h', '1d']:
            raise ValueError(f"OHLCV data is only available for minute, hourly, daily, weekly, monthly and quarterly"
                             f" frequencies. Change data request frequency and try again.")

        # check if tickers are assets
        asset_list, mkts = self.assets, []
        for ticker, mkt in zip(cm_data_req['tickers'], cm_data_req['mkts']):
            if ticker in asset_list:
                mkts.append(mkt)  # keep only asset tickers
            else:
                pass
        # raise error if all tickers are indexes
        if len(mkts) == 0:
            raise ValueError(f"{cm_data_req['tickers']} are not valid assets."
                             f" Use the assets property to get a list of available assets.")

        try:  # fetch OHLCV data
            df = client.get_market_candles(markets=mkts, frequency=cm_data_req['freq'],
                                           start_time=cm_data_req['start_date'], end_time=cm_data_req['end_date'],
                                           timezone=data_req.tz).to_dataframe()

        except Exception as e:
            logging.warning(e)
            logging.warning(f"Failed to pull data for {cm_data_req['tickers']}.")

        else:
            # wrangle data resp
            df = self.wrangle_data_resp(data_req, df)

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
        cm_data_req = ConvertParams(data_source='coinmetrics').convert_to_source(data_req)

        # check if valid on-chain frequency
        if cm_data_req['freq'] not in ['1b', '1d']:
            raise ValueError(f"On-chain data is only available for 'block' and 'd' frequencies."
                             f" Change data request frequency and try again.")

        # check if fields are on-chain fields
        onchain_list, fields_list = self.get_fields_info(data_type='on-chain', as_list=True), []
        for field in cm_data_req['fields']:
            if field in onchain_list:
                fields_list.append(field)  # keep only on-chain fields
            else:
                pass
        # raise error if fields is empty
        if len(fields_list) == 0:
            raise ValueError(f"{cm_data_req['fields']} are not valid on-chain fields."
                             f" Use the get_fiels_info(data_type='on-chain') method to get info"
                             f" on available fields.")

        # check if tickers are assets
        asset_list, tickers = self.assets, []
        for ticker in cm_data_req['tickers']:
            if ticker in asset_list:
                tickers.append(ticker)  # keep only asset tickers
            else:
                pass
        # raise error if all tickers are indexes
        if len(tickers) == 0:
            raise ValueError(f"{cm_data_req['tickers']} are not valid assets. Use the assets property"
                             f" to get info on available assets.")

        try:  # fetch on-chain data
            df = client.get_asset_metrics(assets=tickers, metrics=fields_list, frequency=cm_data_req['freq'],
                                          start_time=cm_data_req['start_date'], end_time=cm_data_req['end_date'],
                                          timezone=data_req.tz).to_dataframe()

        except Exception as e:
            logging.warning(e)
            logging.warning(f"Failed to pull data for {fields_list}.")

        else:
            # wrangle data resp
            df = self.wrangle_data_resp(data_req, df)

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
        cm_data_req = ConvertParams(data_source='coinmetrics').convert_to_source(data_req)

        # check market type
        if data_req.mkt_type not in ['perpetual_future', 'future', 'option']:
            raise ValueError(f"Open interest is only available for 'perpetual_future', 'future' and"
                             f" 'option' market types. Change 'mkt_type' in data request and try again.")

        # check if tickers are assets
        asset_list, tickers = self.assets, []
        for ticker in cm_data_req['tickers']:
            if ticker in asset_list:
                tickers.append(ticker)  # keep only asset tickers
            else:
                pass
        # raise error if all tickers are indexes
        if len(tickers) == 0:
            raise ValueError(f"{cm_data_req['tickers']} are not valid assets."
                             f" Use the assets property to get a list of available assets.")

        try:  # fetch open interest data
            df = client.get_market_open_interest(markets=cm_data_req['mkts'], start_time=cm_data_req['start_date'],
                                                 end_time=cm_data_req['end_date'], timezone=data_req.tz).to_dataframe()

        except Exception as e:
            logging.warning(e)
            logging.warning(f"Failed to pull data for {cm_data_req['tickers']}.")

        else:
            # wrangle data resp
            df = self.wrangle_data_resp(data_req, df)

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
        cm_data_req = ConvertParams(data_source='coinmetrics').convert_to_source(data_req)

        # check market type
        if data_req.mkt_type not in ['perpetual_future', 'future', 'option']:
            raise ValueError(f"Funding rates are only available for 'perpetual_future', 'future' and"
                             f" 'option' market types. Change 'mkt_type' in data request and try again.")

        # check if tickers are assets
        asset_list, tickers = self.assets, []
        for ticker in cm_data_req['tickers']:
            if ticker in asset_list:
                tickers.append(ticker)  # keep only asset tickers
            else:
                pass
        # raise error if all tickers are indexes
        if len(tickers) == 0:
            raise ValueError(f"{cm_data_req['tickers']} are not valid assets."
                             f" Use the assets property to get a list of available assets.")

        try:  # fetch open interest data
            df = client.get_market_funding_rates(markets=cm_data_req['mkts'], start_time=cm_data_req['start_date'],
                                                 end_time=cm_data_req['end_date'], timezone=data_req.tz).to_dataframe()

        except Exception as e:
            logging.warning(e)
            logging.warning(f"Failed to pull data for {cm_data_req['tickers']}.")

        else:
            # wrangle data resp
            df = self.wrangle_data_resp(data_req, df)

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
        cm_data_req = ConvertParams(data_source='coinmetrics').convert_to_source(data_req)

        # check if available frequency
        if cm_data_req['freq'] != 'tick':
            raise ValueError(f"Trades data is only available at the 'tick' frequency."
                             f" Change data request frequency and try again.")

        # check if tickers are assets
        asset_list, tickers = self.assets, []
        for ticker in cm_data_req['tickers']:
            if ticker in asset_list:
                tickers.append(ticker)  # keep only asset tickers
            else:
                pass
        # raise error if all tickers are indexes
        if len(tickers) == 0:
            raise ValueError(f"{cm_data_req['tickers']} are not valid assets."
                             f" Use the assets property to get a list of available assets.")

        try:  # fetch quote data
            df = client.get_market_trades(markets=cm_data_req['mkts'],
                                          start_time=cm_data_req['start_date']).to_dataframe()

        except Exception as e:
            logging.warning(e)
            logging.warning(f"Failed to pull data for {cm_data_req['tickers']}.")

        else:
            # wrangle data resp
            df = self.wrangle_data_resp(data_req, df)

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
        cm_data_req = ConvertParams(data_source='coinmetrics').convert_to_source(data_req)

        # check if available frequency
        if cm_data_req['freq'] != 'tick':
            raise ValueError(f"Quote data is only available at the 'tick' frequency."
                             f" Change data request frequency and try again.")

        # check if tickers are assets
        asset_list, tickers = self.assets, []
        for ticker in cm_data_req['tickers']:
            if ticker in asset_list:
                tickers.append(ticker)  # keep only asset tickers
            else:
                pass
        # raise error if all tickers are indexes
        if len(tickers) == 0:
            raise ValueError(f"{cm_data_req['tickers']} are not valid assets."
                             f" Use the assets property to get a list of available assets.")

        try:  # fetch quote data
            df = client.get_market_quotes(markets=cm_data_req['mkts'], start_time=cm_data_req['start_date'],
                                          end_time=cm_data_req['end_date'], timezone=data_req.tz).to_dataframe()

        except Exception as e:
            logging.warning(e)
            logging.warning(f"Failed to pull data for {cm_data_req['tickers']}.")

        else:
            # wrangle data resp
            df = self.wrangle_data_resp(data_req, df)

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
        cm_data_req = ConvertParams(data_source='coinmetrics').convert_to_source(data_req)

        # check if fields available
        if not all(i in self.fields for i in cm_data_req['fields']):
            raise ValueError("Fields are not available. Check available fields with"
                             " fields property and try again.")
        # fields list
        ohlcv_list = self.get_fields_info(data_type='market', as_list=True)
        onchain_list = self.get_fields_info(data_type='on-chain', as_list=True)
        # empty df
        df = pd.DataFrame()

        # fetch indexes data
        if any(i.upper() in self.indexes for i in cm_data_req['tickers']) and \
                any(i in ohlcv_list for i in cm_data_req['fields']):
            try:
                df0 = self.get_indexes(data_req)
            except Exception as e:
                logging.warning(e)
            else:
                df = pd.concat([df, df0])

        # fetch OHLCV data
        if any(i in self.assets for i in cm_data_req['tickers']) and \
                any(i in ohlcv_list for i in cm_data_req['fields']):
            try:
                df1 = self.get_ohlcv(data_req)
            except Exception as e:
                logging.warning(e)
            else:
                df = pd.concat([df, df1])

        # fetch on-chain data
        if any(i in self.assets for i in cm_data_req['tickers']) and \
                any(i in onchain_list for i in cm_data_req['fields']):
            try:
                df2 = self.get_onchain(data_req)
            except Exception as e:
                logging.warning(e)
            else:
                df = pd.concat([df, df2], axis=1)

        # check if df empty
        if df.empty:
            raise Exception('No data returned. Check data request parameters and try again.')

        # filter df for desired fields and sort index by date
        fields = [field for field in data_req.fields if field in df.columns]
        df = df.loc[:, fields]

        return df.sort_index()

    @staticmethod
    def wrangle_data_resp(data_req, data_resp: pd.DataFrame()):
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
        # convert data request parameters to Coin Metrics format
        cm_data_req = ConvertParams(data_source='coinmetrics').convert_to_source(data_req)
        # convert cols to cryptodatapy format
        df = ConvertParams(data_source='coinmetrics').convert_fields_to_lib(data_req, data_resp)

        # convert date and set datetimeindex
        df['date'] = pd.to_datetime(df['date'])

        # format ticker
        if 'ticker' in df.columns and all(df.ticker.str.contains('-')):
            df['ticker'] = df.ticker.str.split(pat='-', expand=True)[1].str.upper()  # keep asset ticker only
        if 'ticker' in df.columns and data_req.mkt_type == 'perpetual_future':
            df['ticker'] = df.ticker.str.replace(cm_data_req['quote_ccy'].upper(), '')
        elif 'ticker' in df.columns:
            df['ticker'] = df.ticker.str.upper()

        # reset index
        if 'ticker' in df.columns:
            df = df.set_index(['date', 'ticker']).sort_index()
        elif 'institution' in df.columns:  # inst resp
            df = df.set_index(['date', 'institution']).sort_index()

        # reorder cols
        if 'open' in df.columns:
            df = df.loc[:, ['open', 'high', 'low', 'close', 'volume', 'vwap']]  # reorder cols

        # resample frequency
        if data_req.freq == 'tick':
            pass
        elif 'institution' in df.index.names:
            df = df.groupby([pd.Grouper(level='date', freq=data_req.freq), pd.Grouper(level='institution')]).last()
        else:
            df = df.groupby([pd.Grouper(level='date', freq=data_req.freq), pd.Grouper(level='ticker')]).last()

        # re-format datetimeindex
        if data_req.freq in ['d', 'w', 'm', 'q']:
            df.reset_index(inplace=True)
            df.date = pd.to_datetime(df.date.dt.date)
            # reset index
            if 'institution' in df.columns:  # institution resp
                df = df.set_index(['date', 'institution']).sort_index()
            else:
                df = df.set_index(['date', 'ticker']).sort_index()

        # remove bad data and type cast
        if data_req.freq != 'tick':
            df = df.apply(pd.to_numeric, errors='coerce')  # convert to numeric type
        df = df[df != 0].dropna(how='all')  # 0 values
        df = df[~df.index.duplicated()]  # duplicate rows

        # type conversion
        df = df.apply(pd.to_numeric, errors='ignore').convert_dtypes()

        return df
