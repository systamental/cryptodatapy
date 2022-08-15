import pandas as pd
import numpy as np
import logging
import requests
from datetime import datetime, timedelta
from time import sleep
from importlib import resources
from typing import Optional, Union, Any
from cryptodatapy.util.datacredentials import DataCredentials
from cryptodatapy.util.convertparams import ConvertParams
from cryptodatapy.data_vendors.datavendor import DataVendor
from cryptodatapy.data_requests.datarequest import DataRequest
import investpy


# data credentials
data_cred = DataCredentials()


class InvestPy(DataVendor):
    """
    Retrieves data from InvestPy API.
    """

    def __init__(
            self,
            source_type: str = 'library',
            categories: list[str] = ['fx', 'rates', 'eqty', 'cmdty', 'macro'],
            exchanges: list[str] = None,
            assets: list[str] = None,
            indexes: list[str] = None,
            markets: list[str] = None,
            market_types: list[str] = ['spot', 'future'],
            fields: dict[str, list[str]] = None,
            frequencies: dict[str, list[str]] = {'fx': ['d', 'w', 'm', 'q', 'y'],
                                                 'rates': ['d', 'w', 'm', 'q', 'y'],
                                                 'eqty': ['d', 'w', 'm', 'q', 'y'],
                                                 'cmdty': ['d', 'w', 'm', 'q', 'y'],
                                                 'macro': ['1min', '5min', '10min', '15min', '30min',
                                                           '1h', '2h', '4h', '8h', 'd', 'w', 'm', 'q', 'y']},

            base_url: str = None,
            api_key: str = None,
            max_obs_per_call: int = None,
            rate_limit: Any = None
    ):
        """
        Constructor

        Parameters
        ----------
        source_type: str, {'data_vendor', 'exchange', 'library', 'on-chain', 'web'}
            Type of data source, e.g. 'data_vendor', 'exchange', etc.
        categories: list[str], {'crypto', 'fx', 'rates', 'eqty', 'commodities', 'credit', 'macro', 'alt'}
            List of available categories, e.g. ['crypto', 'fx', 'alt']
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
        exchanges: list
            List of available exchanges, e.g. ['Binance', 'Coinbase', 'Kraken', 'FTX']
        base_url: str
            Cryptocompare base url used in GET requests, e.g. 'https://min-api.cryptocompare.com/data/'
            If not provided, default is set to cryptocompare_base_url stored in DataCredentials
        api_key: str
            Cryptocompare api key, e.g. 'dcf13983adf7dfa79a0dfa35adf'
            If not provided, default is set to cryptocompare_api_key stored in DataCredentials
        max_obs_per_call: int
            Maximum number of observations returns per API call.
            If not provided, default is set to cryptocompare_api_limit storeed in DataCredentials
        rate_limit: pd.DataFrame
            Number of API calls made and left by frequency.
        """

        DataVendor.__init__(self, source_type, categories, exchanges, assets, indexes, markets, market_types, fields,
                            frequencies, base_url, api_key, max_obs_per_call, rate_limit)

        # set assets
        if assets is None:
            self.assets = self.get_assets_info(cat=None, as_dict=True)
        if indexes is None:
            self.indexes = self.get_indexes_info(as_list=True)
        # set fields
        if fields is None:
            self.fields = self.get_fields_info()

    @staticmethod
    def get_exchanges_info():
        """
        Get exchanges info.
        """
        return None

    def get_assets_info(self, cat=None, as_dict=False) -> Union[pd.DataFrame, dict[str, list[str]]]:
        """
        Get available assets info.

        Parameters
        ----------
        cat: str, {'crypto', 'fx', 'rates', 'cmdty', 'eqty'}, default None
            Asset class or time series category.
        as_dict: bool, default False
            If True, returns available assets as dict, with cat as dict keys and asssets as values.

        Returns
        -------
        assets: pd.DataFrame or dict
            Info on available assets, by category.
        """
        # store asset info in dict
        assets_info = {}
        # fx
        if cat == 'fx' or cat is None:
            try:
                fx = investpy.currency_crosses.get_currency_crosses(base=None, second=None)
            except Exception as e:
                logging.warning(e)
                logging.warning(f"Failed to get {cat} info.")
            else:
                fx.rename(columns={'name': 'ticker'}, inplace=True)
                assets_info['fx'] = fx.set_index('ticker')
        # rates
        if cat == 'rates' or cat is None:
            try:
                bonds = investpy.bonds.get_bonds()
                etfs = investpy.etfs.get_etfs()
                idx = investpy.indices.get_indices()
            except Exception as e:
                logging.warning(e)
                logging.warning(f"Failed to get {cat} info.")
            else:
                bonds['symbol'] = bonds.name
                etfs = etfs[etfs['asset_class'] == 'bond'].loc[:, ['country', 'name', 'full_name', 'symbol']]
                idx = idx[idx['class'] == 'bonds'].loc[:, ['country', 'name', 'full_name', 'symbol']]
                rates = pd.concat([bonds, idx, etfs]).rename(columns={'symbol': 'ticker'}).copy()
                assets_info['rates'] = rates.set_index('ticker')
        # cmdty
        if cat == 'cmdty' or cat is None:
            try:
                fut = investpy.commodities.get_commodities()
                etfs = investpy.etfs.get_etfs()
                idx = investpy.indices.get_indices()
            except Exception as e:
                logging.warning(e)
                logging.warning(f"Failed to get {cat} info.")
            else:
                fut['symbol'] = fut.name
                fut = fut.loc[:, ['country', 'name', 'full_name', 'currency', 'symbol']]
                etfs = etfs[etfs['asset_class'] == 'commodity'].loc[:, ['country', 'name', 'full_name', 'symbol',
                                                                        'currency']]
                idx = idx[idx['class'] == 'commodities'].loc[:, ['country', 'name', 'full_name', 'symbol', 'currency']]
                cmdty = pd.concat([fut, idx, etfs]).rename(columns={'symbol': 'ticker'}).copy()
                assets_info['cmdty'] = cmdty.set_index('ticker')
        # eqty
        if cat == 'eqty' or cat is None:
            try:
                stocks = investpy.stocks.get_stocks()
                etfs = investpy.etfs.get_etfs()
                idx = investpy.indices.get_indices()
            except Exception as e:
                logging.warning(e)
            else:
                stocks = stocks.loc[:, ['country', 'name', 'full_name', 'symbol', 'currency']]
                etfs = etfs[etfs['asset_class'] == 'equity'].loc[:, ['country', 'name', 'full_name', 'symbol',
                                                                     'currency']]
                idx = idx[(idx['class'] != 'commodities') &
                          (idx['class'] != 'bonds')].loc[:, ['country', 'name', 'full_name', 'symbol', 'currency']]
                eqty = pd.concat([idx, etfs, stocks]).rename(columns={'symbol': 'ticker'}).copy()
                assets_info['eqty'] = eqty.set_index('ticker')
        # macro
        if cat == 'macro':
            raise ValueError(f"Asset info not available for macro data.")

        # not valid cat
        if cat not in self.categories and cat is not None:
            raise ValueError(f"Asset info is only available for cat: {self.categories}.")

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

    @staticmethod
    def get_indexes_info(as_list=False) -> Union[pd.DataFrame, list[str]]:
        """
        Get available indexes info.

        Parameters
        ----------
        as_list: bool, default False
            If True, returns available assets as list.

        Returns
        -------
        indexes: pd.DataFrame or list
            Info on available indexes, by category.
        """
        try:
            indexes = investpy.indices.get_indices()

        except Exception as e:
            logging.warning(e)
            loggin.warning(f"Failed to get indexes info.")

        else:
            # wrangle data resp
            indexes.rename(columns={'symbol': 'ticker'}, inplace=True)
            # set index and sort
            indexes.set_index('ticker', inplace=True)
            indexes.sort_index(inplace=True)

            if as_list:
                indexes = indexes.index.to_list()

            return indexes

    @staticmethod
    def get_markets_info():
        """
        Get market pairs info.
        """
        return None

    @staticmethod
    def get_fields_info(data_type: Optional[str] = 'market', cat=None) -> dict[str, list[str]]:
        """
        Get fields info.

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
        if data_type != 'market':
            raise Exception("No on-chain or off-chain data available."
                            " Investing.com provides market and macro data.")
        # list of fields
        crypto_fields_list = ['date', 'open', 'high', 'low', 'close', 'volume']
        fx_fields_list = ['date', 'open', 'high', 'low', 'close']
        rates_fields_list = ['date', 'open', 'high', 'low', 'close']
        eqty_fields_list = ['date', 'open', 'high', 'low', 'close', 'volume']
        cmdty_fields_list = ['date', 'open', 'high', 'low', 'close', 'volume']
        macro_fields_list = ['actual', 'previous', 'expected', 'surprise']

        # fields dict
        fields = {'crypto': crypto_fields_list,
                  'fx': fx_fields_list,
                  'rates': rates_fields_list,
                  'eqty': eqty_fields_list,
                  'cmdty': cmdty_fields_list,
                  'macro': macro_fields_list,
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
        return None

    def get_indexes(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get indexes OHLCV data.

        Parameters
        ----------
        data_req: DataRequest
            Data request object (dictionary) with parameter values.

        Returns
        -------
        df: pd.DataFrame
            Dataframe with DatetimeIndex (level 0), tickers (level 1) and fields (cols).

        """
        # convert data request parameters to InvestPy format
        ip_data_req = ConvertParams(data_source='investpy').convert_to_source(data_req)
        # get indexes
        idx_df = self.get_indexes_info()
        # empty df to add data
        df = pd.DataFrame()

        # loop through tickers
        for ip_ticker, dr_ticker in zip(ip_data_req['tickers'], data_req.tickers):

            # set number of attempts and bool for while loop
            attempts = 0
            # run a while loop to pull ohlcv prices in case the attempt fails
            while attempts < ip_data_req['trials']:

                try:  # try get request
                    if idx_df.loc[idx_df.name == ip_ticker].shape[0] > 1:  # get country name
                        cty = 'united states'
                        df0 = investpy.indices.get_index_historical_data(ip_ticker, cty,
                                                                         from_date=ip_data_req['start_date'],
                                                                         to_date=ip_data_req['end_date'])
                    elif idx_df.loc[idx_df.name == ip_ticker].shape[0] == 1:
                        cty = idx_df.loc[idx_df.name == ip_ticker].iloc[0]['country']
                        df0 = investpy.indices.get_index_historical_data(ip_ticker, cty,
                                                                         from_date=ip_data_req['start_date'],
                                                                         to_date=ip_data_req['end_date'])
                    else:
                        search_res = investpy.search_quotes(text=ip_ticker)[0]
                        df0 = search_res.retrieve_historical_data(from_date=ip_data_req['start_date'],
                                                                  to_date=ip_data_req['end_date'])
                    sleep(ip_data_req['pause'])
                    break

                except Exception as e:
                    logging.warning(e)
                    attempts += 1
                    sleep(ip_data_req['pause'])
                    if attempts == ip_data_req['trials']:
                        logging.warning(f"Failed to pull {dr_ticker} after many attempts.")
                        break

            # wrangle data resp
            if not df0.empty:
                # wrangle data resp
                df1 = self.wrangle_data_resp(data_req, df0)
                # add ticker to index
                df1['ticker'] = dr_ticker
                df1.set_index(['ticker'], append=True, inplace=True)
                # stack ticker dfs
                df = pd.concat([df, df1])
            else:
                logging.warning("No data was returned. Check index tickers and try again.")

        return df

    def get_etfs(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get ETFs OHLCV data.

        Parameters
        ----------
        data_req: DataRequest
            Data request object (dictionary) with parameter values.

        Returns
        -------
        df: pd.DataFrame
            Dataframe with DatetimeIndex (level 0), tickers (level 1) and fields (cols).
        """
        # convert data request parameters to InvestPy format
        ip_data_req = ConvertParams(data_source='investpy').convert_to_source(data_req)
        # get etfs
        etfs_df = investpy.etfs.get_etfs()
        # empty df to add data
        df = pd.DataFrame()

        # loop through tickers
        for ip_ticker, dr_ticker in zip(ip_data_req['tickers'], data_req.tickers):

            # set number of attempts and bool for while loop
            attempts = 0
            # run a while loop to pull ohlcv prices in case the attempt fails
            while attempts < ip_data_req['trials']:

                try:  # try get request
                    if etfs_df.loc[etfs_df.name == ip_ticker].shape[0] > 1:  # get country name
                        cty = 'united states'
                        df0 = investpy.etfs.get_etf_historical_data(ip_ticker, cty, from_date=ip_data_req['start_date'],
                                                                    to_date=ip_data_req['end_date'])
                    elif etfs_df.loc[etfs_df.name == ip_ticker].shape[0] == 1:
                        cty = etfs_df.loc[etfs_df.name == ip_ticker].iloc[0]['country']
                        df0 = investpy.etfs.get_etf_historical_data(ip_ticker, cty, from_date=ip_data_req['start_date'],
                                                                    to_date=ip_data_req['end_date'])
                    else:
                        search_res = investpy.search_quotes(text=ip_ticker)[0]
                        df0 = search_res.retrieve_historical_data(from_date=ip_data_req['start_date'],
                                                                  to_date=ip_data_req['end_date'])
                    sleep(ip_data_req['pause'])
                    break

                except Exception as e:
                    logging.warning(e)
                    attempts += 1
                    sleep(ip_data_req['pause'])
                    if attempts == ip_data_req['trials']:
                        logging.warning(f"Failed to pull {dr_ticker} after many attempts.")
                        break

                        # wrangle data resp
            if not df0.empty:
                # wrangle data resp
                df1 = self.wrangle_data_resp(data_req, df0)
                # add ticker to index
                df1['ticker'] = dr_ticker
                df1.set_index(['ticker'], append=True, inplace=True)
                # stack ticker dfs
                df = pd.concat([df, df1])
            else:
                logging.warning("No data was returned. Check etf tickers and try again.")

        return df

    def get_stocks(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get stocks OHLCV data.

        Parameters
        ----------
        data_req: DataRequest
            Data request object (dictionary) with parameter values.

        Returns
        -------
        df: pd.DataFrame
            Dataframe with DatetimeIndex (level 0), tickers (level 1) and fields (cols).
        """
        # convert data request parameters to InvestPy format
        ip_data_req = ConvertParams(data_source='investpy').convert_to_source(data_req)
        # get stocks
        stocks_df = investpy.stocks.get_stocks()
        # empty df to add data
        df = pd.DataFrame()

        # loop through tickers
        for ip_ticker, dr_ticker in zip(ip_data_req['tickers'], data_req.tickers):

            # set number of attempts and bool for while loop
            attempts = 0
            # run a while loop to pull ohlcv prices in case the attempt fails
            while attempts < ip_data_req['trials']:

                try:  # try get request
                    if stocks_df.loc[stocks_df.symbol == ip_ticker].shape[0] > 1:  # get country name
                        cty = 'united states'
                        df0 = investpy.stocks.get_stock_historical_data(ip_ticker, cty,
                                                                        from_date=ip_data_req['start_date'],
                                                                        to_date=ip_data_req['end_date'])
                    elif stocks_df.loc[stocks_df.symbol == ip_ticker].shape[0] == 1:
                        cty = stocks_df.loc[stocks_df.symbol == ip_ticker].iloc[0]['country']
                        df0 = investpy.stocks.get_stock_historical_data(ip_ticker, cty,
                                                                        from_date=ip_data_req['start_date'],
                                                                        to_date=ip_data_req['end_date'])
                    else:
                        search_res = investpy.search_quotes(text=ip_ticker)[0]
                        df0 = search_res.retrieve_historical_data(from_date=ip_data_req['start_date'],
                                                                  to_date=ip_data_req['end_date'])
                    sleep(ip_data_req['pause'])
                    break

                except Exception as e:
                    logging.warning(e)
                    attempts += 1
                    sleep(ip_data_req['pause'])
                    if attempts == ip_data_req['trials']:
                        logging.warning(f"Failed to pull {dr_ticker} after many attempts.")
                        break

                        # wrangle data resp
            if not df0.empty:
                # wrangle data resp
                df1 = self.wrangle_data_resp(data_req, df0)
                # add ticker to index
                df1['ticker'] = dr_ticker
                df1.set_index(['ticker'], append=True, inplace=True)
                # stack ticker dfs
                df = pd.concat([df, df1])
            else:
                logging.warning("No data was returned. Check stock tickers and try again.")

        return df

    def get_eqty(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get Eqty OHLCV data.

        Parameters
        ----------
        data_req: DataRequest
            Data request object (dictionary) with parameter values.

        Returns
        -------
        df: pd.DataFrame
            Dataframe with DatetimeIndex (level 0), tickers (level 1) and fields (cols).
        """

        # convert data request parameters to InvestPy format
        ip_data_req = ConvertParams(data_source='investpy').convert_to_source(data_req)
        # get tickers
        idx_df = self.get_indexes_info()
        etfs_df = investpy.etfs.get_etfs()
        stocks_df = investpy.stocks.get_stocks()
        # empty df to add data
        df = pd.DataFrame()

        # loop through tickers
        for ip_ticker, dr_ticker in zip(ip_data_req['tickers'], data_req.tickers):

            # set number of attempts and bool for while loop
            attempts = 0
            # run a while loop to pull ohlcv prices in case the attempt fails
            while attempts < ip_data_req['trials']:

                try:  # try get request
                    if idx_df.loc[idx_df.name == ip_ticker].shape[0] == 1:
                        cty = idx_df.loc[idx_df.name == ip_ticker].iloc[0]['country']
                        df0 = investpy.indices.get_index_historical_data(ip_ticker, cty,
                                                                         from_date=ip_data_req['start_date'],
                                                                         to_date=ip_data_req['end_date'])
                    elif etfs_df.loc[etfs_df.name == ip_ticker].shape[0] == 1:
                        cty = etfs_df.loc[etfs_df.name == ip_ticker].iloc[0]['country']
                        df0 = investpy.etfs.get_etf_historical_data(ip_ticker, cty, from_date=ip_data_req['start_date'],
                                                                    to_date=ip_data_req['end_date'])
                    elif stocks_df.loc[stocks_df.symbol == ip_ticker].shape[0] == 1:
                        cty = stocks_df.loc[stocks_df.symbol == ip_ticker].iloc[0]['country']
                        df0 = investpy.stocks.get_stock_historical_data(ip_ticker, cty,
                                                                        from_date=ip_data_req['start_date'],
                                                                        to_date=ip_data_req['end_date'])
                    else:
                        search_res = investpy.search_quotes(text=ip_ticker)[0]
                        df0 = search_res.retrieve_historical_data(from_date=ip_data_req['start_date'],
                                                                  to_date=ip_data_req['end_date'])
                    sleep(ip_data_req['pause'])
                    break

                except Exception as e:
                    logging.warning(e)
                    attempts += 1
                    sleep(ip_data_req['pause'])
                    if attempts == ip_data_req['trials']:
                        logging.warning(f"Failed to pull {dr_ticker} after many attempts.")
                        break

                        # wrangle data resp
            if not df0.empty:
                # wrangle data resp
                df1 = self.wrangle_data_resp(data_req, df0)
                # add ticker to index
                df1['ticker'] = dr_ticker
                df1.set_index(['ticker'], append=True, inplace=True)
                # stack ticker dfs
                df = pd.concat([df, df1])
            else:
                logging.warning("No data was returned. Check stock tickers and try again.")

        return df

    def get_fx(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get FX OHLCV data.

        Parameters
        ----------
        data_req: DataRequest
            Data request object (dictionary) with parameter values.

        Returns
        -------
        df: pd.DataFrame
            Dataframe with DatetimeIndex (level 0), tickers (level 1) and fields (cols).
        """
        # convert data request parameters to InvestPy format
        ip_data_req = ConvertParams(data_source='investpy').convert_to_source(data_req)
        # empty df to add data
        df = pd.DataFrame()

        # loop through tickers
        for ip_ticker, dr_ticker in zip(ip_data_req['mkts'], data_req.tickers):

            # set number of attempts and bool for while loop
            attempts = 0
            # run a while loop to pull ohlcv prices in case the attempt fails
            while attempts < ip_data_req['trials']:

                try:  # try get request
                    df0 = investpy.currency_crosses.get_currency_cross_historical_data(ip_ticker, from_date=ip_data_req[
                        'start_date'], to_date=ip_data_req['end_date'])
                    sleep(ip_data_req['pause'])
                    break

                except Exception as e:
                    logging.warning(e)
                    attempts += 1
                    sleep(ip_data_req['pause'])
                    if attempts == ip_data_req['trials']:
                        logging.warning(f"Failed to pull {dr_ticker} after many attempts.")
                        break

                        # wrangle data resp
            if not df0.empty:
                # wrangle data resp
                df1 = self.wrangle_data_resp(data_req, df0)
                # add ticker to index
                df1['ticker'] = dr_ticker.upper() + ip_data_req['quote_ccy']
                df1.set_index(['ticker'], append=True, inplace=True)
                # stack ticker dfs
                df = pd.concat([df, df1])
            else:
                logging.warning("No data was returned. Check fx tickers and try again.")

        return df

    def get_rates(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get rates OHLCV data.

        Parameters
        ----------
        data_req: DataRequest
            Data request object (dictionary) with parameter values.

        Returns
        -------
        df: pd.DataFrame
            Dataframe with DatetimeIndex (level 0), tickers (level 1) and fields (cols).
        """
        # convert data request parameters to InvestPy format
        ip_data_req = ConvertParams(data_source='investpy').convert_to_source(data_req)
        # empty df to add data
        df = pd.DataFrame()

        # loop through tickers
        for ip_ticker, dr_ticker in zip(ip_data_req['tickers'], data_req.tickers):

            # set number of attempts and bool for while loop
            attempts = 0
            # run a while loop to pull ohlcv prices in case the attempt fails
            while attempts < ip_data_req['trials']:

                try:  # try get request
                    df0 = investpy.bonds.get_bond_historical_data(ip_ticker, from_date=ip_data_req['start_date'],
                                                                  to_date=ip_data_req['end_date'])
                    sleep(ip_data_req['pause'])
                    break

                except Exception as e:
                    logging.info(e)
                    logging.info(f"Failed to pull {dr_ticker}.")
                    sleep(ip_data_req['pause'])

                    try:
                        search_res = investpy.search_quotes(text=ip_ticker)[0]
                        df0 = search_res.retrieve_historical_data(from_date=ip_data_req['start_date'],
                                                                  to_date=ip_data_req['end_date'])
                        sleep(ip_data_req['pause'])
                        break

                    except Exception as e:
                        logging.warning(e)
                        attempts += 1
                        sleep(ip_data_req['pause'])
                        if attempts == 3:
                            logging.warning(
                                f"Failed to pull data for {dr_ticker} after many attempts.")
                            break

            # wrangle data resp
            if not df0.empty:
                # wrangle data resp
                df1 = self.wrangle_data_resp(data_req, df0)
                # add ticker to index
                df1['ticker'] = dr_ticker
                df1.set_index(['ticker'], append=True, inplace=True)
                # stack ticker dfs
                df = pd.concat([df, df1])
            else:
                logging.warning("No data was returned. Check rates tickers and try again.")

        return df

    def get_cmdty(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get cmdty OHLCV data.

        Parameters
        ----------
        data_req: DataRequest
            Data request object (dictionary) with parameter values.

        Returns
        -------
        df: pd.DataFrame
            Dataframe with DatetimeIndex (level 0), tickers (level 1) and fields (cols).
        """
        # convert data request parameters to InvestPy format
        ip_data_req = ConvertParams(data_source='investpy').convert_to_source(data_req)
        # empty df to add data
        df = pd.DataFrame()

        # loop through tickers
        for ip_ticker, dr_ticker in zip(ip_data_req['tickers'], data_req.tickers):

            # set number of attempts and bool for while loop
            attempts = 0
            # run a while loop to pull ohlcv prices in case the attempt fails
            while attempts < ip_data_req['trials']:

                try:  # try get request
                    df0 = investpy.commodities.get_commodity_historical_data(ip_ticker,
                                                                             from_date=ip_data_req['start_date'],
                                                                             to_date=ip_data_req['end_date'])
                    sleep(ip_data_req['pause'])
                    break

                except Exception as e:
                    logging.info(e)
                    logging.info(f"Failed to pull {dr_ticker}.")
                    sleep(ip_data_req['pause'])

                    try:
                        search_res = investpy.search_quotes(text=ip_ticker)[0]
                        df0 = search_res.retrieve_historical_data(from_date=ip_data_req['start_date'],
                                                                  to_date=ip_data_req['end_date'])
                        sleep(ip_data_req['pause'])
                        break

                    except Exception as e:
                        logging.warning(e)
                        attempts += 1
                        sleep(ip_data_req['pause'])
                        if attempts == 3:
                            logging.warning(
                                f"Failed to pull data for {dr_ticker} after many attempts.")
                            break

            # wrangle data resp
            if not df0.empty:
                # wrangle data resp
                df1 = self.wrangle_data_resp(data_req, df0)
                # add ticker to index
                df1['ticker'] = dr_ticker
                df1.set_index(['ticker'], append=True, inplace=True)
                # stack ticker dfs
                df = pd.concat([df, df1])
            else:
                logging.warning("No data was returned. Check cmdty tickers and try again.")

        return df

    def get_macro_series(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get macro/econ release data.

        Parameters
        ----------
        data_req: DataRequest
            Data request object (dictionary) with parameter values.

        Returns
        -------
        df: pd.DataFrame
            Dataframe with DatetimeIndex (level 0), tickers (level 1) and fields (cols).
        """
        # convert data req params to InvestPy format
        ip_data_req = ConvertParams(data_source='investpy').convert_to_source(data_req)
        # get data calendar
        econ_df = investpy.news.economic_calendar(countries=ip_data_req['ctys'], time_zone='GMT',
                                                  from_date=ip_data_req['start_date'], to_date=ip_data_req['end_date'])
        # emtpy df
        df = pd.DataFrame()

        # loop through tickers, countries
        for dr_ticker, ip_ticker, cty in zip(data_req.tickers, ip_data_req['tickers'], ip_data_req['ctys']):

            # filter data calendar for ticker, country
            df0 = econ_df[(econ_df.event.str.startswith(ip_ticker)) & (econ_df.zone.str.match(cty.lower()))].copy()

            # wrangle data resp
            if not df0.empty:
                # wrangle data resp
                df1 = self.wrangle_data_resp(data_req, df0)
                # add ticker to index
                df1['ticker'] = dr_ticker
                df1.set_index(['ticker'], append=True, inplace=True)
                # stack ticker dfs
                df = pd.concat([df, df1])

            else:
                logging.warning("No data was returned. Check macro tickers and try again.")

        return df

    def get_data(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get data series from InvestPy.

        Parameters
        ----------
        data_req: DataRequest
            Data request object (dictionary) with parameter values.

        Returns
        -------
        df: pd.DataFrame
            Dataframe with DatetimeIndex (level 0), tickers (level 1) and fields (cols), in tidy format.
        """
        # check cat
        if data_req.cat not in self.categories:
            raise ValueError(f"Invalid category. Valid categories are: {self.categories}.")

        # check freq
        if data_req.freq not in self.frequencies[data_req.cat]:
            raise ValueError(f"Invalid data frequency. Valid data frequencies are: {self.frequencies}.")

        # check fields
        if not any(field in self.fields[data_req.cat] for field in data_req.fields):
            raise ValueError("Invalid fields. See '.fields' property for available fields.")

        # fx
        if data_req.cat == 'fx':
            df = self.get_fx(data_req)

        # rates
        elif data_req.cat == 'rates':
            df = self.get_rates(data_req)

        # cmdty
        elif data_req.cat == 'cmdty':
            df = self.get_cmdty(data_req)

        # eqty
        elif data_req.cat == 'eqty':
            df = self.get_indexes(data_req)

        # macro
        elif data_req.cat == 'macro':
            df = self.get_macro_series(data_req)

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
        Get data series from InvestPy library.

        Parameters
        ----------
        data_req: DataRequest
            Data request object (dictionary) with parameter values.
        data_resp: pd.DataFrame
            Data response dataframe to wrangle.

        Returns
        -------
        df: pd.DataFrame
            Dataframe with DatetimeIndex (level 0), tickers (level 1) and fields (cols).
        """
        # create copy df
        df = data_resp.copy()
        # format cols
        if data_req.cat != 'macro':
            # reset index
            df = df.reset_index()
        else:
            # parse date and time to create datetime
            df.time.replace('Tentative', '23:55', inplace=True)
            df['date'] = pd.to_datetime(df.date + df.time, format="%d/%m/%Y%H:%M")
            # replace missing vals
            df.forecast = np.where(np.nan, df.previous, df.forecast)
            # convert cols to cryptodatapy format
        df = ConvertParams(data_source='investpy').convert_fields_to_lib(data_req, df)

        # set index
        df.set_index('date', inplace=True)
        # str and resample to higher freq for econ releases
        if data_req.cat == 'macro':
            df = df.replace('%', '', regex=True).astype(float) / 100  # remove % str
            df['surprise'] = df.actual - df.expected

        # resample freq
        df = df.resample(data_req.freq).last().ffill()

        # filter bad data
        if 'surprise' in df.columns:
            df = pd.concat([df[df.columns.drop('surprise')][df != 0], df.loc[:, ['surprise']]], axis=1)
        else:
            df = df[df != 0]  # 0 values
        df = df[~df.index.duplicated()]  # duplicate rows
        df = df.dropna(how='all').dropna(how='all', axis=1)  # entire row or col NaNs

        # type conversion
        df = ConvertParams().convert_dtypes(df)

        return df
    