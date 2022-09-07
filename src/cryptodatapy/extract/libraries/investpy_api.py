import pandas as pd
import logging
from time import sleep
from importlib import resources
from typing import Optional, Union, Any
from cryptodatapy.util.datacredentials import DataCredentials
from cryptodatapy.extract.libraries.library import Library
from cryptodatapy.extract.datarequest import DataRequest
from cryptodatapy.transform.convertparams import ConvertParams
from cryptodatapy.transform.wrangle import WrangleData
import investpy


# data credentials
data_cred = DataCredentials()


class InvestPy(Library):
    """
    Retrieves data from InvestPy API.
    """

    def __init__(
            self,
            categories: list[str] = ['fx', 'rates', 'eqty', 'cmdty', 'macro'],
            exchanges: Optional[list[str]] = None,
            indexes: Optional[dict[str, list[str]]] = None,
            assets: Optional[dict[str, list[str]]] = None,
            markets: Optional[dict[str, list[str]]] = None,
            market_types: list[str] = ['spot', 'future'],
            fields: Optional[dict[str, list[str]]] = None,
            frequencies: dict[str, list[str]] = {'fx': ['d', 'w', 'm', 'q', 'y'],
                                                 'rates': ['d', 'w', 'm', 'q', 'y'],
                                                 'eqty': ['d', 'w', 'm', 'q', 'y'],
                                                 'cmdty': ['d', 'w', 'm', 'q', 'y'],
                                                 'macro': ['1min', '5min', '10min', '15min', '30min',
                                                           '1h', '2h', '4h', '8h', 'd', 'w', 'm', 'q', 'y']},

            base_url: Optional[str] = None,
            api_key: Optional[str] = None,
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
        api_key: str, optional, default None
            Api key, e.g. 'dcf13983adf7dfa79a0dfa35adf'. If not provided, default is set to
            api_key stored in DataCredentials.
        max_obs_per_call: int, optional, default None
            Maximum number of observations returned per API call. If not provided, default is set to
            api_limit stored in DataCredentials.
        rate_limit: Any, optional, default None
            Number of API calls made and left, by time frequency.
        """
        Library.__init__(self, categories, exchanges, indexes, assets, markets, market_types, fields,
                         frequencies, base_url, api_key, max_obs_per_call, rate_limit)

        self.indexes = self.get_indexes_info(cat=None, as_dict=True)
        self.assets = self.get_assets_info(cat=None, as_dict=True)
        self.fields = self.get_fields_info()

    @staticmethod
    def get_exchanges_info():
        """
        Get exchanges info.
        """
        return None

    @staticmethod
    def get_indexes_info(cat: Optional[str] = None, as_dict: bool = False) -> Union[dict[str, list[str]], pd.DataFrame]:
        """
        Get available indexes info.

        Parameters
        ----------
        cat: str, {'eqty', 'cmdty', 'rates'}, optional, default None
            Asset class.
        as_dict: bool, default False
            Returns available indexes as dictionary, with cat-indexes key-values pairs.

        Returns
        -------
        indexes: dictionary or pd.DataFrame
            Dictionary or dataframe with info on available indexes, by category.
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

            # categories
            cats = {'cmdty': 'commodities', 'rates': 'bonds', 'eqty': ''}

            # indexes dict
            if as_dict:
                idx_dict = {}
                for k in cats.keys():
                    if k == 'eqty':
                        idx_dict[k] = indexes[(indexes['class'] != 'commodities') &
                                              (indexes['class'] != 'bonds')].index.to_list()
                    else:
                        idx_dict[k] = indexes[indexes['class'] == cats[k]].index.to_list()

                # filter by cat
                if cat is not None:
                    indexes = idx_dict[cat]
                else:
                    indexes = idx_dict

            else:
                # filter df by cat
                if cat is not None:
                    if cat == 'eqty':
                        indexes = indexes[(indexes['class'] != 'commodities') & (indexes['class'] != 'bonds')]
                    else:
                        indexes = indexes[indexes['class'] == cats[cat]]

            return indexes

    def get_assets_info(self, cat: Optional[str] = None, as_dict: bool = False) -> \
            Union[dict[str, list[str]], pd.DataFrame]:
        """
        Get assets info.

        Parameters
        ----------
        cat: str, {'crypto', 'fx', 'rates', 'cmdty', 'eqty'}, optional, default None
            Asset class or time series category.
        as_dict: bool, default False
            Returns available assets as dictionary, with cat-assets key-values pairs.

        Returns
        -------
        assets: dictionary or pd.DataFrame
            Dictionary or dataframe with info on available assets, by category.
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
    def get_markets_info():
        """
        Get markets info.
        """
        return None

    @staticmethod
    def get_fields_info(cat: Optional[str] = None) -> dict[str, list[str]]:
        """
        Get fields info.

        Parameters
        ----------
        cat: str, {'crypto', 'eqty', 'fx', 'rates', 'cmdty', 'macro'}, optional, default None
            Asset class or time series category.

        Returns
        -------
        fields_list: dictionary
            Dictionary with info on available fields, by category.
        """
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
        Get indexes data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1) and index OHLCV values data (cols).
        """
        # convert data request parameters to InvestPy format
        ip_data_req = ConvertParams(data_req, data_source='investpy').convert_to_source()
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
                    assert not df0.empty
                    sleep(ip_data_req['pause'])

                except Exception as e:
                    logging.warning(e)
                    attempts += 1
                    sleep(ip_data_req['pause'])
                    if attempts == ip_data_req['trials']:
                        logging.warning(f"Failed to pull {dr_ticker} after many attempts.")
                        break

                else:
                    # wrangle data resp
                    df1 = self.wrangle_data_resp(data_req, df0)
                    # add ticker to index
                    df1['ticker'] = dr_ticker
                    df1.set_index(['ticker'], append=True, inplace=True)
                    # stack ticker dfs
                    df = pd.concat([df, df1])
                    break

        return df

    def get_etfs(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get ETFs data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1) and ETF OHLCV values (cols).
        """
        # convert data request parameters to InvestPy format
        ip_data_req = ConvertParams(data_req, data_source='investpy').convert_to_source()
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
                    assert not df0.empty
                    sleep(ip_data_req['pause'])

                except Exception as e:
                    logging.warning(e)
                    attempts += 1
                    sleep(ip_data_req['pause'])
                    if attempts == ip_data_req['trials']:
                        logging.warning(f"Failed to pull {dr_ticker} after many attempts.")
                        break

                else:
                    # wrangle data resp
                    df1 = self.wrangle_data_resp(data_req, df0)
                    # add ticker to index
                    df1['ticker'] = dr_ticker
                    df1.set_index(['ticker'], append=True, inplace=True)
                    # stack ticker dfs
                    df = pd.concat([df, df1])
                    break

        return df

    def get_stocks(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get stocks data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1) and stocks OHLCV values (cols).
        """
        # convert data request parameters to InvestPy format
        ip_data_req = ConvertParams(data_req, data_source='investpy').convert_to_source()
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
                    assert not df0.empty
                    sleep(ip_data_req['pause'])

                except Exception as e:
                    logging.warning(e)
                    attempts += 1
                    sleep(ip_data_req['pause'])
                    if attempts == ip_data_req['trials']:
                        logging.warning(f"Failed to pull {dr_ticker} after many attempts.")
                        break

                else:
                    # wrangle data resp
                    df1 = self.wrangle_data_resp(data_req, df0)
                    # add ticker to index
                    df1['ticker'] = dr_ticker
                    df1.set_index(['ticker'], append=True, inplace=True)
                    # stack ticker dfs
                    df = pd.concat([df, df1])
                    break

        return df

    def get_eqty(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get equities OHLCV data, for either indexes, ETFs or stocks.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1) and equities OHLCV values (cols).
        """

        # convert data request parameters to InvestPy format
        ip_data_req = ConvertParams(data_req, data_source='investpy').convert_to_source()
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
                    assert not df0.empty
                    sleep(ip_data_req['pause'])

                except Exception as e:
                    logging.warning(e)
                    attempts += 1
                    sleep(ip_data_req['pause'])
                    if attempts == ip_data_req['trials']:
                        logging.warning(f"Failed to pull {dr_ticker} after many attempts.")
                        break

                else:
                    # wrangle data resp
                    df1 = self.wrangle_data_resp(data_req, df0)
                    # add ticker to index
                    df1['ticker'] = dr_ticker
                    df1.set_index(['ticker'], append=True, inplace=True)
                    # stack ticker dfs
                    df = pd.concat([df, df1])
                    break

        return df

    def get_fx(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get FX data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1) and FX OHLC values (cols).
        """
        # convert data request parameters to InvestPy format
        ip_data_req = ConvertParams(data_req, data_source='investpy').convert_to_source()
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
                    assert not df0.empty
                    sleep(ip_data_req['pause'])

                except Exception as e:
                    logging.warning(e)
                    attempts += 1
                    sleep(ip_data_req['pause'])
                    if attempts == ip_data_req['trials']:
                        logging.warning(f"Failed to pull {dr_ticker} after many attempts.")
                        break

                else:
                    # wrangle data resp
                    df1 = self.wrangle_data_resp(data_req, df0)
                    # add ticker to index
                    df1['ticker'] = ip_ticker.replace('/', '')
                    df1.set_index(['ticker'], append=True, inplace=True)
                    # stack ticker dfs
                    df = pd.concat([df, df1])
                    break

        return df

    def get_rates(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get rates data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1) and rates OHLC values (cols).
        """
        # convert data request parameters to InvestPy format
        ip_data_req = ConvertParams(data_req, data_source='investpy').convert_to_source()
        # empty df to add data
        df0, df = pd.DataFrame(), pd.DataFrame()

        # loop through tickers
        for ip_ticker, dr_ticker in zip(ip_data_req['tickers'], data_req.tickers):

            # set number of attempts and bool for while loop
            attempts = 0
            # run a while loop to pull ohlcv prices in case the attempt fails
            while attempts < ip_data_req['trials']:

                try:  # try get request
                    df0 = investpy.bonds.get_bond_historical_data(ip_ticker, from_date=ip_data_req['start_date'],
                                                                  to_date=ip_data_req['end_date'])
                    assert not df0.empty
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
                        assert not df0.empty
                        sleep(ip_data_req['pause'])
                        break

                    except Exception as e:
                        logging.warning(e)
                        attempts += 1
                        sleep(ip_data_req['pause'])
                        if attempts == ip_data_req['trials']:
                            logging.warning(f"Failed to pull data for {dr_ticker} after many attempts.")
                            break

            if not df0.empty:
                # wrangle data resp
                df1 = self.wrangle_data_resp(data_req, df0)
                # add ticker to index
                df1['ticker'] = dr_ticker
                df1.set_index(['ticker'], append=True, inplace=True)
                # stack ticker dfs
                df = pd.concat([df, df1])

        return df

    def get_cmdty(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get cmdty data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1) and commodities OHLCV values (cols).
        """
        # convert data request parameters to InvestPy format
        ip_data_req = ConvertParams(data_req, data_source='investpy').convert_to_source()
        # empty dfs
        df0, df = pd.DataFrame(), pd.DataFrame()

        # loop through tickers
        for ip_ticker, dr_ticker in zip(ip_data_req['tickers'], data_req.tickers):

            # set number of attempts and bool for while loop
            attempts = 0
            # run a while loop to pull ohlcv prices in case the attempt fails
            while attempts < ip_data_req['trials']:

                df0 = None

                try:  # try get request
                    df0 = investpy.commodities.get_commodity_historical_data(ip_ticker,
                                                                             from_date=ip_data_req['start_date'],
                                                                             to_date=ip_data_req['end_date'])
                    assert not df0.empty
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
                        assert not df0.empty
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

            if not df0.empty:
                # wrangle data resp
                df1 = self.wrangle_data_resp(data_req, df0)
                # add ticker to index
                df1['ticker'] = dr_ticker
                df1.set_index(['ticker'], append=True, inplace=True)
                # stack ticker dfs
                df = pd.concat([df, df1])

        return df

    def get_macro_series(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Get macro/econ release data.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        Returns
        -------
        df: pd.DataFrame - MultiIndex
            DataFrame with DatetimeIndex (level 0), ticker (level 1) and macro series/data release (cols).
        """
        # convert data req params to InvestPy format
        ip_data_req = ConvertParams(data_req, data_source='investpy').convert_to_source()

        econ_df = pd.DataFrame()
        # set number of attempts and bool for while loop
        attempts = 0
        # run a while loop to pull ohlcv prices in case the attempt fails
        while attempts < ip_data_req['trials']:

            try:  # try get request
                # get data calendar
                econ_df = investpy.news.economic_calendar(countries=ip_data_req['ctys'], time_zone='GMT',
                                                          from_date=ip_data_req['start_date'],
                                                          to_date=ip_data_req['end_date'])
                assert not econ_df.empty
                break

            except Exception as e:
                logging.warning(e)
                attempts += 1
                sleep(ip_data_req['pause'])
                if attempts == ip_data_req['trials']:
                    raise Exception("Failed to get economic data release calendar after many attempts.")

        # emtpy df
        df = pd.DataFrame()

        # loop through tickers, countries
        if not econ_df.empty:
            for dr_ticker, ip_ticker, cty in zip(data_req.tickers, ip_data_req['tickers'], ip_data_req['ctys']):
                # filter data calendar for ticker, country
                df0 = econ_df[(econ_df.event.str.startswith(ip_ticker)) & (econ_df.zone.str.match(cty.lower()))].copy()
                # wrangle data resp
                df1 = self.wrangle_data_resp(data_req, df0)
                # add ticker to index
                df1['ticker'] = dr_ticker
                df1.set_index(['ticker'], append=True, inplace=True)
                # stack ticker dfs
                df = pd.concat([df, df1])
        else:
            raise Exception("Economic data release calendar was not returned.")

        return df

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
        # check cat
        if data_req.cat not in self.categories:
            raise ValueError(f"Invalid category. Valid categories are: {self.categories}.")

        # check freq
        if data_req.freq not in self.frequencies[data_req.cat]:
            raise ValueError(f"Invalid data frequency. Valid data frequencies are: {self.frequencies}.")

        # check fields
        if not any(field in self.fields[data_req.cat] for field in data_req.fields):
            raise ValueError(f"Invalid fields. Valid fields are: {self.fields}.")

        df = pd.DataFrame()

        try:  # get data
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
                df = self.get_eqty(data_req)
            # macro
            elif data_req.cat == 'macro':
                df = self.get_macro_series(data_req)

        except Exception as e:
            logging.warning(e)
            raise Exception('No data returned. Check data request parameters and try again.')

        else:
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
            Data response from data request.

        Returns
        -------
        df: pd.DataFrame
            Wrangled dataframe with DatetimeIndex (level 0), ticker (level 1), and values for market or macro series
            for selected fields (cols), in tidy format.
        """
        # wrangle data resp
        df = WrangleData(data_req, data_resp, data_source='investpy').tidy_data()

        return df
