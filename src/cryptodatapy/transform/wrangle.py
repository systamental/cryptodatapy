from __future__ import annotations
from typing import Union, Dict, List, Optional, Any
from importlib import resources


import pandas as pd

from cryptodatapy.extract.datarequest import DataRequest


class WrangleInfo:
    """
    Wrangles metadata data responses from various APIs into dataframe or list.

    """

    def __init__(self, data_resp: Union[Dict[str, Any], pd.DataFrame]):
        """
        Constructor

        Parameters
        ----------
        data_resp: pd.DataFrame
            Dataframe containing data response.

        """
        self.data_resp = data_resp

    def cc_exch_info(self, as_list: bool = False) -> Union[pd.DataFrame, list]:
        """
        Converts exchanges info to dataframe or list.

        Parameters
        ----------
        as_list: bool, default False
            Returns info as list.

        Returns
        -------
        exch: pd.DataFrame or list
            Exchanges info converted to dataframe or list.

        """
        exch = pd.DataFrame(self.data_resp['Data']).T  # extract data
        exch.set_index('Name', inplace=True)  # set index name

        if as_list:  # return as list
            exch = list(exch.index)

        return exch

    def cc_indexes_info(self, as_list: bool = False) -> Union[pd.DataFrame, list]:
        """
        Converts indexes info to dataframe or list.

        Parameters
        ----------
        as_list: bool, default False
            Returns info as list.

        Returns
        -------
        indexes: pd.DataFrame or list
            Indexes info converted to dataframe or list.

        """
        indexes = pd.DataFrame(self.data_resp['Data']).T  # extract data
        indexes.index.name = 'ticker'  # set index name

        if as_list:  # return as list
            indexes = list(indexes.index)

        return indexes

    def cc_assets_info(self, as_list: bool = False) -> Union[pd.DataFrame, list]:
        """
        Converts assets info into dataframe or list.

        Parameters
        ----------
        as_list: bool, default False
            Returns info as list.

        Returns
        -------
        assets: pd.DataFrame or list
            Assets info converted to dataframe or list.

        """
        assets = pd.DataFrame(self.data_resp['Data']).T  # extract data
        assets.index.name = 'ticker'  # set index name

        if as_list:  # return as list
            assets = list(assets.index)

        return assets

    def cc_mkts_info(self, as_list: bool = False) -> Union[Dict, List]:
        """
        Converts markets info to dictionary or list.

        Parameters
        ----------
        as_list: bool, default False
            Returns info as list.

        Returns
        -------
        mkts: dictionary or list
            Markets info converted to dictionary or list.

        """
        mkts = self.data_resp['Data']  # extract data
        mkts_dict = {}
        for asset in mkts['pairs']:  # extract pairs
            mkts_dict[asset] = mkts['pairs'][asset]['tsyms']
        mkts = mkts_dict

        if as_list:  # return as list
            pairs = []
            for asset in mkts.keys():
                for quote in mkts[asset]:
                    pairs.append(str(asset + quote))
            mkts = pairs

        return mkts

    def cc_onchain_tickers_info(self, as_list: bool = False) -> Union[pd.DataFrame, List]:
        """
        Converts on-chain tickers info to dataframe or list.

        Parameters
        ----------
        as_list: bool, default False
            Returns info as list.

        Returns
        -------
        tickers: pd.DataFrame or list
            On-chain tickers info converted to dataframe or list.

        """
        tickers = pd.DataFrame(self.data_resp['Data']).T
        tickers['data_available_from'] = pd.to_datetime(tickers.data_available_from, unit='s')  # format date
        tickers.index.name = 'ticker'  # set index name
        if as_list:  # return as list
            tickers = list(tickers.index)
        return tickers

    def cc_onchain_info(self) -> List[str]:
        """
        Converts on-chain fields info to list.

        Returns
        -------
        onchain: list
            On-chain fields info converted to list.

        """
        onchain = self.data_resp['Data']  # extract data
        onchain_list = []
        for key in list(onchain):  # format fields
            if key not in ['id', 'time', 'symbol', 'partner_symbol']:
                onchain_list.append(key)
        onchain = onchain_list

        return onchain

    def cc_social_info(self) -> List[str]:
        """
        Converts social stats info to list.

        Returns
        -------
        social: list
            Social stats fields info converted to list.

        """
        social = self.data_resp['Data']  # extract data
        social_list = []
        for key in list(social[0]):  # format fields
            if key not in ['id', 'time', 'symbol', 'partner_symbol']:
                social_list.append(key)
        social = social_list

        return social

    def cc_rate_limit_info(self) -> pd.DataFrame:
        """
        Converts rate limit info to dataframe.

        Returns
        -------
        rate_limit: pd.DataFrame
            Rate limit info converted to dataframe.

        """
        rate_limit = pd.DataFrame(self.data_resp['Data'])  # extract data
        rate_limit.index.name = 'frequency'  # set index name

        return rate_limit

    def cc_news(self) -> pd.DataFrame:
        """
        Converts news feed to dataframe.

        Returns
        -------
        news: pd.DataFrame
            News feed converted to dataframe.

        """
        news = pd.DataFrame(self.data_resp['Data'])  # extract data

        return news

    def cc_news_sources(self) -> pd.DataFrame:
        """
        Converts news sources to dataframe.

        Returns
        -------
        news_sources: pd.DataFrame
            News sources converted to dataframe.

        """
        news_sources = pd.DataFrame(self.data_resp)  # extract data
        news_sources.set_index('key', inplace=True)  # set index

        return news_sources

    def cc_top_mkt_cap_info(self) -> List[str]:
        """
        Converts top market cap coins info to list.

        Returns
        -------
        top_mkt_cap: pd.DataFrame
            Top market cap coins list.

        """
        top_mkt_cap = pd.DataFrame(self.data_resp['Data'])  # extract data
        tickers = []  # create list of tickers
        for i in top_mkt_cap['CoinInfo']:
            if isinstance(i, dict):
                tickers.append(i['Name'])
        top_mkt_cap = tickers

        return top_mkt_cap

    def cm_meta_resp(self, as_list: bool = False, index_name: Optional[str] = None) -> Union[pd.DataFrame, List[str]]:
        """
        Converts CoinMetrics client metadata response to dataframe or list.

        Parameters
        ----------
        as_list: bool, default False
            Returns metadata as list.
        index_name: str, optional, default None
            Name to give index col.

        Returns
        -------
        meta: pd.DataFrame or list
            Metadata converted to dataframe or list.

        """
        meta = pd.DataFrame(self.data_resp)  # store in df
        meta.set_index(meta.columns[0], inplace=True)  # set index
        if index_name is not None:
            meta.index.name = index_name  # name index col
        # as list
        if as_list:
            meta = list(meta.index)

        return meta

    def cm_inst_info(self, as_dict: bool = False) -> Union[Dict[str, List[str]], pd.DataFrame]:
        """ 
        Converts CoinMetrics client institutions metadata response to dataframe or list.

        Parameters
        ----------
        as_dict: bool, default False
            Returns metadata as dictionary.

        Returns
        -------
        meta: pd.DataFrame or dict
            Metadata converted to dataframe or dictionary.

        """
        inst = pd.DataFrame(self.data_resp)  # store in df
        # indexes list
        if as_dict:
            inst_dict = {}
            for institution in inst.institution:
                metrics_list = []
                for metrics in inst.metrics[0]:
                    metrics_list.append(metrics["metric"])
                inst_dict[institution] = metrics_list
            return inst_dict
        else:
            return inst

    def ip_idx_info(self, cat: Optional[str] = None, as_dict: bool = False) \
            -> Union[Dict[str, List[str]], pd.DataFrame]:
        """
        Wrangle InvestPy indexes info.

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
        # wrangle data resp
        indexes = self.data_resp.rename(columns={"symbol": "ticker"})
        # set index and sort
        indexes.set_index("ticker", inplace=True)
        indexes.sort_index(inplace=True)

        # categories
        cats = {"cmdty": "commodities", "rates": "bonds", "eqty": ""}

        # indexes dict
        if as_dict:
            idx_dict = {}
            for k in cats.keys():
                if k == "eqty":
                    idx_dict[k] = indexes[
                        (indexes["class"] != "commodities")
                        & (indexes["class"] != "bonds")
                        ].index.to_list()
                else:
                    idx_dict[k] = indexes[
                        indexes["class"] == cats[k]
                        ].index.to_list()

            # filter by cat
            if cat is not None:
                indexes = idx_dict[cat]
            else:
                indexes = idx_dict

        else:
            # filter df by cat
            if cat is not None:
                if cat == "eqty":
                    indexes = indexes[
                        (indexes["class"] != "commodities")
                        & (indexes["class"] != "bonds")
                        ]
                else:
                    indexes = indexes[indexes["class"] == cats[cat]]

        return indexes

    def ip_meta_resp(self, data_type: str, as_list: bool = False) -> Union[List[str], pd.DataFrame]:
        """
        Wrangle InvestPy fx info.

        Parameters
        ----------
        data_type: str
            Type of data response to wrangle.
        as_list: bool, default False
            Returns available indexes as list.

        Returns
        -------
        fx: list or pd.DataFrame
            List or dataframe with fx info.

        """
        meta = None

        if data_type == 'fx' or data_type == 'rates' or data_type == 'cmdty':
            meta = self.data_resp.rename(columns={"name": "ticker"})
            meta.set_index('ticker', inplace=True)

        elif data_type == 'indexes' or data_type == 'eqty' or data_type == 'etfs':
            meta = self.data_resp.rename(columns={"symbol": "ticker"})
            meta.set_index('ticker', inplace=True)

        # as list
        if as_list:
            meta = meta.index.to_list()

        return meta

    def gn_assets_info(self, as_list: bool = False) -> Union[pd.DataFrame, list]:
        """
        Converts assets info into dataframe or list.

        Parameters
        ----------
        as_list: bool, default False
            Returns info as list.

        Returns
        -------
        assets: pd.DataFrame or list
            Assets info converted to dataframe or list.

        """
        # format response
        assets = pd.DataFrame(self.data_resp)
        # rename cols and set index
        assets.rename(columns={'symbol': 'ticker'}, inplace=True)
        assets = assets.set_index('ticker')
        # asset list
        if as_list:
            assets = list(assets.index)

        return assets

    def gn_fields_info(self, data_type: Optional[str] = None, as_list: bool = False) -> Union[List[str], pd.DataFrame]:
        """
        Converts fields info into dataframe or list.

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
        # format response
        fields = pd.DataFrame(self.data_resp)
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


class WrangleData:
    """
    Wrangles time series data responses from various APIs into tidy data format.
    """

    def __init__(self, data_req: DataRequest, data_resp: Union[Dict[str, Any], pd.DataFrame]):
        """
        Constructor

        Parameters
        ----------
        data_req: DataRequest
            Data request object with parameter values.
        data_resp: pd.DataFrame
            Dataframe containing data response.

        """
        self.data_req = data_req
        self.data_resp = data_resp

    def cryptocompare(self) -> pd.DataFrame:
        """
        Wrangles CryptoCompare data response to dataframe with tidy data format.

        Returns
        -------
        pd.DataFrame
            Wrangled dataframe into tidy data format.

        """
        # convert fields to lib
        self.convert_fields_to_lib(data_source='cryptocompare')
        # re-order cols
        if 'volume' in self.data_resp.columns:  # ohlcv data resp
            self.data_resp = self.data_resp.loc[:, ['date', 'open', 'high', 'low', 'close', 'volume']]
        elif 'volume' not in self.data_resp.columns and 'close' in self.data_resp.columns:  # indexes data resp
            self.data_resp = self.data_resp.loc[:, ['date', 'open', 'high', 'low', 'close']]
        # convert to datetime
        self.data_resp['date'] = pd.to_datetime(self.data_resp['date'], unit='s')
        # set index
        self.data_resp = self.data_resp.set_index('date').sort_index()
        # filter dates
        self.filter_dates()
        # resample
        self.data_resp = self.data_resp.resample(self.data_req.freq).last()
        # type conversion
        self.data_resp = self.data_resp.apply(pd.to_numeric, errors='coerce').convert_dtypes()
        # remove bad data
        self.data_resp = self.data_resp[self.data_resp != 0]  # 0 values
        # filter dups and NaNs
        self.data_resp = self.data_resp[~self.data_resp.index.duplicated()]  # duplicate rows
        self.data_resp = self.data_resp.dropna(how='all').dropna(how='all', axis=1)  # entire row or col NaNs

        return self.data_resp

    def coinmetrics(self) -> pd.DataFrame:
        """
        Wrangles CoinMetrics data response to dataframe with tidy data format.

        Returns
        -------
        pd.DataFrame
            Wrangled dataframe into tidy data format.

        """
        # convert fields to lib
        self.convert_fields_to_lib(data_source='coinmetrics')
        #  convert to datetime
        self.data_resp['date'] = pd.to_datetime(self.data_resp['date'])
        # convert tickers
        if 'ticker' in self.data_resp.columns and all(self.data_resp.ticker.str.contains('-')):
            self.data_resp['ticker'] = self.data_resp.ticker.str.split(pat='-', expand=True)[1].str.upper()
        if 'ticker' in self.data_resp.columns and self.data_req.mkt_type == 'perpetual_future':
            self.data_resp['ticker'] = self.data_resp.ticker.str.replace('USDT', '')
        elif 'ticker' in self.data_resp.columns:
            self.data_resp['ticker'] = self.data_resp.ticker.str.upper()
        # set index
        if 'ticker' in self.data_resp.columns:
            self.data_resp = self.data_resp.set_index(['date', 'ticker']).sort_index()
        elif 'institution' in self.data_resp.columns:  # inst resp
            self.data_resp = self.data_resp.set_index(['date', 'institution']).sort_index()
        # reorder cols
        if 'open' in self.data_resp.columns:
            self.data_resp = self.data_resp.loc[:, ['open', 'high', 'low', 'close', 'volume', 'vwap']]
        # resample
        if self.data_req.freq == 'tick':
            pass
        elif 'institution' in self.data_resp.index.names:
            self.data_resp = self.data_resp.groupby([pd.Grouper(level='date', freq=self.data_req.freq),
                                                     pd.Grouper(level='institution')]).last()
        else:
            self.data_resp = self.data_resp.groupby([pd.Grouper(level='date', freq=self.data_req.freq),
                                                     pd.Grouper(level='ticker')]).last()
        # reformat index
        if self.data_req.freq in ['d', 'w', 'm', 'q']:
            self.data_resp.reset_index(inplace=True)
            self.data_resp.date = pd.to_datetime(self.data_resp.date.dt.date)
            # reset index
            if 'institution' in self.data_resp.columns:  # institution resp
                self.data_resp = self.data_resp.set_index(['date', 'institution']).sort_index()
            else:
                self.data_resp = self.data_resp.set_index(['date', 'ticker']).sort_index()
        # type conversion
        if self.data_req.freq == 'tick':
            pass
        else:
            self.data_resp = self.data_resp.apply(pd.to_numeric, errors='coerce').convert_dtypes()
        # remove bad data
        self.data_resp = self.data_resp[self.data_resp != 0]  # 0 values
        self.data_resp = self.data_resp[~self.data_resp.index.duplicated()]  # duplicate rows
        self.data_resp = self.data_resp.dropna(how='all').dropna(how='all', axis=1)  # entire row or col NaNs

        return self.data_resp

    def glassnode(self, field: str) -> pd.DataFrame:
        """
        Wrangles Glassnode data response to dataframe with tidy data format.

        Parameters
        ----------
        field: str
            Name of requested field.

        Returns
        -------
        pd.DataFrame
            Wrangled dataframe into tidy data format.

        """
        # create df
        self.data_resp = pd.DataFrame(self.data_resp)
        # convert cols
        if 'v' in self.data_resp.columns:  # on and off-chain data resp
            self.data_resp.rename(columns={'v': field}, inplace=True)
            self.data_resp = self.data_resp.loc[:, ['t', field]]
            # convert fields to lib
            self.convert_fields_to_lib(data_source='glassnode')
        elif 'o' in self.data_resp.columns:  # ohlcv data resp
            self.data_resp = pd.concat([self.data_resp.t, self.data_resp['o'].apply(pd.Series)], axis=1)
            self.data_resp.rename(columns={'t': 'date', 'o': 'open', 'h': 'high', 'c': 'close', 'l': 'low'},
                                  inplace=True)
            self.data_resp = self.data_resp.loc[:, ['date', 'open', 'high', 'low', 'close']]

        # convert to datetime
        self.data_resp['date'] = pd.to_datetime(self.data_resp['date'], unit='s')
        # set index
        self.data_resp = self.data_resp.set_index('date').sort_index()
        # filter dates
        self.filter_dates()
        # resample
        self.data_resp = self.data_resp.resample(self.data_req.freq).last()
        # type conversion
        self.data_resp = self.data_resp.apply(pd.to_numeric, errors='coerce').convert_dtypes()
        # remove bad data
        self.data_resp = self.data_resp[self.data_resp != 0]  # 0 values
        self.data_resp = self.data_resp[~self.data_resp.index.duplicated()]  # duplicate rows
        self.data_resp = self.data_resp.dropna(how='all').dropna(how='all', axis=1)  # entire row or col NaNs

        return self.data_resp

    def tiingo(self, data_type: str) -> pd.DataFrame:
        """
        Wrangles Tiingo data response to dataframe with tidy data format.

        Parameters
        ----------
        data_type: str, {'eqty', 'iex', 'crypto', 'fx'}
            Data type to wrangle.

        Returns
        -------
        pd.DataFrame
            Wrangled dataframe into tidy data format.

        """
        # create df
        if data_type == 'eqty' or data_type == 'crypto':
            self.data_resp = pd.DataFrame(self.data_resp[0]['priceData'])
        else:
            self.data_resp = pd.DataFrame(self.data_resp)
        # convert fields to lib
        self.convert_fields_to_lib(data_source='tiingo')
        # convert to datetime
        self.data_resp['date'] = pd.to_datetime(self.data_resp['date'])
        # set index
        self.data_resp = self.data_resp.set_index('date').sort_index()
        self.data_resp.index = self.data_resp.index.tz_localize(None)
        # resample
        self.data_resp = self.data_resp.resample(self.data_req.freq).last()
        # reformat index
        if self.data_req.freq in ['d', 'w', 'm', 'q']:
            self.data_resp.reset_index(inplace=True)
            self.data_resp.date = pd.to_datetime(self.data_resp.date.dt.date)
            # reset index
            self.data_resp.set_index('date', inplace=True)
        # type conversion
        self.data_resp = self.data_resp.apply(pd.to_numeric, errors='coerce').convert_dtypes()
        # remove bad data
        self.data_resp = self.data_resp[~self.data_resp.index.duplicated()]  # duplicate rows
        self.data_resp = self.data_resp.dropna(how='all').dropna(how='all', axis=1)  # entire row or col NaNs

        return self.data_resp

    def dbnomics(self) -> pd.DataFrame:
        """
        Wrangles DBnomics data response to dataframe with tidy data format.

        Returns
        -------
        pd.DataFrame
            Wrangled dataframe into tidy data format.

        """
        # df = self.convert_cols().convert_fields_to_lib().convert_to_datetime().set_idx().resample_freq(). \
        #     filter_dates().type_conversion().remove_bad_data().data_resp
        # convert fields to lib
        self.convert_fields_to_lib(data_source='dbnomics')
        # convert to datetime
        self.data_resp['date'] = pd.to_datetime(self.data_resp['date'])
        # set index
        self.data_resp = self.data_resp.set_index('date').sort_index()
        # resample
        self.data_resp = self.data_resp.resample(self.data_req.freq).last().ffill()
        # filte dates
        self.filter_dates()
        # type conversion
        self.data_resp = self.data_resp.apply(pd.to_numeric, errors='coerce').convert_dtypes()
        # remove bad data
        self.data_resp = self.data_resp[self.data_resp != 0]  # 0 values
        self.data_resp = self.data_resp[~self.data_resp.index.duplicated()]  # duplicate rows
        self.data_resp = self.data_resp.dropna(how='all').dropna(how='all', axis=1)  # entire row or col NaNs

        return self.data_resp

    def ccxt(self) -> pd.DataFrame:
        """
        Wrangles CCXT data response to dataframe with tidy data format.

        Returns
        -------
        pd.DataFrame
            Wrangled dataframe into tidy data format.

        """

        # df = self.convert_cols().convert_fields_to_lib().convert_to_datetime().set_idx().resample_freq(). \
        #     type_conversion().remove_bad_data().data_resp
        # convert fields to lib
        self.convert_fields_to_lib(data_source='ccxt')
        # convert to datetime
        if 'close' in self.data_resp.columns:
            self.data_resp['date'] = pd.to_datetime(self.data_resp.date, unit='ms')
        elif 'funding_rate' in self.data_resp.columns:
            self.data_resp['date'] = pd.to_datetime(self.data_resp.set_index('date').index). \
                floor('S').tz_localize(None)
        # set index
        self.data_resp = self.data_resp.set_index('date').sort_index()
        # resample
        if 'funding_rate' in self.data_resp.columns and self.data_req.freq in ['d', 'w', 'm', 'q', 'y']:
            self.data_resp = (self.data_resp.funding_rate + 1).cumprod().resample(self.data_req.freq).last(). \
                diff().to_frame()
        # type conversion
        self.data_resp = self.data_resp.apply(pd.to_numeric, errors='coerce').convert_dtypes()
        # remove bad data
        self.data_resp = self.data_resp[self.data_resp != 0]  # 0 values
        self.data_resp = self.data_resp[~self.data_resp.index.duplicated()]  # duplicate rows
        self.data_resp = self.data_resp.dropna(how='all').dropna(how='all', axis=1)  # entire row or col NaNs

        return self.data_resp

    def investpy(self) -> pd.DataFrame:
        """
        Wrangles InvestPy data response to dataframe with tidy data format.

        Returns
        -------
        pd.DataFrame
            Wrangled dataframe into tidy data format.

        """
        # convert cols
        if self.data_req.cat != 'macro':
            # reset index
            self.data_resp = self.data_resp.reset_index()
        else:
            # parse date and time to create datetime
            self.data_resp.time.replace('Tentative', '23:55', inplace=True)
            self.data_resp['date'] = pd.to_datetime(self.data_resp.date + self.data_resp.time,
                                                    format="%d/%m/%Y%H:%M")
        # convert fields to lib
        self.convert_fields_to_lib(data_source='investpy')
        # set index
        self.data_resp = self.data_resp.set_index('date').sort_index()
        # replace % and compute surprise
        if self.data_req.cat == 'macro':
            self.data_resp = self.data_resp.replace('%', '', regex=True).astype(float) / 100  # remove % str
            # replace missing vals
            self.data_resp.expected.fillna(self.data_resp.previous, inplace=True)
            self.data_resp['surprise'] = self.data_resp.actual - self.data_resp.expected
        # resample freq
        self.data_resp = self.data_resp.resample(self.data_req.freq).last().ffill()
        # extend index and fwd fill to current date
        ext_idx = pd.date_range(self.data_resp.index.max(), pd.Timestamp.today()).union(self.data_resp.index)
        self.data_resp = self.data_resp.reindex(ext_idx).ffill()
        # filte dates
        self.filter_dates()
        # type conversion
        self.data_resp = self.data_resp.apply(pd.to_numeric, errors='coerce').convert_dtypes()
        # remove bad data
        self.data_resp = self.data_resp[~self.data_resp.index.duplicated()]  # duplicate rows
        self.data_resp = self.data_resp.dropna(how='all').dropna(how='all', axis=1)  # entire row or col NaNs

        return self.data_resp

    def fred(self) -> pd.DataFrame:
        """
        Wrangles Fred data response to dataframe with tidy data format.

        Returns
        -------
        pd.DataFrame
            Wrangled dataframe into tidy data format.

        """
        # convert tickers to cryptodatapy format
        self.data_resp.columns = self.data_req.tickers  # convert tickers to cryptodatapy format
        # resample to match end of reporting period, not beginning
        self.data_resp = self.data_resp.resample('d').last().ffill().resample(self.data_req.freq).last().stack(). \
            to_frame().reset_index()
        # convert cols
        if self.data_req.cat == 'macro':
            self.data_resp.columns = ['DATE', 'symbol', 'actual']
        else:
            self.data_resp.columns = ['DATE', 'symbol', 'close']
        # convert fields to lib
        self.convert_fields_to_lib(data_source='fred')
        # set index
        self.data_resp.set_index(['date', 'ticker'], inplace=True)
        # type conversion
        self.data_resp = self.data_resp.apply(pd.to_numeric, errors='coerce').convert_dtypes()
        # remove bad data
        self.data_resp = self.data_resp[self.data_resp != 0]  # 0 values
        self.data_resp = self.data_resp[~self.data_resp.index.duplicated()]  # duplicate rows
        self.data_resp = self.data_resp.dropna(how='all').dropna(how='all', axis=1)  # entire row or col NaNs

        return self.data_resp

    def yahoo(self) -> pd.DataFrame:
        """
        Wrangles Yahoo data response to dataframe with tidy data format.

        Returns
        -------
        pd.DataFrame
            Wrangled dataframe into tidy data format.

        """
        # stack and reset index
        self.data_resp = self.data_resp.stack().reset_index()
        # remove name
        self.data_resp.columns.name = None
        # convert fields
        self.convert_fields_to_lib(data_source='yahoo')
        # convert to datetime
        self.data_resp['date'] = pd.to_datetime(self.data_resp['date'])
        # set index
        self.data_resp.set_index(['date', 'ticker'], inplace=True)
        # re-order cols
        self.data_resp = self.data_resp.loc[:, ['open', 'high', 'low', 'close', 'close_adj', 'volume']]
        # type conversion
        self.data_resp = self.data_resp.apply(pd.to_numeric, errors='coerce').convert_dtypes()
        # remove bad data
        self.data_resp = self.data_resp[self.data_resp != 0]  # 0 values
        self.data_resp = self.data_resp[~self.data_resp.index.duplicated()]  # duplicate rows
        self.data_resp = self.data_resp.dropna(how='all').dropna(how='all', axis=1)  # entire row or col NaNs

        return self.data_resp

    def convert_fields_to_lib(self, data_source: str) -> WrangleData:
        """
        Convert cols/fields from data source data resp to CryptoDataPy format.

        Parameters
        ----------
        data_source: str
            Name of data source.

        Returns
        -------
        WrangleData
            WrangleData object with data_resp fields converted to CryptoDataPy format.

        """
        # fields dictionary
        with resources.path('cryptodatapy.conf', 'fields.csv') as f:
            fields_dict_path = f
        # get fields and data resp
        fields_df = pd.read_csv(fields_dict_path, index_col=0, encoding='latin1').copy()
        fields_list = fields_df[str(data_source) + '_id'].to_list()

        # loop through data resp cols
        for col in self.data_resp.columns:
            if self.data_req.source_fields is not None and col in self.data_req.source_fields:
                pass
            elif col in fields_list or col.title() in fields_list or col.lower() in fields_list:
                self.data_resp.rename(columns={col: fields_df[(fields_df[str(data_source) + '_id']
                                      == col.title()) |
                                    (fields_df[str(data_source) + '_id'] == col.lower()) |
                                    (fields_df[str(data_source) + '_id'] == col)].index[0]}, inplace=True)
            elif col == 'index':
                self.data_resp.rename(columns={'index': 'ticker'}, inplace=True)  # rename index col
            elif col == 'asset':
                self.data_resp.rename(columns={'asset': 'ticker'}, inplace=True)  # rename asset col
            elif col == 'level':
                self.data_resp.rename(columns={'level': 'close'}, inplace=True)  # rename level col
            elif col == 'institution':
                pass
            else:
                self.data_resp.drop(columns=[col], inplace=True)

        return self

    def filter_dates(self) -> WrangleData:
        """
        Filter start and end dates to those in data request.

        Parameters
        ----------
        self: WrangleData
            WrangleData object with filtered dates.

        Returns
        -------
        WrangleData
            WrangleData object with data_resp dates filtered.

        """
        if self.data_req.start_date is not None:
            self.data_resp = self.data_resp[(self.data_resp.index >= self.data_req.start_date)]
        if self.data_req.end_date is not None:
            self.data_resp = self.data_resp[(self.data_resp.index <= self.data_req.end_date)]

        return self
