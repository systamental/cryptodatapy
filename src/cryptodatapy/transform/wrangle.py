import numpy as np
import pandas as pd
from cryptodatapy.extract.datarequest import DataRequest
from cryptodatapy.transform.convertparams import ConvertParams
from importlib import resources


class WrangleData():
    """
    Wrangles data responses from various APIs into tidy data format.
    """

    def __init__(self, data_req: DataRequest, data_resp: pd.DataFrame, data_source: str):
        """
        Constructor

        Parameters
        ----------
        data_req: DataRequest
            Data request object with parameter values.
        data_resp: pd.DataFrame
            Dataframe containing data response.
        data_source: str
            Name of data source.
        """
        self.data_req = data_req
        self.data_resp = data_resp
        self.data_source = data_source

    def convert_cols(self) -> pd.DataFrame:
        """
        Convert columns to CryptoDataPy format

        Returns:
        -------
        self: WrangleData
            WrangleData object with data_resp cols converted to CryptoDataPy format.
        """
        if self.data_source == 'coinmetrics':
            # reorder cols
            if 'open' in self.data_resp.columns:
                self.data_resp = self.data_resp.loc[:, ['open', 'high', 'low', 'close', 'volume', 'vwap']]

        elif self.data_source == 'cryptocompare':
            # format col order
            if 'volume' in self.data_resp.columns:  # ohlcv data resp
                self.data_resp = self.data_resp.loc[:, ['date', 'open', 'high', 'low', 'close', 'volume']]
            elif 'volume' not in self.data_resp.columns and 'close' in self.data_resp.columns:  # indexes data resp
                self.data_resp = self.data_resp.loc[:, ['date', 'open', 'high', 'low', 'close']]

        elif self.data_source == 'glassnode':
            # format cols
            self.data_resp.rename(columns={'t': 'date'}, inplace=True)
            if 'o' in self.data_resp.columns:  # ohlcv data resp
                self.data_resp = pd.concat([self.data_resp.date, self.data_resp['o'].apply(pd.Series)], axis=1)
                self.data_resp.rename(columns={'o': 'open', 'h': 'high', 'c': 'close', 'l': 'low'}, inplace=True)
                self.data_resp = self.data_resp.loc[:, ['date', 'open', 'high', 'low', 'close']]
            elif 'v' in self.data_resp.columns:  # on-chain and off-chain data resp
                self.data_resp = self.data_resp.loc[:, ['date', 'v']]

        elif self.data_source == 'fred':
            # rename cols
            if self.data_req.cat == 'macro':
                self.data_resp.columns = ['DATE', 'symbol', 'actual']
            else:
                self.data_resp.columns = ['DATE', 'symbol', 'close']

        elif self.data_source == 'investpy':
            if self.data_req.cat != 'macro':
                # reset index
                self.data_resp = self.data_resp.reset_index()
            else:
                # parse date and time to create datetime
                self.data_resp.time.replace('Tentative', '23:55', inplace=True)
                self.data_resp['date'] = pd.to_datetime(self.data_resp.date + self.data_resp.time,
                                                        format="%d/%m/%Y%H:%M")
                # replace missing vals
                self.data_resp.forecast = np.where(np.nan, self.data_resp.previous, self.data_resp.forecast)

        return self

    def convert_fields_to_lib(self) -> pd.DataFrame:
        """
        Convert cols/fields from data source data resp to CryptoDataPy format.

        Returns:
        -------
        self: WrangleData object
            WrangleData object with data_resp fields converted to CryptoDataPy format.
        """
        # fields dictionary
        with resources.path('cryptodatapy.conf', 'fields.csv') as f:
            fields_dict_path = f
        # get fields and data resp
        fields_df = pd.read_csv(fields_dict_path, index_col=0, encoding='latin1').copy()
        fields_list = fields_df[str(self.data_source) + '_id'].to_list()

        # loop through data resp cols
        for col in self.data_resp.columns:
            if self.data_req.source_fields is not None and col in self.data_req.source_fields:
                pass
            elif col in fields_list or col.title() in fields_list or col.lower() in fields_list:
                self.data_resp.rename(columns={col: fields_df[(fields_df[str(self.data_source) + '_id']
                                      == col.title()) |
                                    (fields_df[str(self.data_source) + '_id'] == col.lower()) |
                                    (fields_df[str(self.data_source) + '_id'] == col)].index[0]}, inplace=True)
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

    def convert_to_datetime(self) -> pd.DataFrame:
        """
        Convert date to Datetime.

        Returns:
        -------
        self: WrangleData object
            WrangleData object with data_resp date converted to datetime.
        """
        if self.data_source == 'coinmetrics' or self.data_source == 'av-daily' or self.data_source == 'av-forex-daily'\
                or self.data_source == 'tiingo':
            self.data_resp['date'] = pd.to_datetime(self.data_resp['date'])

        elif self.data_source == 'cryptocompare' or self.data_source == 'glassnode':
            self.data_resp['date'] = pd.to_datetime(self.data_resp['date'], unit='s')

        elif self.data_source == 'ccxt':
            if 'close' in self.data_resp.columns:
                self.data_resp['date'] = pd.to_datetime(self.data_resp.date, unit='ms')
            elif 'funding_rate' in self.data_resp.columns:
                self.data_resp['date'] = pd.to_datetime(self.data_resp.set_index('date').index). \
                    floor('S').tz_localize(None)

        return self

    def convert_tickers(self) -> pd.DataFrame:
        """
        Convert tickers to CryptoDataPy format.

        Returns:
        -------
        self: WrangleData object
            WrangleData object with data_resp tickers converted.
        """
        # convert quote ccy
        quote_ccy = ConvertParams(self.data_req, self.data_source).convert_quote_ccy_to_source()

        if self.data_source == 'coinmetrics':
            if 'ticker' in self.data_resp.columns and all(self.data_resp.ticker.str.contains('-')):
                self.data_resp['ticker'] = self.data_resp.ticker.str.split(pat='-', expand=True)[1].str.upper()
            if 'ticker' in self.data_resp.columns and self.data_req.mkt_type == 'perpetual_future':
                self.data_resp['ticker'] = self.data_resp.ticker.str.replace(quote_ccy.upper(), '')
            elif 'ticker' in self.data_resp.columns:
                self.data_resp['ticker'] = self.data_resp.ticker.str.upper()

        return self

    def set_idx(self) -> pd.DataFrame:
        """
        Set index.

        Returns:
        -------
        self: WrangleData object
            WrangleData object with data_resp index set.
        """
        if self.data_source == 'coinmetrics':
            if 'ticker' in self.data_resp.columns:
                self.data_resp = self.data_resp.set_index(['date', 'ticker']).sort_index()
            elif 'institution' in self.data_resp.columns:  # inst resp
                self.data_resp = self.data_resp.set_index(['date', 'institution']).sort_index()

        elif self.data_source == 'ccxt' or self.data_source == 'av-daily' or self.data_source == 'av-forex-daily' or \
                self.data_source == 'dbnomics' or self.data_source == 'investpy' or self.data_source == 'cryptocompare'\
                or self.data_source == 'glassnode':
            self.data_resp = self.data_resp.set_index('date').sort_index()

        elif self.data_source == 'tiingo':
            self.data_resp = self.data_resp.set_index('date').sort_index()
            self.data_resp.index = self.data_resp.index.tz_localize(None)

        elif self.data_source == 'fred' or self.data_source == 'yahoo':
            self.data_resp.set_index(['date', 'ticker'], inplace=True)

        return self

    def reformat_idx(self) -> pd.DataFrame:
        """
        Reformat index.

        Returns:
        -------
        self: WrangleData object
            WrangleData object with data_resp index reformatted.
        """
        if self.data_source == 'coinmetrics' or self.data_source == 'tiingo':
            if self.data_req.freq in ['d', 'w', 'm', 'q']:
                self.data_resp.reset_index(inplace=True)
                self.data_resp.date = pd.to_datetime(self.data_resp.date.dt.date)
                # reset index
                if 'institution' in self.data_resp.columns:  # institution resp
                    self.data_resp = self.data_resp.set_index(['date', 'institution']).sort_index()
                elif self.data_source == 'coinmetrics':
                    self.data_resp = self.data_resp.set_index(['date', 'ticker']).sort_index()
                else:
                    self.data_resp.set_index('date', inplace=True)

        return self

    def filter_dates(self) -> pd.DataFrame:
        """
        Filter start and end dates to those in data request.

        Returns:
        -------
        self: WrangleData object
            WrangleData object with data_resp dates filtered.
        """
        if self.data_req.start_date is not None:
            self.data_resp = self.data_resp[(self.data_resp.index >= self.data_req.start_date)]
        if self.data_req.end_date is not None:
            self.data_resp = self.data_resp[(self.data_resp.index <= self.data_req.end_date)]

        return self

    def resample_freq(self) -> pd.DataFrame:
        """
        Resample data frequency.

        Returns:
        -------
        self: WrangleData object
            WrangleData object with data_resp frequency resampled.
        """
        if self.data_source == 'coinmetrics':
            if self.data_req.freq == 'tick':
                pass
            elif 'institution' in self.data_resp.index.names:
                self.data_resp = self.data_resp.groupby([pd.Grouper(level='date', freq=self.data_req.freq),
                                                         pd.Grouper(level='institution')]).last()
            else:
                self.data_resp = self.data_resp.groupby([pd.Grouper(level='date', freq=self.data_req.freq),
                                                         pd.Grouper(level='ticker')]).last()

        elif self.data_source == 'cryptocompare' or self.data_source == 'glassnode' or self.data_source == 'tiingo':
            self.data_resp = self.data_resp.resample(self.data_req.freq).last()

        elif self.data_source == 'ccxt':
            if 'funding_rate' in self.data_resp.columns and self.data_req.freq in ['d', 'w', 'm', 'q', 'y']:
                self.data_resp = (self.data_resp.funding_rate + 1).cumprod().resample(self.data_req.freq).last().\
                    diff().to_frame()

        elif self.data_source == 'investpy':
            # str and resample to higher freq for econ releases
            if self.data_req.cat == 'macro':
                self.data_resp = self.data_resp.replace('%', '', regex=True).astype(float) / 100  # remove % str
                self.data_resp['surprise'] = self.data_resp.actual - self.data_resp.expected
            # resample freq
            self.data_resp = self.data_resp.resample(self.data_req.freq).last().ffill()

        elif self.data_source == 'dbnomics':
            self.data_resp = self.data_resp.resample(self.data_req.freq).last().ffill()

        elif self.data_source == 'fred':
            # resample to match end of reporting period, not beginning
            self.data_resp = self.data_resp.resample('d').last().ffill().resample(self.data_req.freq).last().stack().\
                to_frame().reset_index()

        return self

    def remove_bad_data(self) -> pd.DataFrame:
        """
        Remove duplicate rows.

        Returns:
        -------
        self: WrangleData object
            WrangleData object with bad data removed from data_resp.
        """
        # filter 0s
        if self.data_source == 'investpy':
            if 'surprise' in self.data_resp.columns:
                self.data_resp = \
                    pd.concat([self.data_resp[self.data_resp.columns.drop('surprise')][self.data_resp != 0],
                               self.data_resp.loc[:, ['surprise']]], axis=1)
        else:
            self.data_resp = self.data_resp[self.data_resp != 0]  # 0 values
        # filter dups and NaNs
        self.data_resp = self.data_resp[~self.data_resp.index.duplicated()]  # duplicate rows
        self.data_resp = self.data_resp.dropna(how='all').dropna(how='all', axis=1)  # entire row or col NaNs

        return self

    def type_conversion(self) -> pd.DataFrame:
        """
        Convert data types.

        Returns:
        -------
        self: WrangleData object
            WrangleData object with data_resp dtypes converted.
        """
        if self.data_source == 'coinmetrics' and self.data_req.freq == 'tick':
            pass
        else:
            self.data_resp = self.data_resp.apply(pd.to_numeric, errors='coerce').convert_dtypes()

        return self

    def tidy_data(self) -> pd.DataFrame:
        """
        Wrangle dataframe into tidy data format.

        Returns:
        -------
        df: pd.DataFrame
            Dataframe in tidy data format.
        """
        df = None

        if self.data_source == 'coinmetrics':
            df = self.convert_fields_to_lib().convert_to_datetime().convert_tickers().set_idx().convert_cols().\
                resample_freq().reformat_idx().type_conversion().remove_bad_data().data_resp

        elif self.data_source == 'cryptocompare':
            df = self.convert_fields_to_lib().convert_cols().convert_to_datetime().set_idx().filter_dates().\
                resample_freq().type_conversion().remove_bad_data().data_resp

        elif self.data_source == 'glassnode':
            df = self.convert_cols().convert_to_datetime().set_idx().filter_dates().resample_freq().type_conversion().\
                remove_bad_data().data_resp

        elif self.data_source == 'tiingo':
            df = self.convert_fields_to_lib().convert_to_datetime().set_idx().resample_freq().reformat_idx().\
                type_conversion().remove_bad_data().data_resp

        elif self.data_source == 'ccxt':
            df = self.convert_cols().convert_fields_to_lib().convert_to_datetime().set_idx().resample_freq().\
                type_conversion().remove_bad_data().data_resp

        elif self.data_source == 'investpy':
            df = self.convert_cols().convert_fields_to_lib().convert_to_datetime().set_idx().resample_freq().\
                type_conversion().remove_bad_data().data_resp

        elif self.data_source == 'dbnomics':
            df = self.convert_cols().convert_fields_to_lib().convert_to_datetime().set_idx().resample_freq().\
                filter_dates().type_conversion().remove_bad_data().data_resp

        elif self.data_source == 'fred':
            self.data_resp.columns = self.data_req.tickers  # convert tickers to cryptodatapy format
            df = self.resample_freq().convert_cols().convert_fields_to_lib().convert_to_datetime().set_idx().\
                type_conversion().remove_bad_data().data_resp

        elif self.data_source == 'av-daily' or self.data_source == 'av-forex-daily':
            self.data_resp.reset_index(inplace=True)
            df = self.convert_cols().convert_fields_to_lib().convert_to_datetime().set_idx().type_conversion().\
                remove_bad_data().data_resp

        elif self.data_source == 'yahoo':
            self.data_resp = self.data_resp.stack().reset_index()
            self.data_resp.columns.name = None
            df = self.convert_cols().convert_fields_to_lib().convert_to_datetime().set_idx().type_conversion().\
                remove_bad_data().data_resp

        return df
