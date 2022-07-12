import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import Union, Optional
from importlib import resources
from cryptodatapy.data_requests.datarequest import DataRequest


class ConvertParams():
    """
    Converts data request parameters from CryptoDataPy to data source format.
    """

    def __init__(
            self,
            data_source: str = None,
    ):
        """
        Constructor

        Parameters
        ----------
        data_source: str
            Name of data source, e.g. 'cryptocompare', 'coinmetrics', 'glassnode', etc.
        """
        self.data_source = data_source

    def convert_tickers_to_source(self, data_req: DataRequest) -> list[str]:
        """
        Converts tickers from CryptoDataPy to data source format.
        """
        if self.data_source == 'cryptocompare':
            tickers = [ticker.upper() for ticker in data_req.tickers]
        elif self.data_source == 'coinmetrics':
            tickers = [ticker.lower() for ticker in data_req.tickers]
        else:
            tickers = data_req.tickers

        return tickers

    def convert_tickers_to_mkts_source(self, data_req: DataRequest) -> list[str]:
        """
        Converts from asset tickers to data source market tickers.
        """
        # convert tickers to markets
        mkts_list = []
        tickers, quote_ccy, exch, mkt_type = data_req.tickers, data_req.quote_ccy, data_req.exch, data_req.mkt_type

        if self.data_source == 'coinmetrics':
            for ticker in tickers:
                if mkt_type == 'spot':
                    mkts_list.append(exch + '-' + ticker.lower() + '-' + quote_ccy.lower() + '-' + mkt_type.lower())
                elif mkt_type == 'perpetual_future':
                    if exch == 'binance' or exch == 'bybit' or exch == 'bitmex':
                        mkts_list.append(exch + '-' + ticker.upper() + quote_ccy.upper() + '-' + 'future')
                    elif exch == 'ftx':
                        mkts_list.append(exch + '-' + ticker.upper() + '-' + 'PERP' + '-' + 'future')
                    elif exch == 'okex':
                        mkts_list.append(exch + '-' + ticker.upper() + '-' + quote_ccy.upper() + '-' + 'SWAP' + '-'
                                         + 'future')
                    elif exch == 'huobi':
                        mkts_list.append(exch + '-' + ticker.upper() + '-' + quote_ccy.upper() + '_' + 'SWAP' + '-'
                                         + 'future')
                    elif exch == 'hitbtc':
                        mkts_list.append(exch + '-' + ticker.upper() + quote_ccy.upper() + '_' + 'PERP' + '-' +
                                         'future')
                # TO DO: create deliverable future and option markets list
                elif mkt_type == 'future':
                    pass
                elif mkt_type == 'option':
                    pass
        else:
            mkts_list = None

        return mkts_list

    def convert_freq_to_source(self, data_req: DataRequest) -> str:
        """
        Converts frequencies from CryptoDataPy to data source format.
        """
        if self.data_source == 'cryptocompare':
            if data_req.freq[-3:] == 'min':
                freq = 'histominute'
            elif data_req.freq[-1] == 'h':
                freq = 'histohour'
            else:
                freq = 'histoday'

        elif self.data_source == 'coinmetrics':
            if data_req.freq == 'block':
                freq = '1b'
            elif data_req.freq == 'tick':
                freq = 'tick'
            elif data_req.freq[-1] == 's':
                freq = '1s'
            elif data_req.freq[-3:] == 'min':
                freq = '1m'
            elif data_req.freq[-1] == 'h':
                freq = '1h'
            else:
                freq = '1d'
        else:
            freq = data_req.freq

        return freq

    def convert_quote_ccy_to_source(self, data_req: DataRequest) -> str:
        """
        Converts quote currency from CryptoDataPy to data source format.
        """
        if self.data_source == 'cryptocompare':
            if data_req.quote_ccy is None:
                quote_ccy = 'USD'
            else:
                quote_ccy = data_req.quote_ccy.upper()

        elif self.data_source == 'coinmetrics':
            if data_req.quote_ccy is None:
                quote_ccy = 'usdt'
            else:
                quote_ccy = data_req.quote_ccy.lower()
        else:
            quote_ccy = data_req.quote_ccy

        return quote_ccy

    def convert_exch_to_source(self, data_req: DataRequest) -> str:
        """
        Converts exchange from CryptoDataPy to data source format.
        """
        if self.data_source == 'cryptocompare':
            if data_req.exch is None:
                exch = 'CCCAGG'
            else:
                exch = data_req.exch

        elif self.data_source == 'coinmetrics':
            if data_req.exch is None:
                exch = 'binance'
            else:
                exch = data_req.exch.lower()

        else:
            exch = data_req.exch

        return exch

    def convert_start_date_to_source(self, data_req: DataRequest) -> Union[str, int, datetime, pd.Timestamp]:
        """
        Converts start date from CryptoDataPy to data source format.
        """
        if self.data_source == 'cryptocompare':
            if data_req.freq[-3:] == 'min':  # cryptocompare limits higher frequency data responses
                start_date = round((datetime.now() - timedelta(days=7)).timestamp())
            # no start date
            elif data_req.start_date is None:
                start_date = round(pd.Timestamp('2009-01-03 00:00:00').timestamp())
            else:
                start_date = round(pd.Timestamp(data_req.start_date).timestamp())

        else:
            start_date = data_req.start_date

        return start_date

    def convert_end_date_to_source(self, data_req: DataRequest) -> Union[str, int, datetime, pd.Timestamp]:
        """
        Converts end date from CryptoDataPy to data source format.
        """
        if self.data_source == 'cryptocompare':
            if data_req.end_date is None:
                end_date = round(pd.Timestamp(datetime.utcnow()).timestamp())
            else:
                end_date = round(pd.Timestamp(data_req.end_date).timestamp())

        else:
            end_date = data_req.end_date

        return end_date

    def convert_fields_to_source(self, data_req: DataRequest) -> list[str]:
        """
        Converts fields from CryptoDataPy to data source format.
        """
        # fields dictionary
        with resources.path('cryptodatapy.conf', 'fields_dict.csv') as f:
            fields_dict_path = f
        df = pd.read_csv(fields_dict_path, index_col=0)

        fields = []

        # source fields can be provided in data req
        if data_req.source_fields is not None:
            fields = data_req.source_fields

        # convert to cryptocompare format
        elif self.data_source == 'cryptocompare':
            for field in data_req.fields:
                try:
                    fields.append(df.loc[field, 'cryptocompare_id'])
                except KeyError as e:
                    logging.warning(e)
                    logging.warning(f"Id for {field} could not be found in data dictionary."
                                    f" Try using source field ids.")

        # convert to coinmetrics format
        elif self.data_source == 'coinmetrics':
            for field in data_req.fields:
                try:
                    fields.append(df.loc[field, 'coinmetrics_id'])
                except KeyError as e:
                    logging.warning(e)
                    logging.warning(f"Id for {field} could not be found in data dictionary."
                                    f" Try using source field ids.")

        return fields

    def convert_tz_to_source(self, data_req: DataRequest) -> str:
        """
        Converts timezone from CryptoDataPy to data source format.
        """
        if self.data_source == 'cryptocompare':
            if data_req.tz is None:
                tz = 'UTC'
            else:
                tz = data_req.tz

        elif self.data_source == 'coinmetrics':
            if data_req.tz is None:
                tz = 'UTC'
            else:
                tz = data_req.tz
        else:
            tz = data_req.tz

        return tz

    def convert_inst_to_source(self, data_req: DataRequest) -> str:
        """
        Converts institution names from CryptoDataPy to data source format.
        """
        if self.data_source == 'coinmetrics':
            if data_req.inst is None:
                inst = 'grayscale'
            else:
                inst = data_req.inst
        else:
            inst = data_req.inst

        return inst

    def convert_to_source(self, data_req: DataRequest) -> dict[str, Union[str, int, list[str]]]:
        """
        Converts data request parameters from CryptoDataPy to data source format.
        """
        tickers = self.convert_tickers_to_source(data_req)
        freq = self.convert_freq_to_source(data_req)
        quote_ccy = self.convert_quote_ccy_to_source(data_req)
        exch = self.convert_exch_to_source(data_req)
        mkt_type = data_req.mkt_type
        mkts_list = self.convert_tickers_to_mkts_source(data_req)
        start_date = self.convert_start_date_to_source(data_req)
        end_date = self.convert_end_date_to_source(data_req)
        fields = self.convert_fields_to_source(data_req)
        tz = self.convert_tz_to_source(data_req)
        inst = self.convert_inst_to_source(data_req)
        cat = data_req.cat
        trials = data_req.trials
        pause = data_req.pause
        source_tickers = data_req.source_tickers
        source_freq = data_req.source_freq
        source_fields = data_req.source_fields

        source_params = {'tickers': tickers, 'freq': freq, 'quote_ccy': quote_ccy, 'exch': exch, 'mkt_type': mkt_type,
                         'mkts': mkts_list, 'start_date': start_date, 'end_date': end_date, 'fields': fields, 'tz': tz,
                         'inst': inst, 'cat': cat, 'trials': trials, 'pause': pause, 'source_tickers': source_tickers,
                         'source_freq': source_freq, 'source_fields': source_fields}

        return source_params

    def convert_fields_to_lib(self, data_req: DataRequest, data_resp: pd.DataFrame) -> pd.DataFrame:
        """
        Converts fields from data source data resp to CrptoDataPy format.
        """
        # fields dictionary
        with resources.path('cryptodatapy.conf', 'fields_dict.csv') as f:
            fields_dict_path = f
        # getdata resp and create empty fields list
        data_df, df, fields = pd.read_csv(fields_dict_path, index_col=0), data_resp.copy(), []

        # convert to cryptodatapy format
        if self.data_source == 'cryptocompare':
            for col in df.columns:
                if data_req.source_fields is not None and col in data_req.source_fields:
                    pass
                elif col in data_df.cryptocompare_id.to_list():
                    df.rename(columns={col: data_df[data_df.cryptocompare_id == col].index[0]}, inplace=True)
                else:
                    df.drop(columns=[col], inplace=True)

        elif self.data_source == 'coinmetrics':
            for col in df.columns:
                if data_req.source_fields is not None and col in data_req.source_fields:
                    pass
                elif col in data_df.coinmetrics_id.to_list():
                    df.rename(columns={col: data_df[data_df.coinmetrics_id == col].index[0]}, inplace=True)
                elif col == 'index':
                    df.rename(columns={'index': 'ticker'}, inplace=True)  # rename index col
                elif col == 'asset':
                    df.rename(columns={'asset': 'ticker'}, inplace=True)  # rename asset col
                elif col == 'level':
                    df.rename(columns={'level': 'close'}, inplace=True)  # rename level col
                elif col == 'institution':
                    pass
                else:
                    df.drop(columns=[col], inplace=True)

        return df
