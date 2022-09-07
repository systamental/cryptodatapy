import logging
import pandas as pd
from cryptodatapy.extract.datarequest import DataRequest
from datetime import datetime, timedelta
from importlib import resources
from typing import Union, Optional


class ConvertParams():
    """
    Converts data request parameters from CryptoDataPy to data source format.
    """
    def __init__(
            self,
            data_req: DataRequest = None,
            data_source: str = None,
    ):
        """
        Constructor

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.
        data_source: str
            Name of data source, e.g. 'cryptocompare', 'coinmetrics', 'glassnode', etc.
        """
        self.data_req = data_req
        self.data_source = data_source

    def convert_tickers_to_source(self) -> list[str]:
        """
        Converts tickers from CryptoDataPy to data source format.

        Returns
        -------
        tickers: list
            List of tickers converted to data source format.
        """
        # tickers
        with resources.path('cryptodatapy.conf', 'tickers.csv') as f:
            tickers_path = f
        # get tickers file and create ticker list
        tickers_df, tickers = pd.read_csv(tickers_path, index_col=0, encoding='latin1'), []

        if self.data_req.source_tickers is not None:
            tickers = self.data_req.source_tickers
            self.data_req.tickers = self.data_req.source_tickers

        elif self.data_source == 'cryptocompare' or self.data_source == 'ccxt' or self.data_source == 'av-daily':
            tickers = [ticker.upper() for ticker in self.data_req.tickers]

        elif self.data_source == 'coinmetrics':
            tickers = [ticker.lower() for ticker in self.data_req.tickers]

        elif self.data_source == 'tiingo':
            tickers = [ticker.lower() for ticker in self.data_req.tickers]

        elif self.data_source == 'investpy':
            if self.data_req.cat == 'eqty':
                for ticker in self.data_req.tickers:
                    if len(ticker) > 4:
                        try:
                            tickers.append(tickers_df.loc[ticker, 'investpy_id'])
                        except KeyError:
                            logging.warning(f"{ticker} not found for {self.data_source} data source. Check tickers in "
                                            f"data catalog and try again.")
                            self.data_req.tickers.remove(ticker)
                    else:
                        tickers.append(ticker.upper())
            elif self.data_req.cat != 'fx':
                for ticker in self.data_req.tickers:
                    try:
                        tickers.append(tickers_df.loc[ticker, 'investpy_id'])
                    except KeyError:
                        logging.warning(f"{ticker} not found for {self.data_source} source. Check tickers in "
                                        f"data catalog and try again.")
                        self.data_req.tickers.remove(ticker)
            else:
                tickers = [ticker.upper() for ticker in self.data_req.tickers]

        elif self.data_source == 'dbnomics' or self.data_source == 'fred':
            for ticker in self.data_req.tickers:
                try:
                    tickers.append(tickers_df.loc[ticker, self.data_source + '_id'])
                except KeyError:
                    logging.warning(f"{ticker} not found for {self.data_source} source. Check tickers in"
                                    f" data catalog and try again.")
                    self.data_req.tickers.remove(ticker)

        else:
            tickers = self.data_req.tickers

        return tickers

    def convert_fx_tickers(self) -> list[str]:
        """
        Converts base and quote currency tickers to fx pairs following fx quoting convention.

        Returns
        -------
        fx_pairs: list
            List of fx pairs using quoting convention.
        """
        quote_ccy = self.convert_quote_ccy_to_source()
        fx_pairs = []  # fx pairs list
        # fx groups
        base_ccys = ['EUR', 'GBP', 'AUD', 'NZD']
        # g10_fx = ['USD', 'EUR', 'GBP', 'JPY', 'CHF', 'CAD', 'AUD', 'NZD', 'NOK', 'SEK']
        # dm_fx = ['USD', 'EUR', 'GBP', 'JPY', 'CHF', 'CAD', 'AUD', 'NZD', 'NOK', 'SEK', 'SGD', 'ILS', 'HKD', ]
        # em_fx = ['ARS', 'BRL', 'CHN', 'CLP', 'CNY', 'COP', 'IDR', 'INR', 'KRW', 'MYR', 'MXN', 'PEN', 'PHP', 'RUB',
        #          'TRY', 'TWD', 'ZAR']

        if self.data_req.cat == 'fx':
            for ticker in self.data_req.tickers:
                if ticker.upper() in base_ccys and quote_ccy.upper() == 'USD':
                    fx_pairs.append(ticker.upper() + '/' + quote_ccy.upper())
                elif quote_ccy.upper() == 'USD':
                    fx_pairs.append(quote_ccy.upper() + '/' + ticker.upper())
                else:
                    fx_pairs.append(ticker.upper() + '/' + quote_ccy.upper())

        return fx_pairs

    def convert_tickers_to_mkts_source(self) -> list[str]:
        """
        Converts asset tickers to market tickers in data source format.

        Returns
        -------
        mkts_list: list
            List of markets in data source format.
        """
        # convert tickers to markets
        mkts_list = []
        tickers, mkt_type = self.data_req.tickers, self.data_req.mkt_type
        quote_ccy, exch = self.convert_quote_ccy_to_source(), self.convert_exch_to_source()

        if self.data_req.source_tickers is not None:
            mkts_list = self.data_req.source_tickers
            self.data_req.tickers = self.data_req.source_tickers

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
        elif self.data_source == 'ccxt':
            for ticker in tickers:
                if mkt_type == 'spot':
                    mkts_list.append(ticker.upper() + '/' + quote_ccy.upper())
                elif mkt_type == 'perpetual_future':
                    if exch == 'binanceusdm':
                        mkts_list.append(ticker.upper() + '/' + quote_ccy.upper())
                    elif exch == 'ftx' or exch == 'okx' or exch == 'kucoinfutures' or exch == 'huobipro' \
                            or exch == 'cryptocom' or exch == 'bitfinex2' or exch == 'bybit' or exch == 'mexc3' \
                            or exch == 'aax' or exch == 'bitmex':
                        mkts_list.append(ticker.upper() + '/' + quote_ccy.upper() + ':' + quote_ccy.upper())
                # TO DO: create deliverable future and option markets list
                elif mkt_type == 'future':
                    pass
                elif mkt_type == 'option':
                    pass

        elif self.data_source == 'investpy' or self.data_source == 'av-forex-daily':
            if self.data_req.cat == 'fx':
                mkts_list = self.convert_fx_tickers()

        elif self.data_source == 'tiingo':
            if self.data_req.cat == 'fx':
                fx_list = self.convert_fx_tickers()
                mkts_list = [ticker.lower().replace('/', '') for ticker in fx_list]
            elif self.data_req.cat == 'crypto':
                mkts_list = [ticker.lower() + quote_ccy for ticker in self.data_req.tickers]

        else:
            mkts_list = None

        return mkts_list

    def convert_freq_to_source(self) -> str:
        """
        Converts frequency from CryptoDataPy to data source format.

        Returns
        -------
        freq: str
            Frequency in data source format.
        """
        if self.data_req.source_freq is not None:
            freq = self.data_req.source_freq

        elif self.data_source == 'cryptocompare':
            if self.data_req.freq[-3:] == 'min':
                freq = 'histominute'
            elif self.data_req.freq[-1] == 'h':
                freq = 'histohour'
            else:
                freq = 'histoday'

        elif self.data_source == 'coinmetrics':
            if self.data_req.freq == 'block':
                freq = '1b'
            elif self.data_req.freq == 'tick':
                freq = 'tick'
            elif self.data_req.freq[-1] == 's':
                freq = '1s'
            elif self.data_req.freq[-3:] == 'min':
                freq = '1m'
            elif self.data_req.freq[-1] == 'h':
                freq = '1h'
            else:
                freq = '1d'

        elif self.data_source == 'ccxt':
            if self.data_req.freq == 'tick':
                freq = 'tick'
            elif self.data_req.freq[-3:] == 'min':
                freq = self.data_req.freq.replace('min', 'm')
            elif self.data_req.freq == 'd':
                freq = '1d'
            elif self.data_req.freq == 'w':
                freq = '1w'
            elif self.data_req.freq == 'm':
                freq = '1M'
            elif self.data_req.freq[-1] == 'm':
                freq = self.data_req.freq.replace('m', 'M')
            elif self.data_req.freq == 'q':
                freq = '1q'
            elif self.data_req.freq == 'y':
                freq = '1y'
            else:
                freq = self.data_req.freq

        elif self.data_source == 'glassnode':
            if self.data_req.freq[-3:] == 'min':
                freq = '10m'
            elif self.data_req.freq[-1] == 'h':
                freq = '1h'
            elif self.data_req.freq == 'd':
                freq = '24h'
            elif self.data_req.freq == 'w':
                freq = '1w'
            elif self.data_req.freq == 'm':
                freq = '1month'
            else:
                freq = self.data_req.freq

        elif self.data_source == 'tiingo':
            if self.data_req.freq[-3:] == 'min':
                freq = self.data_req.freq
            elif self.data_req.freq[-1] == 'h':
                freq = '1hour'
            elif self.data_req.freq == 'd':
                freq = '1day'
            elif self.data_req.freq == 'w':
                freq = '1week'
            else:
                freq = self.data_req.freq

        elif self.data_source == 'investpy':
            if self.data_req.cat != 'macro':
                freq = 'Daily'
            else:
                freq = self.data_req.freq

        else:
            freq = self.data_req.freq

        return freq

    def convert_quote_ccy_to_source(self) -> str:
        """
        Converts quote currency from CryptoDataPy to data source format.

        Returns
        -------
        quote_ccy: str
            Quote currency in data source format.
        """
        if self.data_source == 'cryptocompare' or self.data_source == 'glassnode' \
                or self.data_source == 'av-forex-daily' or self.data_source == 'investpy':
            if self.data_req.quote_ccy is None:
                quote_ccy = 'USD'
            else:
                quote_ccy = self.data_req.quote_ccy.upper()

        elif self.data_source == 'coinmetrics':
            if self.data_req.quote_ccy is None:
                quote_ccy = 'usdt'
            else:
                quote_ccy = self.data_req.quote_ccy.lower()

        elif self.data_source == 'ccxt':
            if self.data_req.quote_ccy is None:
                quote_ccy = 'USDT'
            else:
                quote_ccy = self.data_req.quote_ccy.upper()

        elif self.data_source == 'tiingo':
            if self.data_req.quote_ccy is None:
                quote_ccy = 'usd'
            else:
                quote_ccy = self.data_req.quote_ccy.lower()

        else:
            quote_ccy = self.data_req.quote_ccy

        return quote_ccy

    def convert_exch_to_source(self) -> str:
        """
        Converts exchange from CryptoDataPy to data source format.

        Returns
        -------
        exch: str
            Exchange in data source format.
        """
        if self.data_source == 'cryptocompare':
            if self.data_req.exch is None:
                exch = 'CCCAGG'
            else:
                exch = self.data_req.exch

        elif self.data_source == 'coinmetrics':
            if self.data_req.exch is None:
                exch = 'binance'
            else:
                exch = self.data_req.exch.lower()

        elif self.data_source == 'ccxt':
            if self.data_req.mkt_type == 'perpetual_future' and \
                    (self.data_req.exch is None or self.data_req.exch == 'binance'):
                exch = 'binanceusdm'
            elif self.data_req.exch is None:
                exch = 'binance'
            elif self.data_req.exch == 'kucoin' and self.data_req.mkt_type == 'perpetual_future':
                exch = 'kucoinfutures'
            elif self.data_req.exch == 'huobi' and self.data_req.mkt_type == 'perpetual_future':
                exch = 'huobipro'
            elif self.data_req.exch == 'bitfinex' and self.data_req.mkt_type == 'perpetual_future':
                exch = 'bitfinex2'
            elif self.data_req.exch == 'mexc' and self.data_req.mkt_type == 'perpetual_future':
                exch = 'mexc3'
            else:
                exch = self.data_req.exch.lower()

        elif self.data_source == 'tiingo':
            if self.data_req.exch is None and self.data_req.cat == 'eqty' and \
                    self.data_req.freq in ['1min', '5min', '10min', '15min', '30min', '1h', '2h', '4h', '8h']:
                exch = 'iex'
            else:
                exch = self.data_req.exch

        else:
            exch = self.data_req.exch

        return exch

    def convert_tickers_to_ctys_source(self) -> list[str]:
        """
        Maps tickers to their respective countries.

        Returns
        -------
        ctys_list: list
            List of countries in ISO/data source format.
        """
        # tickers
        with resources.path('cryptodatapy.conf', 'tickers.csv') as f:
            tickers_path = f
        # get tickers file and create ticker list
        tickers_df, ctys_list = pd.read_csv(tickers_path, index_col=0, encoding='latin1'), []

        if self.data_source == 'investpy':
            if self.data_req.cat == 'macro':
                for ticker in self.data_req.tickers:
                    try:
                        ctys_list.append(tickers_df.loc[ticker, 'country_name'].lower())
                    except KeyError:
                        logging.warning(f"{ticker} not found for {self.data_source} source. Check tickers in "
                                        f"data catalog and try again.")

        return ctys_list

    def convert_start_date_to_source(self) -> Union[str, int, datetime, pd.Timestamp]:
        """
        Converts start date from CryptoDataPy to data source format.

        Returns
        -------
        start_date: str, int, datetime or pd.Timestamp
            Start date in data source format.
        """
        if self.data_source == 'cryptocompare':
            if self.data_req.freq[-3:] == 'min':  # cryptocompare limits higher frequency data responses
                start_date = round((datetime.now() - timedelta(days=7)).timestamp())
            # no start date
            elif self.data_req.start_date is None:
                start_date = round(pd.Timestamp('2009-01-03 00:00:00').timestamp())
            else:
                start_date = round(pd.Timestamp(self.data_req.start_date).timestamp())

        elif self.data_source == 'ccxt':
            # no start date
            if self.data_req.start_date is None:
                start_date = round(pd.Timestamp('2010-01-01 00:00:00').timestamp() * 1e3)
            else:
                start_date = round(pd.Timestamp(self.data_req.start_date).timestamp() * 1e3)

        elif self.data_source == 'glassnode':
            if self.data_req.start_date is None:
                start_date = round(pd.Timestamp('2009-01-03 00:00:00').timestamp())
            else:
                start_date = round(pd.Timestamp(self.data_req.start_date).timestamp())

        elif self.data_source == 'investpy':
            if self.data_req.start_date is None:
                start_date = pd.Timestamp('1960-01-01').strftime("%d/%m/%Y")
            else:
                start_date = pd.Timestamp(self.data_req.start_date).strftime("%d/%m/%Y")

        elif self.data_source == 'fred' or self.data_source == 'av-daily' or self.data_source == 'av-forex-daily' or \
                self.data_source == 'yahoo':
            if self.data_req.start_date is None:
                start_date = datetime(1920, 1, 1)
            else:
                start_date = self.data_req.start_date

        else:
            start_date = self.data_req.start_date

        return start_date

    def convert_end_date_to_source(self) -> Union[str, int, datetime, pd.Timestamp]:
        """
        Converts end date from CryptoDataPy to data source format.

        Returns
        -------
        end_date: str, int, datetime or pd.Timestamp
            End date in data source format.
        """
        if self.data_source == 'cryptocompare':
            if self.data_req.end_date is None:
                end_date = round(pd.Timestamp(datetime.utcnow()).timestamp())
            else:
                end_date = round(pd.Timestamp(self.data_req.end_date).timestamp())

        elif self.data_source == 'ccxt':
            # no end date
            if self.data_req.end_date is None:
                end_date = round(pd.Timestamp(datetime.utcnow()).timestamp() * 1e3)
            else:
                end_date = round(pd.Timestamp(self.data_req.end_date).timestamp() * 1e3)

        elif self.data_source == 'glassnode':
            if self.data_req.end_date is None:
                end_date = self.data_req.end_date
            else:
                end_date = round(pd.Timestamp(self.data_req.end_date).timestamp())

        elif self.data_source == 'investpy':
            if self.data_req.end_date is None:
                end_date = datetime.utcnow().strftime("%d/%m/%Y")
            else:
                end_date = pd.Timestamp(self.data_req.end_date).strftime("%d/%m/%Y")

        elif self.data_source == 'tiingo' or self.data_source == 'fred' or self.data_source == 'av-daily' or \
                self.data_source == 'av-forex-daily' or self.data_source == 'yahoo':
            if self.data_req.end_date is None:
                end_date = datetime.utcnow()
            else:
                end_date = self.data_req.end_date

        else:
            end_date = self.data_req.end_date

        return end_date

    def convert_fields_to_source(self) -> list[str]:
        """
        Converts fields from CryptoDataPy to data source format.

        Returns
        -------
        fields_list: list
            List of fields in data source format.
        """
        # get fields
        with resources.path('cryptodatapy.conf', 'fields.csv') as f:
            fields_dict_path = f
        fields_df, fields_list = pd.read_csv(fields_dict_path, index_col=0, encoding='latin1'), []

        # when source fields already provided in data req
        if self.data_req.source_fields is not None:
            fields_list = self.data_req.source_fields

        # convert to source format
        else:
            for field in self.data_req.fields:
                try:
                    fields_list.append(fields_df.loc[field, self.data_source + '_id'])
                except KeyError as e:
                    logging.warning(e)
                    logging.warning(f"Id for {field} could not be found in the data catalog."
                                    f" Try using source field ids.")

        return fields_list

    def convert_tz_to_source(self) -> str:
        """
        Converts timezone to data source timezone.

        Returns
        -------
        tz: str
            Timezone for data source using Olson tz database.
        """
        us_timezones = ['fred', 'av-daily', 'av-forex-daily', 'yahoo']
        if self.data_source in us_timezones and self.data_req.tz is None:
            tz = 'America/New_York'
        elif self.data_source not in us_timezones and self.data_req.tz is None:
            tz = 'UTC'
        else:
            tz = self.data_req.tz

        return tz

    def convert_inst_to_source(self) -> str:
        """
        Converts institution names from CryptoDataPy to data source format.

        Returns
        -------
        inst: str
            Institution name converted to data source format.
        """
        if self.data_source == 'coinmetrics':
            if self.data_req.inst is None:
                inst = 'grayscale'
            else:
                inst = self.data_req.inst.lower()

        elif self.data_source == 'glassnode':
            if self.data_req.inst is None:
                inst = 'purpose'
            else:
                inst = self.data_req.inst.lower()
        else:
            inst = self.data_req.inst

        return inst

    def convert_pause_to_source(self) -> str:
        """
        Converts pause to length appropriate for each data source to avoid rate limiting issues.

        Returns
        -------
        pause: str
            Pause time for data source.
        """
        if self.data_source == 'investpy':
            pause = 3
        else:
            pause = self.data_req.pause

        return pause

    def convert_to_source(self) -> dict[str, Union[str, int, list[str]]]:
        """
        Converts data request parameters from CryptoDataPy to data source format.

        Returns
        -------
        source_params: dict
            Dictionary containing all data request parameters converted to data source format.
        """
        tickers = self.convert_tickers_to_source()
        freq = self.convert_freq_to_source()
        quote_ccy = self.convert_quote_ccy_to_source()
        exch = self.convert_exch_to_source()
        ctys = self.convert_tickers_to_ctys_source()
        mkt_type = self.data_req.mkt_type
        mkts_list = self.convert_tickers_to_mkts_source()
        start_date = self.convert_start_date_to_source()
        end_date = self.convert_end_date_to_source()
        fields = self.convert_fields_to_source()
        tz = self.convert_tz_to_source()
        inst = self.convert_inst_to_source()
        cat = self.data_req.cat
        trials = self.data_req.trials
        pause = self.convert_pause_to_source()
        source_tickers = self.data_req.source_tickers
        source_freq = self.data_req.source_freq
        source_fields = self.data_req.source_fields

        source_params = {'tickers': tickers, 'freq': freq, 'quote_ccy': quote_ccy, 'exch': exch, 'ctys': ctys,
                         'mkt_type': mkt_type, 'mkts': mkts_list, 'start_date': start_date, 'end_date': end_date,
                         'fields': fields, 'tz': tz, 'inst': inst, 'cat': cat, 'trials': trials, 'pause': pause,
                         'source_tickers': source_tickers, 'source_freq': source_freq, 'source_fields': source_fields}

        return source_params
