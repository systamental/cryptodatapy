# import libraries
import pandas as pd
import pytz
from typing import Union, Optional
from datetime import datetime


class DataRequest():
    """
    Data request class which contains the parameters for the data you would like to retrieve.
    """
    def __init__(
            self,
            tickers: Union[list[str], str] = 'btc',
            freq: str = 'd',
            quote_ccy: Optional[str] = None,
            exch: Optional[str] = None,
            start_date: Optional[Union[str, datetime, pd.Timestamp]] = None,
            end_date: Optional[Union[str, datetime, pd.Timestamp]] = datetime.utcnow(),
            fields: Union[list[str], str] = ['close'],
            tz: str = 'UTC',
            cat: Optional[str] = None,
            trials: Optional[int] = 3,
            pause: Optional[float] = 0.1,
            source_tickers: Optional[Union[list[str], str]] = None,
            source_freq: Optional[str] = None,
            source_fields: Optional[Union[list[str], str]] = None
    ):
        """
        Constructor

        Parameters
        ----------
        tickers: list or str
            List or string of ticker symbol(s) for assets or names of time series.
            e.g. 'BTC', 'EURUSD', 'SPY', 'gdp', ... List of tickers/names must be from the same category.
        freq: str, {'tick', '1min', '5min', '10min', '30min', '1h', '2h', '4h', '8h', 'b', 'd', 'w', 'm'}, default 'd'
            Frequency of data observations. Defaults to daily 'd' which includes weekends for cryptoassets.
        quote_ccy: str,  optional, default None
            Quote currency for asset, e.g. 'GBP' for EURGBP, 'USD' for BTCUSD aka bitcoin in dollars, etc.
        exch: str,  optional, default None
            Exchange from which to pull data (e.g. 'Binance', 'FTX', 'IEX', ...)
        start_date: str, datetime or pd.Timestamp, optional, default None
            Start date for data request in 'YYYY-MM-DD' string, datetime or pd.Timestamp format,
            e.g. '2010-01-01' for January 1st 2010, datetime(2010,1,1) or pd.Timestamp('2010-01-01').
        end_date, str, datetime or pd.Timestamp, optional, default None
            End date for data request in 'YYYY-MM-DD' string, datetime or pd.Timestamp format,
            e.g. '2020-12-31' for January 31st 2020, datetime(2020,12,31) or pd.Timestamp('2020-12-31').
        fields: list or str, default ['close']
            Fields for data request. OHLCV bars/fields are most common for market data.
        tz: str, {'UTC', 'America/New_York', 'Europe/London', ...}, default 'UTC'
            Timezone for the start/end dates.
        cat: str, {'crypto', 'fx', 'cmdty', 'eqty', 'rates', 'bonds', 'credit', 'macro', 'alt'}
            Category of data, e.g. crypto, fx, rates, or macro.
        trials: int, optional, default None
            Number of times to try data request.
        pause: float,  optional, default None
            Number of seconds to pause between data requests.
        source_tickers: list or str, optional, default None
            List or string of ticker symbol(s) for assets or names for time series in the format used by the
            data source. If None, tickers will be converted from CryptoDataPy format to data source format.
        source_freq: str, optional, default None
            Frequency of observations for asset(s) or time series in format used by data source.If None,
            frequency will be converted from CryptoDataPy format to data source format.
        source_fields: list or str, optional, default None
            List or string of fields for asset(s) or time series in format used by data source.If None,
            fields will be converted from CryptoDataPy format to data source format.
        """

        # params
        self.tickers = tickers  # tickers
        self.freq = freq  # frequency
        self.quote_ccy = quote_ccy  # quote ccy
        self.exch = exch  # exchange
        self.start_date = start_date  # start date
        self.end_date = end_date  # end date
        self.fields = fields  # fields
        self.tz = tz  # tz
        self.cat = cat  # category of asset class or time series
        self.trials = trials  # number of times to try query request
        self.pause = pause  # number of seconds to pause between query request trials
        self.source_tickers = source_tickers  # tickers used by data source
        self.source_freq = source_freq  # frequency used by data source
        self.source_fields = source_fields  # fields used by data source

    @property
    def tickers(self):
        """
        Returns tickers for data request.
        """
        return self._tickers

    @tickers.setter
    def tickers(self, tickers):
        """
        Sets tickers for data request.
        """
        # None
        if tickers is None:
            self._tickers = ['btc']
        # list
        elif isinstance(tickers, list):
            self._tickers = tickers
        # str
        elif isinstance(tickers, str):
            self._tickers = [tickers]
        else:
            raise ValueError('Tickers must be a string or list of strings (symbols).')

    @property
    def freq(self):
        """
        Returns frequency of observations for data request.
        """
        return self._frequency

    @freq.setter
    def freq(self, frequency):
        """
        Sets frequency of observations for data request.
        """
        if frequency not in [None, 'tick', '1min', '5min', '10min', '20min', '30min', '1h', '2h', '4h', '8h',
                             'd', 'w', 'm']:
            raise ValueError(f"{frequency} is an invalid data frequency. Valid frequencies are: 'tick', '1min', "
                             f"'5min', '10min', '15min', '30min', '1h', '2h', '4h', '8h', 'd', 'w', 'm'.")
        else:
            # set categories
            self._frequency = frequency

    @property
    def quote_ccy(self):
        """
        Returns quote currency for data request.
        """
        return self._quote_ccy

    @quote_ccy.setter
    def quote_ccy(self, quote):
        """
        Sets quote currency for data request.
        """
        # none
        if quote is None:
            self._quote_ccy = quote
        # str
        elif isinstance(quote, str):
            self._quote_ccy = quote
        else:
            raise TypeError('Quote currency must be a string.')

    @property
    def exch(self):
        """
        Returns exchange for data request.
        """
        return self._exch

    @exch.setter
    def exch(self, exch):
        """
        Sets exchange for data request.
        """
        # none
        if exch is None:
            self._exch = exch
        # str
        elif isinstance(exch, str):
            self._exch = exch
        else:
            raise TypeError('Exchange must be a string.')

    @property
    def start_date(self):
        """
        Returns start date for data request.
        """
        return self._start_date

    @start_date.setter
    def start_date(self, start_date):
        """
        Sets start date for data request.
        """
        # none
        if start_date is None:
            self._start_date = start_date
        # str date format %Y-%m-%d
        elif isinstance(start_date, str):
            try:
                start_date = datetime.strptime(start_date, '%Y-%m-%d')
            except ValueError:
                raise ValueError('Date must be in "YYYY-MM-DD" string format.')
            else:
                self._start_date = start_date
        # datetime format
        elif isinstance(start_date, datetime):
            self._start_date = start_date
        # pd.Timestamp format
        elif isinstance(start_date, pd.Timestamp):
            self._start_date = start_date
        else:
            raise ValueError('Start date must be in "YYYY-MM-DD" string, datetime or pd.Timestamp format.')

    @property
    def end_date(self):
        """
        Returns end date for data request.
        """
        return self._end_date

    @end_date.setter
    def end_date(self, end_date):
        """
        Sets end date for data request.
        """
        # none
        if end_date is None:
            self._end_date = end_date
        # str date format %Y-%m-%d
        elif isinstance(end_date, str):
            try:
                datetime.strptime(end_date, '%Y-%m-%d')
            except ValueError:
                raise ValueError('Date must be in "YYYY-MM-DD" string format.')
            else:
                self._end_date = end_date
        # datetime format
        elif isinstance(end_date, datetime):
            self._end_date = end_date
        # pd.Timestamp format
        elif isinstance(end_date, pd.Timestamp):
            self._end_date = end_date
        else:
            raise ValueError('End date must be in "YYYY-MM-DD" string, datetime or pd.Timestamp format.')

    @property
    def fields(self):
        """
        Returns fields for data request.
        """
        return self._fields

    @fields.setter
    def fields(self, fields):
        """
        Sets fields for data request.
        """
        if isinstance(fields, str):
            fields = [fields]

        self._fields = fields

    @property
    def tz(self):
        """
        Returns timezone for data request.
        """
        return self._timezone

    @tz.setter
    def tz(self, timezone):
        """
        Sets timezone for data request.
        """
        # all timezones
        timezones = pytz.all_timezones

        if timezone not in timezones:
            raise ValueError(f"{timezone} is an invalid timezone. Valid timezones are: {timezones}.")

        else:
            self._timezone = timezone

    @property
    def cat(self):
        """
        Returns category for data request.
        """
        return self._category

    @cat.setter
    def cat(self, category):
        """
        Sets category for data request.
        """
        # all categories
        categories = ['crypto', 'fx', 'rates', 'equities', 'commodities', 'bonds', 'credit', 'macro', 'alt']
        # none
        if category is None:
            self._category = category
        # check if valid category
        elif category in categories:
            self._category = category
        else:
            raise ValueError(
                f"{category} is an invalid category. Valid categories are: {categories}.")

    @property
    def trials(self):
        """
        Returns number of trials for data request.
        """
        return self._trials

    @trials.setter
    def trials(self, trials):
        """
        Sets number of trials for data request.
        """
        # none
        if trials is None:
            self._trials = trials
        elif isinstance(trials, int) or isinstance(trials, str):
            self._trials = int(trials)
        else:
            raise TypeError('Number of trials must be an integer or string.')

    @property
    def pause(self):
        """
        Returns number of seconds to pause between data requests.
        """
        return self._pause

    @pause.setter
    def pause(self, pause):
        """
        Sets number of seconds to pause between data requests.
        """
        # none
        if pause is None:
            self._pause = pause
        elif isinstance(pause, float):
            self._pause = pause
        else:
            raise TypeError('Number of seconds to pause must be a float.')

    @property
    def source_tickers(self):
        """
        Returns tickers for data request in data source format.
        """
        return self._source_tickers

    @source_tickers.setter
    def source_tickers(self, tickers):
        """
        Sets tickers for data request in data source format.
        """
        # none
        if tickers is None:
            self._source_tickers = tickers
        # list
        elif isinstance(tickers, list):
            self._source_tickers = tickers
        # str
        elif isinstance(tickers, str):
            self._source_tickers = [tickers]
        else:
            raise TypeError('Source tickers must be a string or list of strings (symbols).')

    @property
    def source_freq(self):
        """
        Returns frequency of data request in data source format.
        """
        return self._source_freq

    @source_freq.setter
    def source_freq(self, freq):
        """
        Sets frequency of data request in data source format.
        """
        # none
        if freq is None:
            self._source_freq = freq
        elif isinstance(freq, str):
            self._source_freq = freq
        else:
            raise TypeError('Source data frequency must be a string.')

    @property
    def source_fields(self):
        """
        Returns fields for data request in data source format.
        """
        return self._source_fields

    @source_fields.setter
    def source_fields(self, fields):
        """
        Sets fields for data request in data source format.
        """
        if fields is None:
            self._source_fields = fields
        elif isinstance(fields, list):
            self._source_fields = fields
        elif isinstance(fields, str):
            self._source_fields = fields
        else:
            raise TypeError('Source fields must be a string or list of strings (fields).')
