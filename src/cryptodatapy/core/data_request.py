from datetime import datetime
from typing import List, Optional, Union

import pandas as pd
import pytz


class DataRequest:
    """
    Object for defining data retrieval parameters.

    The DataRequest object serves as the command pattern, holding all necessary
    parameters (assets, dates, frequency, source, etc.) required to fetch and
    process data from any supported data vendor.
    """

    def __init__(
            self,
            source: str = "ccxt",
            tickers: Union[str, List[str]] = "btc",
            quote_ccy: Optional[str] = None,
            markets: Optional[Union[str, List[str]]] = None,
            freq: str = "d",
            exch: Optional[str] = None,
            asset_class: Optional[str] = None,
            cat: Optional[str] = None,
            countries: Optional[Union[str, List[str]]] = None,
            mkt_type: Optional[str] = "spot",
            start_date: Optional[Union[str, datetime, pd.Timestamp]] = None,
            end_date: Optional[Union[str, datetime, pd.Timestamp]] = None,
            fields: Union[str, List[str]] = ["close"],
            tz: Optional[str] = None,
            trials: Optional[int] = 3,
            pause: Optional[float] = 0.1,
            source_tickers: Optional[Union[str, List[str]]] = None,
            source_markets: Optional[Union[str, List[str]]] = None,
            source_freq: Optional[str] = None,
            source_start_date: Optional[Union[str, int, datetime, pd.Timestamp]] = None,
            source_end_date: Optional[Union[str, int, datetime, pd.Timestamp]] = None,
            source_fields: Optional[Union[str, List[str]]] = None
    ):
        """
        Initializes a DataRequest object with specified parameters.

        Parameters
        ----------
        source : str, default 'ccxt'
            Name of the data source/vendor to query.
        tickers : list or str, default 'btc'
            Ticker symbols for base assets (CryptoDataPy format).
            E.g., 'BTC', 'EUR', 'SPY'.
        quote_ccy : str, optional, default None
            Ticker symbol for the quote asset, e.g., 'USDT' for BTCUSDT.
        markets : list or str, optional, default None
            Markets/traded pairs (asset/quote), e.g., 'BTC/USDT', 'EUR/USD'.
        freq : str, default 'd'
            Frequency of data observations. E.g., 'd' (daily), '1h' (one hour).
        exch : str, optional, default None
            Name of the asset exchange, e.g., 'Binance', 'FTX'.
        asset_class : str, optional, default None
            Asset class of data. Options include:
            {'crypto', 'fx', 'cmdty', 'eqty', 'rates', 'bonds', 'credit'}.
        cat : str, optional, default None
            Category of data. Options include:
            {'crypto', 'fx', 'cmdty', 'eqty', 'rates', 'bonds', 'credit', 'macro', 'alt'}.
        countries : list or str, optional, default None
            Country codes for which to pull data, e.g., 'US', 'GB'.
        mkt_type : str, default 'spot'
            Market type, e.g., 'spot', 'future', 'perpetual_future', 'option'.
        start_date : str, datetime, or :py:class:`pd.Timestamp`, optional, default None
            Start date for data request in 'YYYY-MM-DD' string format, datetime, or Timestamp.
            E.g., '2010-01-01'.
        end_date : str, datetime, or :py:class:`pd.Timestamp`, optional, default None
            End date for data request in 'YYYY-MM-DD' string format, datetime, or Timestamp.
            E.g., '2020-12-31'.
        fields : list or str, default 'close'
            Fields for data request. E.g., 'open', 'high', 'low', 'close', 'volume'.
        tz : str, optional, default None
            Timezone for the start/end dates in IANA tz database format.
        trials : int, optional, default 3
            Number of times the HTTP request should be retried.
        pause : float, optional, default 0.1
            Number of seconds to pause between failed API request trials.

        Source-Specific Parameters
        --------------------------
        The following parameters allow the user to bypass conversion logic and provide
        values directly in the format required by the underlying data source/vendor.

        source_tickers : list or str, optional, default None
            Ticker symbols in the source's native format.
        source_markets : list or str, optional, default None
            Markets in the source's native format.
        source_freq : str, optional, default None
            Frequency in the source's native format.
        source_start_date : str, int, datetime, or :py:class:`pd.Timestamp`, optional, default None
            Start date in the source's native format (e.g., milliseconds for some APIs).
        source_end_date : str, int, datetime, or :py:class:`pd.Timestamp`, optional, default None
            End date in the source's native format.
        source_fields : list or str, optional, default None
            Fields in the source's native format.
        """
        self.source = source  # name of data source
        self.tickers = tickers  # tickers
        self.quote_ccy = quote_ccy  # quote ccy
        self.markets = markets  # markets
        self.freq = freq  # frequency
        self.exch = exch  # exchange
        self.asset_class = asset_class  # asset class
        self.cat = cat  # category of asset class or time series
        self.countries = countries  # country codes
        self.mkt_type = mkt_type  # market type
        self.start_date = start_date  # start date
        self.end_date = end_date  # end date
        self.fields = fields  # fields
        self.tz = tz  # tz
        self.trials = trials  # number of times to try query request
        self.pause = pause  # number of seconds to pause between query request trials
        self.source_tickers = source_tickers  # tickers used by data source
        self.source_markets = source_markets
        self.source_freq = source_freq  # frequency used by data source
        self.source_start_date = source_start_date  # start date used by data source
        self.source_end_date = source_end_date  # end date used by data source
        self.source_fields = source_fields  # fields used by data source

    @property
    def source(self):
        """
        Returns data source for data request.
        """
        return self._source

    @source.setter
    def source(self, source):
        """
        Sets data source for data request.
        """
        valid_data_sources = [
            "cryptocompare",
            "coinmetrics",
            "ccxt",
            'defillama',
            "dydx",
            "glassnode",
            "tiingo",
            "investpy",
            "yahoo",
            "alphavantage",
            "polygon",
            "fred",
            "famafrench",
            "dbnomics",
            "wb",
            "aqr"
        ]

        if source in valid_data_sources:
            self._source = source
        else:
            raise ValueError(
                f"{source} is invalid. Valid data sources are: {valid_data_sources}."
            )

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
        if isinstance(tickers, str):
            self._tickers = [tickers]
        elif isinstance(tickers, list):
            self._tickers = tickers
        else:
            raise TypeError("Tickers must be a string or list of strings (tickers).")

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
        if quote is None:
            self._quote_ccy = quote
        elif isinstance(quote, str):
            self._quote_ccy = quote
        else:
            raise TypeError("Quote currency must be a string.")

    @property
    def markets(self):
        """
        Returns markets for data request.
        """
        return self._markets

    @markets.setter
    def markets(self, markets):
        """
        Sets markets for data request.
        """
        if markets is None:
            self._markets = markets
        elif isinstance(markets, str):
            self._markets = [markets]
        elif isinstance(markets, list):
            self._markets = markets
        else:
            raise TypeError("Markets must be a string or list of strings (markets).")

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
        freq_dict = {
            "tick": "bid/ask quote (quotes) or executed trade (trades)",
            "block": "record of the most recent batch or block of transactions validated by the network",
            "1s": "one second",
            "10s": "ten seconds",
            "15s": "fifteen seconds",
            "1min": "one minute",
            "3min": "three minutes",
            "5min": "five minutes",
            "10min": "ten minutes",
            "15min": "fifteen minutes",
            "30min": "thirty minutes",
            "45min": "forty five minutes",
            "1h": "one hour",
            "2h": "two hours",
            "4h": "four hours",
            "6h": "six hours",
            "8h": "eight hours",
            "12h": "twelve hours",
            "b": "business day",
            "d": "daily",
            "3d": "three days",
            "5d": "five days",
            "7d": "seven days",
            "w": "weekly",
            "2w": "two weeks",
            "m": "monthly",
            "3m": "three months",
            "4m": "four months",
            "6m": "six months",
            "q": "quarterly",
            "y": "yearly",
        }

        if frequency is None:
            self._frequency = frequency
        elif frequency not in list(freq_dict.keys()):
            raise ValueError(
                f"{frequency} is an invalid data frequency. Valid frequencies are: {freq_dict}"
            )
        else:
            self._frequency = frequency

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
        if exch is None:
            self._exch = exch
        elif isinstance(exch, str):
            self._exch = exch
        else:
            raise TypeError("Exchange must be a string.")

    @property
    def asset_class(self):
        """
        Returns asset class for data request.
        """
        return self._asset_class

    @asset_class.setter
    def asset_class(self, asset_class):
        """
        Sets asset class for data request.
        """
        valid_asset_classes = [
            "crypto",
            "fx",
            "eqty",
            "cmdty",
            "rates",
            "bonds",
            "credit",
        ]
        if asset_class is None:
            self._asset_class = asset_class
        elif isinstance(asset_class, list):
            raise TypeError("Asset class must be a string.")
        elif asset_class in valid_asset_classes:
            self._asset_class = asset_class
        else:
            raise ValueError(
                f"{asset_class} is an invalid asset class. Valid asset classes are: {valid_asset_classes}."
            )

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
        valid_categories = [
            "crypto",
            "fx",
            "eqty",
            "cmdty",
            "rates",
            "bonds",
            "credit",
            "macro",
            "alt",
        ]
        if category is None:
            self._category = category
        elif isinstance(category, list):
            raise TypeError("Category must be a string.")
        elif category in valid_categories:
            self._category = category
        else:
            raise ValueError(
                f"{category} is an invalid category. Valid categories are: {valid_categories}."
            )

    @property
    def countries(self):
        """
        Returns country codes for data request.
        """
        return self._countries

    @countries.setter
    def countries(self, countries):
        """
        Sets country codes for data request.
        """
        if countries is None:
            self._countries = countries
        elif isinstance(countries, str):
            self._countries = [countries]
        elif isinstance(countries, list):
            self._countries = countries
        else:
            raise TypeError("Country codes must be a string or list of strings.")

    @property
    def mkt_type(self):
        """
        Returns market type for data request.
        """
        return self._mkt_type

    @mkt_type.setter
    def mkt_type(self, mkt_type):
        """
        Sets market type for data request.
        """
        valid_mkt_types = [
            "spot",
            "etf",
            "perpetual_future",
            "future",
            "swap",
            "option",
        ]
        if mkt_type in valid_mkt_types:
            self._mkt_type = mkt_type
        else:
            raise ValueError(
                f"{mkt_type} is invalid. Valid market types are: {valid_mkt_types}."
            )

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
        if start_date is None:
            self._start_date = start_date
        elif isinstance(start_date, str):
            try:
                start_date = datetime.strptime(start_date, "%Y-%m-%d")
            except ValueError:
                raise ValueError('Date must be in "YYYY-MM-DD" string format.')
            else:
                self._start_date = start_date
        elif isinstance(start_date, datetime):
            self._start_date = start_date
        elif isinstance(start_date, pd.Timestamp):
            self._start_date = start_date
        else:
            raise ValueError(
                'Start date must be in "YYYY-MM-DD" string, datetime or pd.Timestamp format.'
            )

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
        if end_date is None:
            self._end_date = end_date
        elif isinstance(end_date, str):
            try:
                end_date = datetime.strptime(end_date, "%Y-%m-%d")
            except ValueError:
                raise ValueError('Date must be in "YYYY-MM-DD" string format.')
            else:
                self._end_date = end_date
        elif isinstance(end_date, datetime):
            self._end_date = end_date
        elif isinstance(end_date, pd.Timestamp):
            self._end_date = end_date
        else:
            raise ValueError(
                'End date must be in "YYYY-MM-DD" string, datetime or pd.Timestamp format.'
            )

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
            self._fields = [fields]
        elif isinstance(fields, list):
            self._fields = fields
        else:
            raise TypeError("Fields must be a string or list of strings.")

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
        valid_timezones = pytz.all_timezones
        if timezone is None:
            self._timezone = timezone
        elif timezone in valid_timezones:
            self._timezone = timezone
        else:
            raise ValueError(
                f"{timezone} is an invalid timezone. Valid timezones are: {valid_timezones}."
            )

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
        if trials is None:
            self._trials = trials
        elif isinstance(trials, int) or isinstance(trials, str):
            self._trials = int(trials)
        else:
            raise TypeError("Number of trials must be an integer or string.")

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
        if pause is None:
            self._pause = pause
        elif isinstance(pause, float) or isinstance(pause, int):
            self._pause = float(pause)
        else:
            raise TypeError("Number of seconds to pause must be an int or float.")

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
        if tickers is None:
            self._source_tickers = tickers
        elif isinstance(tickers, str):
            self._source_tickers = [tickers]
        elif isinstance(tickers, list):
            self._source_tickers = tickers
        else:
            raise TypeError(
                "Source tickers must be a string or list of strings (tickers) in data source's format."
            )

    @property
    def source_markets(self):
        """
        Returns markets for data request in data source format.
        """
        return self._source_markets

    @source_markets.setter
    def source_markets(self, markets):
        """
        Sets markets for data request in data source format.
        """
        if markets is None:
            self._source_markets = markets
        elif isinstance(markets, str):
            self._source_markets = [markets]
        elif isinstance(markets, list):
            self._source_markets = markets
        else:
            raise TypeError(
                "Source markets must be a string or list of strings (markets) in data source's format."
            )

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
        if freq is None:
            self._source_freq = freq
        elif isinstance(freq, str):
            self._source_freq = freq
        else:
            raise TypeError(
                "Source data frequency must be a string in data source's format."
            )

    @property
    def source_start_date(self):
        """
        Returns start date for data request in data source format.
        """
        return self._source_start_date

    @source_start_date.setter
    def source_start_date(self, start_date):
        """
        Sets start date for data request in data source format.
        """
        if start_date is None:
            self._source_start_date = start_date
        elif isinstance(start_date, str):
            self._source_start_date = start_date
        elif isinstance(start_date, int):
            self._source_start_date = start_date
        elif isinstance(start_date, datetime):
            self._source_start_date = start_date
        elif isinstance(start_date, pd.Timestamp):
            self._source_start_date = start_date
        else:
            raise ValueError(
                'Start date must be in "YYYY-MM-DD" string, integer, datetime or pd.Timestamp format.'
            )

    @property
    def source_end_date(self):
        """
        Returns end date for data request in data source format.
        """
        return self._source_end_date

    @source_end_date.setter
    def source_end_date(self, end_date):
        """
        Sets end date for data request in data source format.
        """
        if end_date is None:
            self._source_end_date = end_date
        elif isinstance(end_date, str):
            self._source_end_date = end_date
        elif isinstance(end_date, int):
            self._source_end_date = end_date
        elif isinstance(end_date, datetime):
            self._source_end_date = end_date
        elif isinstance(end_date, pd.Timestamp):
            self._source_end_date = end_date
        else:
            raise ValueError(
                'End date must be in "YYYY-MM-DD" string, integer, datetime or pd.Timestamp format.'
            )

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
        elif isinstance(fields, str):
            self._source_fields = [fields]
        elif isinstance(fields, list):
            self._source_fields = fields
        else:
            raise TypeError(
                "Source fields must be a string or list of strings (fields) in data source's format."
            )
