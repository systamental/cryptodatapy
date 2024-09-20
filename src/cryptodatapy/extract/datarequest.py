from datetime import datetime
from typing import List, Dict, Any, Optional, Union
from time import sleep

import pandas as pd
import requests
import logging
import pytz


class DataRequest:
    """
    Data request class which contains parameters for data retrieval.
    """

    def __init__(
        self,
        source: str = "ccxt",
        tickers: Union[str, List[str]] = "btc",
        freq: str = "d",
        quote_ccy: Optional[str] = None,
        exch: Optional[str] = None,
        mkt_type: Optional[str] = "spot",
        start_date: Optional[Union[str, datetime, pd.Timestamp]] = None,
        end_date: Optional[Union[str, datetime, pd.Timestamp]] = None,
        fields: Union[str, List[str]] = ["close"],
        tz: Optional[str] = None,
        inst: Optional[str] = None,
        cat: Optional[str] = None,
        trials: Optional[int] = 3,
        pause: Optional[float] = 0.1,
        source_tickers: Optional[Union[str, List[str]]] = None,
        source_freq: Optional[str] = None,
        source_fields: Optional[Union[str, List[str]]] = None,
    ):
        """
        Constructor

        Parameters
        ----------
        source: str, default 'ccxt'
            Name of data source.
        tickers: list or str, default 'btc'
            Ticker symbols for assets or time series.
            e.g. 'BTC', 'EURUSD', 'SPY', 'US_Manuf_PMI', 'EZ_Rates_10Y', etc.
        freq: str, default 'd'
            Frequency of data observations. Defaults to daily 'd' which includes weekends for cryptoassets.
        quote_ccy: str,  optional, default None
            Quote currency for base asset, e.g. 'GBP' for EURGBP, 'USD' for BTCUSD (bitcoin in dollars), etc.
        exch: str,  optional, default None
            Name of asset exchange, e.g. 'Binance', 'FTX', 'IEX', 'Nasdaq', etc.
        mkt_type: str, optional, default 'spot'
            Market type, e.g. 'spot ', 'future', 'perpetual_future', 'option'.
        start_date: str, datetime or pd.Timestamp, optional, default None
            Start date for data request in 'YYYY-MM-DD' string, datetime or pd.Timestamp format,
            e.g. '2010-01-01' for January 1st 2010, datetime(2010,1,1) or pd.Timestamp('2010-01-01').
        end_date, str, datetime or pd.Timestamp, optional, default None
            End date for data request in 'YYYY-MM-DD' string, datetime or pd.Timestamp format,
            e.g. '2020-12-31' for January 31st 2020, datetime(2020,12,31) or pd.Timestamp('2020-12-31').
        fields: list or str, default 'close'
            Fields for data request. OHLC bars/fields are the most common fields for market data.
        tz: str, optional, default None
            Timezone for the start/end dates in tz database format.
        inst: str, optional, default None
            Name of institution from which to pull fund data, e.g. 'grayscale', 'purpose', etc.
        cat: str, optional, {'crypto', 'fx', 'cmdty', 'eqty', 'rates', 'bonds', 'credit', 'macro', 'alt'}, default None
            Category of data, e.g. crypto, fx, rates, or macro.
        trials: int, optional, default 3
            Number of times to try data request.
        pause: float,  optional, default 0.1
            Number of seconds to pause between data requests.
        source_tickers: list or str, optional, default None
            List or string of ticker symbols for assets or time series in the format used by the
            data source. If None, tickers will be converted from CryptoDataPy to data source format.
        source_freq: str, optional, default None
            Frequency of observations for assets or time series in format used by data source. If None,
            frequency will be converted from CryptoDataPy to data source format.
        source_fields: list or str, optional, default None
            List or string of fields for assets or time series in format used by data source. If None,
            fields will be converted from CryptoDataPy to data source format.
        """
        # params
        self.source = source  # specific data source
        self.tickers = tickers  # tickers
        self.freq = freq  # frequency
        self.quote_ccy = quote_ccy  # quote ccy
        self.exch = exch  # exchange
        self.mkt_type = mkt_type  # market type
        self.start_date = start_date  # start date
        self.end_date = end_date  # end date
        self.fields = fields  # fields
        self.tz = tz  # tz
        self.inst = inst  # institution
        self.cat = cat  # category of asset class or time series
        self.trials = trials  # number of times to try query request
        self.pause = pause  # number of seconds to pause between query request trials
        self.source_tickers = source_tickers  # tickers used by data source
        self.source_freq = source_freq  # frequency used by data source
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
            "glassnode",
            "tiingo",
            "investpy",
            "yahoo",
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
    def inst(self):
        """
        Returns institution name for data request.
        """
        return self._inst

    @inst.setter
    def inst(self, inst):
        """
        Sets institution's name for data request.
        """
        if inst is None:
            self._inst = inst
        elif isinstance(inst, str):
            self._inst = inst
        else:
            raise TypeError("Institution must be a string.")

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

    def get_req(self, url: str, params: Dict[str, Union[str, int]],
                headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Submits get request to API.

        Parameters
        ----------
        url: str
            Endpoint url for get request. Base urls are stored in DataCredentials.
        params: dict
            Dictionary containing parameter values for get request.
        headers: dict, optional, default None
            Dictionary containing headers for get request.

        Returns
        -------
        resp: dict
            Data response in JSON format.
        """
        # set number of attempts
        attempts = 0
        # run a while loop in case the attempt fails
        while attempts < self.trials:

            # get request
            try:
                resp = requests.get(url, params=params, headers=headers)
                # check for status code
                resp.raise_for_status()

                return resp.json()

            # handle HTTP errors
            except requests.exceptions.HTTPError as http_err:
                status_code = resp.status_code

                # Tailored handling for different status codes
                if status_code == 400:
                    logging.warning(f"Bad Request (400): {resp.text}")
                elif status_code == 401:
                    logging.warning("Unauthorized (401): Check the authentication credentials.")
                elif status_code == 403:
                    logging.warning("Forbidden (403): You do not have permission to access this resource.")
                elif status_code == 404:
                    logging.warning("Not Found (404): The requested resource could not be found.")
                elif status_code == 500:
                    logging.error("Internal Server Error (500): The server encountered an error.")
                elif status_code == 503:
                    logging.error("Service Unavailable (503): The server is temporarily unavailable.")
                else:
                    logging.error(f"HTTP error occurred: {http_err} (Status Code: {status_code})")
                    logging.error(f"Response Content: {resp.text}")

                # Increment attempts and log warning
                attempts += 1
                logging.warning(f"Attempt #{attempts}: Failed to get data due to: {http_err}")
                sleep(self.pause)  # Pause before retrying
                if attempts == self.trials:
                    logging.error("Max attempts reached. Unable to fetch data.")
                    break

            # handle non-HTTP exceptions (e.g., network issues)
            except requests.exceptions.RequestException as req_err:
                attempts += 1
                logging.warning(f"Request error on attempt #{attempts}: {req_err}. "
                                f"Retrying after {self.pause} seconds...")
                sleep(self.pause)
                if attempts == self.trials:
                    logging.error("Max attempts reached. Unable to fetch data due to request errors.")
                    break

            # handle other exceptions
            except Exception as e:
                attempts += 1
                logging.warning(f"An unexpected error occurred: {e}. "
                                f"Retrying after {self.pause} seconds...")
                sleep(self.pause)
                if attempts == self.trials:
                    logging.error("Max attempts reached. Unable to fetch data due to request errors.")
                    break

        # return None if the API call fails
        return None
