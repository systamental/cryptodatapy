import logging
from datetime import datetime, timedelta
from importlib import resources
from typing import Dict, List, Union

import pandas as pd

from cryptodatapy.extract.datarequest import DataRequest


class ConvertParams:
    """
    Converts data request parameters from CryptoDataPy to data source format.
    """

    def __init__(
        self,
        data_req: DataRequest = None,
    ):
        """
        Constructor

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request in CryptoDataPy format.

        """
        self.data_req = data_req

    def to_cryptocompare(self) -> Dict[str, Union[list, str, int, float, None]]:
        """
        Convert tickers from CryptoDataPy to CryptoCompare format.
        """
        # convert tickers
        if self.data_req.source_tickers is not None:
            tickers = self.data_req.source_tickers
            self.data_req.tickers = self.data_req.source_tickers
        else:
            tickers = [ticker.upper() for ticker in self.data_req.tickers]
        # convert freq
        if self.data_req.source_freq is not None:
            freq = self.data_req.source_freq
            self.data_req.freq = self.data_req.source_freq
        else:
            if self.data_req.freq[-3:] == "min":
                freq = "histominute"
            elif self.data_req.freq[-1] == "h":
                freq = "histohour"
            else:
                freq = "histoday"
        # convert quote ccy
        if self.data_req.quote_ccy is None:
            quote_ccy = "USD"
        else:
            quote_ccy = self.data_req.quote_ccy.upper()
        # convert exch
        if self.data_req.exch is None:
            exch = "CCCAGG"
        else:
            exch = self.data_req.exch
        # convert start date
        if self.data_req.freq[-3:] == "min":  # limit to higher frequency data responses
            start_date = round((datetime.now() - timedelta(days=7)).timestamp())
        # no start date
        elif self.data_req.start_date is None:
            start_date = round(pd.Timestamp("2009-01-03 00:00:00").timestamp())
        else:
            start_date = round(pd.Timestamp(self.data_req.start_date).timestamp())
        # convert end date
        if self.data_req.end_date is None:
            end_date = round(pd.Timestamp(datetime.utcnow()).timestamp())
        else:
            end_date = round(pd.Timestamp(self.data_req.end_date).timestamp())
        # fields
        if self.data_req.source_fields is not None:
            fields = self.data_req.source_fields
            self.data_req.fields = self.data_req.source_fields
        else:
            fields = self.convert_fields(data_source='cryptocompare')
        # tz
        if self.data_req.tz is None:
            tz = "UTC"
        else:
            tz = self.data_req.tz

        return {
            "tickers": tickers,
            "freq": freq,
            "quote_ccy": quote_ccy,
            "exch": exch,
            "ctys": None,
            "mkt_type": self.data_req.mkt_type,
            "mkts": None,
            "start_date": start_date,
            "end_date": end_date,
            "fields": fields,
            "tz": tz,
            "inst": None,
            "cat": 'crypto',
            "trials": self.data_req.trials,
            "pause": self.data_req.pause,
            "source_tickers": self.data_req.source_tickers,
            "source_freq": self.data_req.source_freq,
            "source_fields": self.data_req.source_fields,
        }

    def to_coinmetrics(self) -> Dict[str, Union[list, str, int, float, None]]:
        """
        Convert tickers from CryptoDataPy to CoinMetrics format.

        """
        # convert tickers
        if self.data_req.source_tickers is not None:
            tickers = self.data_req.source_tickers
            self.data_req.tickers = self.data_req.source_tickers
        else:
            tickers = [ticker.lower() for ticker in self.data_req.tickers]
        # convert freq
        if self.data_req.source_freq is not None:
            freq = self.data_req.source_freq
            self.data_req.freq = self.data_req.source_freq
        else:
            if self.data_req.freq == "block":
                freq = "1b"
            elif self.data_req.freq == "tick":
                freq = "tick"
            elif self.data_req.freq[-1] == "s":
                freq = "1s"
            elif self.data_req.freq[-3:] == "min":
                freq = "1m"
            elif self.data_req.freq[-1] == "h":
                freq = "1h"
            else:
                freq = "1d"
        # convert quote ccy
        if self.data_req.quote_ccy is None:
            quote_ccy = "usdt"
        else:
            quote_ccy = self.data_req.quote_ccy.lower()
        # convert inst
        if self.data_req.inst is None:
            inst = "grayscale"
        else:
            inst = self.data_req.inst.lower()
        # convert to exch
        if self.data_req.exch is None:
            exch = "binance"
        else:
            exch = self.data_req.exch.lower()
        # fields
        if self.data_req.source_fields is not None:
            fields = self.data_req.source_fields
            self.data_req.fields = self.data_req.source_fields
        else:
            fields = self.convert_fields(data_source='coinmetrics')
        # convert tz
        if self.data_req.tz is None:
            tz = "UTC"
        else:
            tz = self.data_req.tz
        # convert tickers to markets
        mkts_list = []

        if self.data_req.source_tickers is not None:
            mkts_list = self.data_req.source_tickers
            self.data_req.tickers = self.data_req.source_tickers

        for ticker in self.data_req.tickers:
            if self.data_req.mkt_type == "spot":
                mkts_list.append(
                    exch
                    + "-"
                    + ticker.lower()
                    + "-"
                    + quote_ccy.lower()
                    + "-"
                    + self.data_req.mkt_type.lower()
                )
            elif self.data_req.mkt_type == "perpetual_future":
                if exch == "binance" or exch == "bybit" or exch == "bitmex":
                    mkts_list.append(
                        exch
                        + "-"
                        + ticker.upper()
                        + quote_ccy.upper()
                        + "-"
                        + "future"
                    )
                elif exch == "ftx":
                    mkts_list.append(
                        exch + "-" + ticker.upper() + "-" + "PERP" + "-" + "future"
                        )
                elif exch == "okex":
                    mkts_list.append(
                        exch
                        + "-"
                        + ticker.upper()
                        + "-"
                        + quote_ccy.upper()
                        + "-"
                        + "SWAP"
                        + "-"
                        + "future"
                    )
                elif exch == "huobi":
                    mkts_list.append(
                        exch
                        + "-"
                        + ticker.upper()
                        + "-"
                        + quote_ccy.upper()
                        + "_"
                        + "SWAP"
                        + "-"
                        + "future"
                    )
                elif exch == "hitbtc":
                    mkts_list.append(
                        exch
                        + "-"
                        + ticker.upper()
                        + quote_ccy.upper()
                        + "_"
                        + "PERP"
                        + "-"
                        + "future"
                    )

        return {
            "tickers": tickers,
            "freq": freq,
            "quote_ccy": quote_ccy,
            "exch": exch,
            "ctys": None,
            "mkt_type": self.data_req.mkt_type,
            "mkts": mkts_list,
            "start_date": self.data_req.start_date,
            "end_date": self.data_req.end_date,
            "fields": fields,
            "tz": tz,
            "inst": inst,
            "cat": 'crypto',
            "trials": self.data_req.trials,
            "pause": self.data_req.pause,
            "source_tickers": self.data_req.source_tickers,
            "source_freq": self.data_req.source_freq,
            "source_fields": self.data_req.source_fields,
        }

    def to_glassnode(self) -> Dict[str, Union[list, str, int, float, None]]:
        """
        Convert tickers from CryptoDataPy to Glassnode format.

        """
        # convert tickers
        if self.data_req.source_tickers is not None:
            tickers = self.data_req.source_tickers
            self.data_req.tickers = self.data_req.source_tickers
        else:
            tickers = self.data_req.tickers
        # convert freq
        if self.data_req.source_freq is not None:
            freq = self.data_req.source_freq
            self.data_req.freq = self.data_req.source_freq
        else:
            if self.data_req.freq[-3:] == "min":
                freq = "10m"
            elif self.data_req.freq[-1] == "h":
                freq = "1h"
            elif self.data_req.freq == "d":
                freq = "24h"
            elif self.data_req.freq == "w":
                freq = "1w"
            elif self.data_req.freq == "m":
                freq = "1month"
            else:
                freq = self.data_req.freq
        # convert quote ccy
        if self.data_req.quote_ccy is None:
            quote_ccy = "USD"
        else:
            quote_ccy = self.data_req.quote_ccy.upper()
        # convert exch
        exch = self.data_req.exch
        # start date
        if self.data_req.start_date is None:
            start_date = round(pd.Timestamp("2009-01-03 00:00:00").timestamp())
        else:
            start_date = round(pd.Timestamp(self.data_req.start_date).timestamp())
        # end date
        if self.data_req.end_date is None:
            end_date = self.data_req.end_date
        else:
            end_date = round(pd.Timestamp(self.data_req.end_date).timestamp())
        # convert fields
        if self.data_req.source_fields is not None:
            fields = self.data_req.source_fields
            self.data_req.fields = self.data_req.source_fields
        else:
            fields = self.convert_fields(data_source='glassnode')
        # convert tz
        if self.data_req.tz is None:
            tz = "UTC"
        else:
            tz = self.data_req.tz
        # convert inst
        if self.data_req.inst is None:
            inst = "purpose"
        else:
            inst = self.data_req.inst.lower()

        return {
            "tickers": tickers,
            "freq": freq,
            "quote_ccy": quote_ccy,
            "exch": exch,
            "ctys": None,
            "mkt_type": self.data_req.mkt_type,
            "mkts": None,
            "start_date": start_date,
            "end_date": end_date,
            "fields": fields,
            "tz": tz,
            "inst": inst,
            "cat": 'crypto',
            "trials": self.data_req.trials,
            "pause": self.data_req.pause,
            "source_tickers": self.data_req.source_tickers,
            "source_freq": self.data_req.source_freq,
            "source_fields": self.data_req.source_fields,
        }

    def to_tiingo(self) -> Dict[str, Union[list, str, int, float, None]]:
        """
        Convert tickers from CryptoDataPy to Tiingo format.

        """
        # convert tickers
        if self.data_req.source_tickers is not None:
            tickers = self.data_req.source_tickers
            self.data_req.tickers = self.data_req.source_tickers
        else:
            tickers = [ticker.lower() for ticker in self.data_req.tickers]
        # convert freq
        if self.data_req.source_freq is not None:
            freq = self.data_req.source_freq
            self.data_req.freq = self.data_req.source_freq
        else:
            if self.data_req.freq[-3:] == "min":
                freq = self.data_req.freq
            elif self.data_req.freq[-1] == "h":
                freq = "1hour"
            elif self.data_req.freq == "d":
                freq = "1day"
            elif self.data_req.freq == "w":
                freq = "1week"
            else:
                freq = self.data_req.freq
        # convert quote ccy
        if self.data_req.quote_ccy is None:
            quote_ccy = "usd"
        else:
            quote_ccy = self.data_req.quote_ccy.lower()
        # convert exch
        if (
            self.data_req.exch is None
            and self.data_req.cat == "eqty"
            and self.data_req.freq
            in ["1min", "5min", "10min", "15min", "30min", "1h", "2h", "4h", "8h"]
        ):
            exch = "iex"
        else:
            exch = self.data_req.exch
        # convert tickers to mkts
        mkts_list = []
        if self.data_req.source_tickers is not None:
            mkts_list = self.data_req.source_tickers
            self.data_req.tickers = self.data_req.source_tickers
        else:
            if self.data_req.cat == "fx":
                fx_list = self.convert_fx_tickers(quote_ccy=quote_ccy)
                mkts_list = [ticker.lower().replace("/", "") for ticker in fx_list]
            elif self.data_req.cat == "crypto":
                mkts_list = [
                    ticker.lower() + quote_ccy for ticker in self.data_req.tickers
                ]
        # convert start date
        if self.data_req.start_date is None and self.data_req.cat == 'crypto':
            start_date = datetime(2010, 1, 1, 0, 0)
        else:
            start_date = self.data_req.start_date

        # convert end date
        if self.data_req.end_date is None:
            end_date = datetime.utcnow()
        else:
            end_date = self.data_req.end_date
        # convert fields
        if self.data_req.source_fields is not None:
            fields = self.data_req.source_fields
            self.data_req.fields = self.data_req.source_fields
        else:
            fields = self.convert_fields(data_source='tiingo')
        # tz
        if self.data_req.cat == 'eqty' or self.data_req.cat == 'fx':
            tz = "America/New_York"
        else:
            tz = "UTC"

        return {
            "tickers": tickers,
            "freq": freq,
            "quote_ccy": quote_ccy,
            "exch": exch,
            "ctys": None,
            "mkt_type": self.data_req.mkt_type,
            "mkts": mkts_list,
            "start_date": start_date,
            "end_date": end_date,
            "fields": fields,
            "tz": tz,
            "inst": None,
            "cat": self.data_req.cat,
            "trials": self.data_req.trials,
            "pause": self.data_req.pause,
            "source_tickers": self.data_req.source_tickers,
            "source_freq": self.data_req.source_freq,
            "source_fields": self.data_req.source_fields,
        }

    def to_ccxt(self) -> Dict[str, Union[list, str, int, float, None]]:
        """
        Convert tickers from CryptoDataPy to CCXT format.

        """
        # convert tickers
        if self.data_req.source_tickers is not None:
            tickers = [ticker.split('/')[0] for ticker in self.data_req.source_tickers]
            self.data_req.tickers = tickers
        else:
            tickers = [ticker.upper() for ticker in self.data_req.tickers]
        # convert freq
        if self.data_req.source_freq is not None:
            freq = self.data_req.source_freq
            self.data_req.freq = self.data_req.source_freq
        else:
            if self.data_req.freq == "tick":
                freq = "tick"
            elif self.data_req.freq[-3:] == "min":
                freq = self.data_req.freq.replace("min", "m")
            elif self.data_req.freq == "d":
                freq = "1d"
            elif self.data_req.freq == "w":
                freq = "1w"
            elif self.data_req.freq == "m":
                freq = "1M"
            elif self.data_req.freq[-1] == "m":
                freq = self.data_req.freq.replace("m", "M")
            elif self.data_req.freq == "q":
                freq = "1q"
            elif self.data_req.freq == "y":
                freq = "1y"
            else:
                freq = self.data_req.freq
        # convert quote ccy
        if self.data_req.quote_ccy is None:
            quote_ccy = "USDT"
        else:
            quote_ccy = self.data_req.quote_ccy.upper()
        # convert exch
        if self.data_req.mkt_type == "perpetual_future" and (
            self.data_req.exch is None or self.data_req.exch == "binance"
        ):
            exch = "binanceusdm"
        elif self.data_req.exch is None:
            exch = "binance"
        elif (
            self.data_req.exch == "kucoin"
            and self.data_req.mkt_type == "perpetual_future"
        ):
            exch = "kucoinfutures"
        elif (
            self.data_req.exch == "huobi"
            and self.data_req.mkt_type == "perpetual_future"
        ):
            exch = "huobipro"
        elif (
            self.data_req.exch == "bitfinex"
            and self.data_req.mkt_type == "perpetual_future"
        ):
            exch = "bitfinex2"
        elif (
            self.data_req.exch == "mexc"
            and self.data_req.mkt_type == "perpetual_future"
        ):
            exch = "mexc3"
        else:
            exch = self.data_req.exch.lower()
        # convert tickers to mkts
        mkts_list = []
        if self.data_req.source_tickers is not None:
            mkts_list = self.data_req.source_tickers
        else:
            for ticker in self.data_req.tickers:
                if self.data_req.mkt_type == "spot":
                    mkts_list.append(ticker.upper() + "/" + quote_ccy.upper())
                elif self.data_req.mkt_type == "perpetual_future":
                    if exch == "binanceusdm":
                        mkts_list.append(ticker.upper() + "/" + quote_ccy.upper())
                    elif (
                        exch == "ftx"
                        or exch == "okx"
                        or exch == "kucoinfutures"
                        or exch == "huobipro"
                        or exch == "cryptocom"
                        or exch == "bitfinex2"
                        or exch == "bybit"
                        or exch == "mexc3"
                        or exch == "aax"
                        or exch == "bitmex"
                    ):
                        mkts_list.append(
                            ticker.upper()
                            + "/"
                            + quote_ccy.upper()
                            + ":"
                            + quote_ccy.upper()
                        )
        # convert start date
        if self.data_req.start_date is None:
            start_date = round(
                pd.Timestamp("2010-01-01 00:00:00").timestamp() * 1e3
            )
        else:
            start_date = round(
                pd.Timestamp(self.data_req.start_date).timestamp() * 1e3
            )
        # convert end date
        if self.data_req.end_date is None:
            end_date = round(pd.Timestamp(datetime.utcnow()).timestamp() * 1e3)
        else:
            end_date = round(pd.Timestamp(self.data_req.end_date).timestamp() * 1e3)
        # convert fields
        if self.data_req.source_fields is not None:
            fields = self.data_req.source_fields
            self.data_req.fields = self.data_req.source_fields
        else:
            fields = self.convert_fields(data_source='ccxt')
        # tz
        if self.data_req.tz is None:
            tz = "UTC"
        else:
            tz = self.data_req.tz

        return {
            "tickers": tickers,
            "freq": freq,
            "quote_ccy": quote_ccy,
            "exch": exch,
            "ctys": None,
            "mkt_type": self.data_req.mkt_type,
            "mkts": mkts_list,
            "start_date": start_date,
            "end_date": end_date,
            "fields": fields,
            "tz": tz,
            "inst": None,
            "cat": 'crypto',
            "trials": self.data_req.trials,
            "pause": self.data_req.pause,
            "source_tickers": self.data_req.source_tickers,
            "source_freq": self.data_req.source_freq,
            "source_fields": self.data_req.source_fields,
        }

    def to_investpy(self) -> Dict[str, Union[list, str, int, float, None]]:
        """
        Convert tickers from CryptoDataPy to InvestPy format.

        """
        # convert tickers
        with resources.path("cryptodatapy.conf", "tickers.csv") as f:
            tickers_path = f
        tickers_df, tickers = pd.read_csv(tickers_path, index_col=0, encoding="latin1"), []

        if self.data_req.source_tickers is not None:
            tickers = self.data_req.source_tickers
            self.data_req.tickers = self.data_req.source_tickers
        else:
            if self.data_req.cat == "eqty":
                for ticker in self.data_req.tickers:
                    if len(ticker) > 4:
                        try:
                            tickers.append(tickers_df.loc[ticker, "investpy_id"])
                        except KeyError:
                            logging.warning(
                                f"{ticker} not found for InvestPY data source. Check tickers in "
                                f"data catalog and try again."
                            )
                            self.data_req.tickers.remove(ticker)
                    else:
                        tickers.append(ticker.upper())
            elif self.data_req.cat != "fx":
                for ticker in self.data_req.tickers:
                    try:
                        tickers.append(tickers_df.loc[ticker, "investpy_id"])
                    except KeyError:
                        logging.warning(
                            f"{ticker} not found for InvestPy data source. Check tickers in "
                            f"data catalog and try again."
                        )
                        self.data_req.tickers.remove(ticker)
            else:
                tickers = [ticker.upper() for ticker in self.data_req.tickers]
        # convert freq
        if self.data_req.source_freq is not None:
            freq = self.data_req.source_freq
            self.data_req.freq = self.data_req.source_freq
        elif self.data_req.cat != "macro":
            freq = "Daily"
        else:
            freq = self.data_req.freq
        # convert quote ccy
        if self.data_req.quote_ccy is None:
            quote_ccy = "USD"
        else:
            quote_ccy = self.data_req.quote_ccy.upper()
        # convert ctys
        ctys_list = []
        if self.data_req.cat == "macro":
            for ticker in self.data_req.tickers:
                try:
                    ctys_list.append(tickers_df.loc[ticker, "country_name"].lower())
                except KeyError:
                    logging.warning(
                        f"{ticker} not found for {self.data_req.source} source. Check tickers in "
                        f"data catalog and try again."
                    )
        # convert tickers to markets
        mkts_list = []
        if self.data_req.source_tickers is not None:
            mkts_list = self.data_req.source_tickers
            self.data_req.tickers = self.data_req.source_tickers
        else:
            if self.data_req.cat == "fx":
                mkts_list = self.convert_fx_tickers(quote_ccy=quote_ccy)
        # convert start date
        if self.data_req.start_date is None:
            start_date = pd.Timestamp("1970-01-01").strftime("%d/%m/%Y")
        else:
            start_date = pd.Timestamp(self.data_req.start_date).strftime("%d/%m/%Y")
        # convert end date
        if self.data_req.end_date is None:
            end_date = datetime.utcnow().strftime("%d/%m/%Y")
        else:
            end_date = pd.Timestamp(self.data_req.end_date).strftime("%d/%m/%Y")
        # convert fields
        if self.data_req.source_fields is not None:
            fields = self.data_req.source_fields
            self.data_req.fields = self.data_req.source_fields
        else:
            fields = self.convert_fields(data_source='investpy')

        return {
            "tickers": tickers,
            "freq": freq,
            "quote_ccy": quote_ccy,
            "exch": self.data_req.exch,
            "ctys": ctys_list,
            "mkt_type": self.data_req.mkt_type,
            "mkts": mkts_list,
            "start_date": start_date,
            "end_date": end_date,
            "fields": fields,
            "tz": self.data_req.tz,
            "inst": None,
            "cat": self.data_req.cat,
            "trials": self.data_req.trials,
            "pause": self.data_req.pause,
            "source_tickers": self.data_req.source_tickers,
            "source_freq": self.data_req.source_freq,
            "source_fields": self.data_req.source_fields,
        }

    def to_dbnomics(self) -> Dict[str, Union[list, str, int, float, None]]:
        """
        Convert tickers from CryptoDataPy to DBnomics format.

        """
        # convert tickers
        with resources.path("cryptodatapy.conf", "tickers.csv") as f:
            tickers_path = f
        tickers_df, tickers = pd.read_csv(tickers_path, index_col=0, encoding="latin1"), []

        if self.data_req.source_tickers is not None:
            tickers = self.data_req.source_tickers
            self.data_req.tickers = self.data_req.source_tickers
        else:
            for ticker in self.data_req.tickers:
                try:
                    tickers.append(tickers_df.loc[ticker, "dbnomics_id"])
                except KeyError:
                    logging.warning(
                        f"{ticker} not found for DBnomics source. Check tickers in"
                        f" data catalog and try again."
                    )
                    self.data_req.tickers.remove(ticker)
        # convert freq
        if self.data_req.source_freq is not None:
            freq = self.data_req.source_freq
            self.data_req.freq = self.data_req.source_freq
        else:
            freq = self.data_req.freq
        # quote ccy
        quote_ccy = self.data_req.quote_ccy
        # fields
        if self.data_req.source_fields is not None:
            fields = self.data_req.source_fields
            self.data_req.fields = self.data_req.source_fields
        else:
            fields = self.convert_fields(data_source='dbnomics')

        return {
            "tickers": tickers,
            "freq": freq,
            "quote_ccy": quote_ccy,
            "exch": self.data_req.exch,
            "ctys": None,
            "mkt_type": None,
            "mkts": None,
            "start_date": self.data_req.start_date,
            "end_date": self.data_req.end_date,
            "fields": fields,
            "tz": self.data_req.tz,
            "inst": None,
            "cat": self.data_req.cat,
            "trials": self.data_req.trials,
            "pause": self.data_req.pause,
            "source_tickers": self.data_req.source_tickers,
            "source_freq": self.data_req.source_freq,
            "source_fields": self.data_req.source_fields,
        }

    def to_fred(self) -> Dict[str, Union[list, str, int, float, None]]:
        """
        Convert tickers from CryptoDataPy to Fred format.

        """
        # convert tickers
        with resources.path("cryptodatapy.conf", "tickers.csv") as f:
            tickers_path = f
        tickers_df, tickers = pd.read_csv(tickers_path, index_col=0, encoding="latin1"), []

        if self.data_req.source_tickers is not None:
            tickers = self.data_req.source_tickers
            self.data_req.tickers = self.data_req.source_tickers
        else:
            for ticker in self.data_req.tickers:
                try:
                    tickers.append(tickers_df.loc[ticker, "fred_id"])
                except KeyError:
                    logging.warning(
                        f"{ticker} not found for Fred data source. Check tickers in"
                        f" data catalog and try again."
                    )
                    self.data_req.tickers.remove(ticker)
        # convert freq
        if self.data_req.source_freq is not None:
            freq = self.data_req.source_freq
            self.data_req.freq = self.data_req.source_freq
        else:
            freq = self.data_req.freq
        # convert quote ccy
        quote_ccy = self.data_req.quote_ccy
        # start date
        if self.data_req.start_date is None:
            start_date = datetime(1920, 1, 1)
        else:
            start_date = self.data_req.start_date
        # end date
        if self.data_req.end_date is None:
            end_date = datetime.utcnow()
        else:
            end_date = self.data_req.end_date
        # fields
        if self.data_req.source_fields is not None:
            fields = self.data_req.source_fields
            self.data_req.fields = self.data_req.source_fields
        else:
            fields = self.convert_fields(data_source='fred')
        # tz
        if self.data_req.tz is None:
            tz = "America/New_York"
        else:
            tz = self.data_req.tz

        return {
            "tickers": tickers,
            "freq": freq,
            "quote_ccy": quote_ccy,
            "exch": self.data_req.exch,
            "ctys": None,
            "mkt_type": self.data_req.mkt_type,
            "mkts": None,
            "start_date": start_date,
            "end_date": end_date,
            "fields": fields,
            "tz": tz,
            "inst": None,
            "cat": self.data_req.cat,
            "trials": self.data_req.trials,
            "pause": self.data_req.pause,
            "source_tickers": self.data_req.source_tickers,
            "source_freq": self.data_req.source_freq,
            "source_fields": self.data_req.source_fields,
        }

    def to_yahoo(self) -> Dict[str, Union[list, str, int, float, None]]:
        """
        Convert tickers from CryptoDataPy to Yahoo Finance format.

        """
        # convert tickers
        if self.data_req.source_tickers is not None:
            tickers = self.data_req.source_tickers
            self.data_req.tickers = self.data_req.source_tickers
        else:
            tickers = [ticker.upper() for ticker in self.data_req.tickers]
        # convert freq
        if self.data_req.source_freq is not None:
            freq = self.data_req.source_freq
            self.data_req.freq = self.data_req.source_freq
        else:
            freq = self.data_req.freq
        # convert quote ccy
        quote_ccy = self.data_req.quote_ccy
        # start date
        if self.data_req.start_date is None:
            start_date = datetime(1920, 1, 1)
        else:
            start_date = self.data_req.start_date
        # end date
        if self.data_req.end_date is None:
            end_date = datetime.utcnow()
        else:
            end_date = self.data_req.end_date
        # fields
        if self.data_req.source_fields is not None:
            fields = self.data_req.source_fields
            self.data_req.fields = self.data_req.source_fields
        else:
            fields = self.convert_fields(data_source='yahoo')
        # tz
        if self.data_req.tz is None:
            tz = "America/New_York"
        else:
            tz = self.data_req.tz

        return {
            "tickers": tickers,
            "freq": freq,
            "quote_ccy": quote_ccy,
            "exch": self.data_req.exch,
            "ctys": None,
            "mkt_type": self.data_req.mkt_type,
            "mkts": None,
            "start_date": start_date,
            "end_date": end_date,
            "fields": fields,
            "tz": tz,
            "inst": None,
            "cat": self.data_req.cat,
            "trials": self.data_req.trials,
            "pause": self.data_req.pause,
            "source_tickers": self.data_req.source_tickers,
            "source_freq": self.data_req.source_freq,
            "source_fields": self.data_req.source_fields,
        }

    def convert_fx_tickers(self, quote_ccy: str) -> List[str]:
        """
        Converts base and quote currency tickers to fx pairs following fx quoting convention.

        Parameters
        ---------
        quote_ccy: str
            Quote currency

        Returns
        -------
        quote_ccy: str
            Quote currency.

        """
        mkts = []  # fx pairs list
        # fx groups
        base_ccys = ["EUR", "GBP", "AUD", "NZD"]
        # g10_fx = ['USD', 'EUR', 'GBP', 'JPY', 'CHF', 'CAD', 'AUD', 'NZD', 'NOK', 'SEK']
        # dm_fx = ['USD', 'EUR', 'GBP', 'JPY', 'CHF', 'CAD', 'AUD', 'NZD', 'NOK', 'SEK', 'SGD', 'ILS', 'HKD', ]
        # em_fx = ['ARS', 'BRL', 'CHN', 'CLP', 'CNY', 'COP', 'IDR', 'INR', 'KRW', 'MYR', 'MXN', 'PEN', 'PHP', 'RUB',
        #          'TRY', 'TWD', 'ZAR']

        for ticker in self.data_req.tickers:
            if ticker.upper() in base_ccys and quote_ccy.upper() == "USD":
                mkts.append(ticker.upper() + "/" + quote_ccy.upper())
            elif quote_ccy.upper() == "USD":
                mkts.append(quote_ccy.upper() + "/" + ticker.upper())
            else:
                mkts.append(ticker.upper() + "/" + quote_ccy.upper())

        return mkts

    def convert_fields(self, data_source: str) -> List[str]:
        """
        Converts fields from CryptoDataPy to data source format.

        Parameters
        ---------
        data_source: str
            Name of data source for fields conversions.

        Returns
        -------
        fields_list: list
            List of fields in data source format.

        """
        # get fields
        with resources.path("cryptodatapy.conf", "fields.csv") as f:
            fields_dict_path = f
        fields_df, fields_list = (
            pd.read_csv(fields_dict_path, index_col=0, encoding="latin1"),
            [],
        )

        # when source fields already provided in data req
        if self.data_req.source_fields is not None:
            fields_list = self.data_req.source_fields

        # convert to source format
        else:
            for field in self.data_req.fields:
                try:
                    fields_list.append(fields_df.loc[field, data_source + "_id"])
                except KeyError as e:
                    logging.warning(e)
                    logging.warning(
                        f"Id for {field} could not be found in the data catalog."
                        f" Try using source field ids."
                    )

        return fields_list
