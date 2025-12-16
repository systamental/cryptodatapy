import logging
from datetime import datetime
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Tuple, Union
from importlib import resources
import pandas as pd

from cryptodatapy.core.data_request import DataRequest

logger = logging.getLogger(__name__)


class BaseParamConverter(ABC):
    """
    Abstract Base Class for all parameter conversion logic.

    This class defines the interface (the 'convert' method) and provides common
    utility methods needed by all data source-specific parameter converters to
    translate a DataRequest object into source-specific parameter dictionary.
    """

    def __init__(self, data_req: DataRequest):
        """
        Stores the input DataRequest object.

        Parameters
        ----------
        data_req : DataRequest
            The standard CryptoDataPy data request object.
        """
        self.data_req = data_req

    def _convert_case_tickers(self, case: str = 'upper') -> List[str]:
        """
        Helper function to handle simple ticker conversion logic. This method
        is placed in the base class to avoid repetition in subclasses that
        only require simple case changes.

        If source_tickers are already provided in the DataRequest, they are
        used directly. Otherwise, standard DataRequest tickers are converted
        to the specified case ('upper' or 'lower').

        Parameters
        ----------
        case : str
            The desired case for tickers: 'upper' or 'lower'.

        Returns
        -------
        List[str]
            The list of converted tickers.
        """
        if self.data_req.source_tickers is not None:
            # If source tickers are pre-specified, use them directly
            return self.data_req.source_tickers

        if case == 'upper':
            return [ticker.upper() for ticker in self.data_req.tickers]
        elif case == 'lower':
            return [ticker.lower() for ticker in self.data_req.tickers]
        else:
            # Fallback to original tickers if case is invalid/not specified
            logger.warning(
                f"Invalid case '{case}' specified for simple ticker conversion. Returning original tickers."
            )
            return self.data_req.tickers

    def _convert_quote_ccy(self, default_ccy: str = 'USD', case: str = 'upper') -> str:
        """
        Converts the quote currency to the format required by the vendor.

        This method serves as a default implementation for the most common
        conversion, but allows subclasses to customize the default currency
        and the case requirement.

        Parameters
        ----------
        default_ccy : str, optional
            The currency to return if self.data_req.quote_ccy is None.
            Defaults to 'USD'.
        case : str, optional
            The desired case for the currency: 'upper', 'lower', or 'none'.
            Defaults to 'upper'.

        Returns
        -------
        str
            The converted quote currency string.
        """
        ccy = self.data_req.quote_ccy

        if ccy is None:
            ccy = default_ccy

        if case == 'upper':
            return ccy.upper()
        elif case == 'lower':
            return ccy.lower()
        elif case == 'none':
            return ccy
        else:
            logger.warning(
                f"Invalid case '{case}' specified for quote currency conversion. Returning original currency."
            )
            return ccy

    def _convert_markets(self) -> List[str]:
        """
        Implementation of the most common market conversion pattern.

        The most common market type conversion is for 'spot' markets, which
        usually involves creating a simple BASE/QUOTE market identifier.
        """
        if self.data_req.source_markets:
            # If source_markets is already provided, use it directly.
            # Note: The adapter must handle extracting source_tickers from these.
            return self.data_req.source_markets

        if self.data_req.mkt_type == "spot" or self.data_req.mkt_type is None:
            # This logic mimics the simple spot market construction seen BASE/QUOTE format, e.g., 'BTC/USD'.
            quote_ccy = self.data_req.quote_ccy.upper() if self.data_req.quote_ccy else 'USD'
            return [
                f"{ticker.upper()}/{quote_ccy}"
                for ticker in self.data_req.tickers
            ]
        if self.data_req.mkt_type == "perpetual_future":
            quote_ccy = self.data_req.quote_ccy.upper() if self.data_req.quote_ccy else 'USDT'
            return [
                f"{ticker.upper()}/{quote_ccy}:{quote_ccy}"
                for ticker in self.data_req.tickers
            ]

        return []

    def _convert_freq(self) -> str:
        """
        Converts frequency from CryptoDataPy format (e.g., '30min', 'm')
        to the common canonical API format (e.g., '30m', '1M').

        This serves as a robust default for many exchange APIs.

        Returns
        -------
        str
            The converted canonical frequency string.
        """
        freq = self.data_req.freq
        if freq is None:
            return "1d"
        elif freq == "tick":
            return "tick"
        elif freq[-3:] == "min":
            return freq.replace("min", "m")
        elif freq[-1] == "h":
            return freq
        elif freq == "w":
            return "1w"
        elif freq == "m":
            return "1M"  # Standard for Monthly
        elif freq == "q":
            return "1q"
        elif freq == "y":
            return "1y"
        else:
            return "1d"

    def _load_field_map(self, source: str) -> pd.DataFrame:
        """
        Loads the field mapping file (fields.csv) and returns the relevant
        data source's mapping for use in converting fields.

        Parameters
        ----------
        source : str
            The name of the data source (e.g., 'cryptocompare', 'fred').

        Returns
        -------
        pd.DataFrame
            A DataFrame containing the mapping from CryptoDataPy fields to source fields.
        """
        try:
            # get fields.csv in cryptodatapy.conf
            with resources.path("cryptodatapy.metadata", "fields.csv") as f:
                fields_path = f

            # The field mapping file should contain columns like:
            # index | cryptodatapy_id | source_a_id | source_b_id
            fields_df = pd.read_csv(fields_path, index_col=0, encoding="latin1")

            # filter to only the required source fields col
            source_field_col = f'{source}_id'

            if source_field_col not in fields_df.columns:
                logger.warning(
                    f"'{source_field_col}' not found in fields.csv. "
                    f"Falling back to input fields."
                )
                return pd.DataFrame()  # Return empty if no specific map exists

            return fields_df

        except FileNotFoundError:
            logger.error("Field mapping file (fields.csv) not found.")
            return pd.DataFrame()

    def convert_fields(self, data_source: str) -> List[str]:
        """
        Converts the standard CryptoDataPy fields (e.g., 'close', 'volume')
        in data_req.fields to the vendor-specific field names.

        Parameters
        ----------
        data_source : str
            The name of the data source/vendor.

        Returns
        -------
        List[str]
            A list of source-specific field names.
        """
        # load the field mapping DataFrame
        fields_df = self._load_field_map(data_source)

        if self.data_req.source_fields is not None:
            # If source_fields are already provided, use them directly
            return self.data_req.source_fields

        elif fields_df.empty:
            # if mapping fails or is unavailable, use parameter fields
            return self.data_req.fields

        else:
            # create a mapping dictionary: {field: source_field}
            mapping_dict = fields_df[f"{data_source}_id"].to_dict()

            # map each field in data_req.fields to source field
            source_fields = []
            for field in self.data_req.fields:
                source_field = mapping_dict.get(field, field)

                # append if the field is not a missing value placeholder (like NaN or 'None')
                if pd.notna(source_field) and source_field is not None:
                    source_fields.append(str(source_field))
                else:
                    logger.warning(
                        f"Field '{field}' has no mapping for {data_source} and will be ignored."
                    )

            return source_fields

    def _get_standardized_timestamp(
            self,
            date_input: Optional[Union[str, datetime]],
            is_end_date: bool
    ) -> Optional[pd.Timestamp]:
        """
        Converts any date input (str, datetime, or None) into a standardized,
        timezone-naive Pandas Timestamp in UTC, applying sensible defaults.

        Parameters
        ----------
        date_input : Optional[Union[str, datetime]]
            The input date from data_req.start_date or data_req.end_date.
        is_end_date : bool
            True if processing end_date (defaults to now), False if start_date.

        Returns
        -------
        Optional[pd.Timestamp]
            The normalized Timestamp, or None if no date is provided and no default applies.
        """
        if date_input is not None:
            # convert input to Timestamp, attempting to handle timezones gracefully
            try:
                ts = pd.Timestamp(date_input)
                if ts.tzinfo is None:
                    # assume UTC if no timezone is provided
                    ts = ts.tz_localize('UTC')
                # convert/remove timezone for uniformity
                return ts.tz_convert('UTC').tz_localize(None)
            except Exception as e:
                logger.error(f"Failed to normalize date input '{date_input}': {e}")
                return None

        # Apply Base Class Defaults for None inputs
        if is_end_date:
            # Default end date to current UTC time
            return pd.Timestamp.utcnow().tz_localize(None)
        else:
            # Base class does NOT assume a default start date.
            # Subclasses must handle their specific required start defaults (e.g., '2009-01-03')
            # or complex logic (e.g., CryptoCompare 7-day lookback).
            return None

    def _convert_dates(
            self,
            format_type: str,
            default_start_str: Optional[str] = None
    ) -> Tuple[Optional[Union[str, int, datetime]], Optional[Union[str, int, datetime]]]:
        """
        Converts data request start and end dates to a specified source format.

        This utility performs the standard conversion from the DataRequest's
        start/end dates to the output format required by the vendor (e.g.,
        timestamp or YYYY-MM-DD string).

        Args:
            format_type (str): The desired output format for the source:
                - 'str_ymd': YYYY-MM-DD string format
                - 'str_dmy': DD/MM/YYYY string format
                - 'ts_sec': Unix timestamp in seconds
                - 'ts_ms': Unix timestamp in milliseconds
                - 'int_year': Integer year
                - 'direct': Direct assignment (no conversion)
            default_start_str (Optional[str]): A specific string to use if
                                               self.data_req.start_date is None.
                                               If None, the start date remains None.

        Returns
        -------
        Tuple[Optional[Union[str, int, datetime]], Optional[Union[str, int, datetime]]]
            A tuple containing the converted start_date and end_date.
        """

        # 1. Get Standardized Timestamps
        # The base class handles its own default for end_date (now), but relies
        # on the optional 'default_start_str' for the start date.

        start_dt = self._get_standardized_timestamp(self.data_req.start_date, is_end_date=False)
        end_dt = self._get_standardized_timestamp(self.data_req.end_date, is_end_date=True)

        # Apply specific start date default if requested by the subclass
        if start_dt is None and default_start_str is not None:
            start_dt = pd.Timestamp(default_start_str, tz='UTC').tz_localize(None)

        # 2. Format the Dates
        def _format_date(dt: Optional[pd.Timestamp], original_input: Any) -> Optional[Union[str, int, datetime]]:
            """Formats a single standardized Timestamp."""
            if format_type == 'direct':
                return original_input  # Return original input if 'direct' is requested
            if dt is None:
                return None

            if format_type == 'str_ymd':
                return dt.strftime('%Y-%m-%d')
            elif format_type == 'str_dmy':
                return dt.strftime("%d/%m/%Y")
            elif format_type == 'ts_sec':
                return int(dt.timestamp())
            elif format_type == 'ts_ms':
                return round(dt.timestamp() * 1e3)
            elif format_type == 'int_year':
                return int(dt.year)
            else:
                return dt.to_pydatetime()  # Fallback to standard datetime object

        converted_start = _format_date(start_dt, self.data_req.start_date)
        converted_end = _format_date(end_dt, self.data_req.end_date)

        return converted_start, converted_end

    @abstractmethod
    def convert(self, **kwargs: Any) -> Dict[str, Any]:
        """
        Abstract method that must be implemented by all subclasses.

        This method is responsible for translating all relevant fields (tickers,
        freq, dates, etc.) from the standard DataRequest format into a
        dictionary of parameters expected by the specific data vendor's API.

        The adapter is responsible for passing any necessary metadata
        (like lists of known assets/metrics/indexes) via **kwargs.

        Parameters
        ----------
        **kwargs : Any
            Additional keyword arguments for flexibility, typically vendor metadata.

        Returns
        -------
        Dict[str, Any]
            A dictionary containing the vendor-specific parameters, which may
            include a list of requests under a key like 'requests'.
        """
        raise NotImplementedError("Subclasses must implement the convert method.")
