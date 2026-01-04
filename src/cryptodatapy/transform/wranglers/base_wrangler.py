from __future__ import annotations
import logging
from abc import ABC, abstractmethod
from typing import Union, Dict, List, Optional
from importlib import resources
import pandas as pd

from cryptodatapy.extract.datarequest import DataRequest

logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)


# --- Field Map Loading Function (Retained/Modified for clarity) ---

def _load_field_map() -> Dict[str, Dict[str, str]]:
    """
    Loads fields.csv from the config path and creates a nested dictionary map:
    {'vendor_name': {'vendor_field_lower': 'CRYPTODATAPY_FIELD'}}
    """
    try:
        # Load fields.csv from the configuration resource path
        with resources.path('cryptodatapy.conf', 'fields.csv') as f:
            fields_df = pd.read_csv(f, index_col='id', encoding='latin1').copy()
    except Exception as e:
        logging.error(f"Failed to load field map: {e}")
        return {}

    field_map: Dict[str, Dict[str, str]] = {}
    vendor_cols = [col for col in fields_df.columns if col.endswith('_id')]

    for vendor_col in vendor_cols:
        vendor_name = vendor_col.replace('_id', '')
        vendor_map: Dict[str, str] = {}

        for target_field, vendor_field in fields_df[vendor_col].items():
            if pd.notna(vendor_field):
                vendor_map[str(vendor_field).lower()] = target_field

        field_map[vendor_name] = vendor_map

    return field_map


class BaseDataWrangler(ABC):
    """
    Base class for vendor-specific data wranglers.
    Handles common data cleaning, filtering, and field mapping operations.
    """
    _FIELD_MAP = _load_field_map()  # Load map once
    _DEFAULT_AGG_MAP = {
        # OHLCV
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
        'base_volume': 'sum',
        'quote_volume': 'sum',
        # Derivatives/Perpetuals
        'funding_rate': 'sum',
        'oi': 'last',
        'oi_value': 'last',
    }

    def __init__(self, data_req: DataRequest, data_resp: Union[Dict, pd.DataFrame]):
        """
        Constructor initializes request data and data response.

        Parameters
        ----------
        data_req: DataRequest
            Data request object with parameter values.
        data_resp: dictionary or pd.DataFrame
            Raw data response from the API.
        """
        self.data_req = data_req
        self.data_resp = data_resp
        self.field_map = self._FIELD_MAP.get(data_req.source)

    def _convert_fields_to_lib(self, data_source: str) -> None:
        """
        Convert columns from vendor field names to CryptoDataPy standard field names
        using the dictionary map. Mutates self.data_resp.
        """
        vendor_map = self._FIELD_MAP.get(data_source)
        if not vendor_map:
            logging.warning(f"No field map found for data source: {data_source}. Skipping field conversion.")
            return

        rename_dict: Dict[str, str] = {}

        # Identify columns to rename and columns that are unmapped/should be dropped
        mapped_target_fields = set()
        for col in self.data_resp.columns:
            col_lower = col.lower()
            target_name = vendor_map.get(col_lower)

            if target_name:
                rename_dict[col] = target_name
                mapped_target_fields.add(target_name)
            else:
                # Handle special/unmapped columns.
                # ticker
                if col_lower in ['symbol', 'asset', 'market', 'ticker']:
                    rename_dict[col] = 'ticker'

        self.data_resp.rename(columns=rename_dict, inplace=True)

    def _set_index_and_sort(self, index_cols: Union[str, List[str]] = 'date') -> None:
        """
        Sets the index and sorts the DataFrame by the index.

        It ensures that if 'date' is part of the index, it is converted to a
        date-only Timestamp (time component set to 00:00:00) while retaining
        the datetime64[ns] dtype for optimal index performance.
        """
        df = self.data_resp

        # already existing MultiIndex (just sort and return)
        if isinstance(df.index, pd.MultiIndex) or isinstance(df.index, pd.DatetimeIndex):
            df.sort_index(inplace=True)
            return

        # ensure index_cols is a list for consistent checking
        if isinstance(index_cols, str):
            index_cols = [index_cols]

        # check if all required columns exist
        if all(col in df.columns for col in index_cols):

            # standardize 'date' to UTC DatetimeIndex
            if 'date' in index_cols and 'date' in df.columns:
                try:
                    dt_series = pd.to_datetime(df['date'], errors='coerce')

                    # ensure the time component is midnight for daily data consistency
                    dt_series = dt_series.dt.normalize()

                    # if tz aware, convert to UTC
                    if dt_series.dt.tz is not None:
                        dt_series = dt_series.dt.tz_convert('UTC')
                    # if tz naive (like from a simple Unix timestamp or date string), localize it to UTC
                    else:
                        dt_series = dt_series.dt.tz_localize('UTC')

                    df['date'] = dt_series

                except Exception as e:
                    logging.warning(f"Failed to convert 'date' column to UTC DatetimeIndex: {e}.")

            # set the index and sort
            df.set_index(index_cols, inplace=True)
            df.sort_index(inplace=True)

        else:
            logging.warning(f"Index columns {index_cols} not found for setting index. Index not modified.")

    def _filter_dates(self) -> None:
        """Filters data response based on start and end dates in data_req."""
        start_date = self.data_req.start_date
        end_date = self.data_req.end_date

        if start_date and self.data_resp.index.names[0] == 'date':
            self.data_resp = self.data_resp.loc[start_date:, :]
        if end_date and self.data_resp.index.names[0] == 'date':
            self.data_resp = self.data_resp.loc[:end_date, :]

    def _resample(self, agg_func: Optional[Union[str, Dict[str, str]]] = None) -> None:
        """
        Resamples the DataFrame to the frequency in the data_req.

        Logic:
        1. If agg_func is a string ('last', 'sum'), it applies to all columns.
        2. If agg_func is None, it uses the DEFAULT_AGG_MAP for known columns.
        3. If a column isn't in the map, it defaults to 'last'.

        Parameters
        ----------
        agg_func: str or dict, optional
            Aggregation function(s) to use during resampling. If a string is provided,
            it applies to all columns. If a dict is provided, it should map column names to aggregation functions.
            If None, the default aggregation map is used.

        """
        freq = self.data_req.freq

        if freq == 'tick' or self.data_resp.empty:
            return

            # 1. Handle Frequency Mapping
        freq_mapping = {'b': 'B', 'd': 'D', 'w': 'W', 'ms': 'MS', 'm': 'ME', 'q': 'QE', 'y': 'YE'}
        pd_freq = freq_mapping.get(freq.lower(), freq)

        # aggregation function mapping
        if isinstance(agg_func, dict):
            final_agg_map = agg_func
        elif isinstance(agg_func, str):
            final_agg_map = {col: agg_func for col in self.data_resp.columns}
        else:
            final_agg_map = {}
            for col in self.data_resp.columns:
                # use map if exists, otherwise default to 'last' (safest for snapshots)
                final_agg_map[col] = self._DEFAULT_AGG_MAP.get(col, 'last')

        # apply resampling
        try:
            # group by date (resampled) and ticker to maintain the panel structure
            grouped = self.data_resp.groupby([
                pd.Grouper(level='date', freq=pd_freq),
                pd.Grouper(level='ticker')
            ])

            self.data_resp = grouped.agg(final_agg_map)

            # post-processing: forward fill within tickers to handle gaps
            self.data_resp = self.data_resp.groupby('ticker').ffill()

            # restore index order
            self.data_resp = self.data_resp.reorder_levels(['date', 'ticker']).sort_index()

        except Exception as e:
            logger.error(f"Failed to resample data: {e}")

    def _reorder_columns(self, requested_fields: bool = False) -> None:
        """Reorders columns based on the provided column order list.

        Parameters
        ----------
        requested_fields: bool
            If True, only requested fields are kept and ordered. If False, all columns are kept.
        """
        if self.data_req.source_fields is None:
            returned_fields = [field for field in self.data_req.fields if field in self.data_resp.columns]
            missing_fields = [field for field in self.data_resp.columns if field not in self.data_req.fields]
            if requested_fields:
                reordered_fields = returned_fields
            else:
                reordered_fields = returned_fields + missing_fields
            self.data_resp = self.data_resp[reordered_fields]

        else:
            self.data_req.fields = self.data_resp.columns.tolist()

    def _clean_data(self) -> None:
        """Removes duplicates, NaNs (full row/col), and 0 values."""

        # Remove duplicate index entries (duplicate rows)
        if self.data_resp.index.duplicated().any():
            self.data_resp = self.data_resp[~self.data_resp.index.duplicated()]

        # Remove rows and columns consisting entirely of NaNs
        self.data_resp = self.data_resp.dropna(how='all', axis=0)  # Drop rows
        self.data_resp = self.data_resp.dropna(how='all', axis=1)  # Drop columns

    def _convert_types(self) -> None:
        """
        Converts columns to appropriate numeric types, explicitly excluding known
        string/metadata columns, and uses standard pandas dtypes.
        """
        # define categorical columns that should NEVER be converted to numeric
        EXCLUDE_COLS = ['date', 'time', 'ticker', 'symbol', 'name', 'type', 'category', 'status', 'period']

        try:
            df = self.data_resp

            # identify numeric columns using a blacklist approach
            candidate_num_cols = [col for col in df.columns if col not in EXCLUDE_COLS]

            # 'coerce' error handling ensures non-numeric values (like 'N/A')
            # are turned into NaN, which is essential for data cleaning.
            df[candidate_num_cols] = df[candidate_num_cols].apply(
                pd.to_numeric, errors='coerce'
            )

            self.data_resp = df.convert_dtypes(
                convert_string=False,
                convert_integer=False,
                convert_boolean=True  # Optional: can be set to True
            )

        except Exception as e:
            logger.warning(f"Error during final type conversion: {e}")

    @abstractmethod
    def wrangle(self) -> pd.DataFrame:
        """
        Abstract method for wrangling. Must be implemented by child classes.
        """
        raise NotImplementedError("Wrangler must implement the wrangle() method.")
