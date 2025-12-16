from __future__ import annotations
import logging
from abc import ABC, abstractmethod
from typing import Union, Dict, List
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

    def _resample(self, agg_func='last') -> None:
        """
        Resamples a MultiIndex DataFrame, grouping by all index levels except 'date',
        and applies the resampling to the 'date' level.

        The expected index is typically (date, index).

        Parameters
        ----------
        agg_func : str, optional
            The aggregation function to use during resampling. Defaults to 'sum'.
        """
        freq = self.data_req.freq

        if freq == 'tick':
            return  # No resampling needed for tick data

        # freq map
        freq_mapping = {
            'b': 'B',
            'd': 'D',
            'w': 'W',
            'ms': 'MS',
            'm': 'ME',
            'qs': 'QS',
            'q': 'QE',
            'ys': 'YS',
            'y': 'YE'
        }

        # map freq to pandas
        if freq in freq_mapping.keys():
            freq = freq_mapping[freq]

        # check date
        if 'date' not in self.data_resp.index.names:
            logger.error("DataFrame must have a 'date' level in its MultiIndex for resampling.")

        # apply the resampling and aggregation
        if agg_func in ['sum', 'mean', 'last', 'first']:
            # self.data_resp = getattr(grouped_data.resample(freq, level='date'), agg_func)()
            self.data_resp = getattr(self.data_resp.groupby([pd.Grouper(level='date', freq=freq),
                                                             pd.Grouper(level='ticker')]), agg_func)()
        else:
            logger.warning(f"Unsupported aggregation function '{agg_func}'. Returning original DataFrame.")

        # forward fill for higher freq
        self.data_resp = self.data_resp.groupby('ticker').ffill()
        # reorder index
        self.data_resp = self.data_resp.reorder_levels(['date', 'ticker']).sort_index()

    def _reorder_columns(self) -> None:
        """Reorders columns based on the provided column order list."""
        if self.data_req.source_fields is None:
            returned_fields = [field for field in self.data_req.fields if field in self.data_resp.columns]
            missing_fields = [field for field in self.data_resp.columns if field not in self.data_req.fields]
            reordered_fields = returned_fields + missing_fields
            self.data_resp = self.data_resp[reordered_fields]
        else:
            self.data_req.fields = self.data_resp.columns.tolist()

    def _clean_data(self) -> None:
        """Removes duplicates, NaNs (full row/col), and 0 values."""

        # Remove duplicate index entries (duplicate rows)
        if self.data_resp.index.duplicated().any():
            self.data_resp = self.data_resp[~self.data_resp.index.duplicated()]

        # Remove 0 values (often erroneous in financial time series)
        self.data_resp = self.data_resp[self.data_resp != 0]

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
