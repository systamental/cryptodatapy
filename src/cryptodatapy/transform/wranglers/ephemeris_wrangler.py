"""
Data wrangler for Ephemeris data.

Transforms raw ephemeris DataFrames into the standardized CryptoDataPy
MultiIndex (date, ticker) format.
"""
from __future__ import annotations
import logging
from typing import Union, Dict

import pandas as pd

from cryptodatapy.transform.wranglers.base_wrangler import BaseDataWrangler

logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)


class EphemerisWrangler(BaseDataWrangler):
    """
    Handles ephemeris data wrangling. The raw data is already a well-structured
    MultiIndex DataFrame from Ephemeris.get_all_planets(), so the wrangling
    is minimal — mainly standardizing the index and cleaning.
    """

    def __init__(self, data_req, data_resp: Union[Dict, pd.DataFrame]):
        super().__init__(data_req, data_resp)

    def wrangle(self) -> pd.DataFrame:
        """
        Wrangle raw ephemeris data into standardized format.

        The input data_resp is expected to be a DataFrame with MultiIndex
        (date, ticker) from Ephemeris.get_all_planets().

        Returns
        -------
        pd.DataFrame
            Cleaned MultiIndex DataFrame with (date, ticker) index.
        """
        if isinstance(self.data_resp, pd.DataFrame) and self.data_resp.empty:
            logger.warning("Empty ephemeris data received.")
            return pd.DataFrame()

        df = self.data_resp

        # If data already has the correct MultiIndex, work with it directly
        if isinstance(df.index, pd.MultiIndex):
            # Reset to flat DataFrame for pipeline processing
            df = df.reset_index()

        if not {'date', 'ticker'}.issubset(df.columns):
            logger.warning("Ephemeris response missing required index columns ['date', 'ticker'].")
            return pd.DataFrame()

        # Preserve intraday timestamps; do not normalize to midnight.
        df['date'] = pd.to_datetime(df['date'], errors='coerce', utc=True)
        df = df.dropna(subset=['date'])

        self.data_resp = df.set_index(['date', 'ticker']).sort_index()
        self._clean_data()
        self._convert_types()

        return self.data_resp
