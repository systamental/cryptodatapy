"""
Parameter converter for the Ephemeris data source.
"""
import logging
from typing import Dict, Any, List

from cryptodatapy.core.data_request import DataRequest
from cryptodatapy.extract.params.base_param_converter import BaseParamConverter

logger = logging.getLogger(__name__)

SUPPORTED_FIELDS = ['longitude', 'declination', 'speed', 'latitude', 'is_retrograde']
FIELD_ALIASES = {'close': 'longitude'}


class EphemerisParamConverter(BaseParamConverter):
    """
    Converts a standard DataRequest into parameters for the Ephemeris library.
    """

    def __init__(self, data_req: DataRequest):
        super().__init__(data_req)

    def convert(self, **kwargs: Any) -> Dict[str, Any]:
        """
        Convert DataRequest to ephemeris-specific parameters.

        Returns
        -------
        Dict[str, Any]
            Dictionary with keys: planets, fields, start_date, end_date, freq, geo.
        """
        req = self.data_req

        # Planets are passed as tickers
        planets = self._convert_case_tickers(case='lower')

        # Fields
        fields = self._convert_fields(req.source_fields if req.source_fields else req.fields)

        # Dates — use base class utility, default start to 2009-01-03 (BTC genesis)
        start_date, end_date = self._convert_dates(
            format_type='str_ymd',
            default_start_str='2009-01-03'
        )

        # Frequency
        freq = req.source_freq if req.source_freq else (req.freq or 'd')

        # Coordinate system — default geocentric
        geo = True

        return {
            'planets': planets,
            'fields': fields,
            'start_date': start_date,
            'end_date': end_date,
            'freq': freq,
            'geo': geo,
        }

    @staticmethod
    def _convert_fields(fields: List[str]) -> List[str]:
        """
        Normalize requested fields to supported ephemeris output fields.

        The generic DataRequest default is ['close']; for ephemeris that maps to
        longitude so default requests still return meaningful data.
        """
        if not fields:
            return SUPPORTED_FIELDS.copy()

        normalized = [FIELD_ALIASES.get(field, field) for field in fields]
        supported_only = [field for field in normalized if field in SUPPORTED_FIELDS]

        if not supported_only:
            logger.warning(
                "No supported ephemeris fields requested; defaulting to all supported fields."
            )
            return SUPPORTED_FIELDS.copy()

        # Keep order and remove duplicates
        return list(dict.fromkeys(supported_only))
