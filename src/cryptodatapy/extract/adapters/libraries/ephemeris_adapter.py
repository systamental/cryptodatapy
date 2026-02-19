"""
Adapter for the Swiss Ephemeris library.

Implements the BaseLibraryAdapter interface to provide planetary ephemeris
data through the standard CryptoDataPy DataClient pipeline.
"""
import logging
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from cryptodatapy.extract.adapters.base_adapter import BaseLibraryAdapter
from cryptodatapy.core.data_request import DataRequest
from cryptodatapy.extract.libraries.ephemeris import Ephemeris, SUPPORTED_PLANETS
from cryptodatapy.extract.params.libraries.ephemeris_param_converter import EphemerisParamConverter
from cryptodatapy.transform.wranglers.ephemeris_wrangler import EphemerisWrangler

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(name)s: %(message)s')
logger = logging.getLogger(__name__)


class EphemerisAdapter(BaseLibraryAdapter):
    """
    Adapter for retrieving planetary ephemeris data via Swiss Ephemeris.
    Implements the BaseLibraryAdapter contract.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.assets: Optional[Union[pd.DataFrame, list]] = None
        self.fields: Optional[Union[pd.DataFrame, list]] = None

    # --------------------------------------------------------------------------
    # BaseLibraryAdapter abstract method implementations
    # --------------------------------------------------------------------------

    def _init_client(self, **kwargs) -> Ephemeris:
        """Instantiate an Ephemeris client with the given parameters."""
        return Ephemeris(
            start_date=kwargs.get('start_date', '2009-01-03'),
            end_date=kwargs.get('end_date', pd.Timestamp.utcnow().strftime('%Y-%m-%d')),
            freq=kwargs.get('freq', 'd'),
            geo=kwargs.get('geo', True),
        )

    def get_rate_limit_info(self) -> Optional[Any]:
        """No rate limits for local ephemeris computation."""
        return None

    def get_assets_info(self, as_list: bool = False, **kwargs) -> Union[pd.DataFrame, List[str]]:
        """
        Returns the list of supported planets as 'assets'.

        Parameters
        ----------
        as_list : bool, default False
            If True, returns a list of planet names.

        Returns
        -------
        Union[pd.DataFrame, list]
            Planet metadata.
        """
        planets = SUPPORTED_PLANETS
        if as_list:
            return planets
        return pd.DataFrame({
            'ticker': planets,
            'name': [p.replace('_', ' ').title() for p in planets],
            'type': 'planet',
        }).set_index('ticker')

    def get_markets_info(self, as_list: bool = False, **kwargs) -> Union[pd.DataFrame, List[str]]:
        """Ephemeris has no markets concept. Returns empty."""
        if as_list:
            return []
        return pd.DataFrame()

    def get_fields_info(self, as_list: bool = False, **kwargs) -> Union[pd.DataFrame, List[str]]:
        """
        Returns available ephemeris fields.

        Returns
        -------
        Union[pd.DataFrame, list]
            Available field names.
        """
        fields = ['longitude', 'declination', 'speed', 'latitude', 'is_retrograde']
        if as_list:
            return fields
        return pd.DataFrame({
            'field': fields,
            'description': [
                'Ecliptic longitude (0-360 degrees)',
                'Equatorial declination (-90 to +90 degrees)',
                'Daily speed in degrees/day (negative = retrograde)',
                'Ecliptic latitude',
                'Binary retrograde indicator (1 = retrograde)',
            ],
        }).set_index('field')

    # --------------------------------------------------------------------------
    # ETL Pipeline
    # --------------------------------------------------------------------------

    def _convert_params_to_vendor(self, data_req: DataRequest) -> Dict[str, Any]:
        """Convert DataRequest to ephemeris parameters."""
        converter = EphemerisParamConverter(data_req)
        vendor_params = converter.convert()

        # DataRequest defaults tickers to ["btc"]; for ephemeris, default to all planets.
        if data_req.source_tickers is None and data_req.tickers == ['btc']:
            vendor_params['planets'] = SUPPORTED_PLANETS.copy()

        return vendor_params

    def _fetch_raw_data(self, vendor_params: Dict[str, Any]) -> pd.DataFrame:
        """Compute ephemeris data using the Ephemeris library."""
        eph = self._init_client(
            start_date=vendor_params['start_date'],
            end_date=vendor_params['end_date'],
            freq=vendor_params['freq'],
            geo=vendor_params['geo'],
        )

        raw_data = eph.get_all_planets(
            planets=vendor_params['planets'],
            fields=vendor_params['fields'],
        )

        return raw_data

    def _transform_raw_response(self, data_req: DataRequest, raw_data: Any) -> pd.DataFrame:
        """Wrangle raw ephemeris data into standardized format."""
        return EphemerisWrangler(
            data_req=data_req,
            data_resp=raw_data,
        ).wrangle()

    def get_data(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Main entry point — compute ephemeris data for the given DataRequest.

        Parameters
        ----------
        data_req : DataRequest
            Standardized data request.

        Returns
        -------
        pd.DataFrame
            MultiIndex DataFrame with (date, ticker) index and ephemeris fields.
        """
        vendor_params = self._convert_params_to_vendor(data_req)
        raw_data = self._fetch_raw_data(vendor_params)
        return self._transform_raw_response(data_req, raw_data)
