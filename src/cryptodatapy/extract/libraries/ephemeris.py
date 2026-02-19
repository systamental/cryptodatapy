"""
Swiss Ephemeris wrapper for computing planetary positions.

Provides geocentric and heliocentric planetary longitudes, declinations,
speeds, and latitudes using the pyswisseph library.
"""
import logging
from datetime import datetime
from typing import List, Optional, Union

import numpy as np
import pandas as pd
import swisseph as swe

logger = logging.getLogger(__name__)

# Swiss Ephemeris planet ID mapping
PLANET_MAP = {
    'sun': swe.SUN,
    'moon': swe.MOON,
    'mercury': swe.MERCURY,
    'venus': swe.VENUS,
    'mars': swe.MARS,
    'jupiter': swe.JUPITER,
    'saturn': swe.SATURN,
    'uranus': swe.URANUS,
    'neptune': swe.NEPTUNE,
    'pluto': swe.PLUTO,
    'north_node': swe.MEAN_NODE,
    'chiron': swe.CHIRON,
}

SUPPORTED_PLANETS = list(PLANET_MAP.keys())

# Frequency to pandas offset mapping
FREQ_MAP = {
    'd': 'D',
    'w': 'W',
    'm': 'MS',
    'b': 'B',
    '1h': 'h',
}


class Ephemeris:
    """
    Wrapper around Swiss Ephemeris (swisseph) for computing planetary
    ephemeris data over a date range.

    Parameters
    ----------
    start_date : str or datetime
        Start date for ephemeris computation (inclusive).
    end_date : str or datetime
        End date for ephemeris computation (inclusive).
    freq : str, default 'd'
        Frequency of output data. 'd' for daily, 'w' for weekly, etc.
    geo : bool, default True
        If True, compute geocentric positions. If False, heliocentric.
    """

    def __init__(
        self,
        start_date: Union[str, datetime],
        end_date: Union[str, datetime],
        freq: str = 'd',
        geo: bool = True,
    ):
        self.start_date = pd.Timestamp(start_date)
        self.end_date = pd.Timestamp(end_date)
        self.freq = freq
        self.geo = geo
        self._iflag = swe.FLG_SWIEPH | swe.FLG_SPEED
        if not geo:
            self._iflag |= swe.FLG_HELCTR

        # Generate date index
        pd_freq = FREQ_MAP.get(freq, freq.upper())
        self.dates = pd.date_range(self.start_date, self.end_date, freq=pd_freq)

    @staticmethod
    def _datetime_to_jd(dt: pd.Timestamp) -> float:
        """Convert a pandas Timestamp to Julian Day number."""
        return swe.julday(dt.year, dt.month, dt.day, dt.hour + dt.minute / 60.0)

    def _calc_planet(self, planet_id: int, jd: float) -> tuple:
        """
        Calculate planet position for a given Julian Day.

        Returns
        -------
        tuple
            (longitude, latitude, distance, speed_lon, speed_lat, speed_dist)
        """
        try:
            result, _ = swe.calc_ut(jd, planet_id, self._iflag)
            return result
        except swe.Error as e:
            logger.warning(f"swisseph error for planet {planet_id} at JD {jd}: {e}")
            return (np.nan,) * 6

    def get_planet_longitude(self, planet: str, dates: Optional[pd.DatetimeIndex] = None) -> pd.Series:
        """
        Get ecliptic longitude (0-360) for a planet over the date range.

        Parameters
        ----------
        planet : str
            Planet name (e.g., 'mars', 'jupiter').
        dates : pd.DatetimeIndex, optional
            Custom date index. Uses self.dates if None.

        Returns
        -------
        pd.Series
            Longitude values indexed by date.
        """
        if dates is None:
            dates = self.dates
        planet_id = PLANET_MAP[planet.lower()]
        values = [self._calc_planet(planet_id, self._datetime_to_jd(d))[0] for d in dates]
        return pd.Series(values, index=dates, name=f'{planet}_longitude', dtype='float64')

    def get_planet_declination(self, planet: str, dates: Optional[pd.DatetimeIndex] = None) -> pd.Series:
        """
        Get declination (-90 to +90) for a planet.

        Declination is computed by converting ecliptic coordinates to equatorial.

        Parameters
        ----------
        planet : str
            Planet name.
        dates : pd.DatetimeIndex, optional
            Custom date index.

        Returns
        -------
        pd.Series
            Declination values indexed by date.
        """
        if dates is None:
            dates = self.dates
        planet_id = PLANET_MAP[planet.lower()]
        decls = []
        for d in dates:
            jd = self._datetime_to_jd(d)
            result = self._calc_planet(planet_id, jd)
            lon, lat = result[0], result[1]
            # Convert ecliptic to equatorial to get declination
            eps = swe.calc_ut(jd, swe.ECL_NUT)[0][0]  # obliquity of ecliptic
            ra, decl = swe.cotrans((lon, lat, 1.0), -eps)[:2]
            decls.append(decl)
        return pd.Series(decls, index=dates, name=f'{planet}_declination', dtype='float64')

    def get_planet_speed(self, planet: str, dates: Optional[pd.DatetimeIndex] = None) -> pd.Series:
        """
        Get daily speed in degrees/day for a planet. Negative = retrograde.

        Parameters
        ----------
        planet : str
            Planet name.
        dates : pd.DatetimeIndex, optional
            Custom date index.

        Returns
        -------
        pd.Series
            Speed values indexed by date.
        """
        if dates is None:
            dates = self.dates
        planet_id = PLANET_MAP[planet.lower()]
        values = [self._calc_planet(planet_id, self._datetime_to_jd(d))[3] for d in dates]
        return pd.Series(values, index=dates, name=f'{planet}_speed', dtype='float64')

    def get_planet_latitude(self, planet: str, dates: Optional[pd.DatetimeIndex] = None) -> pd.Series:
        """
        Get ecliptic latitude for a planet.

        Parameters
        ----------
        planet : str
            Planet name.
        dates : pd.DatetimeIndex, optional
            Custom date index.

        Returns
        -------
        pd.Series
            Latitude values indexed by date.
        """
        if dates is None:
            dates = self.dates
        planet_id = PLANET_MAP[planet.lower()]
        values = [self._calc_planet(planet_id, self._datetime_to_jd(d))[1] for d in dates]
        return pd.Series(values, index=dates, name=f'{planet}_latitude', dtype='float64')

    def get_all_planets(
        self,
        dates: Optional[pd.DatetimeIndex] = None,
        planets: Optional[List[str]] = None,
        fields: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Compute ephemeris data for multiple planets and fields.

        Parameters
        ----------
        dates : pd.DatetimeIndex, optional
            Custom date index. Uses self.dates if None.
        planets : list of str, optional
            Planet names. Defaults to all supported planets.
        fields : list of str, optional
            Fields to compute. Options: 'longitude', 'declination', 'speed',
            'latitude', 'is_retrograde'. Defaults to all.

        Returns
        -------
        pd.DataFrame
            MultiIndex DataFrame with (date, ticker) index and field columns.
        """
        if dates is None:
            dates = self.dates
        if planets is None:
            planets = SUPPORTED_PLANETS
        if fields is None:
            fields = ['longitude', 'declination', 'speed', 'latitude', 'is_retrograde']

        field_methods = {
            'longitude': self.get_planet_longitude,
            'declination': self.get_planet_declination,
            'speed': self.get_planet_speed,
            'latitude': self.get_planet_latitude,
        }

        all_data = []

        for planet in planets:
            planet_lower = planet.lower()
            if planet_lower not in PLANET_MAP:
                logger.warning(f"Unknown planet '{planet}', skipping.")
                continue

            planet_df = pd.DataFrame(index=dates)
            planet_df.index.name = 'date'

            for field in fields:
                if field == 'is_retrograde':
                    # Derived from speed
                    if 'speed' not in planet_df.columns:
                        speed = self.get_planet_speed(planet_lower, dates)
                        planet_df['speed'] = speed.values
                    planet_df['is_retrograde'] = (planet_df['speed'] < 0).astype(int)
                elif field in field_methods:
                    series = field_methods[field](planet_lower, dates)
                    planet_df[field] = series.values

            # Only keep requested fields
            keep_cols = [f for f in fields if f in planet_df.columns]
            planet_df = planet_df[keep_cols]
            planet_df['ticker'] = planet_lower
            all_data.append(planet_df)

        if not all_data:
            return pd.DataFrame()

        result = pd.concat(all_data, axis=0)
        result = result.reset_index()
        result = result.set_index(['date', 'ticker']).sort_index()

        return result

    def get_lunar_phase(self, dates: Optional[pd.DatetimeIndex] = None) -> pd.Series:
        """
        Compute Moon-Sun elongation (0-360) as a measure of lunar phase.

        0 = New Moon, 180 = Full Moon.

        Parameters
        ----------
        dates : pd.DatetimeIndex, optional
            Custom date index.

        Returns
        -------
        pd.Series
            Lunar phase angle indexed by date.
        """
        if dates is None:
            dates = self.dates
        moon_lon = self.get_planet_longitude('moon', dates)
        sun_lon = self.get_planet_longitude('sun', dates)
        phase = (moon_lon - sun_lon) % 360
        phase.name = 'lunar_phase'
        return phase

    def get_lunar_nodes(self, dates: Optional[pd.DatetimeIndex] = None) -> pd.DataFrame:
        """
        Compute North and South lunar node longitudes.

        Parameters
        ----------
        dates : pd.DatetimeIndex, optional
            Custom date index.

        Returns
        -------
        pd.DataFrame
            DataFrame with 'north_node' and 'south_node' columns.
        """
        if dates is None:
            dates = self.dates
        north = self.get_planet_longitude('north_node', dates)
        south = (north + 180) % 360
        return pd.DataFrame({
            'north_node': north.values,
            'south_node': south.values,
        }, index=dates)
