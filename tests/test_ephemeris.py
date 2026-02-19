import pandas as pd
import pytest

pytest.importorskip("swisseph")

from cryptodatapy.extract.datarequest import DataRequest
from cryptodatapy.extract.getdata import GetData
from cryptodatapy.extract.libraries.ephemeris import SUPPORTED_PLANETS


def test_ephemeris_getdata_daily_explicit_fields() -> None:
    req = DataRequest(
        source="ephemeris",
        tickers=["sun", "moon"],
        fields=["longitude", "is_retrograde"],
        start_date="2024-01-01",
        end_date="2024-01-03",
        freq="d",
    )

    df = GetData(req).get_series()

    assert not df.empty, "Dataframe was returned empty."
    assert isinstance(df.index, pd.MultiIndex), "Dataframe should be MultiIndex."
    assert df.index.names == ["date", "ticker"], "Unexpected index names."
    assert list(df.columns) == ["longitude", "is_retrograde"], "Unexpected columns."
    assert set(df.index.get_level_values("ticker").unique()) == {"sun", "moon"}, "Tickers missing."


def test_ephemeris_getdata_default_fields_fallback() -> None:
    req = DataRequest(
        source="ephemeris",
        tickers=["sun"],
        start_date="2024-01-01",
        end_date="2024-01-02",
        freq="d",
    )

    df = GetData(req).get_series()

    assert not df.empty, "Default field request should return data."
    assert "longitude" in df.columns, "Default field fallback should include longitude."
    assert "close" not in df.columns, "Source-native fields should be returned."


def test_ephemeris_getdata_default_ticker_uses_all_planets() -> None:
    req = DataRequest(
        source="ephemeris",
        start_date="2024-01-01",
        end_date="2024-01-02",
        freq="d",
    )

    df = GetData(req).get_series()
    returned_tickers = set(df.index.get_level_values("ticker").unique())
    supported_tickers = set(SUPPORTED_PLANETS)

    assert not df.empty, "Default ephemeris request should return data."
    # Chiron can be unavailable if asteroid ephemeris files are not installed.
    required_tickers = supported_tickers - {"chiron"}
    assert required_tickers.issubset(returned_tickers), \
        "Default ephemeris request should include the standard supported planets."
    assert returned_tickers.issubset(supported_tickers), "Returned unsupported ephemeris tickers."


def test_ephemeris_getdata_hourly_keeps_intraday_timestamps() -> None:
    req = DataRequest(
        source="ephemeris",
        tickers=["sun"],
        fields=["longitude"],
        start_date="2024-01-01",
        end_date="2024-01-02",
        freq="1h",
    )

    df = GetData(req).get_series()
    unique_datetimes = df.index.get_level_values("date").unique()

    assert not df.empty, "Hourly ephemeris request should return data."
    assert len(unique_datetimes) > 2, "Intraday timestamps were collapsed."


def test_ephemeris_getdata_assets_meta() -> None:
    req = DataRequest(
        source="ephemeris",
        tickers=["sun"],
        fields=["longitude"],
        start_date="2024-01-01",
        end_date="2024-01-02",
    )

    gd = GetData(req)
    df = gd.get_series()
    assets = gd.get_meta(method="get_assets_info", as_list=True)

    assert not df.empty, "GetData path should return ephemeris data."
    assert "sun" in assets, "Ephemeris assets metadata should include sun."
