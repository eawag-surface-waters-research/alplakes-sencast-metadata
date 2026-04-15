import json
import os
import sys

import numpy as np
import pytest
from osgeo import gdal, ogr, osr

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from functions import extract_tiff_subsection, get_latest, pixel_coordinates
from conftest import (
    TIFF_ORIGIN_X, TIFF_ORIGIN_Y, TIFF_PIXEL_SIZE, TIFF_WIDTH, TIFF_HEIGHT,
    _create_tiff,
)

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _load_lake_geojson():
    with open(os.path.join(FIXTURES_DIR, "lakes.geojson")) as f:
        return json.load(f)


def _make_polygon(lon_min, lat_min, lon_max, lat_max, epsg=4326):
    """Return an ogr.Geometry polygon in the given projection."""
    ring = ogr.Geometry(ogr.wkbLinearRing)
    for x, y in [
        (lon_min, lat_min), (lon_max, lat_min),
        (lon_max, lat_max), (lon_min, lat_max),
        (lon_min, lat_min),
    ]:
        ring.AddPoint(x, y)
    poly = ogr.Geometry(ogr.wkbPolygon)
    poly.AddGeometry(ring)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(epsg)
    poly.AssignSpatialReference(srs)
    return poly


# ---------------------------------------------------------------------------
# pixel_coordinates
# ---------------------------------------------------------------------------

class TestPixelCoordinates:
    def test_returns_within_raster_bounds(self, synthetic_tiff):
        ds = gdal.Open(synthetic_tiff)
        # Polygon inside the raster: covers cols 25-75, rows 25-75
        poly = _make_polygon(8.25, 47.25, 8.75, 47.75)
        min_xp, min_yp, max_xp, max_yp, new_min_x, new_min_y = pixel_coordinates(ds, poly)
        assert 0 <= min_xp < max_xp <= TIFF_WIDTH
        assert 0 <= min_yp < max_yp <= TIFF_HEIGHT
        ds = None

    def test_pixel_extent_matches_polygon(self, synthetic_tiff):
        ds = gdal.Open(synthetic_tiff)
        # Polygon covers exactly the left half of the raster (cols 0-50)
        poly = _make_polygon(8.0, 47.0, 8.5, 48.0)
        min_xp, min_yp, max_xp, max_yp, _, _ = pixel_coordinates(ds, poly)
        assert min_xp == 0
        assert max_xp == 50
        ds = None

    def test_clamps_to_raster_bounds(self, synthetic_tiff):
        ds = gdal.Open(synthetic_tiff)
        # Polygon extends well outside the raster
        poly = _make_polygon(5.0, 44.0, 12.0, 51.0)
        min_xp, min_yp, max_xp, max_yp, _, _ = pixel_coordinates(ds, poly)
        assert min_xp == 0
        assert min_yp == 0
        assert max_xp == TIFF_WIDTH
        assert max_yp == TIFF_HEIGHT
        ds = None


# ---------------------------------------------------------------------------
# extract_tiff_subsection
# ---------------------------------------------------------------------------

class TestExtractTiffSubsection:
    def test_returns_stats_for_lake(self, synthetic_tiff, tmp_path):
        result = extract_tiff_subsection(synthetic_tiff, str(tmp_path), _load_lake_geojson())
        assert "test_lake" in result

    def test_all_stat_keys_present(self, synthetic_tiff, tmp_path):
        result = extract_tiff_subsection(synthetic_tiff, str(tmp_path), _load_lake_geojson())
        stats = result["test_lake"]
        for key in ("pixels", "valid_pixels", "min", "max", "mean", "p10", "p90", "file", "commit", "reproduce"):
            assert key in stats, f"Missing key: {key}"

    def test_commit_and_reproduce_default_to_false(self, synthetic_tiff, tmp_path):
        result = extract_tiff_subsection(synthetic_tiff, str(tmp_path), _load_lake_geojson())
        assert result["test_lake"]["commit"] == "False"
        assert result["test_lake"]["reproduce"] == "False"

    def test_valid_pixels_lte_total_pixels(self, synthetic_tiff, tmp_path):
        result = extract_tiff_subsection(synthetic_tiff, str(tmp_path), _load_lake_geojson())
        s = result["test_lake"]
        assert s["valid_pixels"] <= s["pixels"]

    def test_stat_ordering(self, synthetic_tiff, tmp_path):
        result = extract_tiff_subsection(synthetic_tiff, str(tmp_path), _load_lake_geojson())
        s = result["test_lake"]
        assert s["min"] <= s["p10"] <= s["mean"] <= s["p90"] <= s["max"]

    def test_output_tif_created(self, synthetic_tiff, tmp_path):
        result = extract_tiff_subsection(synthetic_tiff, str(tmp_path), _load_lake_geojson())
        out_file = os.path.join(str(tmp_path), "test_lake", result["test_lake"]["file"])
        assert os.path.isfile(out_file)
        ds = gdal.Open(out_file)
        assert ds is not None
        assert ds.RasterXSize > 0
        assert ds.RasterYSize > 0
        ds = None

    def test_mask_band_reduces_valid_pixels(self, synthetic_tiff_with_mask, tmp_path):
        result_masked = extract_tiff_subsection(
            synthetic_tiff_with_mask, str(tmp_path), _load_lake_geojson()
        )
        # Create an unmasked version in a sibling tmp dir for comparison
        from conftest import TIFF_FILENAME, _create_tiff as _ct
        unmasked_path = str(tmp_path / "unmasked" / TIFF_FILENAME)
        os.makedirs(os.path.dirname(unmasked_path), exist_ok=True)
        _ct(unmasked_path, with_mask=False)
        result_clean = extract_tiff_subsection(
            unmasked_path, str(tmp_path / "clean_out"), _load_lake_geojson()
        )
        assert result_masked["test_lake"]["valid_pixels"] < result_clean["test_lake"]["valid_pixels"]

    def test_lake_outside_raster_is_skipped(self, synthetic_tiff, tmp_path):
        outside_geojson = {
            "type": "FeatureCollection",
            "features": [{
                "type": "Feature",
                "properties": {"key": "far_away_lake"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[20.0, 60.0], [21.0, 60.0], [21.0, 61.0], [20.0, 61.0], [20.0, 60.0]]],
                },
            }],
        }
        result = extract_tiff_subsection(synthetic_tiff, str(tmp_path), outside_geojson)
        assert "far_away_lake" not in result

    def test_all_nan_lake_is_skipped(self, tmp_path):
        """A lake region covered entirely by NaN values should be skipped."""
        tiff_path = str(tmp_path / "nan.tif")
        values = np.full((TIFF_HEIGHT, TIFF_WIDTH), np.nan, dtype=np.float32)
        _create_tiff(tiff_path, values=values)
        result = extract_tiff_subsection(tiff_path, str(tmp_path / "out"), _load_lake_geojson())
        assert "test_lake" not in result


# ---------------------------------------------------------------------------
# get_latest
# ---------------------------------------------------------------------------

class TestGetLatest:
    def _record(self, dt, vp, p=10000):
        return {"dt": dt, "vp": vp, "p": p, "k": "file.tif", "min": 0, "max": 1, "mean": 0.5,
                "p10": 0.1, "p90": 0.9, "c": "False", "r": "False"}

    def test_empty_returns_empty_dict(self):
        assert get_latest([]) == {}

    def test_single_record_returned(self):
        rec = self._record("20240101T120000", 100)
        assert get_latest([rec]) == rec

    def test_most_recent_date_wins(self):
        old = self._record("20240101T120000", 500)
        new = self._record("20240201T120000", 100)
        assert get_latest([old, new]) == new

    def test_same_day_prefers_more_valid_pixels(self):
        low_vp = self._record("20240101T060000", 100)
        high_vp = self._record("20240101T120000", 900)
        # Both share the same 8-char date prefix "20240101"
        result = get_latest([low_vp, high_vp])
        assert result["vp"] == 900

    def test_different_days_ignores_pixel_count(self):
        """Older date with more pixels should NOT win over newer date."""
        old_high = self._record("20240101T120000", 9000)
        new_low = self._record("20240102T120000", 10)
        assert get_latest([old_high, new_low]) == new_low
