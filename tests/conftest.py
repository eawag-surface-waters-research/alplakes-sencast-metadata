import json
import os
import sys

import numpy as np
import pytest
from osgeo import gdal, osr

# Ensure src/ is on the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")

# Synthetic TIFF parameters — a 100×100 raster covering 8.0–9.0°E, 47.0–48.0°N
TIFF_ORIGIN_X = 8.0
TIFF_ORIGIN_Y = 48.0
TIFF_PIXEL_SIZE = 0.01   # 1° / 100 pixels
TIFF_WIDTH = 100
TIFF_HEIGHT = 100
TIFF_FILENAME = "COLLECTION_ST_L8_20240512T202405_194027.tif"
TIFF_FILENAME2 = "COLLECTION_ST_L8_20240601T102030_194027.tif"


def _create_tiff(path, with_mask=False, mask_band_value=1, values=None):
    """
    Create a synthetic Float32 GeoTiff at *path*.

    Band 1: known pixel values (arange reshaped to 100×100, scaled by 0.001)
    Band 2 (optional): quality mask — 0 everywhere except a 10×10 block
                       in rows 10-20 / cols 10-20, which is set to mask_band_value.
    """
    driver = gdal.GetDriverByName("GTiff")
    n_bands = 2 if with_mask else 1
    ds = driver.Create(path, TIFF_WIDTH, TIFF_HEIGHT, n_bands, gdal.GDT_Float32)

    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    ds.SetProjection(srs.ExportToWkt())
    ds.SetGeoTransform((
        TIFF_ORIGIN_X, TIFF_PIXEL_SIZE, 0,
        TIFF_ORIGIN_Y, 0, -TIFF_PIXEL_SIZE,
    ))

    if values is None:
        values = np.arange(TIFF_WIDTH * TIFF_HEIGHT, dtype=np.float32).reshape(
            TIFF_HEIGHT, TIFF_WIDTH
        ) * 0.001
    ds.GetRasterBand(1).WriteArray(values)

    if with_mask:
        mask = np.zeros((TIFF_HEIGHT, TIFF_WIDTH), dtype=np.uint8)
        # Mask block at rows 35-45, cols 35-45 — inside the test_lake polygon area
        # (lake covers rows 25-75, cols 25-75 of this raster)
        mask[35:45, 35:45] = mask_band_value
        ds.GetRasterBand(2).WriteArray(mask)

    ds.FlushCache()
    ds = None
    return path


@pytest.fixture(scope="session")
def synthetic_tiff(tmp_path_factory):
    """Standard synthetic TIFF — no mask band."""
    d = tmp_path_factory.mktemp("tiff")
    path = str(d / TIFF_FILENAME)
    _create_tiff(path)
    return path


@pytest.fixture(scope="session")
def synthetic_tiff_with_mask(tmp_path_factory):
    """Synthetic TIFF with Band 2 mask covering rows 10-20, cols 10-20."""
    d = tmp_path_factory.mktemp("tiff_mask")
    path = str(d / TIFF_FILENAME)
    _create_tiff(path, with_mask=True)
    return path


@pytest.fixture(scope="session")
def synthetic_tiff2(tmp_path_factory):
    """A second synthetic TIFF with a different date (for append / get_latest tests)."""
    d = tmp_path_factory.mktemp("tiff2")
    path = str(d / TIFF_FILENAME2)
    # Slightly different values so stats differ
    values = (
        np.arange(TIFF_WIDTH * TIFF_HEIGHT, dtype=np.float32).reshape(
            TIFF_HEIGHT, TIFF_WIDTH
        ) * 0.002
    )
    _create_tiff(path, values=values)
    return path


@pytest.fixture(scope="session")
def lake_geojson():
    """Load the minimal lake geometry fixture."""
    with open(os.path.join(FIXTURES_DIR, "lakes.geojson")) as f:
        return json.load(f)


@pytest.fixture
def expected_dir():
    """Path to tests/fixtures/expected/."""
    return os.path.join(FIXTURES_DIR, "expected")


@pytest.fixture
def tiff_dirs(tmp_path):
    """Return a dict of tmp directories mirroring the pipeline layout."""
    dirs = {
        "local_tiff": str(tmp_path / "local_tiff"),
        "local_tiff_cropped": str(tmp_path / "local_tiff_cropped"),
        "local_metadata": str(tmp_path / "local_metadata"),
    }
    for d in dirs.values():
        os.makedirs(d, exist_ok=True)
    return dirs
