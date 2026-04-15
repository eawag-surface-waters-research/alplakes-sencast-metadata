"""
Generate golden output files for the unit test suite.

Run this script once (or after intentional changes) to create/update the
expected JSON files in tests/fixtures/expected/.

Usage:
    cd /path/to/sencast-metadata
    python tests/generate_golden.py

After running, inspect the diff and commit:
    git diff tests/fixtures/expected/
    git add tests/fixtures/expected/
"""

import json
import os
import shutil
import sys
import tempfile

import numpy as np
from osgeo import gdal, osr

# Resolve paths relative to this script
TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
FIXTURES_DIR = os.path.join(TESTS_DIR, "fixtures")
EXPECTED_DIR = os.path.join(FIXTURES_DIR, "expected")
SRC_DIR = os.path.join(TESTS_DIR, "..", "src")

sys.path.insert(0, SRC_DIR)
from functions import add_file  # noqa: E402

# Parameters must match conftest.py exactly
TIFF_ORIGIN_X = 8.0
TIFF_ORIGIN_Y = 48.0
TIFF_PIXEL_SIZE = 0.01
TIFF_WIDTH = 100
TIFF_HEIGHT = 100
TIFF_FILENAME = "COLLECTION_ST_L8_20240512T202405_194027.tif"
REMOTE_TIFF = "s3://eawagrs/test/tiff"


def create_synthetic_tiff(path):
    driver = gdal.GetDriverByName("GTiff")
    ds = driver.Create(path, TIFF_WIDTH, TIFF_HEIGHT, 1, gdal.GDT_Float32)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    ds.SetProjection(srs.ExportToWkt())
    ds.SetGeoTransform((
        TIFF_ORIGIN_X, TIFF_PIXEL_SIZE, 0,
        TIFF_ORIGIN_Y, 0, -TIFF_PIXEL_SIZE,
    ))
    values = np.arange(TIFF_WIDTH * TIFF_HEIGHT, dtype=np.float32).reshape(
        TIFF_HEIGHT, TIFF_WIDTH
    ) * 0.001
    ds.GetRasterBand(1).WriteArray(values)
    ds.FlushCache()
    ds = None


def main():
    with tempfile.TemporaryDirectory() as tmpdir:
        local_tiff = os.path.join(tmpdir, "tiff")
        local_tiff_cropped = os.path.join(tmpdir, "tiff_cropped")
        local_metadata = os.path.join(tmpdir, "metadata")
        for d in (local_tiff, local_tiff_cropped, local_metadata):
            os.makedirs(d)

        tiff_path = os.path.join(local_tiff, TIFF_FILENAME)
        create_synthetic_tiff(tiff_path)
        print(f"Created synthetic TIFF: {tiff_path}")

        with open(os.path.join(FIXTURES_DIR, "lakes.geojson")) as f:
            geojson = json.load(f)

        add_file(TIFF_FILENAME, local_tiff, local_tiff_cropped, local_metadata, REMOTE_TIFF, geojson)
        print("Processed TIFF through add_file")

        # Copy outputs to tests/fixtures/expected/
        for lake in os.listdir(local_metadata):
            src_lake_dir = os.path.join(local_metadata, lake)
            dst_lake_dir = os.path.join(EXPECTED_DIR, lake)
            os.makedirs(dst_lake_dir, exist_ok=True)
            for fname in os.listdir(src_lake_dir):
                src = os.path.join(src_lake_dir, fname)
                dst = os.path.join(dst_lake_dir, fname)
                shutil.copy(src, dst)
                print(f"  Wrote golden file: tests/fixtures/expected/{lake}/{fname}")

    print("\nDone. Review changes with:")
    print("  git diff tests/fixtures/expected/")
    print("  git add tests/fixtures/expected/")


if __name__ == "__main__":
    main()
