import json
import os
import shutil
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from functions import add_file, remove_file
from conftest import TIFF_FILENAME, TIFF_FILENAME2

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")
REMOTE_TIFF = "s3://eawagrs/test/tiff"


def _load_geojson():
    with open(os.path.join(FIXTURES_DIR, "lakes.geojson")) as f:
        return json.load(f)


def _copy_tiff(src, dst_dir):
    """Copy a TIFF into dst_dir and return just the filename."""
    filename = os.path.basename(src)
    shutil.copy(src, os.path.join(dst_dir, filename))
    return filename


# ---------------------------------------------------------------------------
# add_file
# ---------------------------------------------------------------------------

class TestAddFile:
    def test_creates_all_three_json_files(self, synthetic_tiff, tiff_dirs):
        filename = _copy_tiff(synthetic_tiff, tiff_dirs["local_tiff"])
        add_file(filename, tiff_dirs["local_tiff"], tiff_dirs["local_tiff_cropped"],
                 tiff_dirs["local_metadata"], REMOTE_TIFF, _load_geojson())

        meta_dir = os.path.join(tiff_dirs["local_metadata"], "test_lake")
        assert os.path.isfile(os.path.join(meta_dir, "ST.json"))
        assert os.path.isfile(os.path.join(meta_dir, "ST_latest.json"))
        assert os.path.isfile(os.path.join(meta_dir, "ST_public.json"))

    def test_json_structure(self, synthetic_tiff, tiff_dirs):
        filename = _copy_tiff(synthetic_tiff, tiff_dirs["local_tiff"])
        add_file(filename, tiff_dirs["local_tiff"], tiff_dirs["local_tiff_cropped"],
                 tiff_dirs["local_metadata"], REMOTE_TIFF, _load_geojson())

        with open(os.path.join(tiff_dirs["local_metadata"], "test_lake", "ST.json")) as f:
            data = json.load(f)

        assert isinstance(data, list)
        assert len(data) == 1
        entry = data[0]
        for key in ("dt", "k", "p", "vp", "min", "max", "mean", "p10", "p90", "c", "r"):
            assert key in entry, f"Missing key '{key}' in ST.json entry"
        assert entry["dt"] == "20240512T202405"

    def test_public_json_structure(self, synthetic_tiff, tiff_dirs):
        filename = _copy_tiff(synthetic_tiff, tiff_dirs["local_tiff"])
        add_file(filename, tiff_dirs["local_tiff"], tiff_dirs["local_tiff_cropped"],
                 tiff_dirs["local_metadata"], REMOTE_TIFF, _load_geojson())

        with open(os.path.join(tiff_dirs["local_metadata"], "test_lake", "ST_public.json")) as f:
            data = json.load(f)

        assert isinstance(data, list)
        assert len(data) == 1
        entry = data[0]
        for key in ("datetime", "name", "url", "valid_pixels"):
            assert key in entry, f"Missing key '{key}' in ST_public.json entry"
        assert entry["url"].startswith("https://")
        assert entry["valid_pixels"].endswith("%")

    def test_latest_json_is_single_object(self, synthetic_tiff, tiff_dirs):
        filename = _copy_tiff(synthetic_tiff, tiff_dirs["local_tiff"])
        add_file(filename, tiff_dirs["local_tiff"], tiff_dirs["local_tiff_cropped"],
                 tiff_dirs["local_metadata"], REMOTE_TIFF, _load_geojson())

        with open(os.path.join(tiff_dirs["local_metadata"], "test_lake", "ST_latest.json")) as f:
            data = json.load(f)

        assert isinstance(data, dict)
        assert data.get("dt") == "20240512T202405"

    def test_idempotent_no_duplicate(self, synthetic_tiff, tiff_dirs):
        """Calling add_file twice for the same TIFF should not create duplicate entries."""
        filename = _copy_tiff(synthetic_tiff, tiff_dirs["local_tiff"])
        geojson = _load_geojson()
        add_file(filename, tiff_dirs["local_tiff"], tiff_dirs["local_tiff_cropped"],
                 tiff_dirs["local_metadata"], REMOTE_TIFF, geojson)
        add_file(filename, tiff_dirs["local_tiff"], tiff_dirs["local_tiff_cropped"],
                 tiff_dirs["local_metadata"], REMOTE_TIFF, geojson)

        with open(os.path.join(tiff_dirs["local_metadata"], "test_lake", "ST.json")) as f:
            data = json.load(f)
        assert len(data) == 1

    def test_appends_second_date(self, synthetic_tiff, synthetic_tiff2, tiff_dirs):
        """Two TIFFs with different dates → two entries in ST.json."""
        geojson = _load_geojson()
        for src in (synthetic_tiff, synthetic_tiff2):
            filename = _copy_tiff(src, tiff_dirs["local_tiff"])
            add_file(filename, tiff_dirs["local_tiff"], tiff_dirs["local_tiff_cropped"],
                     tiff_dirs["local_metadata"], REMOTE_TIFF, geojson)

        with open(os.path.join(tiff_dirs["local_metadata"], "test_lake", "ST.json")) as f:
            data = json.load(f)
        assert len(data) == 2
        dates = [e["dt"] for e in data]
        assert "20240512T202405" in dates
        assert "20240601T102030" in dates


# ---------------------------------------------------------------------------
# remove_file
# ---------------------------------------------------------------------------

class TestRemoveFile:
    def test_remove_clears_metadata_entry(self, synthetic_tiff, tiff_dirs):
        filename = _copy_tiff(synthetic_tiff, tiff_dirs["local_tiff"])
        geojson = _load_geojson()
        add_file(filename, tiff_dirs["local_tiff"], tiff_dirs["local_tiff_cropped"],
                 tiff_dirs["local_metadata"], REMOTE_TIFF, geojson)
        remove_file(filename, tiff_dirs["local_metadata"])

        with open(os.path.join(tiff_dirs["local_metadata"], "test_lake", "ST.json")) as f:
            data = json.load(f)
        assert data == []

    def test_remove_updates_latest_to_empty(self, synthetic_tiff, tiff_dirs):
        filename = _copy_tiff(synthetic_tiff, tiff_dirs["local_tiff"])
        geojson = _load_geojson()
        add_file(filename, tiff_dirs["local_tiff"], tiff_dirs["local_tiff_cropped"],
                 tiff_dirs["local_metadata"], REMOTE_TIFF, geojson)
        remove_file(filename, tiff_dirs["local_metadata"])

        with open(os.path.join(tiff_dirs["local_metadata"], "test_lake", "ST_latest.json")) as f:
            data = json.load(f)
        assert data == {}

    def test_remove_second_entry_keeps_first(self, synthetic_tiff, synthetic_tiff2, tiff_dirs):
        geojson = _load_geojson()
        for src in (synthetic_tiff, synthetic_tiff2):
            filename = _copy_tiff(src, tiff_dirs["local_tiff"])
            add_file(filename, tiff_dirs["local_tiff"], tiff_dirs["local_tiff_cropped"],
                     tiff_dirs["local_metadata"], REMOTE_TIFF, geojson)

        # Remove only the second TIFF
        remove_file(TIFF_FILENAME2, tiff_dirs["local_metadata"])

        with open(os.path.join(tiff_dirs["local_metadata"], "test_lake", "ST.json")) as f:
            data = json.load(f)
        assert len(data) == 1
        assert data[0]["dt"] == "20240512T202405"


# ---------------------------------------------------------------------------
# Golden file comparison
# ---------------------------------------------------------------------------

class TestGoldenOutputs:
    """
    Compare add_file outputs against committed golden files in tests/fixtures/expected/.
    Run `python tests/generate_golden.py` to regenerate when outputs change intentionally.
    """

    def test_matches_golden_st_json(self, synthetic_tiff, tiff_dirs):
        golden_path = os.path.join(FIXTURES_DIR, "expected", "test_lake", "ST.json")
        if not os.path.isfile(golden_path):
            pytest.skip("Golden file not yet generated — run: python tests/generate_golden.py")

        filename = _copy_tiff(synthetic_tiff, tiff_dirs["local_tiff"])
        add_file(filename, tiff_dirs["local_tiff"], tiff_dirs["local_tiff_cropped"],
                 tiff_dirs["local_metadata"], REMOTE_TIFF, _load_geojson())

        with open(os.path.join(tiff_dirs["local_metadata"], "test_lake", "ST.json")) as f:
            actual = json.load(f)
        with open(golden_path) as f:
            expected = json.load(f)

        assert actual == expected, _json_diff("ST.json", expected, actual)

    def test_matches_golden_st_latest_json(self, synthetic_tiff, tiff_dirs):
        golden_path = os.path.join(FIXTURES_DIR, "expected", "test_lake", "ST_latest.json")
        if not os.path.isfile(golden_path):
            pytest.skip("Golden file not yet generated — run: python tests/generate_golden.py")

        filename = _copy_tiff(synthetic_tiff, tiff_dirs["local_tiff"])
        add_file(filename, tiff_dirs["local_tiff"], tiff_dirs["local_tiff_cropped"],
                 tiff_dirs["local_metadata"], REMOTE_TIFF, _load_geojson())

        with open(os.path.join(tiff_dirs["local_metadata"], "test_lake", "ST_latest.json")) as f:
            actual = json.load(f)
        with open(golden_path) as f:
            expected = json.load(f)

        assert actual == expected, _json_diff("ST_latest.json", expected, actual)

    def test_matches_golden_st_public_json(self, synthetic_tiff, tiff_dirs):
        golden_path = os.path.join(FIXTURES_DIR, "expected", "test_lake", "ST_public.json")
        if not os.path.isfile(golden_path):
            pytest.skip("Golden file not yet generated — run: python tests/generate_golden.py")

        filename = _copy_tiff(synthetic_tiff, tiff_dirs["local_tiff"])
        add_file(filename, tiff_dirs["local_tiff"], tiff_dirs["local_tiff_cropped"],
                 tiff_dirs["local_metadata"], REMOTE_TIFF, _load_geojson())

        with open(os.path.join(tiff_dirs["local_metadata"], "test_lake", "ST_public.json")) as f:
            actual = json.load(f)
        with open(golden_path) as f:
            expected = json.load(f)

        assert actual == expected, _json_diff("ST_public.json", expected, actual)


def _json_diff(name, expected, actual):
    import difflib
    exp_lines = json.dumps(expected, indent=2).splitlines(keepends=True)
    act_lines = json.dumps(actual, indent=2).splitlines(keepends=True)
    diff = "".join(difflib.unified_diff(exp_lines, act_lines, fromfile=f"expected/{name}", tofile=f"actual/{name}"))
    return f"\n{diff}"
