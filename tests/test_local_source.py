"""Tests for the local-folder source path through run()."""
import json
import os
import shutil
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import main
from functions import is_remote, uri_to_url, update_json

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")


class TestIsRemote:
    def test_s3_uri_is_remote(self):
        assert is_remote("s3://bucket/key") is True

    def test_https_uri_is_remote(self):
        assert is_remote("https://example.com/x") is True

    def test_local_path_is_not_remote(self):
        assert is_remote("/tmp/foo") is False
        assert is_remote("./foo") is False
        assert is_remote("foo") is False

    def test_none_is_not_remote(self):
        assert is_remote(None) is False


class TestUriToUrlLocal:
    def test_local_path_becomes_file_url(self, tmp_path):
        path = str(tmp_path / "out.tif")
        url = uri_to_url(path)
        assert url == "file://" + os.path.abspath(path)

    def test_s3_unchanged(self):
        assert uri_to_url("s3://b/k").startswith("https://b.s3")


class TestUpdateJson:
    def test_creates_file_when_missing(self, tmp_path):
        path = str(tmp_path / "a" / "b.json")
        update_json(path, lambda d: d + [{"x": 1}])
        with open(path) as f:
            assert json.load(f) == [{"x": 1}]

    def test_mutates_existing(self, tmp_path):
        path = str(tmp_path / "b.json")
        with open(path, "w") as f:
            json.dump([{"x": 1}], f)
        update_json(path, lambda d: d + [{"x": 2}])
        with open(path) as f:
            assert json.load(f) == [{"x": 1}, {"x": 2}]


class TestRunLocalSource:
    def test_processes_local_tiff_folder(self, synthetic_tiff, tmp_path, monkeypatch):
        # rclone must NOT be invoked for a local source
        import functions

        def fail_rclone(cmd, *a, **kw):
            if cmd and cmd[0] == "rclone":
                raise AssertionError("rclone was invoked unexpectedly: {}".format(cmd))
            import subprocess as _sp
            return _sp.run(cmd, *a, **kw)

        monkeypatch.setattr(functions.subprocess, "run", fail_rclone)

        source_dir = tmp_path / "source"
        source_dir.mkdir()
        shutil.copy(synthetic_tiff, source_dir / os.path.basename(synthetic_tiff))

        params = {
            "remote_tiff": str(source_dir),
            "local_tiff": str(tmp_path / "unused"),
            "remote_tiff_cropped": None,
            "local_tiff_cropped": str(tmp_path / "cropped"),
            "remote_metadata": None,
            "local_metadata": str(tmp_path / "metadata"),
            "metadata_summary": None,
            "metadata_name": None,
            "lake_geometry": None,
            "upload": False,
            "reprocess": False,
            "lakes": None,
            "period": None,
        }

        # Pre-place the geojson so download_file is not called
        geo_path = str(tmp_path / "lakes.geojson")
        shutil.copy(os.path.join(FIXTURES_DIR, "lakes.geojson"), geo_path)

        main.run(params, lake_geometry=geo_path)

        meta_dir = os.path.join(params["local_metadata"], "test_lake")
        assert os.path.isfile(os.path.join(meta_dir, "ST.json"))
        assert os.path.isfile(os.path.join(meta_dir, "ST_latest.json"))
        assert os.path.isfile(os.path.join(meta_dir, "ST_public.json"))

        with open(os.path.join(meta_dir, "ST_public.json")) as f:
            public = json.load(f)
        assert public[0]["url"].startswith("file://")

    def test_missing_local_source_raises(self, tmp_path):
        params = {
            "remote_tiff": str(tmp_path / "does_not_exist"),
            "local_tiff": "/x",
            "remote_tiff_cropped": None,
            "local_tiff_cropped": str(tmp_path / "cropped"),
            "remote_metadata": None,
            "local_metadata": str(tmp_path / "metadata"),
            "metadata_summary": None,
            "metadata_name": None,
            "lake_geometry": None,
            "upload": False,
            "reprocess": False,
            "lakes": None,
            "period": None,
        }
        with pytest.raises(ValueError, match="does not exist"):
            main.run(params, lake_geometry=os.path.join(FIXTURES_DIR, "lakes.geojson"))
