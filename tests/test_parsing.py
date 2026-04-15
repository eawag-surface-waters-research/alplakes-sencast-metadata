import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from functions import properties_from_filename, uri_to_url


class TestPropertiesFromFilename:
    def test_with_tile(self):
        result = properties_from_filename("COLLECTION_ST_L8_20240512T202405_194027.tif")
        assert result == {
            "processor": "COLLECTION",
            "parameter": "ST",
            "satellite": "L8",
            "date": "20240512T202405",
            "tile": "194027",
        }

    def test_without_tile(self):
        # Last part is exactly 15 chars → no tile
        result = properties_from_filename("COLLECTION_ST_L8_20240512T202405.tif")
        assert result["tile"] is None
        assert result["date"] == "20240512T202405"
        assert result["satellite"] == "L8"
        assert result["parameter"] == "ST"
        assert result["processor"] == "COLLECTION"

    def test_multipart_parameter(self):
        result = properties_from_filename("COLLECTION_CHL_CI_L8_20240512T202405_194027.tif")
        assert result["parameter"] == "CHL_CI"
        assert result["processor"] == "COLLECTION"
        assert result["satellite"] == "L8"
        assert result["tile"] == "194027"

    def test_strips_path_prefix(self):
        result = properties_from_filename("/some/path/COLLECTION_ST_L8_20240512T202405_194027.tif")
        assert result["processor"] == "COLLECTION"
        assert result["parameter"] == "ST"

    def test_date_format(self):
        result = properties_from_filename("PROC_PARAM_L9_20230101T000000_999999.tif")
        assert result["date"] == "20230101T000000"
        assert result["satellite"] == "L9"


class TestUriToUrl:
    def test_basic_conversion(self):
        uri = "s3://eawagrs/metadata/collection/zurich/ST.json"
        url = uri_to_url(uri)
        assert url == "https://eawagrs.s3.eu-central-1.amazonaws.com/metadata/collection/zurich/ST.json"

    def test_deep_path(self):
        uri = "s3://mybucket/a/b/c/d/file.tif"
        url = uri_to_url(uri)
        assert url == "https://mybucket.s3.eu-central-1.amazonaws.com/a/b/c/d/file.tif"

    def test_single_segment_path(self):
        uri = "s3://bucket/file.tif"
        url = uri_to_url(uri)
        assert url == "https://bucket.s3.eu-central-1.amazonaws.com/file.tif"
