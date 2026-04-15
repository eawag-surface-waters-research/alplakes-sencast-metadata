"""
Integration tests — require S3 credentials and a test dataset in the bucket.

Setup (one-time):
  1. Upload 2-3 real sample TIFFs to:
         s3://eawagrs/test/sencast-metadata/tiffs/
  2. Set the INTEGRATION_REMOTE_TIFF env var if using a different prefix.
  3. Run the pipeline once locally to generate golden outputs:
         RUN_INTEGRATION=1 pytest tests/test_integration.py --generate-golden
     (or manually copy the metadata outputs to tests/fixtures/expected/integration/)
  4. Commit tests/fixtures/expected/integration/ to the repo.

Running:
  pytest -m integration -v
  # or via env var:
  RUN_INTEGRATION=1 pytest tests/test_integration.py -v
"""

import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from functions import add_file, rclone_sync

FIXTURES_DIR = Path(__file__).parent / "fixtures"
GOLDEN_INTEGRATION_DIR = FIXTURES_DIR / "expected" / "integration"
DEFAULT_REMOTE_TIFF = "s3://eawagrs/test/sencast-metadata/tiffs"


def _skip_if_not_integration():
    if not os.environ.get("RUN_INTEGRATION"):
        pytest.skip("Set RUN_INTEGRATION=1 to run integration tests")


@pytest.mark.integration
def test_full_pipeline_matches_golden(tmp_path):
    """
    Download sample TIFFs from S3, process them through add_file, and compare
    every output JSON against the golden files in tests/fixtures/expected/integration/.
    """
    _skip_if_not_integration()

    if not GOLDEN_INTEGRATION_DIR.exists() or not any(GOLDEN_INTEGRATION_DIR.rglob("*.json")):
        pytest.skip(
            "No golden integration files found. "
            "Upload sample TIFFs to S3, run the pipeline once, copy outputs to "
            f"{GOLDEN_INTEGRATION_DIR}, then commit."
        )

    remote_tiff = os.environ.get("INTEGRATION_REMOTE_TIFF", DEFAULT_REMOTE_TIFF)
    local_tiff = tmp_path / "tiffs"
    local_tiff_cropped = tmp_path / "tiffs_cropped"
    local_metadata = tmp_path / "metadata"

    # Sync TIFFs from S3
    rclone_sync(remote_tiff, str(local_tiff))

    tiff_files = list(local_tiff.glob("*.tif"))
    assert tiff_files, f"No TIFFs downloaded from {remote_tiff}"

    # Load lake geometry (use the full lakes.geojson if available, else fixture)
    lakes_path = FIXTURES_DIR / "lakes.geojson"
    with open(lakes_path) as f:
        geojson = json.load(f)

    # Process every downloaded TIFF
    for tiff in tiff_files:
        add_file(
            tiff.name,
            str(local_tiff),
            str(local_tiff_cropped),
            str(local_metadata),
            remote_tiff,
            geojson,
        )

    # Compare outputs against golden files
    mismatches = []
    for golden_file in sorted(GOLDEN_INTEGRATION_DIR.rglob("*.json")):
        relative = golden_file.relative_to(GOLDEN_INTEGRATION_DIR)
        actual_file = local_metadata / relative
        if not actual_file.exists():
            mismatches.append(f"MISSING: {relative}")
            continue
        with open(actual_file) as f:
            actual = json.load(f)
        with open(golden_file) as f:
            expected = json.load(f)
        if actual != expected:
            import difflib
            exp_lines = json.dumps(expected, indent=2).splitlines(keepends=True)
            act_lines = json.dumps(actual, indent=2).splitlines(keepends=True)
            diff = "".join(difflib.unified_diff(
                exp_lines, act_lines,
                fromfile=f"expected/{relative}",
                tofile=f"actual/{relative}",
            ))
            mismatches.append(diff)

    assert not mismatches, "\n\n".join(mismatches)
