# Tests

Two layers:

- **Unit tests** — fast, hermetic, run against synthetic in-memory GeoTIFFs. No network, no S3, no credentials.
- **Integration test** — exercises the full pipeline against real sample TIFFs in S3 and compares outputs to a committed golden snapshot.

## Layout

```
tests/
├── conftest.py              # synthetic TIFF fixtures + shared constants
├── test_parsing.py          # filename / URI parsing
├── test_processing.py       # TIFF cropping, pixel coords, get_latest
├── test_metadata.py         # add_file / remove_file end-to-end on synthetic TIFFs
├── test_integration.py      # full pipeline vs S3 (marked `integration`)
├── generate_golden.py       # regenerates fixtures/expected/ for unit tests
└── fixtures/
    ├── lakes.geojson              # minimal synthetic test_lake polygon (unit tests)
    ├── lakes_integration.geojson  # real Lake Geneva polygon (integration test)
    └── expected/
        ├── test_lake/             # golden JSON for unit tests
        └── integration/           # golden JSON for the integration test
```

## Running

From the repo root:

```bash
# Unit tests only (default)
python -m pytest -m "not integration" -v

# Integration test (requires S3 creds + RUN_INTEGRATION=1)
RUN_INTEGRATION=1 pytest -m integration -v

# Single file / single test
pytest tests/test_processing.py -v
pytest tests/test_metadata.py::test_add_file_creates_metadata -v
```

The `integration` marker is registered in [pytest.ini](../pytest.ini); without `RUN_INTEGRATION=1` the integration test self-skips even when the marker is selected.

## Golden files (unit tests)

`test_metadata.py` compares the JSON written by `add_file` against snapshots in `fixtures/expected/test_lake/`. Regenerate after any **intentional** change to the output schema or values:

```bash
python tests/generate_golden.py
git diff tests/fixtures/expected/      # review carefully
git add tests/fixtures/expected/
```

The synthetic TIFF used by `generate_golden.py` matches the parameters in [conftest.py](conftest.py) exactly (origin, pixel size, dimensions, filename) — keep them in sync if you edit either file.

## Integration test setup (one-time)

1. Upload 2–3 representative TIFFs to `s3://eawagrs/test/sencast-metadata/tiffs/` (override with `INTEGRATION_REMOTE_TIFF`). Each TIFF's footprint must overlap the polygon in `fixtures/lakes_integration.geojson` (Lake Geneva, ~6.14–6.93°E / 46.20–46.52°N) — otherwise `add_file` will silently skip it and produce no metadata.
2. Run the pipeline locally against `lakes_integration.geojson` and copy the resulting `metadata/` tree into `tests/fixtures/expected/integration/`.
3. Commit that directory. Subsequent runs diff against this snapshot and fail on any mismatch.

If `fixtures/expected/integration/` is empty, the integration test skips with an instructive message rather than failing.

To swap in a different lake, replace `fixtures/lakes_integration.geojson` (extract another feature from the repo-root `lakes.geojson`) and regenerate the golden tree.

## Fixtures provided by [conftest.py](conftest.py)

| Fixture | Description |
|---|---|
| `synthetic_tiff` | 100×100 Float32 GeoTIFF over 8–9°E / 47–48°N, single band |
| `synthetic_tiff_with_mask` | Same, with a Band 2 quality mask block at rows 35–45 / cols 35–45 |
| `synthetic_tiff2` | Second TIFF with a different date — for `get_latest` / append tests |
| `lake_geojson` | Loaded `fixtures/lakes.geojson` — a single polygon (`test_lake`) covering rows 25–75 / cols 25–75 of the synthetic raster |
| `tiff_dirs` | Per-test tmp dirs mirroring the pipeline layout (`local_tiff`, `local_tiff_cropped`, `local_metadata`) |
| `expected_dir` | Path to `fixtures/expected/` |

## Troubleshooting

- **`ImportError: osgeo`** — install GDAL into the active env (`conda env update -f environment.yml` or your distro's `python3-gdal`).
- **Golden diff after an unrelated change** — likely a floating-point or ordering drift; inspect the diff before regenerating. Don't blindly re-run `generate_golden.py`.
- **Integration test downloads nothing** — check `rclone.conf` is configured and the bucket prefix matches `INTEGRATION_REMOTE_TIFF`.
