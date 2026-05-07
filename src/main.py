import os
import time
import json
import logging
import argparse
from datetime import timedelta
import functions

logger = logging.getLogger(__name__)


def run(params, lake_geometry="lakes.geojson"):
    start_time = time.monotonic()
    source = params["remote_tiff"]
    source_is_remote = functions.is_remote(source)
    logger.info("Starting run: source=%s reprocess=%s", source, params.get("reprocess"))

    if source_is_remote:
        local_tiff = params["local_tiff"]
    else:
        local_tiff = os.path.abspath(source)
        if not os.path.isdir(local_tiff):
            raise ValueError("Local source folder does not exist: {}".format(local_tiff))

    if functions.is_remote(params["remote_metadata"]):
        functions.rclone_sync(params["remote_metadata"], params["local_metadata"], extension="*.json")

    needs_download = params.get("reprocess") or not os.path.exists(lake_geometry)
    if needs_download and functions.is_remote(params.get("lake_geometry")):
        functions.download_file(params["lake_geometry"], lake_geometry)
    with open(lake_geometry, "r") as f:
        geometry = json.load(f)

    geometry, missing = functions._filter_geometry(geometry, params.get("lakes"))
    if missing:
        logger.warning("Geometry missing for the following lakes: %s", missing)
        return

    period_match = functions._period_filter(params.get("period"))

    if params.get("reprocess"):
        if source_is_remote:
            logger.info("Reprocessing metadata")
            functions.rclone_sync(source, local_tiff)
        added_files = list(functions._walk_tiffs(local_tiff))
        removed_files = []
    elif source_is_remote:
        logger.info("Looking for updates from %s", source)
        added_files, removed_files = functions.rclone_sync(source, local_tiff, dry_run=True)
        if not added_files and not removed_files:
            logger.info("No updates, exiting.")
            return
        functions.rclone_sync(source, local_tiff)
    else:
        added_files = list(functions._walk_tiffs(local_tiff))
        removed_files = []

    logger.info("Files to add: %d, files to remove: %d", len(added_files), len(removed_files))
    failed = []
    added_count = 0
    skipped_count = 0
    removed_count = 0
    for file in added_files:
        if period_match and not period_match(file):
            skipped_count += 1
            continue
        try:
            functions.add_file(file, local_tiff, params["local_tiff_cropped"], params["local_metadata"],
                               source, geometry)
            added_count += 1
        except Exception:
            full_path = os.path.join(local_tiff, file)
            if source_is_remote and os.path.isfile(full_path):
                os.remove(full_path)
            logger.exception("Failed to add %s", file)
            failed.append(file)

    for file in removed_files:
        try:
            functions.remove_file(file, params["local_metadata"])
            removed_count += 1
        except Exception:
            logger.exception("Failed to remove %s", file)
            failed.append(file)

    if params["upload"]:
        if functions._is_set(params.get("metadata_summary")) and functions.is_remote(params["metadata_summary"]):
            logger.info("Checking for metadata summary updates")
            functions.metadata_summary(params["metadata_summary"], params["metadata_name"],
                                       os.path.abspath(params["local_metadata"]))

        if functions.is_remote(params["remote_tiff_cropped"]):
            logger.info("Uploading cropped tiffs to remote")
            functions.rclone_sync(params["local_tiff_cropped"], params["remote_tiff_cropped"])
        if functions.is_remote(params["remote_metadata"]):
            logger.info("Uploading metadata to remote")
            functions.rclone_sync(params["local_metadata"], params["remote_metadata"], extension="*.json")

    elapsed = timedelta(seconds=int(time.monotonic() - start_time))
    logger.info(
        "Run complete in %s: %d added, %d removed, %d skipped, %d failed",
        elapsed, added_count, removed_count, skipped_count, len(failed),
    )
    if failed:
        raise ValueError("Failed for: {}".format(", ".join(failed)))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    parser = argparse.ArgumentParser()
    parser.add_argument('--remote_tiff', '-rt', help="URI of remote tiff folder, or path to a local folder", type=str)
    parser.add_argument('--local_tiff', '-lt', help="Path of local tiff folder (used as rclone target for remote sources)", type=str, default="/local_tiff")
    parser.add_argument('--remote_tiff_cropped', '-rtc', help="URI of remote cropped tiff folder", type=str, default=None)
    parser.add_argument('--local_tiff_cropped', '-ltc', help="Path of local cropped tiff folder", type=str, default="/local_tiff_cropped")
    parser.add_argument('--lake_geometry', '-g', help="URL of lakes geojson", type=str)
    parser.add_argument('--remote_metadata', '-rm', help="URI of remote metadata folder", type=str, default=None)
    parser.add_argument('--metadata_summary', '-ms', help="URI of remote metadata summary", type=str, default=None)
    parser.add_argument('--metadata_name', '-mn', help="Name of dataset in metadata summary", type=str)
    parser.add_argument('--local_metadata', '-lm', help="Path of local metadata folder", type=str, default="/local_metadata")
    parser.add_argument('--upload', '-u', help='Upload cropped files and metadata', action='store_true')
    parser.add_argument('--reprocess', '-r', help='Reprocess full dataset', action='store_true')
    parser.add_argument('--lakes', '-n', help='Comma separated list of lakes to reprocess e.g. geneva,zurich', type=str, default=None)
    parser.add_argument('--period', '-p', help='Time period to reprocess YYYYMMDD_YYYYMMDD', type=str, default=None)
    args = parser.parse_args()
    run(vars(args))
