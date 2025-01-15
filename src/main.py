import os
import re
import json
import argparse
from datetime import datetime
import functions


def reprocess(params, lake_geometry="lakes.geojson"):
    print("Reprocessing metadata")
    functions.rclone_sync(params["remote_tiff"], params["local_tiff"])
    functions.rclone_sync(params["remote_metadata"], params["local_metadata"], extension="*.json")
    functions.download_file(params["lake_geometry"], lake_geometry)
    with open(lake_geometry, 'r') as f:
        geometry = json.load(f)

    if params["lakes"] is not False and params["lakes"].lower() != "false":
        new_lakes = [l.strip() for l in params["lakes"].split(",")]
        print("Only parsing new lakes: {}".format(new_lakes))
        geometry["features"] = [f for f in geometry["features"] if f["properties"]["key"] in new_lakes]
        missing = [x for x in new_lakes if x not in [f["properties"]["key"] for f in geometry["features"]]]
        if len(missing) > 0:
            print("Geometry missing for the following lakes: {}".format(missing))
            return

    period = False
    if params["period"] is not False and params["period"].lower() != "false":
        period = True
        start_end = params["period"].split("_")
        start = datetime.strptime(start_end[0], "%Y%m%d")
        end = datetime.strptime(start_end[1], "%Y%m%d")
        print("Only processing files between {} and {}".format(start, end))

    failed = []
    for root, dirs, files in os.walk(params["local_tiff"]):
        for file in files:
            if not file.endswith(".tif"):
                continue
            if period:
                match = re.search(r"\d{8}T\d{6}", file)
                if match:
                    dt = datetime.strptime(match.group(0), "%Y%m%dT%H%M%S")
                    if dt < start or dt > end:
                        continue
            try:
                functions.add_file(os.path.join(os.path.relpath(root, params["local_tiff"]), file),
                                   params["local_tiff"], params["local_tiff_cropped"], params["local_metadata"],
                                   params["remote_tiff"], geometry)
            except Exception as e:
                failed.append(file)
                print(e)

    if params["upload"]:
        if "metadata_summary" in params:
            print("Checking for metadata summary updates")
            functions.metadata_summary(params["metadata_summary"], params["metadata_name"],
                                       os.path.abspath(params["local_metadata"]))

        print("Uploading to remote")
        functions.rclone_sync(params["local_tiff_cropped"], params["remote_tiff_cropped"])
        functions.rclone_sync(params["local_metadata"], params["remote_metadata"], extension="*.json")

    if len(failed) > 0:
        raise ValueError("Failed for: {}".format(", ".join(failed)))


def main(params, lake_geometry="lakes.geojson"):
    print("Looking for updates from {}".format(params["remote_tiff"]))
    added_files, removed_files = functions.rclone_sync(params["remote_tiff"], params["local_tiff"], dry_run=True)
    if len(added_files) == 0 and len(removed_files) == 0:
        print("No updates, exiting.")
        return

    functions.rclone_sync(params["remote_tiff"], params["local_tiff"])
    functions.rclone_sync(params["remote_metadata"], params["local_metadata"], extension="*.json")
    if not os.path.exists(lake_geometry):
        functions.download_file(params["lake_geometry"], lake_geometry)
    with open(lake_geometry, 'r') as f:
        geometry = json.load(f)

    failed = []
    for file in added_files:
        try:
            functions.add_file(file, params["local_tiff"], params["local_tiff_cropped"], params["local_metadata"],
                               params["remote_tiff"], geometry)
        except Exception as e:
            os.remove(os.path.join(params["local_tiff"], file))
            print(e)
            failed.append(file)

    for file in removed_files:
        try:
            functions.remove_file(file, params["local_metadata"])
        except Exception as e:
            print(e)
            failed.append(file)

    if params["upload"]:
        if "metadata_summary" in params:
            print("Checking for metadata summary updates")
            functions.metadata_summary(params["metadata_summary"], params["metadata_name"],
                                       os.path.abspath(params["local_metadata"]))

        print("Uploading to remote")
        functions.rclone_sync(params["local_tiff_cropped"], params["remote_tiff_cropped"])
        functions.rclone_sync(params["local_metadata"], params["remote_metadata"], extension="*.json")

    if len(failed) > 0:
        raise ValueError("Failed for: {}".format(", ".join(failed)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--remote_tiff', '-rt', help="URI of remote tiff folder", type=str)
    parser.add_argument('--local_tiff', '-lt', help="Path of local tiff folder", type=str, default="/local_tiff")
    parser.add_argument('--remote_tiff_cropped', '-rtc', help="URI of remote cropped tiff folder", type=str, default=False)
    parser.add_argument('--local_tiff_cropped', '-ltc', help="Path of local cropped tiff folder", type=str, default="/local_tiff_cropped")
    parser.add_argument('--lake_geometry', '-g', help="URL of lakes geojson", type=str)
    parser.add_argument('--remote_metadata', '-rm', help="URI of remote metadata folder", type=str)
    parser.add_argument('--metadata_summary', '-ms', help="URI of remote metadata summary", type=str)
    parser.add_argument('--metadata_name', '-mn', help="Name of dataset in metadata summary", type=str)
    parser.add_argument('--local_metadata', '-lm', help="Path of local metadata folder", type=str, default="/local_metadata")
    parser.add_argument('--upload', '-u', help='Upload cropped files and metadata', action='store_true')
    parser.add_argument('--reprocess', '-r', help='Reprocess full dataset', action='store_true')
    parser.add_argument('--lakes', '-n', help='Comma separated list of lakes to reprocess e.g. geneva,zurich', type=str, default=False)
    parser.add_argument('--period', '-p', help='Time period to reprocess YYYYMMDD_YYYYMMDD', type=str, default=False)
    args = parser.parse_args()
    params = vars(args)
    if params["reprocess"]:
        reprocess(params)
    else:
        main(params)
