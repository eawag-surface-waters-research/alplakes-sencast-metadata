import os
import json
import argparse
import functions


def reprocess(params, lake_geometry="lakes.json"):
    print("Reprocessing metadata")
    functions.rclone_sync(params["remote_tiff"], params["local_tiff"])
    functions.rclone_sync(params["remote_metadata"], params["local_metadata"], extension="*.json")
    functions.download_file(params["lake_geometry"], lake_geometry)
    with open(lake_geometry, 'r') as f:
        geometry = json.load(f)

    if params["new_lakes"]:
        existing_lakes = os.listdir(params["local_metadata"])
        new_lakes = [f["properties"]["key"] for f in geometry["features"] if f["properties"]["key"] not in existing_lakes]
        print("Only parsing new lakes: {}".format(new_lakes))
        geometry["features"] = [f for f in geometry["features"] if f["properties"]["key"] not in existing_lakes]
        if len(geometry["features"]) == 0:
            print("No additional lakes found")
            return

    failed = []
    for root, dirs, files in os.walk(params["local_tiff"]):
        for file in files:
            try:
                functions.add_file(os.path.join(os.path.relpath(root, params["local_tiff"]), file),
                                   params["local_tiff"], params["local_tiff_cropped"], params["local_metadata"],
                                   params["remote_tiff"], geometry)
            except Exception as e:
                failed.append(file)
                print(e)

    if "metadata_summary" in params:
        print("Checking for metadata summary updates")
        functions.metadata_summary(params["metadata_summary"], params["metadata_name"], os.path.abspath(params["local_metadata"]))

    if params["upload"]:
        print("Uploading to remote")
        functions.rclone_sync(params["local_tiff_cropped"], params["remote_tiff_cropped"])
        functions.rclone_sync(params["local_metadata"], params["remote_metadata"], extension="*.json")

    if len(failed) > 0:
        raise ValueError("Failed for: {}".format(", ".join(failed)))


def main(params, lake_geometry="lakes.json"):
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

    if "metadata_summary" in params:
        print("Checking for metadata summary updates")
        functions.metadata_summary(params["metadata_summary"], params["metadata_name"], os.path.abspath(params["local_metadata"]))

    if params["upload"]:
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
    parser.add_argument('--new_lakes', '-n', help='Only reprocess data for new lakes', action='store_true')
    args = parser.parse_args()
    params = vars(args)
    if params["reprocess"]:
        reprocess(params)
    else:
        main(params)
