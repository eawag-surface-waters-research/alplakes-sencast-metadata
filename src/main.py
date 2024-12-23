import os
import json
import argparse
import functions


def main(params, lake_geometry="lakes.json"):
    print("Looking for updates from {}".format(params["download"]))
    added_files, removed_files = functions.rclone_sync(params["download"], params["filesystem"], dry_run=True)
    if len(added_files) == 0 and len(removed_files) == 0:
        print("No updates, exiting.")
        return

    functions.rclone_sync(params["download"], params["filesystem"])
    if not os.path.exists(lake_geometry):
        functions.download_file(params["geometry"], lake_geometry)
    with open(lake_geometry, 'r') as f:
        geometry = json.load(f)

    failed = []

    for file in added_files:
        try:
            functions.extract_tiff_subsection(os.path.join(params["filesystem"], file),
                                              os.path.join(params["filesystem"]+"_output", os.path.dirname(file)),
                                              geometry)
        except Exception as e:
            raise
            print(e)
            failed.append(file)





if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--download', '-d', help="URI of folder with satellite files", type=str)
    parser.add_argument('--filesystem', '-f', help="Path to local storage filesystem", type=str)
    parser.add_argument('--geometry', '-g', help="URL of lakes geojson", type=str)
    parser.add_argument('--upload', '-u', help='Upload cropped files', action='store_true')
    parser.add_argument('--remote', '-r', help="URL of S3 folder to upload cropped satellite files", type=str, default=False)
    parser.add_argument('--aws_id', '-i', help="AWS access key id", type=str, default=False)
    parser.add_argument('--aws_key', '-k', help="AWS secret access key", type=str, default=False)
    args = parser.parse_args()
    main(vars(args))
