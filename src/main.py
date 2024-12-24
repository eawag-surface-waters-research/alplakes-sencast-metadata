import os
import json
import argparse
import functions
from src.functions import download_file


def main(params, lake_geometry="lakes.json"):
    print("Looking for updates from {}".format(params["remote_tiff"]))
    added_files, removed_files = functions.rclone_sync(params["remote_tiff"], params["local_tiff"], dry_run=True)
    if len(added_files) == 0 and len(removed_files) == 0:
        print("No updates, exiting.")
        return

    functions.rclone_sync(params["remote_tiff"], params["local_tiff"])
    functions.rclone_sync(params["remote_metadata"], params["local_metadata"])
    if not os.path.exists(lake_geometry):
        functions.download_file(params["lake_geometry"], lake_geometry)
    with open(lake_geometry, 'r') as f:
        geometry = json.load(f)

    failed = []
    for file in added_files:
        try:
            properties = functions.properties_from_filename(file)
            metadata = functions.extract_tiff_subsection(os.path.join(params["local_tiff"], file),
                                                         params["local_tiff_cropped"],
                                                         geometry)
            for lake in metadata.keys():
                metadata_file_path = os.path.join(lake, properties["parameter"])
                lake_metadata_file = os.path.join(params["local_metadata"], metadata_file_path + ".json")
                if os.path.isfile(lake_metadata_file):
                    with open(lake_metadata_file, 'r') as f:
                        lake_metadata = json.load(f)
                else:
                    lake_metadata = []
                lake_metadata = [l for l in lake_metadata if l["k"] != metadata[lake]["file"]]
                lake_metadata.append({"dt": properties["date"],
                                      "k": metadata[lake]["file"],
                                      "p": metadata[lake]["pixels"],
                                      "vp": metadata[lake]["valid_pixels"],
                                      "min": metadata[lake]["min"],
                                      "max": metadata[lake]["max"],
                                      "mean": metadata[lake]["mean"],
                                      "p10": metadata[lake]["p10"],
                                      "p90": metadata[lake]["p90"],
                                      })
                # Add human readable info for tiles (not per lake)

                # Sync to buckets
                """x = {"datetime": d["datetime"], "name": d["key"].split("/")[-1],
                 "url": "{}/{}".format(bucket_url, d["key"]),
                 "valid_pixels": "{}%".format(round(float(d["valid_pixels"]) / float(max_pixels) * 100))}"""
                os.makedirs(os.path.dirname(lake_metadata_file), exist_ok=True)
                with open(lake_metadata_file, 'w') as f:
                    json.dump(lake_metadata, f, separators=(',', ':'))
        except Exception as e:
            raise
            print(e)
            failed.append(file)

    if len(failed) > 0:
        raise ValueError("Failed for: {}".format(", ".join(failed)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--remote_tiff', '-rt', help="URI of remote tiff folder", type=str)
    parser.add_argument('--local_tiff', '-lt', help="Path of local tiff folder", type=str)
    parser.add_argument('--remote_tiff_cropped', '-rtc', help="URI of remote cropped tiff folder", type=str, default=False)
    parser.add_argument('--local_tiff_cropped', '-ltc', help="Path of local cropped tiff folder", type=str)
    parser.add_argument('--lake_geometry', '-g', help="URL of lakes geojson", type=str)
    parser.add_argument('--remote_metadata', '-rm', help="URI of remote metadata folder", type=str)
    parser.add_argument('--local_metadata', '-lm', help="Path of local metadata folder", type=str)
    parser.add_argument('--upload', '-u', help='Upload cropped files', action='store_true')
    parser.add_argument('--aws_id', '-i', help="AWS access key id", type=str, default=False)
    parser.add_argument('--aws_key', '-k', help="AWS secret access key", type=str, default=False)
    args = parser.parse_args()
    main(vars(args))
