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
                os.makedirs(os.path.dirname(lake_metadata_file), exist_ok=True)
                with open(lake_metadata_file, 'w') as f:
                    json.dump(lake_metadata, f, separators=(',', ':'))

                public_metadata_file = os.path.join(params["local_metadata"], metadata_file_path + "_public.json")
                if os.path.isfile(public_metadata_file):
                    with open(public_metadata_file, 'r') as f:
                        public_metadata = json.load(f)
                else:
                    public_metadata = []
                public_metadata = [l for l in public_metadata if l["k"] != metadata[lake]["file"]]
                public_metadata.append({
                    "datetime": properties["date"],
                    "name": os.path.basename(file),
                     "url": functions.uri_to_url(os.path.join(params["remote_tiff"], file)),
                     "valid_pixels": "{}%".format(round(float(metadata[lake]["valid_pixels"]) / float(metadata[lake]["pixels"]) * 100))
                })
                with open(public_metadata_file, 'w') as f:
                    json.dump(public_metadata, f, separators=(',', ':'))

                filtered = [d for d in lake_metadata if d['vp'] / d['p'] > 0.1]
                if len(filtered) > 0:
                    sorted_list = sorted(filtered, key=lambda x: x['dt'])
                    latest = sorted_list[-1]
                    if len(filtered) > 1:
                        try:
                            for i in range(2, min(len(filtered), 5) + 1):
                                if sorted_list[-i]["dt"][:8] == latest["dt"][:8] and sorted_list[-i]["vp"] > latest[
                                    "vp"]:
                                    latest = sorted_list[-i]
                        except:
                            print("Failed to check for same day image with more pixels")
                    with open(os.path.join(params["local_metadata"], metadata_file_path + "_latest.json"), 'w') as f:
                        json.dump(latest, f, separators=(',', ':'))
        except Exception as e:
            raise
            print(e)
            failed.append(file)

    if params["upload"]:
        print("Uploading to remote")
        # Sync cropped tiffs to remote
        # Sync metadata to remote
        # Need to sort credentials - should be in env

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
    parser.add_argument('--local_metadata', '-lm', help="Path of local metadata folder", type=str, default="/local_metadata")
    parser.add_argument('--upload', '-u', help='Upload cropped files', action='store_true')
    args = parser.parse_args()
    main(vars(args))
