import os
import json
import tempfile
import requests
import subprocess
import numpy as np
from osgeo import gdal, ogr, osr

conda_env_path = os.environ.get("CONDA_PREFIX")
if conda_env_path:
    proj_data_path = os.path.join(conda_env_path, "share", "proj")
    os.environ["PROJ_DATA"] = proj_data_path


def add_file(file, local_tiff, local_tiff_cropped, local_metadata, remote_tiff, geometry):
    print("Adding: {}".format(file))
    properties = properties_from_filename(file)
    metadata = extract_tiff_subsection(os.path.join(local_tiff, file), local_tiff_cropped, geometry)
    for lake in metadata.keys():
        metadata_file_path = os.path.join(lake, properties["parameter"])
        lake_metadata_file = os.path.join(local_metadata, metadata_file_path + ".json")
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
                              "c": metadata[lake]["commit"],
                              "r": metadata[lake]["reproduce"]
                              })
        os.makedirs(os.path.dirname(lake_metadata_file), exist_ok=True)
        with open(lake_metadata_file, 'w') as f:
            json.dump(lake_metadata, f, separators=(',', ':'))

        public_metadata_file = os.path.join(local_metadata, metadata_file_path + "_public.json")
        if os.path.isfile(public_metadata_file):
            with open(public_metadata_file, 'r') as f:
                public_metadata = json.load(f)
            public_metadata = [l for l in public_metadata if l["name"] != metadata[lake]["file"]]
        else:
            public_metadata = []
        public_metadata.append({
            "datetime": properties["date"],
            "name": os.path.basename(file),
            "url": uri_to_url(os.path.join(remote_tiff, file)),
            "valid_pixels": "{}%".format(
                round(float(metadata[lake]["valid_pixels"]) / float(metadata[lake]["pixels"]) * 100))
        })
        with open(public_metadata_file, 'w') as f:
            json.dump(public_metadata, f, separators=(',', ':'))

        filtered = [d for d in lake_metadata if d['vp'] / d['p'] > 0.1]
        if len(filtered) > 0:
            latest = get_latest(filtered)
        else:
            latest = {}
        with open(os.path.join(local_metadata, metadata_file_path + "_latest.json"), 'w') as f:
            json.dump(latest, f, separators=(',', ':'))


def remove_file(file, local_metadata):
    print("Removing: {}".format(file))
    properties = properties_from_filename(file)
    for lake in os.listdir(local_metadata):
        metadata_file_path = os.path.join(local_metadata, lake, properties["parameter"])
        meta_file = metadata_file_path + ".json"
        public_file = metadata_file_path + "_public.json"
        if os.path.isfile(meta_file):
            with open(meta_file, 'r') as f:
                meta = json.load(f)
            if len([i for i in meta if os.path.splitext(os.path.basename(file))[0] in i["k"]]) > 0:
                meta = [i for i in meta if os.path.splitext(os.path.basename(file))[0] not in i["k"]]
                latest = get_latest([d for d in meta if d['vp'] / d['p'] > 0.1])
                with open(metadata_file_path + "_latest.json", 'w') as f:
                    print("   Deleting from: {}".format(metadata_file_path + "_latest.json"))
                    json.dump(latest, f, separators=(',', ':'))
                with open(meta_file, 'w') as f:
                    print("   Deleting from: {}".format(meta_file))
                    json.dump(meta, f, separators=(',', ':'))
        if os.path.isfile(public_file):
            with open(public_file, 'r') as f:
                public = json.load(f)
            if len([i for i in public if i["name"] == os.path.basename(file)]) > 0:
                public = [i for i in public if i["name"] != os.path.basename(file)]
                with open(public_file, 'w') as f:
                    print("   Deleting from: {}".format(public_file))
                    json.dump(public, f, separators=(',', ':'))


def download_file(url, save_path):
    """
    Downloads a file from a given URL and saves it to the specified path.

    Args:
        url (str): The URL of the file to download.
        save_path (str): The local path where the file should be saved.
    """
    response = requests.get(url)
    if response.status_code == 200:
        with open(save_path, "wb") as file:
            file.write(response.content)
        return True
    else:
        return False


def metadata_summary(uri, name, folder):
    edits = False
    try:
        response = requests.get(uri_to_url(uri))
        summary = response.json()
    except Exception as e:
        summary = {}
    for lake in os.listdir(folder):
        if lake not in summary:
            summary[lake] = {}
        parameters = list(set([f.replace(".json", "") for f in os.listdir(os.path.join(folder, lake)) if "_latest" not in f and "_public" not in f]))
        parameters.sort()
        if name not in summary[lake] or parameters != summary[lake][name]:
            edits = True
            summary[lake][name] = parameters
    if edits:
        print("   Uploading edited metadata file")
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=True) as temp_file:
            json.dump(summary, temp_file, separators=(',', ':'))
            temp_file.flush()
            try:
                subprocess.run(["rclone", "copyto", temp_file.name, uri, "--s3-no-check-bucket"], check=True)
            except Exception as e:
                print(e)
                print("Failed to upload summary file")


def polygon_raster_mask(raster, geometry):
    """
    Creates a raster mask based on a polygon and a input raster

    Parameters:
    - raster (gdal.Dataset): Opened gdal.Dataset file
    - geometry (ogr.Geometry): Polygon as a ogr.Geometry object.
    """
    driver = ogr.GetDriverByName("Memory")
    data_source = driver.CreateDataSource("temp")
    spatial_ref = osr.SpatialReference()
    spatial_ref.ImportFromWkt(raster.GetProjection())
    layer = data_source.CreateLayer("polygon", srs=spatial_ref)
    field_name = ogr.FieldDefn("id", ogr.OFTInteger)
    layer.CreateField(field_name)
    feature = ogr.Feature(layer.GetLayerDefn())
    feature.SetGeometry(geometry)
    feature.SetField("id", 1)
    layer.CreateFeature(feature)
    mask_driver = gdal.GetDriverByName("MEM")
    mask_raster = mask_driver.Create("", raster.RasterXSize, raster.RasterYSize, 1, gdal.GDT_Byte)
    mask_raster.SetGeoTransform(raster.GetGeoTransform())
    mask_raster.SetProjection(raster.GetProjection())
    gdal.RasterizeLayer(mask_raster, [1], layer, burn_values=[1])  # Inside polygon = 1
    mask_geometry = mask_raster.GetRasterBand(1).ReadAsArray()
    return mask_geometry


def pixel_coordinates(raster, geometry):
    """
    Calculates pixel values from raster and geometry

    Parameters:
    - raster (gdal.Dataset): Opened gdal.Dataset file
    - geometry (ogr.Geometry): Polygon as a ogr.Geometry object.
    """
    min_x, max_x, min_y, max_y = geometry.GetEnvelope()
    geotransform = raster.GetGeoTransform()
    min_x_pixel = int(np.floor((min_x - geotransform[0]) / geotransform[1]))
    max_x_pixel = int(np.ceil((max_x - geotransform[0]) / geotransform[1]))
    min_y_pixel = int(np.floor((max_y - geotransform[3]) / geotransform[5]))
    max_y_pixel = int(np.ceil((min_y - geotransform[3]) / geotransform[5]))

    min_x_pixel = max(min_x_pixel, 0)
    max_x_pixel = min(max_x_pixel, raster.RasterXSize)
    min_y_pixel = max(min_y_pixel, 0)
    max_y_pixel = min(max_y_pixel, raster.RasterYSize)

    new_min_x = geotransform[0] + min_x_pixel * geotransform[1]
    new_min_y = geotransform[3] + min_y_pixel * geotransform[5]

    return min_x_pixel, min_y_pixel, max_x_pixel, max_y_pixel, new_min_x, new_min_y


def extract_tiff_subsection(input_file, output_dir, geojson, small_view=500):
    raster = gdal.Open(input_file)
    geotransform = raster.GetGeoTransform()
    projection = raster.GetProjection()
    file_metadata = raster.GetMetadata()

    if raster.RasterCount == 2:
        band = raster.GetRasterBand(1).ReadAsArray()
        mask = raster.GetRasterBand(2).ReadAsArray()
        band[mask == 1] = np.nan
    else:
        band = raster.GetRasterBand(1).ReadAsArray()

    metadata = {}

    for lake in geojson["features"]:
        key = lake["properties"]["key"]

        if lake["geometry"]["coordinates"][0][0] != lake["geometry"]["coordinates"][0][-1]:
            lake["geometry"]["coordinates"][0].append(lake["geometry"]["coordinates"][0][0])

        polygon_geometry = ogr.CreateGeometryFromJson(json.dumps(lake["geometry"]))
        min_x_pixel, min_y_pixel, max_x_pixel, max_y_pixel, min_x, min_y = pixel_coordinates(raster, polygon_geometry)

        if max_x_pixel < 0 or max_y_pixel < 0 or min_x_pixel > raster.RasterXSize or min_y_pixel > raster.RasterYSize:
            continue

        cropped_band = band[min_y_pixel:max_y_pixel, min_x_pixel:max_x_pixel]
        mask_geometry = polygon_raster_mask(raster, polygon_geometry)
        cropped_band[mask_geometry[min_y_pixel:max_y_pixel, min_x_pixel:max_x_pixel] != 1] = np.nan

        if np.isnan(cropped_band).all():
            continue

        print("  Extracting lake {}".format(key))
        os.makedirs(os.path.join(output_dir, key), exist_ok=True)
        name, extension = os.path.splitext(os.path.basename(input_file))
        temp_file = os.path.join(output_dir, key,  "{}_temp{}".format(name, extension))
        main_file = os.path.join(output_dir, key, "{}_{}{}".format(name, key, extension))
        lowres_file = os.path.join(output_dir, key, "{}_{}_lowres{}".format(name, key, extension))

        metadata[key] = {
            "pixels": np.count_nonzero(mask_geometry == 1),
            "valid_pixels": np.count_nonzero(~np.isnan(cropped_band)),
            "min": np.round(np.nanmin(cropped_band).astype(np.float64),5),
            "max": np.round(np.nanmax(cropped_band).astype(np.float64),5),
            "mean": np.round(np.nanmean(cropped_band).astype(np.float64),5),
            "p10": np.round(np.nanpercentile(cropped_band, 10),5),
            "p90": np.round(np.nanpercentile(cropped_band, 90),5),
            "file": os.path.basename(main_file),
            "commit": file_metadata["Commit Hash"] if "Commit Hash" in file_metadata else "False",
            "reproduce": file_metadata["Reproduce"] if "Reproduce" in file_metadata else "False"
        }

        driver = gdal.GetDriverByName("GTiff")
        out_dataset = driver.Create(temp_file, max_x_pixel - min_x_pixel, max_y_pixel - min_y_pixel, 1, gdal.GDT_Float32)
        out_geotransform = (min_x, geotransform[1], geotransform[2], min_y, geotransform[4], geotransform[5])
        out_dataset.SetGeoTransform(out_geotransform)
        out_dataset.SetProjection(projection)
        out_band = out_dataset.GetRasterBand(1)
        out_band.WriteArray(cropped_band)
        out_band.SetNoDataValue(np.nan)
        out_dataset.FlushCache()

        # Compress file
        translate_options = gdal.TranslateOptions(gdal.ParseCommandLine(
            '-co TILED=YES -co COPY_SRC_OVERVIEWS=YES -co COMPRESS=DEFLATE'))
        gdal.Translate(main_file, out_dataset, options=translate_options)
        os.remove(temp_file)

        # Create low resolution version
        if os.path.isfile(lowres_file):
            os.remove(lowres_file)
        dataset = gdal.Open(main_file)
        geo_transform = dataset.GetGeoTransform()
        scale_factor = max(np.floor(dataset.RasterXSize/small_view), np.floor(dataset.RasterYSize/small_view))
        if scale_factor > 1:
            gdal.Warp(lowres_file, dataset, xRes=geo_transform[1]*scale_factor, yRes=geo_transform[5]*scale_factor, resampleAlg=gdal.GRA_Bilinear)
            if os.path.getsize(lowres_file) > os.path.getsize(main_file):
                os.remove(lowres_file)
            else:
                metadata[key]["file"] = os.path.basename(lowres_file)
    return metadata


def uri_to_url(uri):
    parts = uri.split("/")
    return "https://{}.s3.eu-central-1.amazonaws.com/{}".format(parts[2], "/".join(parts[3:]))


def properties_from_filename(filename):
    parts = os.path.splitext(os.path.basename(filename))[0].split("_")
    if len(parts[-1]) == 15:
        tile = None
        date = parts[-1]
        satellite = parts[-2]
        processor = parts[0]
        parameter = "_".join(parts[1:-2])
    else:
        tile = parts[-1]
        date = parts[-2]
        satellite = parts[-3]
        processor = parts[0]
        parameter = "_".join(parts[1:-3])

    return {
        "processor": processor,
        "parameter": parameter,
        "satellite": satellite,
        "date": date,
        "tile": tile
    }

def get_latest(file_list):
    if len(file_list) == 0:
        return {}
    sorted_list = sorted(file_list, key=lambda x: x['dt'])
    latest = sorted_list[-1]
    if len(file_list) > 1:
        try:
            for i in range(2, min(len(file_list), 5) + 1):
                if sorted_list[-i]["dt"][:8] == latest["dt"][:8] and sorted_list[-i]["vp"] > latest[
                    "vp"]:
                    latest = sorted_list[-i]
        except:
            print("Failed to check for same day image with more pixels")
    return latest


def rclone_sync(remote, local_dir, dry_run=False, extension="*.tif"):
    """
    Compare files between the local directory and the remote, and return three lists:
    - Added: Files that are in remote but not in local.
    - Modified: Files that differ in local and remote.
    - Removed: Files that are in local but not in remote.
    """
    os.makedirs(local_dir, exist_ok=True)
    command = ["rclone", "sync", remote, local_dir, "--include", extension]
    if dry_run:
        command.append("--dry-run")

    result = subprocess.run(command, capture_output=True, text=True, check=True)

    if dry_run:
        output = result.stderr
        added_files = []
        removed_files = []

        for line in output.splitlines():
            if "Skipped copy as" in line:
                added_files.append(line.split(": Skipped")[0].split("NOTICE: ")[1])
            elif "Skipped delete as" in line:
                removed_files.append(line.split(": Skipped")[0].split("NOTICE: ")[1])

        return added_files, removed_files
    else:
        return
