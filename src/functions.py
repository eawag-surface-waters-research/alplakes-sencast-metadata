import os
import re
import json
import logging
import tempfile
import requests
import subprocess
from datetime import datetime
import numpy as np
from osgeo import gdal, ogr, osr

logger = logging.getLogger(__name__)

conda_env_path = os.environ.get("CONDA_PREFIX")
if conda_env_path:
    proj_data_path = os.path.join(conda_env_path, "share", "proj")
    os.environ["PROJ_DATA"] = proj_data_path


def is_remote(path):
    return isinstance(path, str) and "://" in path


def update_json(path, mutate, default=None):
    if os.path.isfile(path):
        with open(path, "r") as f:
            data = json.load(f)
    else:
        data = [] if default is None else default
    data = mutate(data)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, separators=(",", ":"))
    return data

def _is_set(v):
    if v is None or v is False:
        return False
    if isinstance(v, str) and v.lower() == "false":
        return False
    return True


def _filter_geometry(geometry, lakes_arg):
    if not _is_set(lakes_arg):
        return geometry, []
    new_lakes = [l.strip() for l in lakes_arg.split(",")]
    logger.info("Only parsing new lakes: %s", new_lakes)
    keys = {f["properties"]["key"] for f in geometry["features"]}
    geometry = {**geometry, "features": [f for f in geometry["features"] if f["properties"]["key"] in new_lakes]}
    missing = [x for x in new_lakes if x not in keys]
    return geometry, missing


def _period_filter(period_arg):
    if not _is_set(period_arg):
        return None
    start_end = period_arg.split("_")
    start = datetime.strptime(start_end[0], "%Y%m%d")
    end = datetime.strptime(start_end[1], "%Y%m%d")
    logger.info("Only processing files between %s and %s", start, end)

    def matches(filename):
        match = re.search(r"\d{8}T\d{6}", filename)
        if not match:
            return True
        dt = datetime.strptime(match.group(0), "%Y%m%dT%H%M%S")
        return start <= dt <= end

    return matches


def _walk_tiffs(root_dir):
    """Yield TIFF paths relative to root_dir."""
    for root, _dirs, files in os.walk(root_dir):
        for file in files:
            if file.endswith(".tif"):
                yield os.path.join(os.path.relpath(root, root_dir), file)


def add_file(file, local_tiff, local_tiff_cropped, local_metadata, remote_tiff, geometry):
    logger.info("Adding: %s", file)
    properties = properties_from_filename(file)
    metadata = extract_tiff_subsection(os.path.join(local_tiff, file), local_tiff_cropped, geometry)
    cropped_files = []
    for lake in metadata.keys():
        m = metadata[lake]
        cropped_files.extend(m.get("cropped_files", []))
        base = os.path.join(local_metadata, lake, properties["parameter"])

        def upsert_full(data, m=m, properties=properties):
            data = [l for l in data if l["k"] != m["file"]]
            data.append({"dt": properties["date"], "k": m["file"], "p": m["pixels"],
                         "vp": m["valid_pixels"], "min": m["min"], "max": m["max"],
                         "mean": m["mean"], "p10": m["p10"], "p90": m["p90"],
                         "c": m["commit"], "r": m["reproduce"]})
            return data

        full = update_json(base + ".json", upsert_full)

        def upsert_public(data, m=m, properties=properties, file=file):
            data = [l for l in data if l["name"] != m["file"]]
            data.append({
                "datetime": properties["date"],
                "name": os.path.basename(file),
                "url": uri_to_url(os.path.join(remote_tiff, file)),
                "valid_pixels": "{}%".format(round(float(m["valid_pixels"]) / float(m["pixels"]) * 100)),
            })
            return data

        update_json(base + "_public.json", upsert_public)

        filtered = [d for d in full if d['vp'] / d['p'] > 0.1]
        latest = get_latest(filtered) if filtered else {}
        update_json(base + "_latest.json", lambda _: latest, default={})
    return cropped_files


def remove_file(file, local_metadata):
    logger.info("Removing: %s", file)
    properties = properties_from_filename(file)
    stem = os.path.splitext(os.path.basename(file))[0]
    name = os.path.basename(file)
    if not os.path.isdir(local_metadata):
        return
    for lake in os.listdir(local_metadata):
        base = os.path.join(local_metadata, lake, properties["parameter"])
        meta_file = base + ".json"
        public_file = base + "_public.json"

        if os.path.isfile(meta_file):
            with open(meta_file, "r") as f:
                meta = json.load(f)
            if any(stem in i["k"] for i in meta):
                meta = [i for i in meta if stem not in i["k"]]
                logger.info("Deleting from: %s", meta_file)
                update_json(meta_file, lambda _: meta)
                latest = get_latest([d for d in meta if d['vp'] / d['p'] > 0.1])
                logger.info("Deleting from: %s", base + "_latest.json")
                update_json(base + "_latest.json", lambda _: latest, default={})

        if os.path.isfile(public_file):
            with open(public_file, "r") as f:
                public = json.load(f)
            if any(i["name"] == name for i in public):
                logger.info("Deleting from: %s", public_file)
                update_json(public_file, lambda _: [i for i in public if i["name"] != name])


def download_file(url, save_path):
    """
    Downloads a file from a given URL and saves it to the specified path.

    Args:
        url (str): The URL of the file to download.
        save_path (str): The local path where the file should be saved.
    """
    logger.info("Downloading %s -> %s", url, save_path)
    response = requests.get(url)
    if response.status_code == 200:
        with open(save_path, "wb") as file:
            file.write(response.content)
        return True
    else:
        raise ValueError("Failed to download from: {}".format(url))


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
        logger.info("Uploading edited metadata file")
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=True) as temp_file:
            json.dump(summary, temp_file, separators=(',', ':'))
            temp_file.flush()
            try:
                subprocess.run(["rclone", "copyto", temp_file.name, uri, "--s3-no-check-bucket"], check=True)
            except Exception:
                logger.exception("Failed to upload summary file")
    else:
        logger.info("No metadata summary changes")


def polygon_raster_mask(raster, geometry, window):
    """
    Creates a raster mask for a polygon, sized to the cropped window rather than
    the full source raster.

    Parameters:
    - raster (gdal.Dataset): Opened gdal.Dataset file (used for projection + pixel size).
    - geometry (ogr.Geometry): Polygon as an ogr.Geometry object.
    - window (tuple): (min_x_pixel, min_y_pixel, max_x_pixel, max_y_pixel, min_x, min_y)
        as returned by ``pixel_coordinates``.
    """
    min_x_pixel, min_y_pixel, max_x_pixel, max_y_pixel, min_x, min_y = window
    width = max_x_pixel - min_x_pixel
    height = max_y_pixel - min_y_pixel
    src_geotransform = raster.GetGeoTransform()

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
    mask_raster = mask_driver.Create("", width, height, 1, gdal.GDT_Byte)
    mask_raster.SetGeoTransform((min_x, src_geotransform[1], src_geotransform[2],
                                 min_y, src_geotransform[4], src_geotransform[5]))
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
        window = pixel_coordinates(raster, polygon_geometry)
        min_x_pixel, min_y_pixel, max_x_pixel, max_y_pixel, min_x, min_y = window

        if max_x_pixel < 0 or max_y_pixel < 0 or min_x_pixel > raster.RasterXSize or min_y_pixel > raster.RasterYSize:
            logger.debug("Skipping lake %s: outside raster bounds", key)
            continue

        if max_x_pixel - min_x_pixel <= 0 or max_y_pixel - min_y_pixel <= 0:
            logger.debug("Skipping lake %s: empty cropped window", key)
            continue

        cropped_band = np.copy(band[min_y_pixel:max_y_pixel, min_x_pixel:max_x_pixel])
        mask_geometry = polygon_raster_mask(raster, polygon_geometry, window)
        cropped_band[mask_geometry != 1] = np.nan

        if np.isnan(cropped_band).all():
            logger.debug("Skipping lake %s: all pixels NaN", key)
            continue

        logger.info("Extracting lake %s", key)
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
            "reproduce": file_metadata["Reproduce"] if "Reproduce" in file_metadata else "False",
            "cropped_files": [main_file],
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
                metadata[key]["cropped_files"].append(lowres_file)
    return metadata


def uri_to_url(uri):
    if uri.startswith("s3://"):
        parts = uri.split("/")
        return "https://{}.s3.eu-central-1.amazonaws.com/{}".format(parts[2], "/".join(parts[3:]))
    return "file://" + os.path.abspath(uri)


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
        except Exception:
            logger.exception("Failed to check for same day image with more pixels")
    return latest


RCLONE_PERF_FLAGS = [
    "--transfers", "16",
    "--checkers", "32",
    "--fast-list",
    "--s3-no-check-bucket",
    "--s3-upload-concurrency", "8",
]


def rclone_sync(remote, local_dir, dry_run=False, extension="*.tif"):
    """
    Compare files between the local directory and the remote, and return three lists:
    - Added: Files that are in remote but not in local.
    - Modified: Files that differ in local and remote.
    - Removed: Files that are in local but not in remote.
    """
    os.makedirs(local_dir, exist_ok=True)
    command = ["rclone", "sync", remote, local_dir, "--include", extension] + RCLONE_PERF_FLAGS
    if dry_run:
        command.append("--dry-run")

    logger.debug("rclone %s", " ".join(command))
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

        logger.debug("rclone dry-run: %d added, %d removed", len(added_files), len(removed_files))
        return added_files, removed_files
    else:
        return


def rclone_copy_files(src_root, dst_root, rel_paths):
    """
    Copy a known list of files between src_root and dst_root using --files-from
    and --no-traverse, so rclone never lists the destination tree.
    """
    if not rel_paths:
        return
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
        f.write("\n".join(rel_paths))
        list_path = f.name
    try:
        command = [
            "rclone", "copy", src_root, dst_root,
            "--files-from", list_path,
            "--no-traverse",
        ] + RCLONE_PERF_FLAGS
        logger.debug("rclone %s", " ".join(command))
        subprocess.run(command, check=True)
    finally:
        os.remove(list_path)
