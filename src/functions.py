import os
import json
import requests
import subprocess
import numpy as np
from osgeo import gdal, ogr, osr

conda_env_path = os.environ.get("CONDA_PREFIX")
if conda_env_path:
    proj_data_path = os.path.join(conda_env_path, "share", "proj")
    os.environ["PROJ_DATA"] = proj_data_path


def download_file(url, save_path):
    """
    Downloads a file from a given URL and saves it to the specified path.

    Args:
        url (str): The URL of the file to download.
        save_path (str): The local path where the file should be saved.
    """
    # Send a GET request to the URL
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Open the file in binary write mode and save the content
        with open(save_path, "wb") as file:
            file.write(response.content)
        print(f"File downloaded successfully to {save_path}")
    else:
        print(f"Failed to download file. Status code: {response.status_code}")


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

    if raster.RasterCount == 2:
        band = raster.GetRasterBand(1).ReadAsArray()
        mask = raster.GetRasterBand(2).ReadAsArray()
        band[mask == 1] = np.nan
    else:
        band = raster.GetRasterBand(1).ReadAsArray()

    for lake in geojson["features"]:
        key = lake["properties"]["key"]

        polygon_geometry = ogr.CreateGeometryFromJson(json.dumps(lake["geometry"]))
        min_x_pixel, min_y_pixel, max_x_pixel, max_y_pixel, min_x, min_y = pixel_coordinates(raster, polygon_geometry)

        if max_x_pixel < 0 or max_y_pixel < 0 or min_x_pixel > raster.RasterXSize or min_y_pixel > raster.RasterYSize:
            continue

        cropped_band = band[min_y_pixel:max_y_pixel, min_x_pixel:max_x_pixel]
        mask_geometry = polygon_raster_mask(raster, polygon_geometry)
        cropped_band[mask_geometry[min_y_pixel:max_y_pixel, min_x_pixel:max_x_pixel] != 1] = np.nan

        if np.isnan(cropped_band).all():
            continue

        print("Processing {}".format(key))
        os.makedirs(os.path.join(output_dir, key), exist_ok=True)
        name, extension = os.path.splitext(os.path.basename(input_file))
        temp_file = os.path.join(output_dir, key,  "{}_temp{}".format(name, extension))
        main_file = os.path.join(output_dir, key, "{}_{}{}".format(name, key, extension))
        lowres_file = os.path.join(output_dir, key, "{}_{}_lowres{}".format(name, key, extension))

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


def rclone_sync(remote, local_dir, dry_run=False):
    """
    Compare files between the local directory and the remote, and return three lists:
    - Added: Files that are in remote but not in local.
    - Modified: Files that differ in local and remote.
    - Removed: Files that are in local but not in remote.
    """
    os.makedirs(local_dir, exist_ok=True)
    command = [
        "rclone", "sync", remote, local_dir, "--include", "*.tif"
    ]
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