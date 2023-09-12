import os
import shutil
import tempfile
from pathlib import Path
from typing import Sequence, Union

from osgeo import gdal


def mosaic_folder(folder_path: Union[str, Path], output_path: Union[str, Path], prefix: str = "") -> None:
	"""

	:param folder_path: Location of the folder
	:type folder_path: Union[str, Path]
	:param output_path: Output destination
	:type output_path: Union[str, Path]
	:param prefix: Used to find the files of interest.
	:type prefix: Str
	:return: None
	"""
	tifs = [os.path.join(folder_path, filename) for filename in os.listdir(folder_path) if filename.endswith(".tif") and filename.startswith(prefix)]

	if len(tifs) == 1:  # if we only got one image back, don't both mosaicking, though this will also skip generating overviews.
		shutil.move(tifs[0], output_path)  # just move the output image to the "mosaic" name, then return
		return

	mosaic_rasters(tifs, output_path)


def mosaic_rasters(raster_paths: Sequence[Union[str, Path]],
					output_path: Union[str, Path],
					add_overviews: bool = True) -> None:
	"""
	Adapted from https://gis.stackexchange.com/a/314580/1955 and
	https://www.gislite.com/tutorial/k8024 along with other basic lookups on GDAL Python bindings

	:param raster_paths: Location of the raster
	:type raster_paths: Sequence[Union[str, Path]]
	:param output_path: Output destination
	:type output_path: Union[str, Path]
	:param add_overviews:
	:type add_overviews: Bool
	:return: None
	"""

	# gdal.SetConfigOption("GTIFF_SRC_SOURCE", "GEOKEYS")
	vrt_path = tempfile.mktemp(suffix=".vrt", prefix="mosaic_rasters_")

	vrt_options = gdal.BuildVRTOptions(resampleAlg='nearest', resolution="highest")
	my_vrt = gdal.BuildVRT(vrt_path, raster_paths, options=vrt_options)
	# my_vrt = None
	my_vrt.FlushCache()  # write the VRT out
	print(f"VRT at {vrt_path}")

	# now let's export it to the output_path as a geotiff
	driver = gdal.GetDriverByName("GTIFF")  # we'll use VRT driver.CreateCopy
	vrt_data = gdal.Open(vrt_path)
	output = driver.CreateCopy(output_path, vrt_data, 0, ["COMPRESS=DEFLATE", ])
	output.FlushCache()
	print("GeoTIFF Output")

	if add_overviews:
		dataset = gdal.Open(output_path)
		gdal.SetConfigOption("COMPRESS_OVERVIEW", "DEFLATE")
		dataset.BuildOverviews(overviewlist=[2, 4, 8, 16, 32, 64, 128])

	print("Overviews Generated")
