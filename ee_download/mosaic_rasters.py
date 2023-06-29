import os
import tempfile
from collections.abc import Iterable
from pathlib import Path
from typing import Union

from osgeo import gdal


def mosaic_folder(folder_path: Union[str, Path], output_path: Union[str, Path], prefix: str = "") -> None:
	tifs = [os.path.join(folder_path, filename) for filename in os.listdir(folder_path) if
			filename.endswith(".tif") and filename.startswith(prefix)]
	mosaic_rasters(tifs, output_path)


def mosaic_rasters(raster_paths: Iterable[Union[str, Path]], output_path: Union[str, Path], add_overviews: bool = True) -> None:
	"""
		Adapted from https://gis.stackexchange.com/a/314580/1955 and
		https://www.gislite.com/tutorial/k8024 along with other basic lookups on GDAL Python bindings
	:param raster_paths:
	:param output_path:
	:param add_overviews:
	:return:
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
