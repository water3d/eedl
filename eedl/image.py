import csv
import os
import shutil
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import ee
import fiona
import rasterstats
from ee import EEException

from . import google_cloud
from . import mosaic_rasters



DEFAULTS = dict(
	CRS='EPSG:4326',
	TILE_SIZE=12800,
	EXPORT_FOLDER="ee_exports"

)


def _get_fiona_args(polygon_path: Union[str, Path]) -> Dict[str, Union[str, Path]]:
	"""
	A simple utility that detects if, maybe, we're dealing with an Esri File Geodatabase. This is the wrong way
	to do this, but it'll work in many situations
	:param polygon_path:
	:return:
	"""

	parts = os.path.split(polygon_path)
	# if the folder name ends with .gdb and the "filename" doesn't have an extension, assume it's an FGDB
	if (parts[0].endswith(".gdb") or parts[0].endswith(".gpkg")) and "." not in parts[1]:
		return {'fp': parts[0], 'layer': parts[1]}
	else:
		return {'fp': polygon_path}


def download_images_in_folder(source_location: Union[str, Path], download_location: Union[str, Path], prefix: str) -> None:
	"""
	Handles pulling data from Google Drive over to a local location, filtering by a filename prefix and folder
	:param source_location:
	:param download_location:
	:param prefix:
	:return:
	"""
	folder_search_path: Union[str, Path] = source_location
	files = [filename for filename in os.listdir(folder_search_path) if filename.startswith(prefix)]

	os.makedirs(download_location, exist_ok=True)

	for filename in files:
		shutil.move(os.path.join(folder_search_path, filename), os.path.join(download_location, filename))


class TaskRegistry:
	INCOMPLETE_STATUSES = ("READY", "UNSUBMITTED", "RUNNING")
	COMPLETE_STATUSES = ["COMPLETED"]
	FAILED_STATUSES = ["CANCEL_REQUESTED", "CANCELLED", "FAILED"]

	def __init__(self) -> None:
		self.images: List[Image] = []
		self.callback: Optional[str] = None

	def add(self, image) -> None:
		self.images.append(image)

	@property
	def incomplete_tasks(self) -> List[ee.image.Image]:
		initial_tasks = [image for image in self.images if image._last_task_status['state'] in self.INCOMPLETE_STATUSES]
		for image in initial_tasks:  # update anything that's currently running or waiting first
			image._check_task_status()

		return [image for image in self.images if image._last_task_status['state'] in self.INCOMPLETE_STATUSES]

	@property
	def complete_tasks(self) -> List[ee.image.Image]:
		return [image for image in self.images if image._last_task_status['state'] in self.COMPLETE_STATUSES + self.FAILED_STATUSES]

	@property
	def downloadable_tasks(self) -> List[ee.image.Image]:
		return [image for image in self.complete_tasks if image.task_data_downloaded is False and image._last_task_status['state'] not in self.FAILED_STATUSES]

	def download_ready_images(self, download_location: Union[str, Path]) -> None:
		for image in self.downloadable_tasks:
			print(f"{image.filename} is ready for download")
			image.download_results(download_location=download_location, callback=self.callback)

	def wait_for_images(self,
						download_location: Union[str, Path],
						sleep_time: int = 10,
						callback: Optional[str] = None,
						try_again_disk_full: bool = True) -> None:

		self.callback = callback
		while len(self.incomplete_tasks) > 0 or len(self.downloadable_tasks) > 0:
			try:
				self.download_ready_images(download_location)
			except OSError:
				if try_again_disk_full:
					print("OSError reported. Disk may be full - will try again - clear space")
					pass
				else:
					raise

			time.sleep(sleep_time)


main_task_registry = TaskRegistry()


class Image:
	"""
	The main class that does all the work. Any use of this package should instantiate this class for each export
	the user wants to do. As we refine this, we may be able to provide just a single function in this module named
	"export" or something of that sort for people who don't need access to control class behavior. That will likely
	follow all the other enhancements, like converting the exports into async code.

	The class has no required arguments as of 6/16/2023, but that may change. Any arguments provided get applied
	directly to the class and override any defaults. Options include:

	:param crs: Coordinate Reference System to use for exports in a format Earth Engine understands, such as "EPSG:3310"
	:param tile_size: the number of pixels per side of tiles to export
	:param export_folder: the name of the folder in the chosen export location that will be created for the export

	This docstring needs to be checked to ensure it's in a standard format that Sphinx will render
	"""

	def __init__(self, drive_root_folder: Union[str, Path], **kwargs) -> None:
		# TODO: We shouldn't define a default drive root folder. This should always be provided by the user,
		#  but we need to figure out where in the workflow this happens.

		# Check if the path is valid before we do anything else

		if not os.path.exists(drive_root_folder):
			raise NotADirectoryError("The provided path is not a valid directory")

		self.crs: Optional[str] = None
		self.tile_size: Optional[int] = None
		self.export_folder: Optional[Union[str, Path]] = None
		self.mosaic_image: Optional[str] = None
		self.task: Optional[ee.batch.Task] = None
		self.bucket: Optional[str] = None
		self._ee_image: Optional[ee.image.Image] = None
		self.output_folder: Optional[Union[str, Path]] = None

		# set the defaults here - this is a nice strategy where we get to define constants near the top that aren't buried in code, then apply them here
		for key in DEFAULTS:
			setattr(self, key.lower(), DEFAULTS[key])

		for key in kwargs:  # now apply any provided keyword arguments over the top of the defaults.
			setattr(self, key, kwargs[key])

		self._last_task_status = {"state": "UNSUBMITTED"}
		# this will be the default status initially, so always assume it's UNSUBMITTED if we haven't gotten anything
		# from the server. "None" would work too, but then we couldn't just check the status
		self.task_data_downloaded = False
		self.export_type = "Drive"  # other option is "Cloud"

		self.drive_root_folder = drive_root_folder

		self.filename_description = ""

	def _set_names(self, filename_prefix: str = "") -> None:
		self.description = filename_prefix
		self.filename = f"{self.filename_description}_{filename_prefix}"

	@staticmethod
	def _initialize() -> None:
		try:
			ee.Initialize()
		except EEException:
			ee.Authenticate()
			ee.Initialize()

	def export(self,
				image: ee.image.Image,
				filename_prefix: str,
				export_type: str = "Drive",
				clip: Optional[ee.geometry.Geometry] = None,
				**export_kwargs) -> None:

		# If image does not have a clip attribute, the error message is not very helpful. This allows for a custom error message:
		if not isinstance(image, ee.image.Image):
			raise ValueError("Invalid image provided for export")

		self._initialize()

		self._ee_image = image

		self._set_names(filename_prefix)

		ee_kwargs = {
			'description': self.description,
			'fileNamePrefix': self.filename,
			'scale': 30,
			'maxPixels': 1e12,
			# multiple of shardSize default 256. Should split into about 9 tiles
			'fileDimensions': self.tile_size,
			'crs': self.crs
		}

		# Get a silent error if clip is not of type ee.geometry.Geometry
		if isinstance(clip, ee.geometry.Geometry):
			ee_kwargs["region"] = self._ee_image.clip(clip)
		elif clip:
			raise ValueError("Invalid geometry provided for export")

		# override any of these defaults with anything else provided
		ee_kwargs.update(export_kwargs)

		if export_type.lower() == "drive":
			if "folder" not in ee_kwargs:
				ee_kwargs['folder'] = self.export_folder
			self.task = ee.batch.Export.image.toDrive(self._ee_image, **ee_kwargs)
		elif export_type.lower() == "cloud":
			# add the folder to the filename here for Google Cloud
			ee_kwargs['fileNamePrefix'] = f"{self.export_folder}/{ee_kwargs['fileNamePrefix']}"
			self.bucket = str(ee_kwargs['bucket'])
			self.task = ee.batch.Export.image.toCloudStorage(self._ee_image, **ee_kwargs)

		# export_type is not valid
		else:
			raise ValueError("Invalid value for export_type. Did you mean drive or cloud?")

		self.task.start()

		self.export_type = export_type

		main_task_registry.add(self)

	def download_results(self, download_location: Union[str, Path], callback: Optional[str] = None) -> None:
		"""

		:return:
		"""
		# need an event loop that checks self.task.status(), which
		# will get the current state of the task

		# state options
		# == "CANCELLED", "CANCEL_REQUESTED", "COMPLETED",
		# "FAILED", "READY", "SUBMITTED" (maybe - double check that - it might be that it waits with UNSUBMITTED),
		# "RUNNING", "UNSUBMITTED"

		folder_search_path = os.path.join(str(self.drive_root_folder), str(self.export_folder))
		self.output_folder = os.path.join(str(download_location), str(self.export_folder))
		if self.export_type.lower() == "drive":
			download_images_in_folder(folder_search_path, self.output_folder, prefix=self.filename)

		elif self.export_type.lower() == "cloud":
			google_cloud.download_public_export(str(self.bucket), self.output_folder, f"{self.export_folder}/{self.filename}")

		else:
			raise ValueError("Unknown export_type (not one of 'drive', 'cloud') - can't download")

		self.task_data_downloaded = True

		if callback:
			callback_func = getattr(self, callback)
			callback_func()

	def mosaic(self) -> None:
		self.mosaic_image = os.path.join(str(self.output_folder), f"{self.filename}_mosaic.tif")
		mosaic_rasters.mosaic_folder(str(self.output_folder), self.mosaic_image, prefix=self.filename)

	def zonal_stats(self,
					polygons: Union[str, Path],
					keep_fields: Tuple = ("UniqueID", "CLASS2"),
					stats: Tuple = ('min', 'max', 'mean', 'median', 'std', 'count', 'percentile_10', 'percentile_90'),
					report_threshold: int = 1000,
					write_batch_size: int = 2000) -> None:
		"""

		:param polygons:
		:param keep_fields:
		:param stats:
		:param report_threshold: After how many iterations should it print out the feature number it's on. Defaults to 1000.
		Set to None to disable
		:param write_batch_size: How many zones should we store up before writing to the disk?
		:return:

		"""
		# note use of gen_zonal_stats, which uses a generator. That should mean that until we coerce it to a list on
		# the next line, each item isn't evaluated, which should prevent us from needing to store a geojson
		# representation of all the polygons at one time since we'll strip it off (it'd be reallllly bad to try to
		# keep all of it

		# A silly hack to get fiona to open GDB data by splitting it only if the input is a gdb data item,
		# then providing anything else as kwargs. But fiona requires the main item to be an arg, not a kwarg
		kwargs = _get_fiona_args(polygons)
		main_file_path = kwargs['fp']
		del kwargs['fp']

		with fiona.open(main_file_path, **kwargs) as polys_open:

			zstats_results_geo = rasterstats.gen_zonal_stats(polys_open, self.mosaic_image, stats=stats, geojson_out=True, nodata=-9999)

			fieldnames = stats + keep_fields

			# here's a first approach that still stores a lot in memory - it's commented out because we're instead
			# going to just generate them one by one and write them to a file directly.
			#
			# ok, so this next line is doing a lot of work. It's a dictionary comprehension inside a list
			# comprehension - we're going through each item in the results, then accessing just the properties key
			# and constructing a new dictionary just for the keys we want to keep - the keep fields (the key and a
			# class field by default) and the stats fields zstats_results = [{key: poly['properties'][key] for key
			# in fieldnames} for poly in zstats_results_geo]

			i = 0
			with open(os.path.join(str(self.output_folder), f"{self.filename}_zstats.csv"), 'w', newline='') as csv_file:
				writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
				writer.writeheader()
				results = []
				# get the result for the polygon, then filter the keys with the dictionary comprehension below
				for poly in zstats_results_geo:
					result = {key: poly['properties'][key] for key in fieldnames}

					for key in result:  # truncate the floats
						if type(result[key]) is float:
							result[key] = f"{result[key]:.5f}"

					i += 1
					results.append(result)
					if i % write_batch_size == 0:
						# then write the lone result out one at a time to not store it all in RAM
						writer.writerows(results)
						results = []

					if report_threshold and i % report_threshold == 0:
						print(i)

	def _check_task_status(self) -> Dict[str, Union[Dict[str, str], bool]]:

		if self.task is None:
			raise ValueError('Error checking task status. Task is None. It likely means that the export task was not'
							 ' properly created and the code needs to be re-run.')

		new_status = self.task.status()

		changed = False
		if self._last_task_status != new_status:
			changed = True
			self._last_task_status = new_status

		return {'status': self._last_task_status, 'changed': changed}
