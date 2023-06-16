import time
import os
import shutil
import csv


import ee
import rasterstats
import fiona

from . import mosaic_rasters
from . import google_cloud

try:
	ee.Initialize()
except:  # not sure what error it raises right now
	ee.Authenticate()
	ee.Initialize()


DEFAULTS = dict(
	CRS='EPSG:4326',
	TILE_SIZE=12800,
	EXPORT_FOLDER="ee_exports"

)

def _get_fiona_args(polygon_path):
	"""
		A simple utility that detects if, maybe, we're dealing with an Esri File Geodatabase. This is the wrong way
		to do this, but it'll work in many situations
	:param polygon_path:
	:return:
	"""

	parts = os.path.split(polygon_path)
	if (parts[0].endswith(".gdb") or parts[0].endswith(".gpkg")) and "." not in parts[1]:  # if the folder name ends with .gdb and the "filename" doesn't have an extension, assume it's an FGDB
		return {'fp': parts[0], 'layer': parts[1]}
	else:
		return {'fp': polygon_path}


def download_images_in_folder(source_location, download_location, prefix):
	"""
		Handles pulling data from Google Drive over to a local location, filtering by a filename prefix and folder
	:param source_location:
	:param download_location:
	:param prefix:
	:return:
	"""
	folder_search_path = source_location
	files = [filename for filename in os.listdir(folder_search_path) if filename.startswith(prefix)]

	os.makedirs(download_location, exist_ok=True)

	for filename in files:
		shutil.move(os.path.join(folder_search_path, filename), os.path.join(download_location, filename))



class TaskRegistry(object):
	INCOMPLETE_STATUSES = ("READY", "UNSUBMITTED", "RUNNING")
	COMPLETE_STATUSES = ["COMPLETED"]
	FAILED_STATUSES = ["CANCEL_REQUESTED", "CANCELLED", "FAILED"]

	def __init__(self):
		self.images = []
		self.callback = None

	def add(self, image):
		self.images.append(image)

	@property
	def incomplete_tasks(self):
		initial_tasks = [image for image in self.images if image._last_task_status['state'] in self.INCOMPLETE_STATUSES]
		for image in initial_tasks:  # update anything that's currently running or waiting first
			image._check_task_status()

		return [image for image in self.images if image._last_task_status['state'] in self.INCOMPLETE_STATUSES]

	@property
	def complete_tasks(self):
		return [image for image in self.images if image._last_task_status['state'] in self.COMPLETE_STATUSES + self.FAILED_STATUSES]

	@property
	def downloadable_tasks(self):
		return [image for image in self.complete_tasks if image.task_data_downloaded is False and image._last_task_status['state'] not in self.FAILED_STATUSES]

	def download_ready_images(self, download_location):
		for image in self.downloadable_tasks:
			print(f"{image.filename} is ready for download")
			image.download_results(download_location=download_location, callback=self.callback)

	def wait_for_images(self, download_location, sleep_time=10, callback=None, try_again_disk_full=True):
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

class Image(object):
	def __init__(self, drive_root_folder=r"G:\My Drive"):
		for key in DEFAULTS:  # set the defaults here
			setattr(self, key.lower(), DEFAULTS[key])

		self._last_task_status = {"state": "UNSUBMITTED"}
		self.task_data_downloaded = False
		self.export_type = "Drive"  # other option is "Cloud"

		self.drive_root_folder = drive_root_folder

		self.filename_description = ""

	def _set_names(self, filename_prefix=""):
		self.description = f"{self.pixel_reducer}ET_{self.year}-{self.start_date}--{self.end_date}_{filename_prefix}"
		self.filename = f"{self.pixel_reducer}_et_{self.year}-{self.start_date}--{self.end_date}_{self.filename_description}_{filename_prefix}"

	def export(self, image, filename_prefix="", export_type="Drive", clip=None, **export_kwargs):
		self.results = image

		self._set_names(filename_prefix)

		if clip:  # clip must be a geometry or feature in Earth Engine.
			self.results.clip(clip)

		ee_kwargs = {
			'description': self.description,
			'fileNamePrefix': self.filename,
			'scale': 30,
			'maxPixels': 1e12,
			'fileDimensions': self.tile_size,  # multiple of shardSize default 256. Should split into about 9 tiles
			'crs': self.crs
		}
		ee_kwargs.update(export_kwargs)  # override any of these defaults with anything else provided

		if export_type == "Drive":
			if "folder" not in ee_kwargs:
				ee_kwargs['folder'] = self.export_folder

			self.task = ee.batch.Export.image.toDrive(self.results, **ee_kwargs)
			self.task.start()
		elif export_type == "Cloud":
			ee_kwargs['fileNamePrefix'] = f"{self.export_folder}/{ee_kwargs['fileNamePrefix']}"  # add the folder to the filename here for Google Cloud
			self.bucket = ee_kwargs['bucket']
			self.task = ee.batch.Export.image.toCloudStorage(self.results, **ee_kwargs)
			self.task.start()

		self.export_type = export_type

		main_task_registry.add(self)

	def download_results(self, download_location, callback=None):
		"""

		:return:
		"""
		# need an event loop that checks self.task.status(), which
		# will get the current state of the task

		# state options
		# == "CANCELLED", "CANCEL_REQUESTED", "COMPLETED",
		# "FAILED", "READY", "SUBMITTED" (maybe - double check that - it might be that it waits with UNSUBMITTED),
		# "RUNNING", "UNSUBMITTED"

		folder_search_path = os.path.join(self.drive_root_folder, self.export_folder)
		self.output_folder = os.path.join(download_location, self.export_folder)

		if self.export_type == "Drive":
			download_images_in_folder(folder_search_path, self.output_folder, prefix=self.filename)
		elif self.export_type == "Cloud":
			google_cloud.download_public_export(self.bucket, self.output_folder, f"{self.export_folder}/{self.filename}")
		else:
			raise ValueError("Unknown export_type (not one of 'Drive', 'Cloud') - can't download")

		self.task_data_downloaded = True

		if callback:
			callback_func = getattr(self, callback)
			callback_func()

	def mosaic(self):
		self.mosaic_image = os.path.join(self.output_folder, f"{self.filename}_mosaic.tif")
		mosaic_rasters.mosaic_folder(self.output_folder, self.mosaic_image, prefix=self.filename)

	def zonal_stats(self, polygons, keep_fields=("UniqueID", "CLASS2"),
					stats=('min', 'max', 'mean', 'median', 'std', 'count', 'percentile_10', 'percentile_90'),
					report_threshold=1000,
					write_batch_size=2000):
		"""

		:param polygons:
		:param keep_fields:
		:param stats:
		:param report_threshold: After how many iterations should it print out the feature number it's on. Defaults to 1000. Set to None to disable
		:param write_batch_size: How many zones should we store up before writing to the disk?
		:return:
		"""
		# note use of gen_zonal_stats, which uses a generator. That should mean that until we coerce it to a list on the next line,
		# each item isn't evaluated, which should prevent us from needing to store a geojson representation of all of the polygons
		# at one time since we'll strip it off (it'd be reallllly bad to try to keep all of it

		# A silly hack to get fiona to open GDB data by splitting it only if the input is a gdb data item, then providing anything else as kwargs. But fiona requires the main item to be an arg, not a kwarg
		kwargs = _get_fiona_args(polygons)
		main_file_path = kwargs['fp']
		del kwargs['fp']

		with fiona.open(main_file_path, **kwargs) as polys_open:

			zstats_results_geo = rasterstats.gen_zonal_stats(polys_open, self.mosaic_image, stats=stats, geojson_out=True, nodata=-9999)

			fieldnames = stats + keep_fields

			# here's a first approach that still stores a lot in memory - it's commented out because we're instead
			# going to just generate them one by one and write them to a file directly.
			#
			# ok, so this next line is doing a lot of work. It's a dictionary comprehension inside a list comprehension -
			# we're going through each item in the results, then accessing just the properties key and constructing a new
			# dictionary just for the keys we want to keep - the keep fields (the key and a class field by defaiult) and the stats fields
			#zstats_results = [{key: poly['properties'][key] for key in fieldnames} for poly in zstats_results_geo]

			i = 0
			with open(os.path.join(self.output_folder, f"{self.filename}_zstats.csv"), 'w', newline='') as csv_file:
				writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
				writer.writeheader()
				results = []
				for poly in zstats_results_geo:  # get the result for the polygon, then filter the keys with the dictionary comprehension below
					result = {key: poly['properties'][key] for key in fieldnames}

					for key in result:  # truncate the floats
						if type(result[key]) is float:
							result[key] = f"{result[key]:.5f}"

					i += 1
					results.append(result)
					if i % write_batch_size == 0:
						writer.writerows(results)  # then write the lone result out one at a time to not store it all in RAM
						results = []

					if report_threshold and i % report_threshold == 0:
						print(i)

	def _check_task_status(self):
		new_status = self.task.status()

		changed = False
		if self._last_task_status != new_status:
			changed = True
			self._last_task_status = new_status

		return {'status': self._last_task_status, 'changed': changed}

