import time
import os
import shutil
import csv


import ee
import openet.ssebop as ssebop
import rasterstats
import fiona

from . import mosaic_rasters

try:
	ee.Initialize()
except:  # not sure what error it raises right now
	ee.Authenticate()
	ee.Initialize()


DEFAULTS = dict(
	ET_REFERENCE_SOURCE='projects/climate-engine/cimis/daily',
	ET_REFERENCE_BAND='ETr_ASCE',
	ET_REFERENCE_FACTOR=1.0,
	ET_REFERENCE_RESAMPLE='nearest',
	ET_REFERENCE_DATE_TYPE='daily',
	INTERP_DAYS=32,
	# Interpolation method - currently only LINEAR is supported
	INTERP_METHOD='LINEAR',
	CLOUD_COVER=50,
	ET_PALETTE=[
		'DEC29B', 'E6CDA1', 'EDD9A6', 'F5E4A9', 'FFF4AD', 'C3E683', '6BCC5C',
		'3BB369', '20998F', '1C8691', '16678A', '114982', '0B2C7A'],
	CRS='EPSG:3310',
	# STUDY_AREA = ee.Geometry({"type": "Polygon", "coordinates":[ [ [ -121.836, 36.8706 ], [ -121.824151, 36.912247 ], [ -121.815, 36.9452 ], [ -121.814784, 36.945169 ], [ -121.489582, 38.08815 ], [ -122.973, 38.3039 ], [ -122.951223, 38.378287 ], [ -122.616922, 39.520217 ], [ -124.098837, 39.735876 ], [ -124.099, 39.7359 ], [ -124.093234, 39.754971 ], [ -124.076528, 39.810231 ], [ -123.666485, 41.166525 ], [ -123.650629, 41.218972 ], [ -123.644, 41.2409 ], [ -123.643785, 41.240869 ], [ -121.485, 40.9267 ], [ -121.502496, 40.874376 ], [ -121.889549, 39.716847 ], [ -120.416212, 39.502531 ], [ -120.416, 39.5025 ], [ -120.806849, 38.291111 ], [ -119.332, 38.0766 ], [ -119.355163, 38.002424 ], [ -119.7107, 36.863872 ], [ -118.233, 36.649 ], [ -118.239331, 36.628081 ], [ -118.684336, 35.157609 ], [ -118.688, 35.1455 ], [ -120.167913, 35.360572 ], [ -120.229336, 35.157609 ], [ -120.233, 35.1455 ], [ -122.233187, 35.436182 ], [ -122.234, 35.4363 ], [ -121.835726, 36.87056 ], [ -121.836, 36.8706 ] ] ]}),
	STUDY_AREA=ee.FeatureCollection("projects/ucm-fallow-training/assets/central_valley_alluvial_boundary").geometry(),
	TILE_SIZE=12800,
	COLLECTIONS=['LANDSAT/LC08/C02/T1_L2'],
	PIXEL_REDUCER="max",
	EXPORT_FOLDER="et_exports_sseboper"

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


class TaskRegistry(object):
	INCOMPLETE_STATUSES = ("READY", "UNSUBMITTED", "RUNNING")
	COMPLETE_STATUSES = ["COMPLETED"]
	FAILED_STATUSES = ["CANCEL_REQUESTED", "CANCELLED", "FAILED"]

	def __init__(self):
		self.images = []

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
			image.download_results(download_location=download_location)

	def wait_for_images(self, download_location, sleep_time=10):
		while len(self.incomplete_tasks) > 0 or len(self.downloadable_tasks) > 0:
			self.download_ready_images(download_location)
			time.sleep(sleep_time)


main_task_registry = TaskRegistry()

class SSEBOPer(object):
	def __init__(self, drive_root_folder=r"G:\My Drive"):
		for key in DEFAULTS:  # set the defaults here
			setattr(self, key.lower(), DEFAULTS[key])

		self._last_task_status = {"state": "UNSUBMITTED"}
		self.task_data_downloaded = False

		self.drive_root_folder = drive_root_folder

		self.filename_description = ""

	def run(self, year, start, end):
		self.year = year
		self.start_date = f'{year}-{start}'
		self.end_date = f'{year}-{end}'

		self.study_region = self.study_area.bounds(1, 'EPSG:4326').coordinates().getInfo()

		self.results = self._run_ssebop(self.start_date, self.end_date, self.pixel_reducer)

	def _run_ssebop(self, start_date, end_date, pixel_reducer):
		model_object = ssebop.Collection(collections=self.collections,
										 et_reference_source=self.et_reference_source,
										 et_reference_band=self.et_reference_band,
										 et_reference_factor=self.et_reference_factor,
										 et_reference_resample=self.et_reference_resample,
										 et_reference_date_type=self.et_reference_date_type,
										 start_date=start_date,
										 end_date=end_date,
										 cloud_cover_max=self.cloud_cover,
										 geometry=self.study_area)
		# get the computed ET for the overpass dates only
		overpass_date_collection = model_object.overpass(variables=['et', 'et_reference', 'et_fraction'])
		# now reduce it to a single image for the time period of interest by taking the pixel mean for overlapping data
		# this will be a bit backward because we want to be able to select a different reducer later
		et_collection = overpass_date_collection.select(['et'])
		reducer = getattr(et_collection, pixel_reducer)  # get the function for the reducer
		return reducer()  # call the reducer

	def comparison(self, year, time1_start, time1_end, time2_start, time2_end, method="divide"):
		start_date1 = f'{year}-{time1_start}'
		end_date1 = f'{year}-{time1_end}'
		start_date2 = f'{year}-{time2_start}'
		end_date2 = f'{year}-{time2_end}'

		self.year = year
		self.start_date = start_date1
		self.end_date = end_date2

		image1 = self._run_ssebop(start_date1, end_date1, self.pixel_reducer)
		image2 = self._run_ssebop(start_date2, end_date2, self.pixel_reducer)

		# get the comparison function - e.g. image.subtract, image.divide
		compare_func = getattr(image2, method)
		output = compare_func(image1)

		self.filename_description = f"comparison_{method}"

		self.results = output

	def _set_names(self, filename_prefix=""):
		self.description = f"{self.pixel_reducer}ET_{self.year}-{self.start_date}--{self.end_date}_{filename_prefix}"
		self.filename = f"et_{self.year}-{self.start_date}--{self.end_date}_{self.filename_description}_{filename_prefix}"

	def export(self, filename_prefix=""):
		self._set_names(filename_prefix)
		self.task = ee.batch.Export.image.toDrive(self.results, **{
			'description': self.description,
			'folder': self.export_folder,
			'fileNamePrefix': self.filename,
			'scale': 30,
			'maxPixels': 1e12,
			'fileDimensions': self.tile_size,  # multiple of shardSize default 256. Should split into about 9 tiles
			'crs': self.crs
		})

		self.task.start()

		main_task_registry.add(self)

	def download_results(self, download_location):
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
		files = [filename for filename in os.listdir(folder_search_path) if filename.startswith(self.filename) ]

		self.output_folder = os.path.join(download_location, self.export_folder)
		os.makedirs(os.path.join(download_location, self.export_folder), exist_ok=True)

		for filename in files:
			print(filename)
			shutil.move(os.path.join(folder_search_path, filename), os.path.join(self.output_folder, filename))

		self.task_data_downloaded = True

	def mosaic(self):
		self.mosaic_image = os.path.join(self.output_folder, f"{self.filename}_mosaic.tif")
		mosaic_rasters.mosaic_folder(self.output_folder, self.mosaic_image)

	def zonal_stats(self, polygons, keep_fields=("UniqueID", "CLASS2"), stats=('min', 'max', 'mean', 'median', 'std', 'count', 'percentile_1', 'percentile_5', 'percentile_10', 'percentile_90', 'percentile_95', 'percentile_99')):
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

			with open(os.path.join(self.output_folder, f"{self.filename}_zstats.csv"), 'wb') as csv_file:
				writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
				writer.writeheader()
				for poly in zstats_results_geo:  # get the result for the polygon, then filter the keys with the dictionary comprehension below
					result = {key: poly['properties'][key] for key in fieldnames}
					writer.writerow(result)  # then write the lone result out one at a time to not store it all in RAM

	def _check_task_status(self):
		new_status = self.task.status()

		changed = False
		if self._last_task_status != new_status:
			changed = True
			self._last_task_status = new_status

		return {'status': self._last_task_status, 'changed': changed}

# for testing
if __name__ == "__main__":
	#tester = SSEBOPer()

	images = []

	#for year in (2020, 2021, 2022):
	#	for month in (("05-01", "05-31"), ("06-01", "06-30"), ("07-01", "07-31"), ("08-01", "08-31")):
	#		if not (year == 2022 and month[0] == "08-01"):  # skip august for now in 2022
	#			runner = SSEBOPer()
	#			runner.run(year, month[0], month[1])
	#			runner.export()
	#			images.append(runner)

	runner = SSEBOPer()
	runner.run(2022, "07-01", "07-31")
	runner.export()
	images.append(runner)

	main_task_registry.wait_for_images(download_location=r"D:\ET_Summers\ee_exports_monthly", sleep_time=60)

	for image in images:
		image.mosaic()
		#image.zonal_stats()

	#tester.export()
	#tester.download_results()