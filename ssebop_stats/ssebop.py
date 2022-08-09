import time

import ee
import openet.ssebop as ssebop

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
	PIXEL_REDUCER="mean",
	EXPORT_FOLDER="et_exports_sseboper"

)


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
		initial_tasks = [image for image in self.images if image._last_task_status in self.INCOMPLETE_STATUSES]
		for image in initial_tasks:  # update anything that's currently running or waiting first
			image.task._check_task_status()

		return [image for image in self.images if image._last_task_status in self.INCOMPLETE_STATUSES]

	@property
	def complete_tasks(self):
		return [image for image in self.images if image._last_task_status in self.COMPLETE_STATUSES + self.FAILED_STATUSES]

	@property
	def downloadable_tasks(self):
		return [image for image in self.complete_tasks if image.task_data_downloaded == False]

	def download_ready_images(self):
		for image in self.downloadable_tasks:
			print(f"{image.filename} is ready for download")
			image.download_results()

	def wait_for_images(self, sleep_time=10):
		while len(self.incomplete_tasks) > 0 or len(self.downloadable_tasks) > 0:
			self.download_ready_images()
			time.sleep(sleep_time)


main_task_registry = TaskRegistry()

class SSEBOPer(object):
	def __init__(self):
		for key in DEFAULTS:  # set the defaults here
			setattr(self, key.lower(), DEFAULTS[key])

		self._last_task_status = "UNSUBMITTED"
		self.task_data_downloaded = False

	def run(self, year, start, end):
		self.year = year
		self.start_date = f'{year}-{start}'
		self.end_date = f'{year}-{end}'

		self.study_region = self.study_area.bounds(1, 'EPSG:4326').coordinates().getInfo()

		model_object = ssebop.Collection(collections=self.collections,
										 et_reference_source=self.et_reference_source,
										 et_reference_band=self.et_reference_band,
										 et_reference_factor=self.et_reference_factor,
										 et_reference_resample=self.et_reference_resample,
										 et_reference_date_type=self.et_reference_date_type,
										 start_date=self.start_date,
										 end_date=self.end_date,
										 cloud_cover_max=self.cloud_cover,
										 geometry=self.study_area)

		# get the computed ET for the overpass dates only
		overpass_date_collection = model_object.overpass(variables=['et', 'et_reference', 'et_fraction'])

		# now reduce it to a single image for the time period of interest by taking the pixel mean for overlapping data
		# this will be a bit backward because we want to be able to select a different reducer later
		et_collection = overpass_date_collection.select(['et'])
		reducer = getattr(et_collection, self.pixel_reducer)  # get the function for the reducer
		self.results = reducer()  # call the reducer

	def export(self, filename_prefix=""):
		description = f"{self.pixel_reducer}ET_{self.year}-{self.start_date}--{self.end_date}_{filename_prefix}"
		self.filename = f"et_{self.year}-{self.start_date}--{self.end_date}_{filename_prefix}"
		self.task = ee.batch.Export.image.toDrive(self.results, **{
			'description': description,
			'folder': self.export_folder,
			'fileNamePrefix': self.filename,
			'scale': 30,
			'maxPixels': 1e12,
			'fileDimensions': self.tile_size,  # multiple of shardSize default 256. Should split into about 9 tiles
			'crs': self.crs
		})

		self.task.start()

		main_task_registry.add(self)

	def download_results(self, drive_path="G:\My Drive"):

		# need an event loop that checks self.task.status(), which
		# will get the current state of the task

		# state options
		# == "CANCELLED", "CANCEL_REQUESTED", "COMPLETED",
		# "FAILED", "READY", "SUBMITTED" (maybe - double check that - it might be that it waits with UNSUBMITTED),
		# "RUNNING", "UNSUBMITTED"
		print(self.task)

		self.task_data_downloaded = True

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

	for year in (2020, 2021, 2022):
		for month in (("05-01", "05-31"), ("06-01", "06-30"), ("07-01", "07-31"), ("08-01", "08-31")):
			if not (year == 2022 and month[0] == "08-01"):  # skip august for now in 2022
				runner = SSEBOPer()
				runner.run(year, month[0], month[1])
				runner.export()

	main_task_registry.wait_for_images(60)

	#tester.export()
	#tester.download_results()