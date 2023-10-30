import os
import itertools
import datetime

from .core import safe_fiona_open
from .image import EEDLImage, TaskRegistry

import ee
from ee import ImageCollection


class GroupedCollectionExtractor():

	def __init__(self, **kwargs):
		self.keep_image_objects = False  # Whether to store the EEDLImage objects on this class, so they can be accessed when it's done. We don't just to not use the RAM on large exports.
		self.all_images = []  # All the exported images are saved here. They can then be operated on once the extractor is complete.
		self.skip_existing = True  # A feature allowing it to resume from crashes. If the mosaic image exists, it skips doing any processing on the rest of it.
		self.on_error = "log"
		self.filename_description = ""

		self.collection = None
		self.collection_band = None
		self.time_start = None
		self.time_end = None
		self.mosaic_by_date = True
		self.areas_of_interest_path = None  # The path to a spatial data file readable by Fiona/GEOS that has features defining AOIs to extract individually.

		self.strict_clip = True  # May be necessary for some things to behave, so keeping this as a default to True. People can disable if they know what they're doing (maybe faster).
		self.export_type = "drive"
		self.drive_root_folder = None
		self.cloud_bucket = None
		self.download_folder = None  # Local folder name after downloading for processing.
		self.export_folder = None  # Drive/cloud export folder name.

		self.zonal_run = True
		self.zonal_areas_of_interest_attr = None  # What is the attribute on each of the AOI polygons that tells us what items to use in the zonal extraction.
		self.zonal_features_path = None  # What polygons to use as inputs for zonal stats.
		self.zonal_features_area_of_interest_attr = None  # What field in the zonal features has the value that should match zonal_areas_of_interest_attr?
		self.zonal_features_preserve_fields = None  # What fields to preserve, as a tuple - typically an ID and anything else you want.
		self.zonal_stats_to_calc = ()  # What statistics to output by zonal feature.
		self.zonal_use_points = False
		self.zonal_inject_date: bool = False
		self.zonal_inject_group_id: bool = False
		self.zonal_nodata_value: int = 0

		self.merge_sqlite = True  # Should we merge all outputs to a single SQLite database.
		self.merge_grouped_csv = True  # Should we merge CSV by grouped item.
		self.merge_final_csv = False  # Should we merge all output tables.

		self._all_outputs = list()  # For storing the paths to all output csv files.

		self.max_fiona_features_load = 1000  # Threshold where we switch from keeping fiona features in memory as a list to using itertools.tee to split the iterator.

		for kwarg in kwargs:
			setattr(self, kwarg, kwargs[kwarg])

	def _single_item_extract(self, image, task_registry, zonal_features, aoi_attr, ee_geom, image_date, aoi_download_folder):
		"""
		This looks a bit silly here, but we need to construct this here so that we have access
		to this method's variables since we can't pass them in and it can't be a class function.

		Args:
			image:
			task_registry:
			zonal_features:
			aoi_attr:
			ee_geom:
			image_date:
			aoi_download_folder:

		Returns:
			None
		"""

		export_image = EEDLImage(
			task_registry=task_registry,
			drive_root_folder=self.drive_root_folder,
			cloud_bucket=self.cloud_bucket,
			filename_description=self.filename_description
		)
		export_image.zonal_polygons = zonal_features
		export_image.zonal_use_points = self.zonal_use_points
		export_image.zonal_keep_fields = self.zonal_features_preserve_fields
		export_image.zonal_stats_to_calc = self.zonal_stats_to_calc
		export_image.zonal_nodata_value = self.zonal_nodata_value
		export_image.date_string = image_date

		zonal_inject_constants = {}
		if self.zonal_inject_date:
			zonal_inject_constants["date"] = image_date
		if self.zonal_inject_group_id:
			zonal_inject_constants["group_id"] = aoi_attr

		export_image.zonal_inject_constants = zonal_inject_constants

		filename_suffix = f"{aoi_attr}_{image_date}"
		if self.skip_existing and export_image.check_mosaic_exists(aoi_download_folder, self.export_folder, f"{self.filename_description}_{filename_suffix}"):
			print(f"Image {filename_suffix} exists and skip_existing=True. Skipping")
			return

		export_image.export(image,
							export_type=self.export_type,
							filename_suffix=filename_suffix,
							clip=ee_geom,
							strict_clip=self.strict_clip,
							folder=self.export_folder,  # The folder to export to in Google Drive
							)  # This all needs some work still so that.

	def extract(self):
		collection = self._get_and_filter_collection()

		# Now we need to get each polygon to filter the bounds to and make a new collection with filterBounds for just
		# that geometry

		self._all_outputs = list()
		features = safe_fiona_open(self.areas_of_interest_path)
		try:
			num_complete = 0
			for feature in features:
				print(f"Number of complete AOIs: {num_complete}")
				task_registry = TaskRegistry()

				ee_geom = ee.Geometry.Polygon(feature['geometry']['coordinates'][0])  # WARNING: THIS DOESN'T CHECK CRS
				aoi_collection = collection.filterBounds(ee_geom)

				# Get some variables defined for use in extracting the zonal stats.
				aoi_attr = feature.properties[self.zonal_areas_of_interest_attr]  # This is the value we'll search for in the zonal features.
				zonal_features_query = f"{self.zonal_features_area_of_interest_attr} = '{aoi_attr}'"
				aoi_download_folder = os.path.join(self.download_folder, aoi_attr)

				fiona_zonal_features = safe_fiona_open(self.zonal_features_path)
				try:
					zonal_features_filtered = fiona_zonal_features.filter(where=zonal_features_query)

					image_list = aoi_collection.toList(aoi_collection.size()).getInfo()
					indicies_and_dates = [(im['properties']['system:index'], im['properties']['system:time_start']) for im in image_list]

					"""
					if len(zonal_features_filtered) < self.max_fiona_features_load:
					#	zonal_features_filtered = list(zonal_features_filtered)  # this *would* be inefficient, but we're going to re-use it so many times, it's not terrible, exce
					#	using_tee = False
					# else:
					# using an itertools tee may not be more efficient than a list, but it also might, because
					# even if we iterate through all features and all features remain queued for other iterations
					# it may not load all attributes, etc, for each feature if fiona lazy loads anything. It won't
					# be that much slower in any case, though the complexity of maintaining the code here is something
					# to consider
					"""
					zonal_features_filtered_tee = itertools.tee(zonal_features_filtered, len(image_list))
					using_tee = True

					for i, image_info in enumerate(indicies_and_dates):
						if using_tee:
							zonal_features = zonal_features_filtered_tee[i - 1]
						else:
							zonal_features = zonal_features_filtered

						image = aoi_collection.filter(ee.Filter.eq("system:time_start", image_info[1])).first()  # Get the image from the collection again based on ID.
						timsetamp_in_seconds = int(str(image_info[1])[:-3])  # We could divide by 1000, but then we'd coerce back from a float. This is precise.
						date_string = datetime.datetime.fromtimestamp(timsetamp_in_seconds, tz=datetime.timezone.utc).strftime("%Y-%m-%d")

						self._single_item_extract(image, task_registry, zonal_features, aoi_attr, ee_geom, date_string, aoi_download_folder)

					# Ok, now that we have a collection for the AOI, we need to iterate through all the images
					# in the collection as we normally would in a script, but also extract the features of interest for use
					# in zonal stats. Right now the zonal stats code only accepts files. We might want to make it accept
					# some kind of fiona iterator - can we filter fiona objects by attributes?
					# Fiona supports SQL queries on open and zonal stats now supports receiving an open fiona object.

					task_registry.setup_log(os.path.join(self.download_folder, "eedl_processing_error_log.txt"))
					task_registry.wait_for_images(aoi_download_folder, sleep_time=15, callback="mosaic_and_zonal", try_again_disk_full=False, on_failure=self.on_error)

					if self.keep_image_objects:
						self.all_images.extend(task_registry.images)

				finally:
					fiona_zonal_features.close()

				num_complete += 1
		finally:
			features.close()

	def _get_and_filter_collection(self):
		collection = ImageCollection(self.collection)

		if self.time_start or self.time_end:
			collection = collection.filterDate(self.time_start, self.time_end)

		if self.collection_band:
			collection = collection.select(self.collection_band)

		if self.mosaic_by_date:  # We're supposed to take the images in the collection and merge them so that all images on one date are a single image.
			collection = mosaic_by_date(collection)

		return collection


def mosaic_by_date(image_collection):
	"""
	Adapted to Python from code found via https://gis.stackexchange.com/a/343453/1955
	:param image_collection: An image collection
	:return: ee.ImageCollection
	"""
	image_list = image_collection.toList(image_collection.size())

	unique_dates = image_list.map(lambda im: ee.Image(im).date().format("YYYY-MM-dd")).distinct()

	def _make_mosaicked_image(d):
		d = ee.Date(d)

		image = image_collection.filterDate(d, d.advance(1, "day")).mosaic()

		image_w_props = image.set(
			"system:time_start", d.millis(),
			"system:id", d.format("YYYY-MM-dd"),
			"system:index", d.format("YYYY-MM-dd")
		).rename(d.format("YYYY-MM-dd")),

		return image_w_props[0]

	mosaic_imlist = unique_dates.map(_make_mosaicked_image)

	return ee.ImageCollection(mosaic_imlist)
