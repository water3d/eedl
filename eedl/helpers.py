import os

from .core import _get_fiona_args
from .image import EEDLImage, TaskRegistry

import ee
from ee import ImageCollection

import fiona


class GroupedCollectionExtractor():

	def __init__(self, **kwargs):
		self.collection = None
		self.collection_band = None
		self.time_start = None
		self.time_end = None
		self.areas_of_interest_path = None  # the path to a spatial data file readable by Fiona/GEOS that has features defining AOIs to extract individually

		self.strict_clip = False
		self.export_type = "drive"
		self.drive_root_folder = None
		self.download_folder = None  # local folder name after downloading for processing
		self.export_folder = None  # drive/cloud export folder name

		self.zonal_run = True
		self.zonal_areas_of_interest_attr = None  # what is the attribute on each of the AOI polygons that tells us what items to use in the zonal extraction
		self.zonal_features_path = None  # what polygons to use as inputs for zonal stats
		self.zonal_features_area_of_interest_attr = None  # what field in the zonal features has the value that should match zonal_areas_of_interest_attr?
		self.zonal_features_preserve_fields = None  # what fields to preserve, as a tuple - typically an ID and anything else you want
		self.zonal_stats = None  # what statistics to output by zonal feature

		self.merge_sqlite = True  # should we merge all outputs to a single SQLite database
		self.merge_grouped_csv = True  # should we merge CSV by grouped item
		self.merge_final_csv = False  # should we merge all output tables

		self._all_outputs = list()  # for storing the paths to all output csvs

		for kwarg in kwargs:
			setattr(self, kwarg, kwargs[kwarg])

	def extract(self):
		collection = ImageCollection(self.collection)

		if self.time_start or self.time_end:
			collection = collection.filterDate(self.time_start, self.time_end)

		if self.collection_band:
			collection = collection.select(self.collection_band)

		# now we need to get each polygon to filter the bounds to and make a new collection with filterBounds for just
		# that geometry

		kwargs = _get_fiona_args(self.areas_of_interest_path)
		main_file_path = kwargs['fp']
		del kwargs['fp']

		self._all_outputs = list()
		with fiona.open(main_file_path, **kwargs) as features:
			for feature in features:
				task_registry = TaskRegistry()

				ee_geom = ee.Geometry.Polygon(feature['geometry']['coordinates'])  # WARNING: THIS DOESN'T CHECK CRS
				aoi_collection = collection.filterBounds(ee_geom)

				# get some variables defined for use in extracting the zonal stats
				aoi_attr = feature['attributes'][self.zonal_areas_of_interest_attr]  # this is the value we'll search for in the zonal features
				zonal_features_query = f"{self.zonal_features_area_of_interest_attr} = \"{aoi_attr}\""

				def _single_item_extract(image, state):
					"""
						This looks a bit silly here, but we need to construct this here so that we have access
						to this method's variables since we can't pass them in and it can't be a class function.
					:param image:
					:param state:
					:return:
					"""
					export_image = EEDLImage(task_registry=task_registry, drive_root_folder=self.drive_root_folder)
					export_image.export(image,
										export_type=self.export_type,
										folder=self.export_folder,
										filename_suffix=f"-{aoi_attr}_{self.time_start}-{self.time_end}",
										clip=ee_geom,
										strict_clip=self.strict_clip
					)   # this all needs some work still so that

					return state

				# ok, now that we have a collection for the AOI, we need to iterate through all the images
				# in the collection as we normally would in a script, but also extract the features of interest for use
				# in zonal stats. Right now the zonal stats code only accepts files. We might want to make it accept
				# some kind of fiona iterator - can we filter fiona objects by attributes?
				# fiona supports SQL queries on open and zonal stats now supports receiving an open fiona object

				aoi_collection.iterate(_single_item_extract, None)

				download_folder = os.path.join(self.download_folder, aoi_attr)
				task_registry.wait_for_images(download_folder, sleep_time=60, callback="mosaic_and_zonal")

				# then we process the tables by AOI group after processing them by individual image
