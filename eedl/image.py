import os
import shutil
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import ee
from ee import EEException

from . import google_cloud
from . import mosaic_rasters
from . import zonal

DEFAULTS = dict(
	CRS='EPSG:4326',
	TILE_SIZE=12800,
	EXPORT_FOLDER="ee_exports"

)


def download_images_in_folder(source_location: Union[str, Path], download_location: Union[str, Path], prefix: str) -> None:
	"""
	Handles pulling data from Google Drive over to a local location, filtering by a filename prefix and folder

	:param source_location: Directory to search for files
	:type source_location: Union[str, Path]
	:param download_location: Destination for files with the specified prefix
	:type download_location: Union[str, Path]
	:param prefix: A prefix to use to filter items in the folder - only files where the name matches this prefix will be moved
	:type prefix: str
	:return: None
	"""
	folder_search_path: Union[str, Path] = source_location
	files = [filename for filename in os.listdir(folder_search_path) if filename.startswith(prefix)]

	os.makedirs(download_location, exist_ok=True)

	for filename in files:
		shutil.move(os.path.join(folder_search_path, filename), os.path.join(download_location, filename))


class TaskRegistry:
	"""
	The TaskRegistry class makes it convent to manage arbitrarily many Earth Engine images that are in varying states of being downloaded.
	"""
	INCOMPLETE_STATUSES = ("READY", "UNSUBMITTED", "RUNNING")
	COMPLETE_STATUSES = ["COMPLETED"]
	FAILED_STATUSES = ["CANCEL_REQUESTED", "CANCELLED", "FAILED"]

	def __init__(self) -> None:
		"""
		Initialized the TaskRegistry class and defaults images to "[]" and the callback function to "None"
		:return: None
		"""
		self.images: List[EEDLImage] = []
		self.callback: Optional[str] = None

	def add(self, image: ee.image.Image) -> None:
		"""
		Adds an Earth Engine image to the list of Earth Engine images

		:param image: Earth Engine image to be added to the list of images
		:type image: ee.image.Image
		:return: None
		"""
		self.images.append(image)

	@property
	def incomplete_tasks(self) -> List[ee.image.Image]:
		"""
		List of Earth Engine images that have not been completed yet

		:return: List of Earth Engine images that have not been completed yet
		:rtype: List[ee.image.Image]
		"""
		initial_tasks = [image for image in self.images if image.last_task_status['state'] in self.INCOMPLETE_STATUSES]
		for image in initial_tasks:  # update anything that's currently running or waiting first
			image._check_task_status()

		return [image for image in self.images if image.last_task_status['state'] in self.INCOMPLETE_STATUSES]

	@property
	def complete_tasks(self) -> List[ee.image.Image]:
		"""
		List of Earth Engine images

		:return: List of Earth Engine images
		:rtype: List[ee.image.Image]
		"""
		return [image for image in self.images if image.last_task_status['state'] in self.COMPLETE_STATUSES + self.FAILED_STATUSES]

	@property
	def failed_tasks(self) -> List[ee.image.Image]:
		return [image for image in self.images if image.last_task_status['state'] in self.FAILED_STATUSES]

	@property
	def downloadable_tasks(self) -> List[ee.image.Image]:
		"""
		List of Earth Engine images that have successfully been downloaded
		:return: List of Earth Engine images that have successfully been downloaded
		:rtype: List[ee.image.Image]
		"""
		return [image for image in self.complete_tasks if image.task_data_downloaded is False and image.last_task_status['state'] not in self.FAILED_STATUSES]

	def download_ready_images(self, download_location: Union[str, Path]) -> None:
		"""

		:param download_location: Destination for downloaded files
		:type download_location: Union[str, Path]
		:return: None
		"""
		for image in self.downloadable_tasks:
			print(f"{image.filename} is ready for download")
			image.download_results(download_location=download_location, callback=self.callback)

	def wait_for_images(self,
						download_location: Union[str, Path],
						sleep_time: int = 10,
						callback: Optional[str] = None,
						try_again_disk_full: bool = True,
						on_failure="raise") -> None:
		"""
		Blocker until there are no more incomplete or downloadable tasks left.

		:param download_location: Destination for downloaded files.
		:type download_location: Union[str, Path]
		:param sleep_time: Time between checking if the disk is full in seconds. Defaults to 10 seconds.
		:type sleep_time: int
		:param callback: Optional callback function. Executed after image has been downloaded.
		:type callback: Optional[str]
		:param try_again_disk_full: Will continuously retry to download images that are ready if disk is full.
		:type try_again_disk_full: bool
		:return: None
		"""

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

		if on_failure == "raise" and len(self.failed_tasks) > 0:
			raise EEException(f"{len(self.failed_tasks)} images failed to export. Example error message from first"
								f" failed image \"{self.failed_tasks[0].last_task_status['description']}\" was"
								f" \"{self.failed_tasks[0].last_task_status['error_message']}\"."
								f" Check https://code.earthengine.google.com/tasks in your web browser to see status and"
								f" messages for all export tasks.")


main_task_registry = TaskRegistry()


class EEDLImage:
	"""
	The main class that does all the work. Any use of this package should instantiate this class for each export
	the user wants to do. As we refine this, we may be able to provide just a single function in this module named
	"export" or something of that sort for people who don't need access to control class behavior. That will likely
	follow all the other enhancements, like converting the exports into async code.

	The class has no required arguments as of 6/16/2023, but that may change. Any arguments provided get applied
	directly to the class and override any defaults. Options include:

	:param crs: Coordinate Reference System to use for exports in a format Earth Engine understands, such as "EPSG:3310"
	:type crs: Optional[str]
	:param tile_size: the number of pixels per side of tiles to export
	:type tile_size: Optional[int]
	:param export_folder: the name of the folder in the chosen export location that will be created for the export
	:type export_folder: Optional[Union[str, Path]]

	This docstring needs to be checked to ensure it's in a standard format that Sphinx will render
	"""

	def __init__(self, **kwargs) -> None:
		"""

		:return: None
		"""

		self.drive_root_folder: Optional[Union[str, Path]] = None
		self.crs: Optional[str] = None
		self.tile_size: Optional[int] = None
		self.export_folder: Optional[Union[str, Path]] = None
		self.mosaic_image: Optional[Union[str, Path]] = None
		self.task: Optional[ee.batch.Task] = None
		self.bucket: Optional[str] = None
		self._ee_image: Optional[ee.image.Image] = None
		self.output_folder: Optional[Union[str, Path]] = None
		self.task_registry = main_task_registry

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

		self.filename_description = ""

	def _set_names(self, filename_suffix: str = "") -> None:
		"""

		:param filename_suffix: Suffix used to later identify files.
		:type filename_suffix: Str
		:return: None
		"""
		self.description = filename_suffix
		self.filename = f"{self.filename_description}_{filename_suffix}"

	@staticmethod
	def _initialize() -> None:
		"""
		Handles the initialization and potentially the authentication of Earth Engine

		:return: None
		"""
		try:  # try just a basic discardable operation used in their docs so that we don't initialize if we don't need to
			_ = ee.Image("NASA/NASADEM_HGT/001")
		except EEException:  # if it fails, try just running initialize
			try:
				ee.Initialize()
			except EEException:  # if that still fails, try authenticating first
				ee.Authenticate()
				ee.Initialize()

	@property
	def last_task_status(self) -> Dict[str, str]:
		"""
		Allows reading the private variable "_last_task_status"
		:return: return the private variable "_last_task_status"
		:rtype: Dict[str, str]
		"""
		return self._last_task_status

	@last_task_status.setter
	def last_task_status(self, new_status: Dict[str, str]) -> None:
		"""
		Sets the value of the private variable "_last_task_status" to a specified value. Realistically, this shouldn't
		be used as the value should only be set from within the object, but it's here in case it's needed.

		:param new_status: Updated status
		:type new_status: Dict[str, str]
		:return:  None
		"""
		self._last_task_status = new_status

	def export(self,
				image: ee.image.Image,
				filename_suffix: str,
				export_type: str = "drive",
				clip: Optional[ee.geometry.Geometry] = None,
				strict_clip: Optional[bool] = False,
				drive_root_folder: Optional[Union[str, Path]] = None,
				**export_kwargs) -> None:
		"""
		Handles the exporting of an image

		:param image: Image for export
		:type image: ee.image.Image
		:param filename_suffix: The unique identifier used internally to identify images.
		:type filename_suffix: Str
		:param export_type: Specifies how the image should be exported. Either "cloud" or "drive". Defaults to "drive".
		:type export_type: Str
		:param clip: Defines the region of interest for export - does not perform a strict clip, which is often slower.
			Instead it uses the Earth Engine export's "region" parameter to clip the results to the bounding box of
			the clip geometry. To clip to the actual geometry, set strict_clip to True
		:type clip: Optional[ee.geometry.Geometry]
		:param strict_clip: When set to True, performs a true clip on the result so that it's not just the bounding box
			but also the actual clipping geometry. Defaults to False
		:type clip: Optional[bool]
		:param drive_root_folder: The folder for exporting if "drive" is selected
		:type drive_root_folder: Optional[Union[str, Path]]
		:return: None
		"""

		if not isinstance(image, ee.image.Image):
			raise ValueError("Invalid image provided for export - please provide a single image (not a collection or another object) of class ee.image.Image for export")

		if export_type.lower() == "drive" and \
			(self.drive_root_folder is None or not os.path.exists(self.drive_root_folder)) and \
			(drive_root_folder is None or not os.path.exists(drive_root_folder)):

			raise NotADirectoryError("The provided path for the Google Drive export folder is not a valid directory but"
										" Drive export was specified. Either change the export type to use Google Cloud"
										" and set that up properly (with a bucket, etc), or set the drive_root_folder"
										" to a valid folder")
		elif export_type.lower() == "drive":
			if drive_root_folder:
				self.drive_root_folder = drive_root_folder

		self._initialize()

		if clip and not isinstance(clip, ee.geometry.Geometry):
			raise ValueError("Invalid geometry provided for clipping. Export parameter `clip` must be an instance of ee.geometry.Geometry")

		if clip and strict_clip and isinstance(clip, ee.geometry.Geometry):  #
			image = image.clip(clip)

		self._ee_image = image

		self._set_names(filename_suffix)

		ee_kwargs = {
			'description': self.description,
			'fileNamePrefix': self.filename,
			'scale': 30,
			'maxPixels': 1e12,
			# multiple of shardSize default 256. Should split into about 9 tiles
			'fileDimensions': self.tile_size,
			'crs': self.crs
		}

		if isinstance(clip, ee.geometry.Geometry):
			ee_kwargs["region"] = clip

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

		self.task_registry.add(self)

	def download_results(self, download_location: Union[str, Path], callback: Optional[str] = None, drive_wait: int = 15) -> None:
		"""

		:param download_location: The directory where the results should be downloaded to
		:type download_location: Union[str, Path]
		:param callback: The callback function called once the image is downloaded
		:type callback: Optional[str]
		:return: None
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
			time.sleep(drive_wait)  # it seems like there's often a race condition where EE reports export complete, but no files are found. Give things a short time to sync up.
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
		"""
		Mosaics the individual images into the full image

		:return: None
		"""
		self.mosaic_image = os.path.join(str(self.output_folder), f"{self.filename}_mosaic.tif")
		mosaic_rasters.mosaic_folder(str(self.output_folder), self.mosaic_image, prefix=self.filename)

	def zonal_stats(self,
					polygons: Union[str, Path],
					keep_fields: Tuple[str, ...] = ("UniqueID", "CLASS2"),
					stats: Tuple[str, ...] = ('min', 'max', 'mean', 'median', 'std', 'count', 'percentile_10', 'percentile_90'),
					report_threshold: int = 1000,
					write_batch_size: int = 2000,
					use_points: bool = False,
					) -> None:
		"""

		:param polygons:
		:type polygons: Union[str, Path]
		:param keep_fields:
		:type keep_fields: tuple[str, ...]
		:param stats:
		:type stats: Tuple[str, ...]
		:param report_threshold: After how many iterations should it print out the feature number it's on. Defaults to 1000.
			Set to None to disable
		:type report_threshold: int
		:param write_batch_size: How many zones should we store up before writing to the disk? Defaults to 2000
		:type write_batch_size: int
		:param use_points:
		:type use_points: bool
		:return: None

		"""

		zonal.zonal_stats(polygons,
							self.mosaic_image,
							self.output_folder,
							self.filename,
							keep_fields=keep_fields,
							stats=stats,
							report_threshold=report_threshold,
							write_batch_size=write_batch_size,
							use_points=use_points)

	def _check_task_status(self) -> Dict[str, Union[Dict[str, str], bool]]:
		"""
		Updates the status is it needs to be changed

		:return: Returns a dictionary of the most up-to-date status and whether it was changed
		:rtype: Dict[str, Union[Dict[str, str], bool]]
		"""

		if self.task is None:
			raise ValueError('Error checking task status. Task is None. It likely means that the export task was not'
							' properly created and the code needs to be re-run.')

		new_status = self.task.status()

		changed = False
		if self.last_task_status != new_status:
			changed = True
			self.last_task_status = new_status

		return {'status': self.last_task_status, 'changed': changed}
