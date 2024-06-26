import os
import io
import shutil
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union
from typing_extensions import TypedDict, NotRequired, Unpack
import traceback
import datetime

import ee
from ee import EEException

from . import google_cloud
from . import mosaic_rasters
from . import zonal


class EEExportDict(TypedDict):
	fileDimensions: Optional[int]
	folder: NotRequired[Optional[Union[str, Path]]]
	crs: Optional[str]
	region: NotRequired[ee.geometry.Geometry]
	description: str
	fileNamePrefix: str
	scale: Union[int, float]
	maxPixels: Union[int, float]
	bucket: NotRequired[Optional[str]]


DEFAULTS = dict(
	CRS='EPSG:4326',
	TILE_SIZE=12800,  # multiple of shardSize default 256
	EXPORT_FOLDER="ee_exports",
	SCALE=30
)


def download_images_in_folder(source_location: Union[str, Path], download_location: Union[str, Path], prefix: str) -> None:
	"""
	Handles pulling data from Google Drive over to a local location, filtering by a filename prefix and folder

	Args:
		source_location (Union[str, Path]): Directory to search for files.
		download_location (Union[str, Path]): Destination for files with the specified prefix.
		prefix (str): A prefix to use to filter items in the folder - only files where the name matches this prefix will be moved.

	Returns:
		None
	"""
	folder_search_path: Union[str, Path] = source_location
	files = [filename for filename in os.listdir(folder_search_path) if filename.startswith(prefix)]

	if len(files) == 0:
		print(f"Likely Error: Could not find files to download for {prefix} in {folder_search_path} - you likely have a misconfiguration in your export parameters. Future steps may fail.")

	os.makedirs(download_location, exist_ok=True)

	for filename in files:
		shutil.move(str(os.path.join(folder_search_path, filename)), str(os.path.join(download_location, filename)))


class TaskRegistry:
	"""
	The TaskRegistry class makes it convenient to manage arbitrarily many Earth Engine images that are in varying states of being downloaded.
	"""
	INCOMPLETE_STATUSES = ("READY", "UNSUBMITTED", "RUNNING")
	COMPLETE_STATUSES = ["COMPLETED"]
	FAILED_STATUSES = ["CANCEL_REQUESTED", "CANCELLED", "FAILED"]

	def __init__(self) -> None:
		"""
		Initialized the TaskRegistry class and defaults images to "[]" and the callback function to "None"

		Returns:
			None
		"""
		self.images: List[EEDLImage] = []
		self.callback: Optional[str] = None
		self.log_file_path: Optional[Union[str, Path]] = None  # the path to the log file
		self.log_file: Optional[io.TextIOWrapper] = None  # the open log file handle
		self.raise_errors: bool = True

	def add(self, image: "EEDLImage") -> None:
		"""
		Adds an Earth Engine image to the list of Earth Engine images.

		Args:
			image (ee.image.Image): Earth Engine image to be added to the list of images

		Returns:
			None
		"""
		self.images.append(image)

	@property
	def incomplete_tasks(self) -> List["EEDLImage"]:
		"""
		List of Earth Engine images that have not been completed yet.

		Returns:
		List[ee.image.Image]: List of Earth Engine images that have not been completed yet.
		"""
		initial_tasks = [image for image in self.images if image.last_task_status['state'] in self.INCOMPLETE_STATUSES]
		for image in initial_tasks:  # update anything that's currently running or waiting first
			image._check_task_status()

		return [image for image in self.images if image.last_task_status['state'] in self.INCOMPLETE_STATUSES]

	@property
	def complete_tasks(self) -> List["EEDLImage"]:
		"""
		List of Earth Engine images.

		Returns:
			List[ee.image.Image]: List of Earth Engine images.
		"""
		return [image for image in self.images if image.last_task_status['state'] in self.COMPLETE_STATUSES + self.FAILED_STATUSES]

	@property
	def failed_tasks(self) -> List["EEDLImage"]:
		"""
		List of Earth Engine images that have either been cancelled or that have failed

		Returns:
			List[ee.image.Image]: List of Earth Engine images that have failed or have been cancelled.
		"""
		return [image for image in self.images if image.last_task_status['state'] in self.FAILED_STATUSES]

	@property
	def downloadable_tasks(self) -> List["EEDLImage"]:
		"""
		List of Earth Engine images that have not been cancelled or have failed.

		Returns:
			List[ee.image.Image]: List of Earth Engine images that have not been cancelled or have failed.
		"""
		return [image for image in self.complete_tasks if image.task_data_downloaded is False and image.last_task_status['state'] not in self.FAILED_STATUSES]

	def download_ready_images(self, download_location: Union[str, Path]) -> None:
		"""
		Downloads all images that are ready to be downloaded.

		Args:
			download_location (Union[str, Path]): Destination for downloaded files.

		Returns:
			None
		"""
		for image in self.downloadable_tasks:
			try:
				print(f"{image.filename} is ready for download")
				image.download_results(download_location=download_location, callback=self.callback)
			except:  # noqa: E722
				# on any error raise or log it
				if self.raise_errors:
					raise

				error_details = traceback.format_exc()
				self.log_error("local", f"Failed to process image {image.filename}. Error details: {error_details}")

	def setup_log(self, log_file_path: Union[str, Path], mode='a'):
		self.log_file_path = log_file_path
		self.log_file = open(self.log_file_path, 'a')

	def log_error(self, error_type: str, error_message: str):
		"""
		Args:
			error_type (str): Options "ee", "local" to indicate whether it was an error on Earth Engine's side or on the local processing side
			error_message (str): The error message to print to the log file

		Returns:
			None
		"""
		message = f"{error_type} Error: {error_message}"
		date_string = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
		print(message)

		if self.log_file:
			self.log_file.write(f"{date_string}: {message}")

	def __del__(self):
		if self.log_file is not None:
			try:
				self.log_file.close()
			except:  # noqa: E722
				# If we get any exception while closing it, don't make noise, just move on. We're just trying to be clean here where we can
				pass

	def wait_for_images(self,
						download_location: Union[str, Path],
						sleep_time: int = 10,
						callback: Optional[str] = None,
						try_again_disk_full: bool = True,
						on_failure: str = "log") -> None:
		"""
		Tells EEDL to wait until there are no more incomplete or downloadable tasks left. It will block execution
		of any following code until all images have been downloaded and processed. Any code that runs afterward
		can rely on the images being downloaded to disk.

		Args:
			download_location (Union[str, Path]): Destination for downloaded files.
			sleep_time (int): Time between checking if downloads are available (complete) in seconds. Defaults to 10 seconds.
				This is also the interval where task statuses are updated on image objects.
			callback (Optional[str]): Optional callback function. Executed after image has been downloaded and allows for
				processing of completed images even while waiting for other images to complete on Earth Engine's servers.
			try_again_disk_full (bool): Will continuously retry to download images that are ready, even if it fails to
				do so initially because the disk is full. This allows you to get the warning that the disk is full, then
				clear out space to allow processing to complete, without restarting the exports or processing.
			on_failure (str): ***Needs language***

		Returns:
			None
		"""

		if on_failure == "raise":
			self.raise_errors = True
		elif on_failure == "log" and self.log_file:  # if they say to log the errors and specified a log file, set raise errors to False
			self.raise_errors = False

		self.callback = callback
		while len(self.incomplete_tasks) > 0 or len(self.downloadable_tasks) > 0:
			try:
				self.download_ready_images(download_location)
			except OSError:
				if try_again_disk_full:
					print("OSError reported. Invalid disk or the disk may be full - will try again - clear space")
					pass
				else:
					raise

			time.sleep(sleep_time)

		if len(self.failed_tasks) > 0:
			message = f"{len(self.failed_tasks)} image(s) failed to export. Example error message from first" \
								f" failed image \"{self.failed_tasks[0].last_task_status['description']}\" was" \
								f" \"{self.failed_tasks[0].last_task_status['error_message']}\"." \
								f" Check https://code.earthengine.google.com/tasks in your web browser to see status and" \
								f" messages for all export tasks."
			if on_failure == "raise":
				raise EEException(message)
			else:
				print(message)


main_task_registry = TaskRegistry()


class EEDLImage:
	"""
	The main class that does all the work. Any use of this package ultimately instantiates this class for each export
	the user wants to do. As we refine this, we may be able to provide just a single function in this module named
	"export" or something of that sort for people who don't need access to control class behavior. That will likely
	follow all the other enhancements, like converting the exports into async code.

	The class has no required arguments as of 6/16/2023, but that may change. Any arguments provided get applied
	directly to the class and override any defaults. It may be good to set :code:`crs` and :code:`scale` here
	to make sure they are set correctly for the images you plan to export, but those can
	also be provided as kwargs to the :code:`.export` method.

	Note that in the arguments below, the ones prefixed by zonal are only used if you configure the :code:`mosaic_and_zonal`
	callback on the TaskRegistry. Otherwise, when calling the :code:`zonal_stats` method, you need to provide the parameters
	there.

	Options at class instantiation include:

	Args:
		crs (Optional[str]): Coordinate Reference System to use for exports in a format Earth Engine understands,
		scale (Optional[int]): Scale parameter to pass to Earth Engine for export. Defaults to 30
		tile_size (Optional[int]): The number of pixels per side of tiles to export
		export_folder (Optional[Union[str, Path]]): The name of the folder in the chosen export location that will be created for the export
		cloud_bucket (Optional[str]): The name of the Google Cloud storage bucket to use for exports - setting this parameter doesn't automatically configure output to the bucket. When running :code:`.export` your also need to specify a cloud export (instead of a `drive` export)
		output_folder (Optional[Union[str, Path]]): The folder, local to your system running the code, to export the finished images and optional zonal statistics files to.
		zonal_polygons: Optional[Union[str, Path]]: The path to a fiona-compatible polygon vector data file (e.g. a shapefile, geopackage layer, or other). Only used with the :code:`mosaic_and_zonal` callback. See note above.
		zonal_stats_to_calc: Optional[Tuple]: Tuple of zonal statistics to calculate. For example :code:`('min', 'max', 'mean')`. See the `documentation
			for the rasterstats package <https://pythonhosted.org/rasterstats/>`_ for full options. Only used with the :code:`mosaic_and_zonal` callback. See note above.
		zonal_keep_fields: Optional[Tuple]: Which fields should be preserved (passed through) from the spatial input data to the output zonal data.
			You will want to at least define the row's ID/key value here (in a tuple, such as :code:`('ID',)` so you can join the zonal stats
			back to spatial data, but you can optionally include any other fields as well. Only used with the :code:`mosaic_and_zonal` callback. See note above.
		zonal_use_points: bool: Switch rasterstats to extract using gen_point_query instead of gen_zonal_stats. See rasterstats
			package documentation for complete information. Get_point_query will get the values of a raster at all vertex
			locations when provided with a polygon or line. If provided points, it will extract those point values. We set
			interpolation to the nearest to perform an exact extraction of the cell values. In this codebase's usage, it's
			assumed that the "features" parameter to this function will be a points dataset (still in the same CRS as the raster)
			when use_points is True. Additionally, when this is True, the `stats` argument to this function is ignored
			as only a single value will be extracted as the attribute `value` in the output CSV. Default is False.
			Only used with the :code:`mosaic_and_zonal` callback. See note above.
		zonal_output_filepath: Optional[Union[str, Path]]:  Only used with the :code:`mosaic_and_zonal` callback. See note above.
		zonal_inject_constants: dict:  Only used with the :code:`mosaic_and_zonal` callback. See note above.
		zonal_nodata_value: int:  Only used with the :code:`mosaic_and_zonal` callback. See note above.
		zonal_all_touched: bool:  Only used with the :code:`mosaic_and_zonal` callback. See note above.
	"""

	def __init__(self, **kwargs) -> None:
		"""
		Initializes many class variables and sets provided kwargs.

		Returns:
			None
		"""

		self.drive_root_folder: Optional[Union[str, Path]] = None
		self.crs: Optional[str] = None
		self.tile_size: Optional[int] = None
		self.export_folder: Optional[Union[str, Path]] = None
		self.mosaic_image: Optional[Union[str, Path]] = None
		self.task: Optional[ee.batch.Task] = None
		self.cloud_bucket: Optional[str] = None
		self._ee_image: Optional[ee.image.Image] = None
		self.output_folder: Optional[Union[str, Path]] = None
		self.task_registry = main_task_registry
		self.scale: Union[int, float] = 1

		self.filename_description = ""
		self.date_string = ""  # For items that want to store a date representation.

		# These values are only used if someone calls the mosaic_and_zonal callback - we need the values defined on.
		# The class to do that.
		self.zonal_polygons: Optional[Union[str, Path]] = None
		self.zonal_stats_to_calc: Optional[Tuple] = None
		self.zonal_keep_fields: Optional[Tuple] = None
		self.zonal_use_points: bool = False
		self.zonal_output_filepath: Optional[Union[str, Path]] = None  # set by self.zonal_stats
		self.zonal_inject_constants: dict = dict()
		self.zonal_nodata_value: int = -9999
		self.zonal_all_touched: bool = False

		# Set the defaults here - this is a nice strategy where we get to define constants near the top that aren't buried in code, then apply them here.
		for key in DEFAULTS:
			setattr(self, key.lower(), DEFAULTS[key])

		for key in kwargs:  # Now apply any provided keyword arguments over the top of the defaults.
			setattr(self, key, kwargs[key])

		self._last_task_status = {"state": "UNSUBMITTED"}
		# This will be the default status initially, so always assume it's UNSUBMITTED if we haven't gotten anything.
		# From the server. "None" would work too, but then we couldn't just check the status.
		self.task_data_downloaded = False
		self.export_type = "Drive"  # The other option is "Cloud".

	def _set_names(self, filename_suffix: str = "") -> None:
		"""
		Args:
			filename_suffix (str): Suffix used to later identify files.

		Returns:
			None
		"""
		self.description = filename_suffix
		self.filename = f"{self.filename_description}_{filename_suffix}"

	@staticmethod
	def _initialize() -> None:
		"""
		Handles the initialization and potentially the authentication of Earth Engine.

		Returns:
			None
		"""
		try:  # Try just a basic discard-able operation used in their docs so that we don't initialize if we don't need to.
			_ = ee.Image("NASA/NASADEM_HGT/001")
		except EEException:  # If it fails, try just running initialize.
			try:
				ee.Initialize()
			except EEException:  # If that still fails, try authenticating first.
				ee.Authenticate()
				ee.Initialize()

	@property
	def last_task_status(self) -> Dict[str, str]:
		"""
		Returns the status of the image's task on Earth Engine's servers, as last reported when it was checked.
		Calling this method does not re-check the task status. Instead, those checks are scheduled via the
		TaskRegistry and stored for access here.

		The task status reported by Earth Engine comes back as a dictionary, which is what is stored here. We pay the most attention
		to the "STATE" key, which indicates where processing of the export is in the lifecycle. See
		https://developers.google.com/earth-engine/guides/processing_environments#task_lifecycle
		for more information on task lifecycles and possible values.

		The following document from Earth Engine further has an example with the full set of keys and
		example values for a task status dictionary: https://developers.google.com/earth-engine/guides/python_install#create-an-export-task:

		Returns:
		Dict[str, str]: Return the private variable "_last_task_status"
		"""
		return self._last_task_status

	@last_task_status.setter
	def last_task_status(self, new_status: Dict[str, str]) -> None:
		"""
		Sets the value of the private variable "_last_task_status" to a specified value. Realistically, this shouldn't
		be used as the value should only be set from within the object, but it's here in case it's needed.

		Args:
			new_status (Dict[str, str]): Status to update the _last_task_status to.

		Returns:
			None
		"""
		self._last_task_status = new_status

	def export(self,
				image: ee.image.Image,
				filename_suffix: str,
				export_type: str = "drive",
				clip: Optional[ee.geometry.Geometry] = None,
				strict_clip: Optional[bool] = False,
				drive_root_folder: Optional[Union[str, Path]] = None,
				**export_kwargs: Unpack[EEExportDict]) -> None:
		"""
		Handles the exporting of an image.

		Determines the values to provide to tile and export the image, then calls Earth Engine's
		image export functionality, creates an export task, and starts the task running. Importantly,
		this code *does not* wait for the task to finish exporting, or do any downloading of the image.
		It just starts the export process on EE's servers in a way that can be tracked later. This is by design
		because if you want to export many images, what is most efficient is to start all of the image
		tasks, and then wait for all of them at once.

		The :code:`drive_root_folder` parameter must be set if :code:`export_type` is :code:`drive`, but is optional
		when :code:`export_type` is :code:`cloud`. When :code:`export_type` is :code:`cloud` you must provide
		the name of the Google Cloud storage bucket to export to in the :code:`bucket` parameter. Both export
		types have configuration requirements and limitations discussed in :ref:`ExportLocations`.

		Note that this method returns None - if you wish to save an object for tracking and to
		obtain the path to the mosaicked image after everything is downloaded, keep the whole class
		instance object (or save it into a list, etc).

		Args:
			image (ee.image.Image): Image for export.
			filename_suffix (str): The unique identifier used internally to identify images.
			export_type (str): Specifies how the image should be exported. Either "cloud" or "drive". Defaults to "drive".
			clip (Optional[ee.geometry.Geometry]): Defines the region of interest for export - does not perform a strict clip, which is often slower.
				Instead, it uses the Earth Engine export's "region" parameter to clip the results to the bounding box of
				the clip geometry. To clip to the actual geometry, set strict_clip to True.
			strict_clip (Optional[bool]): When set to True, performs a true clip on the result so that it's not just the bounding box but also the
				actual clipping geometry. Defaults to False.
			drive_root_folder (Optional[Union[str, Path]]): The folder on your computer that has the root of your
				Google Drive installation (e.g. :code:`G:\\My Drive` on Windows) if "drive" is the provided export type.
			bucket (Optional[str]): The Google Cloud bucket to place the exported images into if using the `cloud` export_type.
			export_kwargs (Unpack[EExportDict]): An optional dictionary of keyword arguments that gets passed directly to
				Earth Engine's image export method. Overrides any values EEDL manually calculates, if a key is set here
				that is also set elsewhere (such as on the class or derived in the method).
		Returns:
			None
		"""

		if not isinstance(image, ee.image.Image):

			raise ValueError("Invalid image provided for export - please provide a single image (not a collection or another object) of class ee.image.Image for export")

		if export_type.lower() == "drive" and \
			(self.drive_root_folder is None or not os.path.exists(self.drive_root_folder)) and \
			(drive_root_folder is None or not os.path.exists(drive_root_folder)):

			raise NotADirectoryError("The provided path for the Google Drive export folder is not a valid directory but"
										" Drive export was specified. Either change the export type to use Google Cloud"
										" and set that up properly (with a bucket, etc), or set the drive_root_folder"
										" to a valid folder.")
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

		ee_kwargs: EEExportDict = {
			'description': self.description,
			'fileNamePrefix': self.filename,
			'scale': self.scale,
			'maxPixels': 1e12,
			'fileDimensions': self.tile_size,
			'crs': self.crs
		}

		if isinstance(clip, ee.geometry.Geometry):
			ee_kwargs["region"] = clip

		# Override any of these defaults with anything else provided.
		ee_kwargs.update(export_kwargs)

		if "folder" not in ee_kwargs:  # If they didn't specify a folder, use the class' default or whatever they defined previously.
			ee_kwargs['folder'] = self.export_folder
		else:
			self.export_folder = ee_kwargs['folder']  # We need to persist this, so we can find the image later on, and so it's picked up by cloud export code below.

		if export_type.lower() == "drive":
			self.task = ee.batch.Export.image.toDrive(self._ee_image, **ee_kwargs)
		elif export_type.lower() == "cloud":
			# Add the folder to the filename here for Google Cloud.
			ee_kwargs['fileNamePrefix'] = f"{self.export_folder}/{ee_kwargs['fileNamePrefix']}"

			if "bucket" not in ee_kwargs:  # If we already defined the bucket on the class, use that.
				ee_kwargs['bucket'] = self.cloud_bucket
			else:  # Otherwise, attempt to retrieve it from the call to this function.
				self.cloud_bucket = str(ee_kwargs['bucket'])

			if "folder" in ee_kwargs:  # We made this part of the filename prefix above, so delete it now, or it will cause an error.
				del ee_kwargs["folder"]

			self.task = ee.batch.Export.image.toCloudStorage(self._ee_image, **ee_kwargs)

		# Export_type is not valid
		else:
			raise ValueError("Invalid value for export_type. Did you mean \"drive\" or \"cloud\"?")

		self.task.start()

		self.export_type = export_type

		self.task_registry.add(self)

	@staticmethod
	def check_mosaic_exists(download_location: Union[str, Path], export_folder: Union[str, Path], filename: str):
		"""
			This function isn't ideal because it duplicates information - you need to pass it in elsewhere and assume
			this file format matches, rather than actually calculating the paths earlier in the process. But that's
			currently necessary because the task registry sets the download location right now. So we want to be able
			to check at any time if the mosaic exists so that we can skip processing - we're using this. Otherwise,
			we'd need to do a big refactor that's probably not worth it.
		"""
		output_file = os.path.join(str(download_location), str(export_folder), f"{filename}_mosaic.tif")
		return os.path.exists(output_file)

	def download_results(self, download_location: Union[str, Path], callback: Optional[str] = None, drive_wait: int = 15) -> None:
		"""

		Handles the download and optional postprocessing of the current image to the folder specified :code:`download_location`.
		Users of EEDL won't need to invoke this except in very advanced situations. The Task Registry automatically
		invokes this method when it detects that the image has completed exporting.

		Args:
			download_location (Union[str, Path]): The directory where the results should be downloaded to. Expects a string path or a Pathlib Path object.
			callback (Optional[str]): The callback function is called once the image has been downloaded.
			drive_wait (int): The amount of time in seconds to wait to allow for files that Earth Engine reports have been exported to actually populate. Default is 15 seconds.

		Returns:
			None
		"""
		# Need an event loop that checks self.task.status(), which will get the current state of the task.

		# state options
		# == "CANCELLED", "CANCEL_REQUESTED", "COMPLETED",
		# "FAILED", "READY", "SUBMITTED" (maybe - double check that - it might be that it waits with UNSUBMITTED),
		# "RUNNING", "UNSUBMITTED"

		self.output_folder = os.path.join(str(download_location), str(self.export_folder))

		if self.export_type.lower() == "drive":
			time.sleep(drive_wait)  # It seems like there's often a race condition where EE reports export complete, but no files are found. Give things a short time to sync up.
			folder_search_path = os.path.join(str(self.drive_root_folder), str(self.export_folder))
			download_images_in_folder(folder_search_path, self.output_folder, prefix=self.filename)

		elif self.export_type.lower() == "cloud":
			google_cloud.download_public_export(str(self.cloud_bucket), self.output_folder, f"{self.export_folder}/{self.filename}")

		else:
			raise ValueError("Unknown export_type (not one of 'drive', 'cloud') - can't download")

		self.task_data_downloaded = True

		if callback:
			callback_func = getattr(self, callback)
			callback_func()

	def mosaic(self) -> None:
		"""
		Mosaics the individual pieces of the image into the complete image.

		EEDL works by configuring Earth Engine to tile large exports - while Earth Engine has limits on individual image
		sizes for export, it can slice large images into tiles so that each individual image fits within those limits.
		EEDL sets Earth Engine to tile exports, then tracks the individual pieces (whether one or thousands) through
		downloading. In this function, it then mosaics those pieces back into one image you can use locally, so you
		don't need to handle the individual tiles (and all of their edges).

		Returns:
			None
		"""
		self.mosaic_image = os.path.join(str(self.output_folder), f"{self.filename}_mosaic.tif")
		mosaic_rasters.mosaic_folder(str(self.output_folder), self.mosaic_image, prefix=self.filename)

	def mosaic_and_zonal(self) -> None:
		"""
			A callback that takes no parameters, but runs both the mosaic and zonal stats methods. Runs zonal stats
			by allowing the user to set all the zonal params on the class instance instead of passing
			them as params. Users of EEDL *could* invoke this, but it's really designed to be
			invoked via a callback on the Task Registry, and for any prior code to configure
			the zonal statistics extractions.
		"""

		if not (self.zonal_polygons and self.zonal_keep_fields and self.zonal_stats_to_calc):
			raise ValueError("Can't run mosaic and zonal callback without `polygons`, `keep_fields, and `stats` values"
								"set on the class instance.")

		try:
			use_points = self.zonal_use_points
		except AttributeError:
			use_points = False

		self.mosaic()
		self.zonal_stats(polygons=self.zonal_polygons,
							keep_fields=self.zonal_keep_fields,
							stats=self.zonal_stats_to_calc,
							use_points=use_points,
							inject_constants=self.zonal_inject_constants,
							nodata_value=self.zonal_nodata_value,
							all_touched=self.zonal_all_touched,
						)

	def zonal_stats(self,
					polygons: Union[str, Path],
					keep_fields: Tuple[str, ...] = ("UniqueID", "CLASS2"),
					stats: Tuple[str, ...] = ('min', 'max', 'mean', 'median', 'std', 'count', 'percentile_10', 'percentile_90'),
					report_threshold: int = 1000,
					write_batch_size: int = 2000,
					use_points: bool = False,
					inject_constants: Optional[dict] = None,
					nodata_value: int = -9999,
					all_touched: bool = False
					) -> None:
		"""
		Args:
			polygons (Union[str, Path]):
			keep_fields (tuple[str, ...]):
			stats ( tuple[str, ...]):
			report_threshold (int): After how many iterations should it print out the feature number it's on. Defaults to 1000.
				Set to None to disable.
			write_batch_size (int): How many zones should we store up before writing to the disk? Defaults to 2000.
			use_points (bool):
			inject_constants(Optional[dict]):
			nodata_value (int):
			all_touched (bool):

		Returns:
			None
		"""
		if inject_constants is None:
			inject_constants = dict()

		self.zonal_output_filepath = zonal.zonal_stats(
							polygons,
							self.mosaic_image,
							self.output_folder,
							self.filename,
							keep_fields=keep_fields,
							stats=stats,
							report_threshold=report_threshold,
							write_batch_size=write_batch_size,
							use_points=use_points,
							inject_constants=inject_constants,
							nodata_value=nodata_value,
							all_touched=all_touched
						)

	def _check_task_status(self) -> Dict[str, Union[Dict[str, str], bool]]:
		"""
		Updates the status if it needs to be changed

		Returns:
			Dict[str, Union[Dict[str, str], bool]]: Returns a dictionary of the most up-to-date status and whether that status was changed
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
