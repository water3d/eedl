import csv
import os
import shutil
import time
from collections.abc import Iterable

import ee
import fiona
import rasterstats

from . import google_cloud
from . import mosaic_rasters

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


def _get_fiona_args(polygon_path: str) -> dict[str, str]:
    print(f'Line 28 type is: {type(polygon_path)}')
    """
        A simple utility that detects if, maybe, we're dealing with an Esri File Geodatabase. This is the wrong way
        to do this, but it'll work in many situations
    :param polygon_path:
    :return:
    """

    parts = os.path.split(polygon_path)
    if (parts[0].endswith(".gdb") or parts[0].endswith(".gpkg")) and "." not in parts[1]:  # if the folder name ends
        # with .gdb and the "filename" doesn't have an extension, assume it's an FGDB
        return {'fp': parts[0], 'layer': parts[1]}
    else:
        return {'fp': polygon_path}


def download_images_in_folder(source_location: str, download_location: str, prefix: str) -> None:
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
    INCOMPLETE_STATUSES = ("READY", "NOT-SUBMITTED", "RUNNING")
    COMPLETE_STATUSES = ["COMPLETED"]
    FAILED_STATUSES = ["CANCEL_REQUESTED", "CANCELLED", "FAILED"]

    def __init__(self) -> None:
        self.images = []
        self.callback = None

    def add(self, image) -> None:
        self.images.append(image)

    @property
    def incomplete_tasks(self) -> Iterable[ee.image.Image]:
        initial_tasks = [image for image in self.images if image._last_task_status['state'] in self.INCOMPLETE_STATUSES]
        for image in initial_tasks:  # update anything that's currently running or waiting first
            image._check_task_status()

        return [image for image in self.images if image._last_task_status['state'] in self.INCOMPLETE_STATUSES]

    @property
    def complete_tasks(self) -> Iterable[ee.image.Image]:
        return [image for image in self.images if
                image._last_task_status['state'] in self.COMPLETE_STATUSES + self.FAILED_STATUSES]

    @property
    def downloadable_tasks(self) -> Iterable[ee.image.Image]:
        return [image for image in self.complete_tasks if
                image.task_data_downloaded is False and image._last_task_status['state'] not in self.FAILED_STATUSES]

    def download_ready_images(self, download_location: str) -> None:
        for image in self.downloadable_tasks:
            print(f"{image.filename} is ready for download")
            image.download_results(download_location=download_location, callback=self.callback)

    def wait_for_images(self, download_location: str, sleep_time: int = 10, callback: str | None = None,
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


class Image(object):
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

    def __init__(self, drive_root_folder: str = r"G:\My Drive", **kwargs) -> None:
        # TODO: We shouldn't define a default drive root folder. This should always be provided by the user,
        #  but we need to figure out where in the workflow this happens.
        self.task = None
        self.bucket = None
        self._ee_image = None
        self.output_folder = None
        for key in DEFAULTS:  # set the defaults here - this is a nice strategy where we get to define constants near
            # the top that aren't buried in code, then apply them here
            setattr(self, key.lower(), DEFAULTS[key])

        for key in kwargs:  # now apply any provided keyword arguments over the top of the defaults.
            setattr(self, key, kwargs[key])

        self._last_task_status = {
            "state": "NOT-SUBMITTED"}  # this will be the default status initially, so always assume it's NOTsubmitted
        # if we haven't gotten anything from the server. "None" would work too, but then we couldn't just check the
        # status
        self.task_data_downloaded = False
        self.export_type = "Drive"  # other option is "Cloud"

        self.drive_root_folder = drive_root_folder

        self.filename_description = ""

    def _set_names(self, filename_prefix: str = "") -> None:
        self.description = filename_prefix
        self.filename = f"{self.filename_description}_{filename_prefix}"

    def export(self, image: ee.image.Image, filename_prefix: str, export_type: str = "Drive",
               clip: ee.geometry.Geometry = None,
               **export_kwargs) -> None:

        self._ee_image = image

        self._set_names(filename_prefix)

        if clip:  # clip must be a geometry or feature in Earth Engine.
            self._ee_image = self._ee_image.clip(clip)

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

            self.task = ee.batch.Export.image.toDrive(self._ee_image, **ee_kwargs)
            self.task.start()
        elif export_type == "Cloud":
            ee_kwargs[
                'fileNamePrefix'] = f"{self.export_folder}/{ee_kwargs['fileNamePrefix']}"  # add the folder to the
            # filename here for Google Cloud
            self.bucket = ee_kwargs['bucket']
            self.task = ee.batch.Export.image.toCloudStorage(self._ee_image, **ee_kwargs)
            self.task.start()

        self.export_type = export_type

        main_task_registry.add(self)

    def download_results(self, download_location: str, callback: str | None = None) -> None:
        """

		:return:
		"""
        # need an event loop that checks self.task.status(), which
        # will get the current state of the task

        # state options
        # == "CANCELLED", "CANCEL_REQUESTED", "COMPLETED",
        # "FAILED", "READY", "SUBMITTED" (maybe - double check that - it might be that it waits with NOT-SUBMITTED),
        # "RUNNING", "NOT-SUBMITTED"

        folder_search_path = os.path.join(self.drive_root_folder, self.export_folder)
        self.output_folder = os.path.join(download_location, self.export_folder)

        if self.export_type == "Drive":
            download_images_in_folder(folder_search_path, self.output_folder, prefix=self.filename)
        elif self.export_type == "Cloud":
            google_cloud.download_public_export(self.bucket, self.output_folder,
                                                f"{self.export_folder}/{self.filename}")
        else:
            raise ValueError("Unknown export_type (not one of 'Drive', 'Cloud') - can't download")

        self.task_data_downloaded = True

        if callback:
            callback_func = getattr(self, callback)
            callback_func()

    def mosaic(self) -> None:
        self.mosaic_image = os.path.join(self.output_folder, f"{self.filename}_mosaic.tif")
        mosaic_rasters.mosaic_folder(self.output_folder, self.mosaic_image, prefix=self.filename)

    def zonal_stats(self, polygons, keep_fields=("UniqueID", "CLASS2"),
                    stats=('min', 'max', 'mean', 'median', 'std', 'count', 'percentile_10', 'percentile_90'),
                    report_threshold=1000,
                    write_batch_size=2000):

        print(f'Line 249 type is: {type(polygons)}')
        print(f'Line 250 type is: {type(keep_fields)}')
        print(f'Line 251 type is: {type(stats)}')
        print(f'Line 252 type is: {type(report_threshold)}')
        print(f'Line 254 type is: {type(write_batch_size)}')
        """

        :param polygons: :param keep_fields: :param stats: :param report_threshold: After how many iterations should 
        it print out the feature number it's on. Defaults to 1000. Set to None to disable :param write_batch_size: 
        How many zones should we store up before writing to the disk? :return:
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

            zstats_results_geo = rasterstats.gen_zonal_stats(polys_open, self.mosaic_image, stats=stats,
                                                             geojson_out=True, nodata=-9999)

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
                        writer.writerows(
                            results)  # then write the lone result out one at a time to not store it all in RAM
                        results = []

                    if report_threshold and i % report_threshold == 0:
                        print(i)

    def _check_task_status(self) -> dict[str, str]:
        new_status = self.task.status()

        changed = False
        if self._last_task_status != new_status:
            changed = True
            self._last_task_status = new_status

        return {'status': self._last_task_status, 'changed': changed}
