import os
from typing import Iterable
import ee
from ee import ImageCollection

from eedl.image import EEDLImage
import eedl

ee.Initialize()


def scv_data_download_for_year(year: str, openet_collection: str = r"OpenET/ENSEMBLE/CONUS/GRIDMET/MONTHLY/v2_0", band: str = "et_ensemble_mad") -> Iterable[EEDLImage, EEDLImage]:
	geometry = ee.FeatureCollection("users/nrsantos/vw_extraction_mask").geometry()

	# so, we need two images per year - one is for all months, the other is for just the winter months
	annual_collection = ImageCollection(openet_collection).filterBounds(geometry).filterDate(f"{year}-01-01", f"{year}-12-31").select(band)

	# for the winter image, we need two date ranges within the same year - Jan-Mar and December. Do that by getting two separate collections and merging them
	winter_collection = ImageCollection(openet_collection).filterBounds(geometry).filterDate(f"{year}-01-01", f"{year}-03-31") \
							.merge(ImageCollection(openet_collection).filterBounds(geometry).filterDate(f"{year}-12-01", f"{year}-12-31")) \
							.select(band)

	# Earth Engine errors out if we try to sum the collections without converting the images into Doubles first, which is too bad because they'd be very efficient otherwise
	annual_collection_doubles = annual_collection.map(lambda image: image.toDouble())
	winter_collection_doubles = winter_collection.map(lambda image: image.toDouble())

	# The docs note that the ET in this collection is "total ET by month as an equivalent depth of water in millimeters."
	# so it's the mean of the models' depths for the whole month. We can sum those safely to get annual totals.
	# now we need to flatten the collections into images we can export, so we'll sum the whole collections

	annual_image = annual_collection_doubles.sum()
	winter_image = winter_collection_doubles.sum()

	# export the annual image and queue it for download
	annual_export_image = EEDLImage(crs="EPSG:4326")
	annual_export_image.export(annual_image,
								filename_suffix=f"valley_water_ensemble_total_et_mm_{year}",
								export_type="Drive",
								drive_root_folder=r"G:\My Drive",
								clip=geometry,
								folder="vw_et_update_2023"
							)

	winter_export_image = EEDLImage(crs="EPSG:4326")
	winter_export_image.export(winter_image,
								filename_suffix=f"valley_water_ensemble_winter_et_mm_{year}",
								export_type="Drive",
								drive_root_folder=r"G:\My Drive",
								clip=geometry,
								folder="vw_et_update_2023"
							)

	return annual_export_image, winter_export_image
	# return (annual_export_image, )


folder_path = os.path.dirname(os.path.abspath(__file__))
field_boundaries_by_year = {
								"2018": os.path.join(folder_path, r"data\liq_field_centroids_by_year.gpkg\fields_liq_centroids_2018_wgs84"),
								"2019": os.path.join(folder_path, r"data\liq_field_centroids_by_year.gpkg\fields_liq_centroids_2019_wgs84"),
								"2020": os.path.join(folder_path, r"data\liq_field_centroids_by_year.gpkg\fields_liq_centroids_2020_wgs84")
							}


def download_updated_vw_et_images_by_year(download_folder: str = r"D:\vw_et_update_2023",
											field_boundaries: dict[str: str] = field_boundaries_by_year) -> None:

	exports_by_year = {}

	print("Running exports")
	for year in ("2018", "2019", "2020"):
		results = scv_data_download_for_year(year)
		exports_by_year[year] = results

	print("Waiting for downloads and mosaicking")
	eedl.image.main_task_registry.wait_for_images(download_folder, sleep_time=60, callback="mosaic")

	# now we need to run the zonal stats
	print("Running Zonal Stats")
	for year in exports_by_year:
		for image in exports_by_year[year]:
			image.zonal_stats(field_boundaries[year], keep_fields=("UniqueID", "CLASS2", "ACRES"), stats=(), use_points=True)


if __name__ == "__main__":
	download_updated_vw_et_images_by_year()
