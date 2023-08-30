import os
import sys

import ee

folder = os.path.dirname(os.path.abspath(__file__))

sys.path.append(os.path.join(os.path.dirname(folder)))

from eedl.helpers import GroupedCollectionExtractor  # noqa: E402

ee.Initialize()

data_geopackage = os.path.join(folder, "data", "test_bound_exports.gpkg")
region_bounds = os.path.join(data_geopackage, "huc8_export_bounds_4326")
field_bounds = os.path.join(data_geopackage, "alfalfa_fields_with_huc8_4326")

extractor = GroupedCollectionExtractor(
	collection="OpenET/ENSEMBLE/CONUS/GRIDMET/MONTHLY/v2_0",
	collection_band="et_ensemble_mad",
	time_start="2019-01-01",
	time_end="2021-12-31",
	mosaic_by_date=True,  # OpenET images are already a single mosaic per date, so we'll just iterate through the images
	areas_of_interest_path=region_bounds,
	strict_clip=True,  # we just want a rectangular BBox clip, not a geometry clip, which can be slower sometimes, but crossing UTM zones created problems without the strict clip - seems like an EE bug. Leave this as True for OpenET exports
	drive_root_folder=r"G:\My Drive",
	export_folder="alfalfa_et_testing",
	download_folder=r"D:\alfalfa_et_testing",
	zonal_run=True,
	zonal_areas_of_interest_attr="huc8",
	zonal_features_path=field_bounds,
	zonal_features_area_of_interest_attr="huc8",
	zonal_features_preserve_fields=("Id", "huc8",),
	zonal_stats_to_calc=("min", "max", "mean", "std", "count"),
	zonal_use_points=False,
)

extractor.extract()
