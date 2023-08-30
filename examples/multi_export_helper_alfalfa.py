import os

from eedl.helpers import GroupedCollectionExtractor

folder = os.path.dirname(os.path.abspath(__file__))
data_geopackage = os.path.join(folder, "data", "test_bound_exports.gpkg")
region_bounds = os.path.join(data_geopackage, "huc8_export_bounds")
field_bounds = os.path.join(data_geopackage, "alfalfa_fields_with_huc8")

extractor = GroupedCollectionExtractor(
	collection="OpenET/ENSEMBLE/CONUS/GRIDMET/MONTHLY/v2_0",
	collection_band="et_ensemble_mad",
	time_start="2019-01-01",
	time_end="2021-12-31",
	mosaic_by_date=False,  # OpenET images are already a single mosaic per date, so we'll just iterate through the images
	areas_of_interest_path=region_bounds,
	strict_clip=False,  # we just want a rectangular BBox clip, not a geometry clip, which can be slower sometimes
	drive_root_folder=r"G:\My Drive",
	export_folder="alfalfa_et_testing",
	download_folder=r"D:\alfalfa_et_testing",
	zonal_run=True,
	zonal_areas_of_interest_attr="huc8",
	zonal_features_path=field_bounds,
	zonal_features_area_of_interest_attr="huc8",
	zonal_features_preserve_fields=("huc8", "ACRES"),
	zonal_stats_to_calc=("min", "max", "mean", "std", "count"),
	zonal_use_points=False,
)
