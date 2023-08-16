import pandas
import pytest  # noqa

from eedl import zonal


def test_zonal_centroids():
	features = r"C:\Users\dsx\CodeLocal\eedl\tests\data\test_vectors.gpkg\test_polys_centroids"
	raster = r"C:\Users\dsx\CodeLocal\eedl\tests\data\_ee_export_test_image.tif"
	output_folder = r"C:\Users\dsx\CodeLocal\eedl\tests\test_outputs"
	filename = "test_results"
	keep_fields = ("UniqueID", "expected_centroid_value")
	stats = ()
	output_csv = zonal.zonal_stats(features, raster, output_folder, filename, keep_fields, stats, use_points=True)

	results = pandas.read_csv(output_csv)
	pandas.testing.assert_series_equal(
		results["value"].astype('float64'),
		results["expected_centroid_value"].astype('float64'),
		check_names=False,
	)
