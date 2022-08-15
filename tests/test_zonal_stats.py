import os
import pytest

from ssebop_stats import ssebop


def test_zstats_on_existing_export():
	s = ssebop.SSEBOPer()
	s.run(2022, "07-01", "07-30")
	s._set_names()
	s.output_folder = os.path.join(r"D:\ET_Summers\ee_exports_monthly", s.export_folder)
	polygons = r"C:\Users\dsx\Downloads\liq_2019.gpkg\liq_2019"
	s.mosaic_image = os.path.join(s.output_folder, f"{s.filename}_mosaic.tif")
	s.zonal_stats(polygons)
