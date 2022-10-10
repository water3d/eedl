import os

from ssebop_stats import ssebop

search_folder = r"D:\ET_Summers\ee_exports_alvar\et_exports_sseboper"
polys = r"C:\Users\dsx\Documents\ArcGIS\Projects\Summer ET Processing\Summer ET Processing.gdb\liq_2019_cv_3310"
keep_fields = ("UniqueID", "CLASS2", "ACRES")

mosaics = [os.path.join(search_folder, filename) for filename in os.listdir(search_folder) if filename.endswith("_mosaic.tif")]

for mosaic in mosaics:
	print(mosaic)
	s = ssebop.SSEBOPer()
	s.mosaic_image = mosaic
	s.output_folder = r"D:\ET_Summers\ee_exports_alvar\et_exports_sseboper"
	s.filename = os.path.splitext(os.path.split(mosaic)[1])[0]
	s.zonal_stats(polys,
				  keep_fields=keep_fields)