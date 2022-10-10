from ssebop_stats import ssebop
import os
import sys
import ee

# tester = SSEBOPer()

images = []

# for year in (2020, 2021, 2022):
#	for month in (("05-01", "05-31"), ("06-01", "06-30"), ("07-01", "07-31"), ("08-01", "08-31")):
#		if not (year == 2022 and month[0] == "08-01"):  # skip august for now in 2022
#			runner = SSEBOPer()
#			runner.run(year, month[0], month[1])
#			runner.export()
#			images.append(runner)

# runner = SSEBOPer()
# runner.run(2022, "07-01", "07-31")
# runner.export()
# images.append(runner)

SKIP_EXPORT = True
ZONAL_ONLY = True

for year in (2022, 2021, 2019, 2018, 2020, 2017, 2016, 2015): # 2014): #, 2013):
	runner_mayaug = ssebop.SSEBOPer()
	runner_mayaug.study_area = ee.FeatureCollection("projects/ucm-fallow-training/assets/ag_regions_outside_cv").geometry()
	runner_mayaug.pixel_reducer = "mean"
	runner_mayaug.run(year, "05-01", "08-31")

	runner_mayjul = ssebop.SSEBOPer()
	runner_mayjul.study_area = ee.FeatureCollection("projects/ucm-fallow-training/assets/ag_regions_outside_cv").geometry()
	runner_mayjul.pixel_reducer = "mean"
	runner_mayjul.run(year, "05-01", "07-31")

	if SKIP_EXPORT:
		runner_mayaug._set_names("water_balance_may_aug_mean_non_cv")
		runner_mayjul._set_names("water_balance_may_jul_mean_non_cv")
		if not ZONAL_ONLY:
			# when resuming where export succeeded, but download failed - this will set everything up correctly, and skip the export
			runner_mayaug.download_results(r"D:\ET_Summers\ee_exports_water_balance_non_cv", callback="mosaic")
			runner_mayjul.download_results(r"D:\ET_Summers\ee_exports_water_balance_non_cv", callback="mosaic")
		else:
			runner_mayaug.output_folder = os.path.join("D:\ET_Summers\ee_exports_water_balance_non_cv", runner_mayaug.export_folder)
			runner_mayaug.mosaic_image = os.path.join(runner_mayaug.output_folder, f"{runner_mayaug.filename}_mosaic.tif")
			runner_mayjul.output_folder = os.path.join("D:\ET_Summers\ee_exports_water_balance_non_cv", runner_mayjul.export_folder)
			runner_mayjul.mosaic_image = os.path.join(runner_mayjul.output_folder, f"{runner_mayjul.filename}_mosaic.tif")
	else:
		runner_mayaug.export(filename_prefix="water_balance_may_aug_mean_non_cv")
		runner_mayjul.export(filename_prefix="water_balance_may_jul_mean_non_cv")
	images.append(runner_mayaug)
	images.append(runner_mayjul)


if not ZONAL_ONLY:
	# wait for images to download. After download, immediately call the image's mosaic function before downloading the next image
	ssebop.main_task_registry.wait_for_images(download_location=r"D:\ET_Summers\ee_exports_august_prelims", sleep_time=60, callback="mosaic")


# do all the mosaicking before any zonal stats since those are slow
#for image in images:
#	print(f"mosaicking {image.filename}")
#	image.mosaic()

for image in images:
	print(f"running zonal stats for {image.filename}")
	image.zonal_stats(r"C:\Users\dsx\Documents\ArcGIS\Projects\Summer ET Processing\Summer ET Processing.gdb\liq_2019_non_cv_3310",
					  keep_fields=("UniqueID", "CLASS2", "ACRES"))

# tester.export()
# tester.download_results()