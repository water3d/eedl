"""
	So, let's run a bunch of SSEBOPers as comparisons, then merge to a new image as bands, then get a mean/standard deviation image

	Something like taking the maximum per-pixel ET from June/July, divided by the minimum per-pixel ET for March/April
	 (skip May because might be transitional and obscure low->high signal we expect). Can be confounded in fields that grow
	 a spring crop.

	For 2022, this will get us at whether or not it's fallow (not whether or not it's idle, which requires between-year comparisons)

	Then, stack those by year, get the average and the standard deviation as new images and export, so now we have
	average, standard deviation, and annual rasters. Take 2022 image and compare pixels to see if value is less than
	average minus (1? 2?) stddevs. Then zonal stats for the whole field with 2019 data and see if can find more separation
	between the fallow and not fallow fields.


"""
import time
import os

import ee
from ssebop_stats import ssebop, ee_manager

images = {}

for year in range(2017, 2023):
	runner = ssebop.SSEBOPer()
	runner.export_folder = f"year_comparison_{year}"
	# runner.tile_size = 12800
	runner.comparison(year, "03-01", "05-31", "06-01", "07-31")
	if year in (2020, 2021, 2022):
		runner.export(scale=30)
	images[year] = runner

main_download_folder = r"D:\ET_Summers\_downloads"

multiband_image = ee.Image([images[image].results for image in images])  # make a multiband image across our entire date range
reduced_mean = ee_manager.Image(multiband_image.reduce(ee.Reducer.mean()))
reduced_std = ee_manager.Image(multiband_image.reduce(ee.Reducer.stdDev()))

# do one more where we basically AND the mean together with the June/July ET. If that > 2mm and ratio >

comparisons_folder = "ssebop_comparisons_reduced"
mean_prefix = "reduced_mean"
std_prefix = "reduced_stddev"
export_params = {
			'fileNamePrefix': mean_prefix,
			'description': mean_prefix,
			'folder': f"{comparisons_folder}_mean",
			'scale': 30,
			'maxPixels': 1e12,
			'fileDimensions': 6400,  # multiple of shardSize default 256. Should split into about 9 tiles
			'crs': ssebop.DEFAULTS['CRS']
		}
reduced_mean.export(**export_params)


export_params['fileNamePrefix'] = std_prefix
export_params['description'] = std_prefix
export_params['folder'] = f"{comparisons_folder}_stddev"
reduced_std.export(**export_params)


ssebop.main_task_registry.wait_for_images(main_download_folder, sleep_time=60)
for image in images:  # then, process the images to mosaics locally after EE is exporting everything else
	if hasattr(image, "task"):  # if we exported it
		images[image].mosaic()
	# images[image].zonal_stats()

# then check for if we can download the merged versions and mosaic them
reduced_mean.download(os.path.join(main_download_folder, f"{comparisons_folder}_mean"))
reduced_mean.mosaic()

reduced_std.download(os.path.join(main_download_folder, f"{comparisons_folder}_std"))
reduced_std.mosaic()


"""
	comparisons_folder = "ssebop_comparisons_reduced"
	comparisons_source_folder = os.path.join("G:", "My Drive", comparisons_folder)
	comparisons_output_folder = os.path.join(main_download_folder, comparisons_folder)
	
	
	
	
	
	
	mean_prefix = "reduced_mean"
	std_prefix = "reduced_stddev"
	export_params = {
				'fileNamePrefix': mean_prefix,
				'description': mean_prefix,
				'folder': f"{comparisons_folder}_mean",
				'scale': 30,
				'maxPixels': 1e12,
				'fileDimensions': ssebop.DEFAULTS['TILE_SIZE'],  # multiple of shardSize default 256. Should split into about 9 tiles
				'crs': ssebop.DEFAULTS['CRS']
			}
	export_mean = ee.batch.Export.image.toDrive(reduced_mean, **export_params)
	
	export_params['fileNamePrefix'] = std_prefix
	export_params['description'] = std_prefix
	export_params['folder'] = f"{comparisons_folder}_stddev"
	export_std = ee.batch.Export.image.toDrive(reduced_std, **export_params)
	
	# now wait for and get the mean and standard deviation rasters
	mean_complete = False
	std_complete = False
	while True:
		if not mean_complete:
			status = export_mean.status()
			if status['state'] == "COMPLETE":
				ssebop.download_images_in_folder(source_location=f"{comparisons_source_folder}_mean", download_location=f"{comparisons_output_folder}_mean", prefix=mean_prefix)
				mosaic_rasters.mosaic_folder(f"{comparisons_output_folder}_mean", os.path.join(f"{comparisons_output_folder}_mean", "comparisons_mean_mosaic.tif"))
				mean_complete = True
		if not std_complete:
			status = export_std.status()
			if status['state'] == "COMPLETE":
				ssebop.download_images_in_folder(source_location=f"{comparisons_source_folder}_stddev",
												 download_location=f"{comparisons_output_folder}_stddev", prefix=std_prefix)
				mosaic_rasters.mosaic_folder(f"{comparisons_output_folder}_stddev",
											 os.path.join(f"{comparisons_output_folder}_stddev", "comparisons_std_mosaic.tif"))
				mean_complete = True
	
		if mean_complete and std_complete:
			break
	
		time.sleep(60)
"""
