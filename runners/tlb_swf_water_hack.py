from ssebop_stats import ssebop
import os
import sys
import ee

images = []

SKIP_EXPORT = True
ZONAL_ONLY = False
DOWNLOAD_LOCATION = r"D:\ET_TLB_SWF"

months = (
	("03-01", "03-31"),
	("04-01", "04-30"),
	("05-01", "05-31"),
	("06-01", "06-30"),
	("07-01", "07-31"),
	("08-01", "08-31"),
	("09-01", "09-30"),
	("10-01", "10-31"),
)

liq_mapping = {
	2018: r"D:\LIQ\TLB\LIQ_TLB_3310.gdb\liq_2018_tlb_3310_filt",
	2019: r"D:\LIQ\TLB\LIQ_TLB_3310.gdb\liq_2019_tlb_3310_filt",
	2020: r"D:\LIQ\TLB\LIQ_TLB_3310.gdb\liq_2020_tlb_3310_filt",
}

for year in (2020,): #, 2018, 2019, 2020):
	for month in months:
		runner = ssebop.SSEBOPer()
		runner.year = year
		runner.pixel_reducer = "mean"
		runner.study_area = ee.FeatureCollection("users/nrsantos/tlb_boundary").geometry()
		runner.run(year, month[0], month[1])

		prefix = "swf_sseb_monthly_v2"

		if SKIP_EXPORT:
			runner._set_names(prefix)
			if not ZONAL_ONLY:
				# when resuming where export succeeded, but download failed - this will set everything up correctly, and skip the export
				runner.download_results(DOWNLOAD_LOCATION, callback="mosaic")
			else:
				runner.output_folder = os.path.join(DOWNLOAD_LOCATION, runner.export_folder)
				runner.mosaic_image = os.path.join(runner.output_folder, f"{runner.filename}_mosaic.tif")
		else:
			runner.export(filename_prefix=prefix, clip=runner.study_area, skipEmptyTiles=True)

		images.append(runner)


if not ZONAL_ONLY:
	# wait for images to download. After download, immediately call the image's mosaic function before downloading the next image
	ssebop.main_task_registry.wait_for_images(download_location=DOWNLOAD_LOCATION, sleep_time=60, callback="mosaic")


for image in images:
	print(f"running zonal stats for {image.filename} against LIQ {image.year}")
	image.zonal_stats(liq_mapping[image.year], keep_fields=("UniqueID", "CLASS2", "ACRES"))
