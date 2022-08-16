OK, so, here's the plan

1. Develop SSEBOP function that does monthlies/seasonal exports to Google Cloud
2. A second function that downloads all the data from GCloud for a given monthly/seasonal
   1. Honestly, it might be easier to still just export to Google Drive and then require
   that the user have Google Drive installed on the computer for now, and this will
    just move data from the export folder
3. Mosaicks the data using GDAL and exports a version with overviews/pyramids
4. Runs zonal stats if we have LandIQ data for the relevant year

Current status is that we have pieces to:
* Run the Earth Engine code and export rasters to Google Drive
* Detect if the raster export is complete
* [Testing] Download the data from Google Drive (or GCloud)
* Mosaic the raster pieces together into a single raster, with overviews
* [Testing] Run zonal statistics on the output

Documentation, docstrings, test, etc are all still missing

# Example Usage
The following example shows some of the places you can customize usage of this code, and how to use it.

The code would export a single raster per year from 1984 - 2021 and run zonal statistics for user-provided polygons.
The rasters would cover May-October of each year, with the pixels being the mean value of observations. It would use
Landsat 5, 7, and 8 data.

```python

import ee
from ssebop_stats import ssebop

images = []

DOWNLOAD_FOLDER = r""  # set the download folder here
ZONAL_POLYGONS_PATH = r""  # path to your zone polygons
KEEP_POLYGON_FIELDS = ("UniqueID", "CLASS2")  # what fields in the zonal polygons should be propogated to the output? These are examples for landIQ, but set your own

for year in range(1984, 2022):  # range isn't inclusive of the end digit, so 2022 gets us years through 2021
	time_range = ("05-01", "10-31")
	runner = ssebop.SSEBOPer(drive_root_folder=r"G:\My Drive")  # make sure to set the path to your Google Drive folder on your computer here
	runner.pixel_reducer = 'mean'  # doing mean instead of sum because I'm not 100% sure yet that sum will actually get the total sum - mean could be later transformed to a total value in postprocessing
	runner.collections = ['LANDSAT/LC08/C02/T1_L2', 'LANDSAT/LE07/C02/T1_L2m' 'LANDSAT/LT05/C02/T1_L2']  # set it to use landsat 8, 7, and 5
	runner.study_area = ee.FeatureCollection('path/to/your/asset/in/earth_engine/that/is/your/area/of/interest').geometry()  # make sure to upload a study are boundary and put the path here
	runner.export_folder = f"ssebop_exports_{year}"
    
	runner.run(year, time_range[0], time_range[1])  # set up the earth engine processing
	runner.export(scale=180)  # could also specify crs as an EE compatible item here - supports all keyword args to ee.batch.Export.image.toDrive
	images.append(runner)
	

# now that all the exports are set up, wait for the exports, which will download the files locally.
ssebop.main_task_registry.wait_for_images(download_location=DOWNLOAD_FOLDER, sleep_time=60)

for image in images:
	image.mosaic()  # will turn the tiles for each year back into a full image
	image.zonal_stats(ZONAL_POLYGONS_PATH, keep_fields=KEEP_POLYGON_FIELDS)  # run the zonal stats for each raster

```