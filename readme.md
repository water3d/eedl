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
* [Still in progress] Download the data from Google Drive (or GCloud)
* Mosaic the raster pieces together into a single raster, with overviews
* [Still in progress] Run zonal statistics on the output

Documentation, docstrings, test, etc are all still missing