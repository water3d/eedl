## Set up data information
liq_mapping = {
	2018: r"D:\LIQ\TLB\LIQ_TLB_3310.gdb\liq_2018_tlb_3310_filt",
	2019: r"D:\LIQ\TLB\LIQ_TLB_3310.gdb\liq_2019_tlb_3310_filt",
	2020: r"D:\LIQ\TLB\LIQ_TLB_3310.gdb\liq_2020_tlb_3310_filt",
}

#    'sph': "specific_humidity",  # data only through 2017
#    'pr': "precipitation_amount",
#    'tmmn': "air_temperature",
#    'rmin': "relative_humidity",


# already complete
#   'pet': "potential_evapotranspiration",
#   'tmmx': "air_temperature",
#   'vs': "wind_speed",
#   'vpd': "vapor_pressure_deficit",

VARS = {
	'rmax': "relative_humidity",
	'srad': "surface_downwelling_shortwave_flux_in_air",
}
months = {
	"Sept": "09",
	"Oct": "10",
	"May": "05",
	"Mar": "03",
	"Jun": "06",
	"Jul": "07",
	"Aug": "08",
	"Apr": "04"
}

YEARS = [2018, 2019, 2020]

TLB_BOUNDARY = r"C:\Users\dsx\Documents\ArcGIS\Projects\LandIQ\tlb_boundary.shp"




import arcpy
import datetime

arcpy.env.overwriteOutput = True
arcpy.env.workspace = r"D:\metdata\arcgis_metdata_extraction\Default.gdb"

# make the Tulare Lake Basin data a feature layer so we only load it up once for speed
tlb_boundary = "tlb_boundary"
arcpy.MakeFeatureLayer_management(TLB_BOUNDARY, tlb_boundary)

liq_feature_layers = {
    2018: "liq_2018",
    2019: "liq_2019",
    2020: "liq_2020",
}

for year in liq_feature_layers:
    arcpy.management.MakeFeatureLayer(liq_mapping[year], liq_feature_layers[year])

TLB_BOUNDARY = r"C:\Users\dsx\Documents\ArcGIS\Projects\LandIQ\tlb_boundary.shp"

import os

is_first = False  # set to True if restarting processing

for var in VARS:  # go by variable since problems are likely to crop up by variable
	for month in months:  # then go by month because the data are in files by month, where we'll extract a slice by year
       
		for year in YEARS:  # then go by year      
			month_numeric = months[month]
            
            # we're restarting here
			if var == "rmax" and month_numeric not in ("04", "08"):  # "07",
				continue
            
			#if var == "rmax" and month_numeric == "07" and year != 2020:
			#	continue

			print(f"{datetime.datetime.now().strftime('%H:%M:%S')}: {year}-{month_numeric}-{var}-{VARS[var]}")
			liq_data = liq_feature_layers[year]

			input_filename = f"D:\\metdata\\metdata_{var}_monthlyTimeSeries_{month}.nc"

			output_dir = f"D:\\metdata\\outputs\\geotiff\\{var}\\{year}"
			output_dir_zonal = f"D:\\metdata\\outputs\\csv\\{var}\\{year}"

			os.makedirs(output_dir, exist_ok=True)
			os.makedirs(output_dir_zonal, exist_ok=True)

			output_filename = f"{output_dir}\\{year}-{month_numeric}-{var}-{VARS[var]}.tif"
			output_zonal_gdb_table_name = f"zonal_{year}_{month_numeric}_{var}"
			output_zonal_filename = f"zonal_{year}_{month_numeric}_{var}_{VARS[var]}.csv"

			multidimensional_layer = "multidimensional_layer"
			# 1 make it into its own layer
			arcpy.md.MakeMultidimensionalRasterLayer(input_filename, multidimensional_layer, VARS[var], "BY_VALUE",
													 None, f"StdTime {year}-{month_numeric}-01T00:00:00", '', '', '',
													 None, '',
													 '-124.787499966667 25.0458333333333 -67.0374999666667 49.4208333333333 GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]]',
													 "NO_DIMENSIONS", None)

			# 2 extract by mask and make it a 30m Teale Albers raster
			with arcpy.EnvManager(outputCoordinateSystem=arcpy.SpatialReference(3310)):
				extracted = arcpy.sa.ExtractByMask(multidimensional_layer, tlb_boundary)
				extracted.save(output_filename)
				arcpy.management.Delete(multidimensional_layer)

			# 3 Zonal Stats
			with arcpy.EnvManager(
					cellSize=30):  # force all of this to happen at 30m so we don't store the raster at 30, but can make sure the LIQ data processes correctly. It's a lot of overhead, but will prevent scale issues
				arcpy.sa.ZonalStatisticsAsTable(liq_data, "UniqueID", output_filename, output_zonal_gdb_table_name, "DATA", "ALL")

				# add the variable and the date to the table so that combined sets of tables can be used
				arcpy.management.CalculateField(output_zonal_gdb_table_name, "year_month",
												f"\"{year}-{month_numeric}-01\"", field_type="TEXT")
				arcpy.management.CalculateField(output_zonal_gdb_table_name, "data_variable", f"\"{var}\"",
												field_type="TEXT")

				# don't join the data for crop class back for 2020 because we are withholding that for prediction
				if year != 2020:
					arcpy.management.AddIndex(output_zonal_gdb_table_name, ['UniqueID'], "idx_UniqueID", "NON_UNIQUE", "NON_ASCENDING")
					arcpy.management.JoinField(output_zonal_gdb_table_name, "UniqueID", liq_data, "UniqueID", ["CLASS2", ])
				else:
					arcpy.management.AddField(output_zonal_gdb_table_name, "CLASS2", "TEXT")

				arcpy.conversion.TableToTable(output_zonal_gdb_table_name, output_dir_zonal, output_zonal_filename)

			# now update the main dataset we'll use for a SQL table
			all_data_name = "zonal_all_years_and_vars"
			if is_first:
				arcpy.conversion.TableToTable(output_zonal_gdb_table_name, r"D:\metdata\arcgis_metdata_extraction\Default.gdb", all_data_name)
			else:
				arcpy.management.Append(output_zonal_gdb_table_name, all_data_name)
			is_first = False

			if arcpy.Exists("extracted"):
				arcpy.management.Delete("extracted")


# cleanup

arcpy.management.Delete(tlb_boundary)
for year in liq_feature_layers:
    arcpy.management.Delete(liq_feature_layers[year])