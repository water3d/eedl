from ssebop_stats import zonal
import subprocess
import os

liq_mapping = {
	2018: r"D:\LIQ\TLB\LIQ_TLB_3310.gdb\liq_2018_tlb_4326_filt",
	2019: r"D:\LIQ\TLB\LIQ_TLB_3310.gdb\liq_2019_tlb_4326_filt",
	2020: r"D:\LIQ\TLB\LIQ_TLB_3310.gdb\liq_2020_tlb_4326_filt",
}

# 'sph': "specific_humidity",

vars = {
	'vpd': "vapor_pressure_deficit",
	'pr': "precipitation_amount",
	'rmin': "relative_humidity",
	'rmax': "relative_humidity",
	'srad': "surface_downwelling_shortwave_flux_in_air",
	'tmmn': "air_temperature",
	'tmmx': "air_temperature",
	'vs': "wind_speed",
	'pet': "potential_evapotranspiration"
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

YEAR_BANDS = {
	2020: 42,
	2019: 41,
	2018: 40,
}

STATS = ('min', 'max', 'mean', 'median', 'std', 'count')

for year in YEAR_BANDS:
	for month in months:
		for var in vars:
			month_numeric = months[month]
			print(f"{year}-{month_numeric}-{var}-{vars[var]}")
			band_id = YEAR_BANDS[year]
			liq_data = liq_mapping[year]

			netcdf_command_part = f"NETCDF:D:\\metdata\\metdata_{var}_monthlyTimeSeries_{month}.nc:{vars[var]}"
			output_dir = f"D:\\metdata\\outputs\\geotiff\\{var}\\{year}"
			output_dir_zonal = f"D:\\metdata\\outputs\\csv\\{var}\\{year}"

			os.makedirs(output_dir, exist_ok=True)
			os.makedirs(output_dir_zonal, exist_ok=True)

			output_filename = f"{output_dir}\\{year}-{month_numeric}-{var}-{vars[var]}-4326.tif"
			#output_filename_3310 = f"{output_dir}\\{year}-{month_numeric}-{var}-{vars[var]}-3310.tif"
			#output_filename_downscaled = f"{output_dir}\\{year}-{month_numeric}-{var}-{vars[var]}-downscaled.tif"
			output_zonal_filename = f"{year}-{month_numeric}-{var}-{vars[var]}-zonal.csv"

			print(["gdal_translate", netcdf_command_part, "-b", str(band_id), "-a_srs", "EPSG:4326", "-of", "GTiff", output_filename])
			subprocess.check_call(["gdal_translate", netcdf_command_part, "-b", str(band_id), "-a_srs", "EPSG:4326", "-of", "GTiff", output_filename])
			#print(["gdalwarp", "-t_srs", "EPSG:3310", output_filename, output_filename_3310])
			#subprocess.check_call(["gdalwarp", "-te", "-120.8 34.9 -118.7 37", "-ts", "5790 7563", output_filename, "-to" "SRC_METHOD=NO_GEOTRANSFORM", output_filename_downscaled])
			zonal.zonal_stats(liq_data, output_filename, output_dir_zonal, output_zonal_filename, stats=STATS, all_touched=True)
