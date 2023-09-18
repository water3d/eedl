from eedl.image import EEDLImage, main_task_registry  # used EEDL code at commit 384183d024a9bd699514852141251467de9b5c9f
import ee

ee.Initialize()


def get_days(month, year):
	days = {
		'01': 31,
		'02': 28 if not year == 2020 else 29,
		'03': 31,
		'04': 30,
		'05': 31,
		'06': 30,
		'07': 31,
		'08': 31,
		'09': 30,
		'10': 31,
		'11': 30,
		'12': 31
	}

	return days[month]


years = (2019, 2020, 2021)
months = ("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12")
collection = "IDAHO_EPSCOR/GRIDMET"
boundary = ee.FeatureCollection("users/nrsantos/swf/western_us_extraction_bound_v2").first().geometry()
band = "eto"
zonal_features_path = r"C:\Users\dsx\Box\#SWFTeam\15_Cross-Cutting_Research\Alfalfa_Flex_Paper\Data\CDL_Alfalfa_Field_Products.gdb\workflow_outputs.gdb\alfalfa_all_years_focal_sum_shrink_polys_only_whuc8_4326"


if __name__ == "__main__":
	for year in years:
		for month in months:
			length = get_days(month, year)  # how many days are in the month
			images = ee.ImageCollection(collection).select(band).filterBounds(boundary)
			filtered = images.filter(ee.Filter.date(f"{year}-{month}-01T00:00", f"{year}-{month}-{length}T23:00"))
			mosaic = filtered.sum()

			export = EEDLImage(
				drive_root_folder=r"G:\My Drive",
				filename_description="gridmet_eto",
				zonal_inject_constants={"date": f"{year}-{month}-01"},
				zonal_polygons=zonal_features_path,
				zonal_keep_fields=("Id", "huc8",),
				zonal_stats_to_calc=("min", "max", "mean", "std", "count"),
				zonal_use_points=False,
				zonal_all_touched=True,
				scale=4638.3,  # value for approximate GRIDMET scale from Earth Engine data catalog.
			)
			export.export(mosaic, filename_suffix=f"{year}_{month}", export_type="drive", clip=boundary)

	main_task_registry.wait_for_images(r"D:\gridmet_eto", callback="mosaic_and_zonal")
