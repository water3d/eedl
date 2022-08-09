"""
	A temporary script to work with ArcGIS tables that we're exporting to CSVs so that we can reduce decimal
	precision. Actually run this code via Python Notebooks in ArcGIS Pro (so have access to arcpy).
"""

import arcpy
import pandas as pd


def table_to_data_frame(in_table, input_fields=None, where_clause=None):
	"""Function will convert an arcgis table into a pandas dataframe with an object ID index, and the selected
	input fields using an arcpy.da.SearchCursor.

	Via https://gist.github.com/d-wasserman/e9c98be1d0caebc2935afecf0ba239a0?permalink_comment_id=3000219#gistcomment-3000219
	"""
	OIDFieldName = arcpy.Describe(in_table).OIDFieldName
	if input_fields:
		final_fields = [OIDFieldName] + input_fields
	else:
		final_fields = [field.name for field in arcpy.ListFields(in_table)]
	data = [row for row in arcpy.da.SearchCursor(in_table, final_fields, where_clause=where_clause)]
	fc_dataframe = pd.DataFrame(data, columns=final_fields)
	fc_dataframe = fc_dataframe.set_index(OIDFieldName, drop=True)
	return fc_dataframe


def export_arcgis_table_to_csv(input_path, output_path):
	df = table_to_data_frame(input_path)

	df.to_csv(output_path, float_format=".3f")
