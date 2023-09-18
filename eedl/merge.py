"""
	A tool to merge separate timeseries outputs into a single data frame or DB table
"""
import sqlite3
import os
from typing import Optional

import pandas
from seaborn import objects as so


def merge_outputs(file_mapping,
					date_field: str = "et_date",
					sqlite_db: Optional[str] = None,
					sqlite_table: Optional[str] = None) -> pandas.DataFrame:
	"""
	Makes output zonal stats files into a data frame and adds a datetime field. Merges all inputs into one DF, and
	can optionally insert into a sqlite database.

	Args:
		file_mapping: A set of tuples with a path to a file and a time value (string or datetime) to associate with it.
		date_field (str): Defaults to "et_date".
		sqlite_db (Optional[str]): Name of a sqlite database.
		sqlite_table (Optional[str]): Name of a table in the database.

	Returns:
		pandas.DataFrame: Pandas data frame with all file and time data.
	"""

	dfs = []
	for mapping in file_mapping:
		path = mapping[0]
		time_value = mapping[1]

		df = pandas.read_csv(path)
		df.loc[:, date_field] = time_value
		dfs.append(df)

	# Merge all the data frames together
	final_df = pandas.concat(dfs)
	final_df.reset_index(inplace=True)

	if sqlite_db:
		with sqlite3.connect(sqlite_db) as conn:
			final_df.to_sql(str(sqlite_table), conn)

	return final_df


def plot_merged(df: pandas.DataFrame, et_field: str, date_field: str = "et_date", uniqueid: str = "UniqueID") -> so.Plot:
	"""
	Creates a seaborn plot of the data

	Args:
		df (pandas.DataFrame): Data source for the plot.
		et_field (str): Name of the variable on the x-axis.
		date_field (str): Name of the variable on the y-axis. Default is "et_date".
		uniqueid (str): Defines additional data subsets that transforms should operate on independently. Default is "UniqueID".

	Returns:
		so.Plot: Returns a seaborn object plot.
	"""
	return (
		so.Plot(df,
				x=date_field,
				y=et_field,
				).add(so.Line(linewidth=0.5, alpha=0.1), group=uniqueid)
		.layout(size=(8, 4))
	)


def merge_csvs_in_folder(folder_path, output_path, sqlite_db=None, sqlite_table=None):
	if sqlite_db and not sqlite_table:
		raise ValueError("Cannot insert into sqlite db without table name")

	csvs = [item for item in os.listdir(folder_path) if item.endswith(".csv")]

	dfs = []
	for csv in csvs:
		print(csv)
		df = pandas.read_csv(os.path.join(folder_path, csv))
		df.drop(columns="index", inplace=True, errors="ignore")
		dfs.append(df)

	# merge all the data frames together
	final_df = pandas.concat(dfs)
	final_df.reset_index(drop=True, inplace=True)

	if output_path:
		final_df.to_csv(output_path)

	if sqlite_db:
		with sqlite3.connect(sqlite_db) as conn:
			final_df.to_sql(str(sqlite_table), conn)

	return final_df


def merge_many(base_folder, subfolder_name="alfalfa_et"):
	output_folder = os.path.join(base_folder, "merged_csvs")
	os.makedirs(output_folder, exist_ok=True)

	folders = [folder for folder in os.listdir(base_folder) if os.path.isdir(os.path.join(base_folder, folder))]
	for folder in folders:
		if folder == "merged_csvs":
			continue
		print(folder)
		output_file = os.path.join(output_folder, f"{folder}.csv")
		input_folder = os.path.join(base_folder, folder, subfolder_name)
		merge_csvs_in_folder(input_folder, output_file)

	print("Merging all CSVs")
	mega_output_csv = os.path.join(output_folder, "_all_csvs_merged.csv")
	mega_output_sqlite = os.path.join(output_folder, "_all_data_merged.sqlite")
	sqlite_table = "merged_data"
	merge_csvs_in_folder(output_folder, mega_output_csv, mega_output_sqlite, sqlite_table)
	print("Done")
