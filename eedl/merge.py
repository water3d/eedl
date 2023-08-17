"""
	A tool to merge separate timeseries outputs into a single data frame or DB table
"""
import sqlite3
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

	:param file_mapping: A set of tuples with a path to a file and a time value (string or datetime) to associate with it.
	:type file_mapping:
	:param date_field: Defaults to "et_date".
	:type date_field: str
	:param sqlite_db: Name of a sqlite database.
	:type sqlite_db: Optional[str]
	:param sqlite_table: Name of a table in the database.
	:type sqlite_table: Optional[str]
	:return: Pandas data frame with all file and time data.
	:rtype: pandas.DataFrame
	"""

	dfs = []
	for mapping in file_mapping:
		path = mapping[0]
		time_value = mapping[1]

		df = pandas.read_csv(path)
		df.loc[:, date_field] = time_value
		dfs.append(df)

	# merge all the data frames together
	final_df = pandas.concat(dfs)
	final_df.reset_index(inplace=True)

	if sqlite_db:
		with sqlite3.connect(sqlite_db) as conn:
			final_df.to_sql(str(sqlite_table), conn)

	return final_df


def plot_merged(df: pandas.DataFrame, et_field: str, date_field: str = "et_date", uniqueid: str = "UniqueID") -> so.Plot:
	"""

	:param df: Data source for the plot
	:type df: pandas.DataFrame
	:param et_field: Name of the variable on the x-axis
	:type et_field: str
	:param date_field: Name of the variable on the y-axis. Default is "et_date"
	:type date_field: str
	:param uniqueid: Defines additional data subsets that transforms should operate on independently. Default is "UniqueID"
	:type uniqueid: str
	:return: Returns a seaborn object plot
	:rtype: so.Plot
	"""
	return (
		so.Plot(df,
				x=date_field,
				y=et_field,
				).add(so.Line(linewidth=0.5, alpha=0.1), group=uniqueid)
		.layout(size=(8, 4))
	)