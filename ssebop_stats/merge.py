"""
	A tool to merge separate timeseries outputs into a single data frame or DB table
"""

import sqlite3
import pandas
from datetime import date
from seaborn import objects as so


def merge_outputs(file_mapping, date_field="et_date", sqlite_db=None, sqlite_table=None):
	"""
		Makes output zonal stats files into a data frame and adds a datetime field. Merges all inputs into one DF, and
		can optionally insert into a sqlite database
	:param file_mapping: a set of tuples with a path to a file and a time value (string or datetime) to associate with it.
	:return: pandas data frame with all file data and times
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
			final_df.to_sql(sqlite_table, conn)

	return final_df


def plot_merged(df, et_field, date_field="et_date", uniqueid="UniqueID"):
	plot = (
		so.Plot(df,
				x=date_field,
				y=et_field,
		).add(so.Line(linewidth=0.5, alpha=0.1), group=uniqueid)
		.layout(size=(8, 4))
	)

	return plot
