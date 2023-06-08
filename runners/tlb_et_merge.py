"""

"""
import pathlib
from string import Template

from ssebop_stats import merge

BASE_FOLDER = pathlib.Path(r"C:\Users\dsx\Box\#SWFTeam\01_SWF_ProjectManagement\Events_and_Visits\UC_Merced_Water_Hack_Challenge_2023\Challenge_Statements\Gas - Evapotranspiration\csv\et")
TEMPLATE_STRING = "mean_et_$year-$year-$month-01--$year-$month-${days}__swf_sseb_monthly_v2_zstats.csv"

template_string = Template(TEMPLATE_STRING)


mapping = [
	(BASE_FOLDER / "2018" / template_string.substitute(year="2018", month="03", days="31"), "2018-03-01"),
	(BASE_FOLDER / "2018" / template_string.substitute(year="2018", month="04", days="30"), "2018-04-01"),
	(BASE_FOLDER / "2018" / template_string.substitute(year="2018", month="05", days="31"), "2018-05-01"),
	(BASE_FOLDER / "2018" / template_string.substitute(year="2018", month="06", days="30"), "2018-06-01"),
	(BASE_FOLDER / "2018" / template_string.substitute(year="2018", month="07", days="31"), "2018-07-01"),
	(BASE_FOLDER / "2018" / template_string.substitute(year="2018", month="08", days="31"), "2018-08-01"),
	(BASE_FOLDER / "2018" / template_string.substitute(year="2018", month="09", days="30"), "2018-09-01"),
	(BASE_FOLDER / "2018" / template_string.substitute(year="2018", month="10", days="31"), "2018-10-01"),
	(BASE_FOLDER / "2019" / template_string.substitute(year="2019", month="03", days="31"), "2019-03-01"),
	(BASE_FOLDER / "2019" / template_string.substitute(year="2019", month="04", days="30"), "2019-04-01"),
	(BASE_FOLDER / "2019" / template_string.substitute(year="2019", month="05", days="31"), "2019-05-01"),
	(BASE_FOLDER / "2019" / template_string.substitute(year="2019", month="06", days="30"), "2019-06-01"),
	(BASE_FOLDER / "2019" / template_string.substitute(year="2019", month="07", days="31"), "2019-07-01"),
	(BASE_FOLDER / "2019" / template_string.substitute(year="2019", month="08", days="31"), "2019-08-01"),
	(BASE_FOLDER / "2019" / template_string.substitute(year="2019", month="09", days="30"), "2019-09-01"),
	(BASE_FOLDER / "2019" / template_string.substitute(year="2019", month="10", days="31"), "2019-10-01"),
	(BASE_FOLDER / "2020" / template_string.substitute(year="2020", month="03", days="31"), "2020-03-01"),
	(BASE_FOLDER / "2020" / template_string.substitute(year="2020", month="04", days="30"), "2020-04-01"),
	(BASE_FOLDER / "2020" / template_string.substitute(year="2020", month="05", days="31"), "2020-05-01"),
	(BASE_FOLDER / "2020" / template_string.substitute(year="2020", month="06", days="30"), "2020-06-01"),
]

df = merge.merge_outputs(mapping, date_field="month_year", sqlite_db=r"C:\Users\dsx\Downloads\et.sqlite", sqlite_table="et")