import os
import re
import requests

# We need to make sure that the version numbers in setup.cfg and __init__.py are the same and that both of them are greater than the most recent version that was pushed to PyPI.

# Get the latest version number from PyPI
latest = requests.get("https://pypi.org/pypi/eedl/json").json()["info"]["version"]

# Get the versions that are listed in __init__.py and setup.cfg
version_number = os.environ.get("VERSION")
setupcfg_version = os.environ.get("SETUPCFG_VERSION")

# Make sure that the local version numbers are the same and that the version number is greater than the one on PyPI
assert version_number == setupcfg_version
assert version_number > latest

"""
This Regular Expression does a few things.
It makes sure the format is x.yyyy.mm.dd where x is any number of consecutive digits.
It makes sure there are no leading 0s except for possibly in the 'x' part of the version number. ie 0.2023.12.9 is valid but 0.2023.12.09 is not valid.
It also checks that the month and day are valid -- month is in [1, 2, 3, ..., 12] and day is in [1, 2, 3, ..., 31].
"""
version_pattern = re.compile(r"^\d*\.\d{4}\.(1[0-2]|[1-9])\.(3[01]|[12][0-9]|[1-9])$")

# Make sure that the version is in the correct format
assert version_pattern.match(version_number)
