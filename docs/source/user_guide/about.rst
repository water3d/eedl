About EEDL and Installation
==================================

What is EEDL?
---------------
EEDL provides simple functionality to download small or large volumes of data from Earth Engine. It combines the functionality
several packages into one, making eedl easier and more stream-lined than working with many individual components to download data from Earth Engine.

Required Access and Configuration
------------------------------------
In addition to installing the Python package for EEDL (below), EEDL requires the following:

1. An Earth Engine Account:
    You need to have an active account on Google Earth Engine.
2. You need to authenticate your Earth Engine account in the copy of Python you use for EEDL
    EEDL will attempt to detect that Earth Engine isn't authenticated and start the sign-in flow,
    but you may find it easier to handle manually before running EEDL for the first time. See
    the `Earth Engine documentation on auth <https://developers.google.com/earth-engine/guides/auth>`_ for more.
    EEDL will install the Earth Engine client for you though - no need to install it separately, but you
    will need to authenticate it after installing EEDL if you have not already done so.
3. Either a Google Cloud Storage bucket in the same Google Account, configured for public reads *or* the Google Drive client installed on the same device as EEDL.
    EEDL tracks and downloads pieces of your images through the export process, but it can currently still only use
    the options Earth Engine has available for exports - that is, Earth Engine can export to Google Cloud Storage buckets,
    or to Google Drive. See our `documentation on export locations <ExportLocations>`_ for more considerations, but you need at least one
    of these items in order to use EEDL.

How to install EEDL
-----------------------
The package is still in development and we have not yet published to PyPI (pip) or conda, but have built infrastructure
for both. Current installation is to download the `GitHub repository <https://github.com/water3d/eedl>`_ then run `python setup.py install`

EEDL is tested on Python 3.8-3.12 on Windows and Linux with both standard CPython and Anaconda distributions. EEDL is pure
Python, but depends on GDAL, which has numerous compiled C++ dependencies. We have been told it works on MacOS and plan
to provide an example of using it in Google Colab notebooks as well.

Windows
__________
Windows users may want to use Anaconda, or `see this writeup about installing GDAL and other spatial packages on Windows <https://github.com/nickrsan/spatial_resources/edit/main/installing_spatial_python_windows.md>`_.

After installing GDAL, EEDL can be installed into your environment from PyPI with :code:`python -m pip install eedl`.

Linux
__________
Linux users should follow the `GDAL installation guide <https://pypi.org/project/GDAL/>`_ and 1) Ensure that the gdal-bin and gdal-dev packages are installed and 2) The gdal version they install
for Python matches the gdal version of the system packages (:code:`ogrinfo --version`). We don't pin a version of GDAL to allow
for this workflow. Further details in the GDAL documentation.

After installing GDAL, EEDL can be installed into your environment from PyPI with :code:`python -m pip install eedl`.

