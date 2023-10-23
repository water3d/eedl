<img src="https://raw.githubusercontent.com/water3d/eedl/release/docs/source/_static/logo/logo_black.png" alt="EEDL Logo">

# Earth Engine Downloader

EEDL is a Python package that makes downloading and processing of bulk data from Earth Engine feasible and simple.
Current support includes individual image exports, as well as a helper class that will iterate through items in a
filtered ImageCollection and export them all iteratively. 

Many existing workflows exist for downloading areas small enough to fit into a single tile, but this tool
uses Earth Engine's functionality to tile larger and full resolution exports, then download the pieces and
reassemble them, with optional further processing the data using an arbitrary function (zonal statistics tools are included).

Earth Engine's export quotas still apply, especially for EECUs. For academic accounts, they are frequently generous - we have
not tested them on a commercial account.

## Installation
EEDL users should take care to install the dependency on GDAL *before* installing EEDL itself. See below for more information.
After installing GDAL, EEDL is available on PyPI via pip as `python -m pip install eedl`, and can also
be downloaded from the [GitHub releases page](https://github.com/water3d/eedl/releases/).

EEDL is tested on Python 3.8-3.11 on Windows and Linux with both standard CPython and Anaconda distributions. EEDL is pure
Python, but depends on GDAL, which has numerous compiled C++ dependencies where installation varies by platform.

### Windows
To install GDAL, Windows users may want to use Anaconda, or [see this writeup about installing GDAL and other spatial packages on Windows](https://github.com/nickrsan/spatial_resources/edit/main/installing_spatial_python_windows.md).

### Linux
Linux users should follow the [GDAL
installation guide](https://pypi.org/project/GDAL/) and 1) Ensure that the gdal-bin and gdal-dev packages are installed and 2) The gdal version they install
for Python matches the gdal version of the system packages (`ogrinfo --version`). We don't pin a version of GDAL to allow
for this workflow.

## Documentation
Documentation is under development at https://eedl.readthedocs.io. API documentation is most complete, but noisy right
now. We are working on additional details to enable full use of the package.

## Licensing
EEDL is licensed under the MIT license. See <a href="https://github.com/water3d/eedl/blob/main/LICENSE">GitHub's license text and summary</a> for more details of what you can do with it.

## Authors
EEDL has been built by Nick Santos and Adam Crawford as part of the [Secure Water Future](https://securewaterfuture.net) project. This work is supported
by Agriculture and Food Research Initiative Competitive Grant no. 
2021-69012-35916 from the USDA National Institute of Food and Agriculture. EEDL was built in support of [Water3D](https://waterdecisions.app)
