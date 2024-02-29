About EEDL and Installation
==================================

What is EEDL?
---------------
EEDL combines the functionality of what normally would take several packages into one. This makes working with eedl easier and more stream-lined that when working with other modules.

How to install EEDL
-----------------------
The package is still in development and we have not yet published to PyPI (pip) or conda, but have built infrastructure
for both. Current installation is to download the `GitHub repository <https://github.com/water3d/eedl>`_ then run `python setup.py install`

EEDL is tested on Python 3.8-3.11 on Windows and Linux with both standard CPython and Anaconda distributions. EEDL is pure
Python, but depends on GDAL, which has numerous compiled C++ dependencies.

Windows
__________
Windows users may want to use Anaconda, or `see this writeup about installing GDAL and other spatial packages on Windows <https://github.com/nickrsan/spatial_resources/edit/main/installing_spatial_python_windows.md>`_.

Linux
__________
Linux users should follow the `GDAL installation guide <https://pypi.org/project/GDAL/>`_ and 1) Ensure that the gdal-bin and gdal-dev packages are installed and 2) The gdal version they install
for Python matches the gdal version of the system packages (`ogrinfo --version`). We don't pin a version of GDAL to allow
for this workflow. Further details in the GDAL documentation

