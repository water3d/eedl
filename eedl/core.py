import os
from pathlib import Path
from typing import Union, Dict

import fiona


def _get_fiona_args(polygon_path: Union[str, Path]) -> Dict[str, Union[str, Path]]:
	"""
	A simple utility that detects if, maybe, we're dealing with an Esri File Geodatabase. This is the wrong way
	to do this, but it'll work in many situations.

	:param polygon_path: File location of polygons.
	:type polygon_path: Union[str, Path]
	:return: Returns the full path and, depending on the file format, the file name in a dictionary.
	:rtype: Dict[str, Union[str, Path]]
	"""

	parts = os.path.split(polygon_path)
	# if the folder name ends with .gdb and the "filename" doesn't have an extension, assume it's an FGDB
	if (parts[0].endswith(".gdb") or parts[0].endswith(".gpkg")) and "." not in parts[1]:
		return {'fp': parts[0], 'layer': parts[1]}
	else:
		return {'fp': polygon_path}


def safe_fiona_open(features_path: Union[str, Path], **extra_kwargs) -> fiona.Collection:
	"""
		Handles opening things in fiona in a way that is safe, even for geodatabases where we need
		to open the geodatabase itself and specify a layer. The caller is responsible for
		ensuring the features are closed (e.g. a try/finally block with a call to features.close()
		in the finally block should immediately follow calling this function.
	:param features_path: A Path object or string path to open with fiona
	:param extra_kwargs: Keyword arguments to directly pass through to fiona. Helpful when trying to filter features, etc
	:return:
	"""
	kwargs = _get_fiona_args(features_path)
	main_file_path = kwargs['fp']
	del kwargs['fp']

	kwargs = {**kwargs, **extra_kwargs}

	return fiona.open(main_file_path, **kwargs)
