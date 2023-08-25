import os
from pathlib import Path
from typing import Union, Dict


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
