import os
import time

import ee

from . import image
from . import mosaic_rasters


class Image(object):
	"""
		Manages an image object so we can download and mosaic it

		realistically, SSEBOPer should be refactored to basically be a subclass inheriting Image, and the image registry in ssebop.py should work with this too
		but I'm not doing all of that right now. Start with this, then make those changes if we keep using this code base
	"""
	def __init__(self, image):
		self.image = image
		self._export_task = None
		self.drive_path = r"G:\My Drive"

	def export(self, **export_params):
		self.export_folder = export_params['folder']
		self.prefix = export_params['fileNamePrefix']
		self._export_task = ee.batch.Export.image.toDrive(self.image, **export_params)
		self._export_task.start()

	def download(self, download_location):
		self.download_location = download_location
		while True:
			status = self._export_task.status()
			if status['state'] == "COMPLETE":
				image.download_images_in_folder(source_location=os.path.join(self.drive_path, self.export_folder),
												 download_location=download_location,
												 prefix=self.prefix)
				break

			time.sleep(60)

	def mosaic(self):
		mosaic_rasters.mosaic_folder(self.download_location, os.path.join(self.download_location, f"{self.prefix}_mosaic.tif"))

	def export_to_mosaic(self, download_location):
		self.export()
		self.download(download_location)
		self.mosaic()
