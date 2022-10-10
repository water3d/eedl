import os

#from google.cloud import storage

import requests
import re
from pathlib import Path

def get_public_export_urls(bucket_name, prefix=""):
	"""
		Downloads items from a *public* Google Storage bucket without using a GCloud login. Filters only to files
		with the specified prefix
	:param bucket_name:
	:param prefix: A prefix to use to filter items in the bucket - only URLs where the path matches this prefix will be returned - defaults to all files
	:return: list of urls
	"""

	base_url = "http://storage.googleapis.com/"
	request_url = f"{base_url}{bucket_name}/"

	# get the content of the bucket (it needs to be public
	listing = requests.get(request_url).text

	# comes back as an XML listing - don't need to parse the XML, just need the values of the Key elements
	pattern = re.compile("\<Key\>(.*?)\<\/Key\>")
	items = pattern.findall(listing)
	# make them into full URLs with the bucket URL at the front and check if the files have the prefix specific
	filtered = [f"{request_url}{item}" for item in items if item.startswith(prefix)]

	return filtered


def download_public_export(bucket_name, output_folder, prefix=""):
	# get the urls of items in the bucket with the specified prefix
	urls = get_public_export_urls(bucket_name, prefix)

	for url in urls:
		filename = url.split("/")[-1]  # get the filename
		output_path = Path(output_folder) / filename  # construct the output path
		response = requests.get(url)  # get the data - this could be a problem if it's larger than fits in RAM - I believe requests has a way to operate as a streambuffer - not looking into that at this moment
		output_path.write_bytes(response.content)  # write it to a file



def download_export(bucket_name, output_folder, prefix, delimiter="/", autodelete=True):
	"""Downloads a blob from the bucket.

	Modified from Google Cloud sample documentation at
	 	https://cloud.google.com/storage/docs/samples/storage-download-file#storage_download_file-python
	 	and
	 	https://cloud.google.com/storage/docs/samples/storage-list-files-with-prefix
	"""
	# The ID of your GCS bucket
	# bucket_name = "your-bucket-name"

	# The ID of your GCS object
	# source_blob_name = "storage-object-name"

	# The path to which the file should be downloaded
	# destination_file_name = "local/path/to/file"

	storage_client = storage.Client()

	bucket = storage_client.bucket(bucket_name)
	blobs = storage_client.list_blobs(bucket_name, prefix=prefix, delimiter=delimiter)

	for blob in blobs:
		if blob.name.startswith(prefix):
			destination_file_name = os.path.join(output_folder, blob.name)
			# Construct a client side representation of a blob.
			# Note `Bucket.blob` differs from `Bucket.get_blob` as it doesn't retrieve
			# any content from Google Cloud Storage. As we don't need additional data,
			# using `Bucket.blob` is preferred here.
			blob_data = bucket.blob(blob.name)
			blob_data.download_to_filename(destination_file_name)
			if autodelete:
				blob_data.delete()

	#print(
	#	"Downloaded storage object {} from bucket {} to local file {}.".format(
	#		source_blob_name, bucket_name, destination_file_name
	#	)
	#)