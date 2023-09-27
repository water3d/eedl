.. _ExportLocations:

Working with Export Locations - Drive vs. Cloud
====================================================
EEDL works by using Earth Engine's built in export mechanisms, which can
send exported images either to Google Drive or to a Google Cloud storage
bucket. In order for EEDL to finish processing exports - that is, for it
to handle downloading and mosaicking images for you, you'll need to
either set up a Google Cloud storage bucket, configured for public access,
or install the Google Drive client on your computer. The package does not
currently use either the Google Cloud APIs or Google Drive APIs, avoiding
the need for authentication, but presenting some limits and requirements on
use.

Google Drive Exports
------------------------

.. note::
    Key takeaway: If you can run the Google Drive client on the same
        computer as you're running EEDL and have enough space in Drive
        for your exports, Drive exports may be a good option for you.

Key Considerations
_____________________
Linux users may need to use the Cloud export option because available options
for accessing Drive folders on Linux are lacking. If you have Drive set up as a directory
on your computer though, you can use this format.

EEDL *does* automatically delete files it processes out of Google Drive, but
it only moves the files to your Drive Trash, which you can only clear
in their web interface. Your storage will continue being occupied for 30 days, until
Drive automatically deletes the files from your Drive Trash, unless you manually
go in and empty your Trash within the Google Drive web interface. For especially
large exports/sets of exports, you may need to clear your trash midway through,
depending on your available Drive storage.

This method is private - files aren't shared or available to anyone else.

Google Cloud Exports
------------------------

Key Considerations
______________________
Exports are public and accessed via public URL structure. Don't export
sensitive information via Cloud. If you want an option to access private
buckets, please get in touch about submitting a pull request.
Our client does *not* delete files automatically as it does not carry
permissions to perform that operation (want it to be able to do that?
Get in touch about submitting a pull request with code for that).
We recommended setting a bucket lifecycle setting that automatically deletes
files after 24 hours to save on costs and not manage files. EEDL will
automatically download files within minutes in most cases.

While Earth Engine's exports *are* publicly available and linkable here,
EEDL does not upload any derived products (mosaics or zonal stats) back
to the bucket, so those do not become public.

.. note::
    Key takeaway: If you can't run the Google Drive client, lack space
        in Drive, want to bill export storage to a Cloud project, want
        the files available externally, or want the most reliable
        export method, using Google Cloud exports may be a good option
        for you.