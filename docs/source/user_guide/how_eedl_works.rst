How EEDL Works, In Detail
===============================
EEDL doesn't do anything particularly fancy or hard, but it handles things that are a pain to manage,
whether you're doing it by hand, or whether you're trying to write code to manage the process. Here's
what EEDL handles for you and how it goes about it. This document is more the story of what's happening
so you can understand the pieces, especially if something goes wrong, than direct information on how to use EEDL.

Summary
-------------
EEDL allows you to get data out of Earth Engine, in bulk, for further analysis or postprocessing. If you're wanting
to export one image, it can do that, but you may be better served by handling it yourself. But once you want many images,
this task can get tedious or impractical.

EEDL manages Earth Engine exports from before you request Earth Engine export the image until
after it is downloaded on your device and ready to use. It can handle this whether it's one image or 10,000.
It manages the tasks of configuring Earth Engine to slice large images into parts, export them through cloud storage,
retrieve them from cloud storage onto your device, reassemble the parts, and optionally run zonal statistics before
returning control to your code for any further work you want to do with the data

Exports
----------
EEDL starts when you want to export any image from Earth Engine. It can be an existing asset that you've loaded
into an :code:`ee.Image` object, or a computed image that hasn't been saved out in any way yet, such as a collection
that you've summed/reduced into a single image.

Exports work like any other export from Earth Engine, and allow the same parameters, but by starting it through EEDL,
EEDL tracks the export so it can download it for you automatically once it's ready.

Slicing
-----------
As part of managing your export, EEDL automatically passes a parameter to Earth Engine's export code that splits your image
into tiles (by default with 12,800 pixels to a side). This is to keep the image within Earth Engine's memory budget for exports.
Larger tiles may not succeed in being exported on Earth Engine's servers. In some cases, such as with multi-band images, you
may need to decrease the tile size in order to stay within Earth Engine's memory limits, at the cost of more files being output
for a single image export. This isn't a problem on its own, but can create problems in some export scenarios, if you end
up with more than 1000 tiles for one image.

Tracking images and their statuses
-----------------------------------
A key piece of EEDL's functionality is that, once you tell it to download your images, it starts tracking the status of
all exports you've initiated on :code:`EEDLImage` objects within a session.

.. note::
    Each :code:`EEDLImage` object instance should only be used for a single export due to how it uses the object
    to track information. Create additional :code:`EEDLImage` objects if you need to manage multiple exports

EEDL only begins updating the status of images once you call :code:`wait_for_images` on your :ref:`Task Registry <EEDLImage>`.
After that, it starts updating the status of all images exported so far in your script and blocks execution until
after all images exported have either downloaded or failed to export.

Two important considerations are involved in tracking:

 1. EEDL will poll, about once a minute, Earth Engine's status endpoint for tasks to find out where in the export
    process the image is. If the image is waiting or exporting, it does nothing more. But once Earth Engine reports
    that the image has completed its export, EEDL begins the download of the image.

 2. EEDL tracks images by their name - it constructs the name from a few pieces of information, but the most important
    is information *you* provide when you initiate each image's export. For now, it's important that you give
    each image a unique name, or else EEDL will mix up the pieces of images when downloading and reassembling them.
    We'd like to change this to automatically assign a unique ID through the export process and assign your name
    only at the end to avoid this issue, but that work has not been done yet.

Exporting through cloud storage
---------------------------------
Earth Engine supports three export targets - Earth Engine assets, Google Drive, and Google Cloud Storage. EEDL
supports and handles exports to Drive and Cloud Storage (Earth Engine assets aren't as accessible outside Earth Engine).

Your choice of which one to export to will vary based upon workflows available to you and each one has unique requirements and
implications for your download. See :ref:`ExportLocations` for more information on this topic. It's important information
to understand before you begin using EEDL. The two most important factors are:

 1. If you wish to use Google Drive exports, you need to have the Google Drive client installed on your computer - EEDL doesn't access files in drive via the API. Get in touch or file an issue if you'd like to work on supporting API access instead (which would streamline EEDL for many workloads)
 2. If you use Google Cloud Storage exports, your Cloud Storage bucket *must* be public. We don't currently support private buckets, but would like to in the future.

Accessing and downloading data in cloud storage
-------------------------------------------------
Once the image status indicates it's ready for download, EEDL will go retrieve all the image parts
that Earth Engine exported from your export location. In Google Drive exports, it will access the
mounted Google Drive folder on your computer and list
the contents of the folder you exported to, then find everything with a name matching the
name you provided at export time (plus other name parts Earth Engine adds). It will then download
those parts by moving all the matching files to the export location you provided as an argument
to :code:`wait_for_images`. Note that this method means EEDL deletes images from Google Drive for you,
though they continue to take space (see :ref:`ExportLocations` for more information). For Google Cloud exports, it will ask for a listing from the bucket's public
API endpoint of all files that match the name string you provided, then initiate HTTP requests to download
each individual file. With this method, we cannot currently delete images from Cloud Storage buckets,
so we recommend a lifecycle policy on the bucket that automatically deletes files after 24 hours, if possible.

Reassembling the pieces
-------------------------
Once EEDL has downloaded all pieces of an image, it executes any configured callbacks (provided
as a string name of the EEDLImage method to :code:`wait_for_images`). The most common callback
is :code:`mosaic`, which takes all the tiles that match the image's name that have been downloaded
and mosaics them back together with GDAL. Currently, it also builds overviews/pyramids and sets lossless
compression parameters on images as well. The final result will be a single image on your device,
in the folder you specified for downloads with roughly the name you provided and :code:`_mosaic`
appended to the end. Because you can't reliably predict the name of the final image, it is stored
on the EEDLImage object as the :code:`mosaic_image` attribute once the export is complete.

Running zonal statistics
------------------------------
EEDL also can run zonal statistics after mosaicking. You can either call the methods manually
after finishing the download loop, but more likely, you'll run the :code:`mosaic_and_zonal`
callback instead of the :code:`mosaic` callback. :code:`mosaic_and_zonal` requires preconfiguration
of the EEDLImage object by providing the path to your polygon dataset (OGR-compatible), the unique
identifier field, and the statistics you'd like to run. This information can be passed as keyword arguments
when creating an EEDLImage object or set as attributes later, but before downloads begin.

Zonal statistics will be produced as CSVs in the same folder as the image. Statistics are produced
by the :code:`rasterstats` package and are subject to its capabilities and limitations. We'd like to have
the option to run zonal stats within Earth Engine (and then initiate a separate export and download) as well
but have not developed the functionality yet.

The advantage of running zonal statistics via the :code:`mosaic_and_zonal` callback is that zonal statistics
are the most time consuming local operation EEDL provides. By running it within the callback, zonal statistics are
run primarily in the time EEDL is waiting for Earth Engine to export other images. For very large polygon datasets it can
take longer, but typical usage is that more polygons are associated with larger images that, themselves, can take longer
to export from Earth Engine, so the two execution times roughly scale together.
