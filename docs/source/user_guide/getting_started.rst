Getting Started with EEDL
============================

At its core, EEDL does just a few things - it take an Earth Engine
Image, exports it in tiles, downloads the pieces and reassembles on your
local machine, giving you the ability to wait for the data to be available
before proceeding. It further has options for running zonal statistics to
output polygon-level summary data in addition to the image exports.

Most work is done with the EEDLImage class, which is your
entry point to almost all functionality of the EEDL package, though EEDL
also includes a set of helper classes that automate common tasks for larger
processes, such as extraction of all dates in an image collection for
multiple areas of interest.

EEDL requires three things in order to run exports

    1. An installed copy of the :code:`eedl` package
    2. An installed and authenticated copy of the :code:`earthengine` package
    3. That you either have Google's :code:`Drive` client installed on the computer that you run your code on, or you have a Google Cloud account with billing set up and a Storage bucket created. See :ref:`here for more considerations <ExportLocations>` on how to run the export.

The simplest script using EEDL would look something like:

.. code-block:: python

    import ee
    import eedl

    # get the first image in the Landsat 9 collection
    # you can do anything that gives you an ee.Image object here instead too.
    image = ee.ImageCollection("LANDSAT/LC09/C02/T1_L2").first()

    # set a download path - the image will be put in this folder
    download_folder = r"C:\Users\your_account\data"

    # instantiate the EEDLImage object
    exporter = eedl.EEDLImage()

    # starts the export of your image from Earth Engine to the chosen export location
    # does not start the download to your computer or block your code execution after
    # starting the export
    exporter.export(image, export_type="Drive", drive_root_folder="G:\My Drive")

    # tell your code to wait for all images you've exported
    # so far to be downloaded and processed before proceeding
    eedl.main_task_registry.wait_for_images(download_location=download_folder, callback="mosaic")

    # print the path to the downloaded image that you can now use in following code
    print(exporter.mosaic_image)

This is an oversimplified example, but it shows the complete workflow. You can export dozens or even hundreds
of images before executing the last line to wait for the images to complete exporting, allowing for large
batch exports, and the class and :code:`.export()` method each support substantial arguments
affecting their behavior.

In this example, we pulled an existing image from an Earth Engine collection by taking the first image in the
Landsat 9 collection. But EEDL isn't limited to existing assets and can also export computed images, exactly
as you would if you were triggering an export in the online code environment, or from other Python code. Any
valid ee.Image object can be exported with EEDL.

How EEDL Works
------------------
When you trigger an EEDL export or multiple exports, many things happen in sequence:

1. You kick off a standard Earth Engine export task - these tasks can be monitored in the Earth Engine Task Manager as usual
2. Earth Engine begins an export, in tiles, the image you provided for export. The tiling is automatically specified by EEDL, but you may need to tune the parameters for some images, especially multiband images.
3. During the export process, you will configure it to export to your Google Drive or to a Google Cloud Storage bucket as an intermediate location for the export.
4. After kicking off your export(s), you tell EEDL to wait for all the images you've exported to be available. EEDL automatically tracks their status, downloads any that are complete, and optionally begins postprocessing (mosaicking, zonal stats) on complete images while waiting for Earth Engine to export remaining images.
5. As part of this process, EEDL will automatically retrieves the images from Drive or Cloud storage. Each of these requires specific configuration for your Cloud storage or Google Drive setup. Multiple images (thousands, even) can be exported into the same folder in Drive or Cloud storage - EEDL tracks the tiles of each image by name to avoid mixing up pieces, so make sure to give each image a unique name or prefix. EEDL doesn't currently add its own unique identifiers to export names (but we'd like to).

.. seealso:: For more information on configuring EEDL to export to Google Drive or Google Cloud Storage, along with caveats, see :ref:`ExportLocations`

Points of Configuration
-------------------------
Note the main points of configuration in this example:

 * Creation of the image object: As stated above, here you can customize your image to any valid Earth Engine image - no need to save it out as an asset or into a collection first
 * When creating the EEDLImage object - The example above shows no arguments, but you can pass in information, such as CRS information, tiling parameters, and configuration options for running zonal statistics, here. You may want to adjust tiling parameters for multi-band exports, for example, since those can run into Earth Engine's per-tile limits more easily. See documentation for :ref:`EEDLImage` for more information on arguments to the class.
 * When triggering the export: When running the export, you can specify where
 * And when waiting for images