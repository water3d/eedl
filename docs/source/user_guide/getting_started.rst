Getting Started with EEDL
============================

At its core, EEDL does just a few things - it take an Earth Engine
Image, exports it in tiles, downloads the pieces and reassembles on your
local machine, giving you the ability to wait for the data to be available
before proceeding.

Most work is done with the EEDLImage class, which is your
entry point to almost all functionality of the EEDL package.

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