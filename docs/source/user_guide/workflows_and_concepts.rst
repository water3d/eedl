Workflows and Concepts
=========================

The EEDLImage Class
----------------------
The EEDLImage class is your main point of entry for all exports not initiated by a helper class.
EEDLImage manages all information related to the export and aids in tracking the status of the export
on Earth Engine, as well as tracking the location of all intermediate products. Most core work in
EEDL is done via methods on the EEDLImage class, but your main points of entry and configuration will
be at instance initialization (when calling :code:`your_image = EEDLImage()`) or when calling the
:code:`EEDLImage.export()` method.

Task Registries
--------------------
Task registries manage groups of EEDLImages you're working with and continually update the status of all
images by polling Earth Engine for status updates at configurable intervals.

Task registries do a fair amount of work, but in most cases, you'll only use them in one line of
code to tell EEDL to wait for all available images, where to save them, and what to do once they're downloaded.
See documentation on the TaskRegistry object under :ref:`EEDLImage` for more information on parameters.

By default, EEDL has a single
task registry at :code:`eedl.image.main_task_registry` that all images are added to, but if you'd like to export
a bunch of images, but segment how you wait for them, you can create as many additional task registries as you
like - just provide a created task registry to an image as its :code:`task_registry` keyword argument.

.. code-block::
    python

    # this example is incomplete - export parameters are missing, but not what the example is about

    from eedl.image import EEDLImage
    image_main_registry = EEDLImage()  # this image goes into the main task registry automatically
    image_main_registry.export()   # note we're missing parameters here for this example

    from eedl.image import main_task_registry, TaskRegistry
    custom_task_registry = TaskRegistry()  # no other arguments needed

    image_custom_registry = EEDLImage(task_registry=custom_task_registry)
    image_custom_registry.export()  # note we're missing parameters here for this example

    # would only download image_main_registry once it's available
    main_task_registry.wait_for_images()  # missing parameters here too

    # ... some additional work you want to do ...

    # would only download image_custom_registry when it's ready
    custom_task_registry.wait_for_images()  # missing parameters here


In that example, once :code:`image_main_registry` finishes exporting,

.. note::
    In the long run, we'd like to remove the concept of the Task Registry in favor of truly
    asynchronous code that runs in the background - where each image manages its own status updates, etc.
    In the meantime, they remain an important concept in EEDL that is mostly transparent for your use.


Tuning Default Values for Exports
------------------------------------


Exporting a Single ee.Image
------------------------------


Exporting a Filtered ee.ImageCollection
------------------------------------------

Helper Classes
----------------------