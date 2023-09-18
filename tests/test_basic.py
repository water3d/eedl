# import ee
# from ee import ImageCollection
import pytest  # noqa
from eedl.image import EEDLImage

# we should change the name of our Image class - it conflicts with the class image in the ee package, and people will
# likely be using both. Let's not cause confusion


def test_class_instantiation():
	image = EEDLImage()
	assert isinstance(image, EEDLImage)
