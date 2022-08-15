"""
	So, let's run a bunch of SSEBOPers as comparisons, then merge to a new image as bands, then get a mean/standard deviation image

	Something like taking the maximum per-pixel ET from June/July, divided by the maximum per-pixel ET for April/May.

	Then, stack those by year, get the average and the standard deviation as new images and export, so now we have
	average, standard deviation, and annual rasters. Take 2022 image and compare pixels to see if value is less than
	average minus (1? 2?) stddevs. Then zonal stats for the whole field with 2019 data and see if can find more separation
	between the fallow and not fallow fields.


"""