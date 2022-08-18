# OK, let's try another approach
# 1 - a function to compare with subtraction
# 2 - then run classification
# 3 - zonal stats with majority (can we do this in EE if we're tiling? Not sure. May still need to download and do it)

SPRING_DOUBLE_DIFFERENCE_THRESHOLD = 0
SPRING_DOUBLE_RAW_ET_THRESHOLD = 2
DOUBLE_FALLOW_DIFFERENCE_THRESHOLD = 1.5
DOUBLE_FALLOW_RAW_ET_THRESHOLD = 3
CROPPED_LIKELY_THRESHOLD = 3
#           Class 0,       1 Sprg,  2 Dbl,    3 Fallow, 4 Likely, 5 Cropped
CLASS_COLORS = ["FFFFFF", "A3FF73", "A3FF73", "FFD37F", "0070FF", "0070FF"]


def simple_classifier(difference_img, raw_summer_et_img):
	"""
	If difference is negative
		and summer ET less than 2:
			Class 1, Spring Crop, Summer Fallow
		else
			Class 2, Double Crop or Perennial
	Else if difference < 1.5
		if summer ET > 3
			class 2 Double Crop Perennial
		else
			Class 3, Fallow
	Else if difference < 3
		Class 4, Probably Cropped
	Else
		Class 5, Cropped
	"""

	# OK, this is going to be funky and I feel like there's a better way, but my brain is mush.
	# We're going to separately get all the image classes with boolean logic, then multiply to turn
	# them into the correct class IDs. But there's going
	# to be some overlap (eg, checking if the difference is less than 1.5 for class 2 and 3),
	# so at the end, to combine, we'll use min() to combine, which takes earlier classes as higher priority.
	# It seems safest to *also* make sure to do the full logic chain for each one, but that's preeetty annoying.
	# could also just do .not() throughout the chain. Also not ideal, but duplicates less logic.

	difference_negative = difference_img.lt(SPRING_DOUBLE_DIFFERENCE_THRESHOLD)
	summer_lt_thresh = raw_summer_et_img.lt(SPRING_DOUBLE_RAW_ET_THRESHOLD)
	class_1_spring = difference_negative.And(summer_lt_thresh)

	class_2_option2 = difference_img.lt(DOUBLE_FALLOW_DIFFERENCE_THRESHOLD).And(
		raw_summer_et_img.lt(DOUBLE_FALLOW_RAW_ET_THRESHOLD))
	class_2_double_crop = difference_negative.And(summer_lt_thresh.Not()).Or(class_2_option2).multiply(
		2)  # make a boolean image, then multiply by 2 to get the class ID there

	class_3_fallow = (difference_img.gt(SPRING_DOUBLE_DIFFERENCE_THRESHOLD)
					  .And(difference_img.lt(DOUBLE_FALLOW_DIFFERENCE_THRESHOLD))
					  .And(raw_summer_et_img.lt(DOUBLE_FALLOW_RAW_ET_THRESHOLD))
					  .multiply(3))

	class_4_likely_cropped = (difference_img.gt(DOUBLE_FALLOW_DIFFERENCE_THRESHOLD)
							  .And(difference_img.lt(CROPPED_LIKELY_THRESHOLD))
							  .multiply(4))

	class_5_cropped = (difference_img.gt(CROPPED_LIKELY_THRESHOLD)
					   .multiply(5))

	# switched to max because otherwise everything is zero (doy), and logic above make sure they shouldn't overlap
	class_img = (class_5_cropped
				 .max(class_2_double_crop)
				 .max(class_3_fallow)
				 .max(class_4_likely_cropped)
				 .max(class_1_spring))

	return class_img
