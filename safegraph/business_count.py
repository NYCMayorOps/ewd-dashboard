#####
#
# Business Count Script
# Author: Steve Scott
# Contact: sscott1@cityops.nyc.gov
#
# Counts existing businesses, businesses opening, and businesses cosing
# pulls from SafeGraph Core-POI dataset
#
# 
#####

# get latest month. no need to loop over all the months
# filter Core POI to NYC
# isolate open on and closed on date
# create a new column for month.
# create a csv for number opened by month
# create csv number closed by month
# assign businesses with null close by date a close date of 1-1-2300
# create csv businesss greater than open and less than closed (unless closed is null) 
