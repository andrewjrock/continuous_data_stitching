# continuous_data_stitching
These files work together to automatically take multiple months of zipped data files and stitch them to historic data files.
The 'parse_Driver_Avg_Temp_Lazy' file is titled as such because it automatically takes each consecutive month's zip files, extracts their contents to a temporary folder, and calls the stitching function. The user need only specify which months to add! 
