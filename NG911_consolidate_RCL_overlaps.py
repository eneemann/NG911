# -*- coding: utf-8 -*-
"""
Created on Thu Feb 13 14:00:46 2020
@author: eneemann

Script to update EMS counts table with overlapping Agency IDs
"""

import arcpy
import os
import time
import pandas as pd
import numpy as np

# Start timer and print start time in UTC
start_time = time.time()
readable_start = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
print("The script start time is {}".format(readable_start))

today = time.strftime("%Y%m%d")

######################
#  Set up variables  #
######################

# Set up databases
ng911_error_db = r"C:\Users\eneemann\Desktop\Neemann\NG911\911 DataMaster\NG911_Data_Errors.gdb"
work_dir = r"C:\Users\eneemann\Desktop\Neemann\NG911\911 DataMaster\RCL_Overlaps"

# Set up environments
arcpy.env.workspace = ng911_error_db
arcpy.env.overwriteOutput = True
arcpy.env.qualifiedFieldNames = False

# Set up feature classes and tables
RCL_errors = os.path.join(ng911_error_db, 'RCL_range_errors_20220315')
RCL_overlaps = os.path.join(ng911_error_db, 'RCL_range_error_20220315_overlaps')
arcpy.management.CopyFeatures(RCL_errors, RCL_overlaps)

# Convert RCL Errors to pandas dataframe
overlap_fields = ['RCL_NGUID', 'Overlap_GUID']
overlap_arr = arcpy.da.TableToNumPyArray(RCL_errors, overlap_fields)
overlap_df = pd.DataFrame(data = overlap_arr)
print(overlap_df.head(5).to_string())

max_overlaps = overlap_df.RCL_NGUID.value_counts().max()

# Add max_overlaps number of fields to counts feature class, df, fields list
ov_fields = []
for num in np.arange(max_overlaps):
    arcpy.AddField_management(RCL_overlaps, f"Overlap_{num + 1}", "TEXT", "", "", 100)
    overlap_df[f"Overlap_{num + 1}"] = None
    ov_fields.append(f"Overlap_{num + 1}")

# Update counts feature class overlap fields with name of overlapping agencies
fields = ['RCL_NGUID'] + ov_fields
print(f'fields used for update cursor: {fields}')
with arcpy.da.UpdateCursor(RCL_overlaps, fields) as update_cursor:
    for u_row in update_cursor:
        # Make temp df to build list of ovelaps for the current OBJECTID
        ov_id = u_row[0]
        temp_df = overlap_df[overlap_df['RCL_NGUID'] == ov_id]
        overlaps = temp_df['Overlap_GUID'].to_list()
        # Update row for each overlap field that has an agency
        for fld in np.arange(len(overlaps)):
            u_row[fld + 1] = overlaps[fld]      # add 1 to u_row assignment to skip over OBJECTID field
        update_cursor.updateRow(u_row)
        del temp_df         # delete temp_df so it can be reused in the next iteration

# Convert RCL Overlaps to pandas dataframe
overlap_arr2 = arcpy.da.TableToNumPyArray(RCL_overlaps, fields)
overlap_df2 = pd.DataFrame(data = overlap_arr2)
print(overlap_df2.head(5).to_string())

# Drop duplicates on RCL_NGUID
no_dups = overlap_df2.drop_duplicates('RCL_NGUID')

# Export table to CSV
out_csv = os.path.join(work_dir, f'RCL_overlaps_{today}_simple.csv')
no_dups.to_csv(out_csv)

# Import into Arcpy Table
arcpy.conversion.TableToTable(out_csv, ng911_error_db, 'Overlaps_NGUID_simple_20220315')

print("Script shutting down ...")
# Stop timer and print end time in UTC
readable_end = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
print("The script end time is {}".format(readable_end))
print("Time elapsed: {:.2f}s".format(time.time() - start_time))
