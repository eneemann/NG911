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
ng911_L = r"\\itwfpcap2\AGRC\agrc\data\ng911\NG911_boundary_work.gdb\EMS_Boundaries"
ng911_emn = r"C:\Users\eneemann\Desktop\Neemann\NG911\NG911_project\NG911_project.gdb"
work_dir = r"C:\Users\eneemann\Desktop\Neemann\NG911\NG911_project\EMS Boundary Descriptions\working_files"

# Set up environments
arcpy.env.workspace = ng911_emn
arcpy.env.overwriteOutput = True
arcpy.env.qualifiedFieldNames = False

# Set up feature classes and tables
# EMS_boundaries = os.path.join(ng911_L, 'NG911_EMS_bounds_20201215')
EMS_boundaries = os.path.join(ng911_emn, 'EMS_Boundaries', 'NG911_EMS_bounds_20201215')
count_polys = os.path.join(ng911_emn, f'NG911_EMS_bounds_{today}_counts')
overlap_table = os.path.join(ng911_emn, f'NG911_EMS_bounds_{today}_overlap_table')

# Delete new layers if they already exist
if arcpy.Exists(count_polys):
    print(f"Deleting {'count_polys'} ...")
if arcpy.Exists(overlap_table):
    print(f"Deleting {'overlap_table'} ...")

# Create polygon counts and overlap table
arcpy.analysis.CountOverlappingFeatures(EMS_boundaries, count_polys, 0, overlap_table)

# Convert overlap table to pandas dataframe
overlap_fields = ['OVERLAP_OID', 'ORIG_OID']
overlap_arr = arcpy.da.TableToNumPyArray(overlap_table, overlap_fields)
overlap_df = pd.DataFrame(data = overlap_arr)
print(overlap_df.head(5).to_string())

# Convert counts to pandas dataframe
count_fields = ['OBJECTID', 'COUNT_']
count_arr = arcpy.da.FeatureClassToNumPyArray(count_polys, count_fields)
count_df = pd.DataFrame(data = count_arr)
print(count_df.head(5).to_string())

# Convert boundaries to pandas dataframe
boundary_fields = ['OBJECTID', 'Agency_ID']
boundary_arr = arcpy.da.FeatureClassToNumPyArray(EMS_boundaries, boundary_fields)
boundary_df =pd.DataFrame(data = boundary_arr)
print(boundary_df.head(5).to_string())

# Join counts to overlap table
join1_df = overlap_df.join(count_df.set_index('OBJECTID'), on='OVERLAP_OID')
print(join1_df.head(5).to_string())
path = os.path.join(work_dir, 'overlap_table_join1.csv')
join1_df.to_csv(path)

# Join boundaries to overlap table
join2_df = join1_df.join(boundary_df.set_index('OBJECTID'), on='ORIG_OID')
print(join2_df.head(5).to_string())
path = os.path.join(work_dir, 'overlap_table_join2.csv')
join2_df.to_csv(path)

# Caculate the max number of overlaps in an overlap region
max_overlaps = join2_df['COUNT_'].max()
print(f'Max number of overlaps: {max_overlaps}')

# Count the number of overlap regions
count_overlaps = join2_df['OVERLAP_OID'].max()
print(f'Number of overlap regions: {count_overlaps}')

# Add max_overlaps number of fields to counts feature class, df, fields list
ov_fields = []
for num in np.arange(max_overlaps):
    arcpy.AddField_management(count_polys, f"Overlap_{num + 1}", "TEXT", "", "", 100)
    join2_df[f"Overlap_{num + 1}"] = None
    ov_fields.append(f"Overlap_{num + 1}")

# Update counts feature class overlap fields with name of overlapping agencies
fields = ['OBJECTID'] + ov_fields
print(f'fields used for update cursor: {fields}')
with arcpy.da.UpdateCursor(count_polys, fields) as update_cursor:
    for u_row in update_cursor:
        # Make temp df to build list of ovelaps for the current OBJECTID
        ov_id = u_row[0]
        temp_df = join2_df[join2_df['OVERLAP_OID'] == ov_id]
        overlaps = temp_df['Agency_ID'].to_list()
        # Update row for each overlap field that has an agency
        for fld in np.arange(len(overlaps)):
            u_row[fld + 1] = overlaps[fld]      # add 1 to u_row assignment to skip over OBJECTID field
        update_cursor.updateRow(u_row)
        del temp_df         # delete temp_df so it can be reused in the next iteration

print("Script shutting down ...")
# Stop timer and print end time in UTC
readable_end = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
print("The script end time is {}".format(readable_end))
print("Time elapsed: {:.2f}s".format(time.time() - start_time))
