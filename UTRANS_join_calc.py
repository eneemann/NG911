# -*- coding: utf-8 -*-
"""
Created on Tue Jan 10 10:14:45 2023

@author: eneemann
"""

import arcpy
from arcpy import env
import os
import time

# Start timer and print start time in UTC
start_time = time.time()
readable_start = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
print("The script start time is {}".format(readable_start))

SGID = r"C:\Users\eneemann\AppData\Roaming\Esri\ArcGISPro\Favorites\SGID_Transportation.sde"
addr_db = r'C:\Users\eneemann\Desktop\Neemann\Address Grids\preserved_guids.gdb'
sgid_roads = os.path.join(SGID, 'SGID.TRANSPORTATION.Roads')
combined_roads = os.path.join(addr_db, 'combined_parts_20230110')
env.workspace = addr_db
env.overwriteOutput = True
env.qualifiedFieldNames = True

#: Print out field names
joined_fields = arcpy.ListFields(sgid_roads)
for f in joined_fields:
    print(f.name)


#: Update cursor guid_str
update_count = 0
# Calculate values into guid_str
#              0            1          2        3
fields = ['guid_str', 'GlobalID']
with arcpy.da.UpdateCursor(sgid_roads, fields) as cursor:
    print("Looping through rows in FC ...")
    for row in cursor:
        row[0] = row[1]
        update_count += 1
        cursor.updateRow(row)
        if update_count % 10000 == 0:
            print(f'Complete row: {update_count}')
print("Total count of updates: {update_count}")







#: Add Join
# arcpy.management.AddJoin(sgid_roads, "GlobalID", combined_roads, "GlobalID")
arcpy.management.AddJoin(sgid_roads, "guid_str", combined_roads, "guid_field")

# arcpy.management.AddJoin("SGID.TRANSPORTATION.Roads", "GlobalID", "combined_parts_20230110", "GlobalID", "KEEP_ALL", "NO_INDEX_JOIN_FIELDS")

# with arcpy.EnvManager(preserveGlobalIds=True):
#     arcpy.management.Append("SGID_Roads_part_2", r"C:\Users\eneemann\Desktop\Neemann\Address Grids\preserved_guids.gdb\combined_parts_20230110", "TEST", None, '', '')

# with arcpy.EnvManager(preserveGlobalIds=True):
#     arcpy.management.Append("SGID_Roads_part_2", r"C:\Users\eneemann\Desktop\Neemann\Address Grids\preserved_guids.gdb\combined_parts_20230110", "NO_TEST", r'ADD_L "AddressSystemLeft" true true false 30 Text 0 0,First,#,C:\Users\eneemann\Desktop\Neemann\Address Grids\preserved_guids.gdb\SGID_Roads_part_2,ADD_L,0,30;ADD_R "AddressSystemRight" true true false 30 Text 0 0,First,#,C:\Users\eneemann\Desktop\Neemann\Address Grids\preserved_guids.gdb\SGID_Roads_part_2,ADD_R,0,30;GlobalID "GlobalID" false false true 38 GlobalID 0 0,First,#,C:\Users\eneemann\Desktop\Neemann\Address Grids\preserved_guids.gdb\SGID_Roads_part_2,GlobalID,-1,-1', '', '')

# with arcpy.EnvManager(preserveGlobalIds=True):
#     arcpy.management.Append("SGID_Roads_part_2", r"C:\Users\eneemann\Desktop\Neemann\Address Grids\preserved_guids.gdb\combined_parts_20230110", "TEST_AND_SKIP", None, '', '')

#: Print out field names
joined_fields = arcpy.ListFields(sgid_roads)
for f in joined_fields:
    print(f.name)


#: Update cursor to calculate over values from join
update_count = 0
# Calculate values into ADDRSYS_L and ADDRSYS_R
#              0            1          2        3
fields = ['ADDRSYS_L', 'ADDRSYS_R', 'ADD_L', 'ADD_R']
with arcpy.da.UpdateCursor(sgid_roads, fields) as cursor:
    print("Looping through rows in FC ...")
    for row in cursor:
        row[0] = row[2]
        row[1] = row[3]
        update_count += 1
        cursor.updateRow(row)
        if update_count % 10000 == 0:
            print(f'Complete row: {update_count}')
print("Total count of updates: {update_count}")
    
#: Remove Join
arcpy.management.RemoveJoin(sgid_roads)


print("Script shutting down ...")
# Stop timer and print end time in UTC
readable_end = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
print("The script end time is {}".format(readable_end))
print("Time elapsed: {:.2f}s".format(time.time() - start_time)) 