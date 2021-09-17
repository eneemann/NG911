# -*- coding: utf-8 -*-
"""
Created on Thu Sep 16 15:20:42 2021
@author: eneemann
Script to flag NG911 errors on Address Points
"""

import arcpy
import os
import time
from xxhash import xxh64

# Start timer and print start time in UTC
start_time = time.time()
readable_start = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
print("The script start time is {}".format(readable_start))

######################
#  Set up variables  #
######################

# Set up databases (SGID must be changed based on user's path)
ng911_db = r"\\itwfpcap2\AGRC\agrc\data\ng911\SpatialStation_live_data\UtahNG911GIS.gdb"
error_db = r"C:\Users\eneemann\Desktop\Neemann\NG911\911 DataMaster\NG911_Errors_20210812.gdb"

arcpy.env.workspace = ng911_db
arcpy.env.overwriteOutput = True
arcpy.env.qualifiedFieldNames = False

today = time.strftime("%Y%m%d")

addpts = os.path.join(ng911_db, r'AddressPoints')
addpts_working = os.path.join(error_db, f'AddressPoints_errors_{today}')

## Make a copy of the data to work on
arcpy.management.CopyFeatures(addpts, addpts_working)
arcpy.AddField_management(addpts_working, "Error_UGRC", "TEXT", "", "", 100)

print("Time elapsed: {:.2f}s".format(time.time() - start_time)) 


addr_count = int(arcpy.management.GetCount(addpts_working)[0])

#: Check for duplicate attributes
digests = set([])

description = arcpy.da.Describe(addpts_working)
print(f'Working on Duplicates for: {addpts_working}')
skip_fields = ['Error_UGRC', 'Long', 'Lat', description['shapeFieldName']]


if description['hasGlobalID']:
    skip_fields.append(description['globalIDFieldName'])

if description['hasOID']:
    skip_fields.append(description['OIDFieldName'])

fields = [field.name for field in description['fields'] if field.name not in skip_fields]
mandatory_fields = ['Site_NGUID', 'State', 'County', 'Add_Number', 'LSt_Name', 'MSAGComm']
empties = [None, 'none', 'null', '', ' ', '  ']

#: Add OID and Error_UGRC at the end, so we can ignore them in the hash
fields.append('OID@')
fields.append('Error_UGRC')

oid_index = fields.index('OID@')
notes_index = fields.index('Error_UGRC')

oids_with_issues = []

duplicate_count = 0
required_count = 0
print("Looping through rows in FC ...")
with arcpy.da.UpdateCursor(addpts_working, fields) as update_cursor:
    mandatory_idx = [fields.index(item) for item in fields if item in mandatory_fields]
    for row in update_cursor:
        comment = None
        object_id = row[oid_index]
        if object_id % 100000 == 0:
            print(f'working on OBJECTID: {object_id}')

        #: Has all fields except for OID and Error_UGRC, which are the last fields
        hasher = xxh64(str(row[:-2]))
        digest = hasher.hexdigest()

        if digest in digests:
            oids_with_issues.append(object_id)
            comment = 'attribute duplicate'
            duplicate_count += 1

        digests.add(digest)
        
        #: Check mandatory fields
        row_mandatory = [row[i] for i in mandatory_idx]
        if any(val is None or str(val).strip().casefold() in empties for val in row_mandatory):
            oids_with_issues.append(object_id)
            required_count += 1
            if comment is None or comment in ('', ' '):
                comment = 'required value missing'
            else:
                comment += ', required value missing'
        
        row[notes_index] = comment
        update_cursor.updateRow(row)

print(f"Total count of attribute duplicates is: {duplicate_count} or {round((duplicate_count/addr_count)*100, 3)}%")
print(f"Total count of rows missing required value: {required_count} or {round((required_count/addr_count)*100, 3)}%")

#oid_set = set(oids_with_issues)
#print('\nSelect statement to view errors in ArcGIS:')
#sql = f'OBJECTID IN ({", ".join([str(oid) for oid in oid_set])})'
#print(sql)



##########################
#  Call Functions Below  #
##########################

print("Script shutting down ...")
# Stop timer and print end time in UTC
readable_end = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
print("The script end time is {}".format(readable_end))
print("Time elapsed: {:.2f}s".format(time.time() - start_time))