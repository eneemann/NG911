# -*- coding: utf-8 -*-
"""
Created on Thu Sep 16 15:20:42 2021
@author: eneemann
Script to flag NG911 errors on Address Points
"""

import arcpy
import os
import time
from datetime import datetime
import pandas as pd
import math
from xxhash import xxh64
import re

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
#arcpy.management.CopyFeatures(addpts, addpts_working)
#arcpy.AddField_management(addpts_working, "Error_UGRC", "TEXT", "", "", 100)

print("Time elapsed: {:.2f}s".format(time.time() - start_time)) 

# Get the spatial reference for later use
#sr = arcpy.Describe(addpts_working).spatialReference
#print(sr)

addr_count = arcpy.management.GetCount(addpts_working)

# Check for duplicate attributes
digests = set([])

description = arcpy.da.Describe(addpts_working)
print(f'Working on Duplicates for: {addpts_working}')
skip_fields = ['Error_UGRC', 'Long', 'Lat', description['shapeFieldName']]


if description['hasGlobalID']:
    skip_fields.append(description['globalIDFieldName'])

if description['hasOID']:
    skip_fields.append(description['OIDFieldName'])

fields = [field.name for field in description['fields'] if field.name not in skip_fields]

#: Add OID and Error_UGRC at the end, so we can ignore them in the hash
fields.append('OID@')
fields.append('Error_UGRC')

#: include or exclude shape field depending on if working on table or feature class
#if is_table:
#    oid_index = fields.index('OID@')
#
#    with arcpy.da.SearchCursor(self.table_name, fields) as search_cursor:
#        for row in search_cursor:
#            object_id = row[oid_index]
#
#            hasher = xxh64(f'{row[:-1]}')
#            digest = hasher.hexdigest()
#
#            if digest in digests:
#                report['issues'].append(str(object_id))
#                self.oids_with_issues.append(object_id)
#
#            digests.add(digest)
            
#fields.append('SHAPE@WKT')           
#shapefield_index = fields.index('SHAPE@WKT')
oid_index = fields.index('OID@')
notes_index = fields.index('Error_UGRC')

oids_with_issues = []

#truncate_shape_precision = re.compile(r'(\d+\.\d{2})(\d+)')

duplicate_count = 0
required_count = 0
print("Looping through rows in FC ...")
with arcpy.da.UpdateCursor(addpts_working, fields) as update_cursor:
    for row in update_cursor:
#        shape_wkt = row[shapefield_index]
        object_id = row[oid_index]
        if object_id % 100000 == 0:
            print(f'working on OBJECTID: {object_id}')
#        if shape_wkt is None:
#            continue

        #: trim some digits to help with hash matching
#        generalized_wkt = truncate_shape_precision.sub(r'\1', shape_wkt)

        #: Has all fields except for OID, which is the last field
        hasher = xxh64(str(row[:-2]))
        digest = hasher.hexdigest()

        if digest in digests:
            oids_with_issues.append(object_id)
            comment = 'attribute duplicate'
            duplicate_count += 1

        digests.add(digest)
        
        # check mandatory fields
        
        
        oids_with_issues.append(object_id)
        required_count += 1
        
        if comment is None or comment in ('', ' '):
            comment = 'required value missing'
        else:
            comment += ', required value missing'
        row[notes_index] = comment
        update_cursor.updateRow(row)

print(f"Total count of attribute duplicates is: {duplicate_count} or {round(duplicate_count/addr_count, 3}%")
print(f"Total count of rows missing required value: {required_count} or {round(required_count/addr_count, 3}%")

oid_set = set(oids_with_issues)
print('\nSelect statement to view errors in ArcGIS:')
sql = f'OBJECTID IN ({", ".join([str(oid) for oid in oid_set])})'
print(sql)

# Check for missing required fields
    
'required value missing'


##query = "TOADDR_L <> 0 AND TOADDR_R <> 0"
#query = "UTRANS_NOTES LIKE '%python flip%'"
#    #          0           1          2             3          4           5           6
#fields = ['UNIQUE_ID', 'SHAPE@', 'PREDIR', 'UTRANS_NOTES', 'OBJECTID', 'TOADDR_L', 'TOADDR_R']
#with arcpy.da.UpdateCursor(addpts_working, fields, query) as update_cursor:
#    print("Looping through rows in FC to check for flipped segments ...")
#    for row in update_cursor:
#        if row[4] % 10000 == 0:
#            print('working on OBJECTID: {}'.format(row[4]))
##        if row[5] == 0 and row[6] == 0:
##            continue
#        shape_obj = row[1]
#        predir = row[2]
#        if shape_obj.partCount > 1:
#            print("OBJECTID {} has multiple parts!".format(row[4]))
#            multi_parts.append(row[4])
#            multi_count += 1
#            continue
#        
#        is_reversed, ang = reversed_check(shape_obj, predir)
#        
#        checks += 1
#        if is_reversed:
##            print("flipping OBJECTID {}".format(row[0]))
#            shape_rev, multipart = reverse_line(shape_obj)
#            row[1] = shape_rev
#            row[3] = 'python flip: {0} {1}'.format(predir, round(ang, 1))
#            flip_count += 1
#            flips.append(row[4])
#        else:
#            row[3] = 'might need flipped: {0} {1}'.format(predir, round(ang, 1))
#        update_cursor.updateRow(row)
#print("Total number of checks is: {}".format(checks))
#print("Total count of flipped segments is: {}".format(flip_count))
#print("Total count of multipart segments is: {}".format(len(multi_parts)))


##########################
#  Call Functions Below  #
##########################

print("Script shutting down ...")
# Stop timer and print end time in UTC
readable_end = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
print("The script end time is {}".format(readable_end))
print("Time elapsed: {:.2f}s".format(time.time() - start_time))


#field_list = ['good', 'good', 'okay', None]
#test = 'NULL'
#
#
#empties = [None, 'none', 'null', '', ' ', '  ']
#
#if any(x is None or x.strip().casefold() in empties for x in field_list):
#    print("Empty was found")
#else:
#    print('not found')
#
#
#print(isinstance(test, str))
#
#if not test or test.strip().casefold() in ('none', 'null', '', ' ', '  '):
#    print('yes it is "empty"')
    


    