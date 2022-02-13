# -*- coding: utf-8 -*-
"""
Created on Mon Sep 20 1:10:42 2021
@author: eneemann
Script to flag NG911 errors on Road Centerlines
"""

import arcpy
import os
import time

# Start timer and print start time in UTC
start_time = time.time()
readable_start = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
print("The script start time is {}".format(readable_start))

######################
#  Set up variables  #
######################

# Set up databases (SGID must be changed based on user's path)
ng911_db = r"\\itwfpcap2\AGRC\agrc\data\ng911\SpatialStation_live_data\UtahNG911GIS.gdb"
error_db = r"C:\Users\eneemann\Desktop\Neemann\NG911\911 DataMaster\NG911_Data_Errors.gdb"

arcpy.env.workspace = ng911_db
arcpy.env.overwriteOutput = True
arcpy.env.qualifiedFieldNames = False

today = time.strftime("%Y%m%d")

rcls = os.path.join(ng911_db, r'RoadCenterlines')
rcls_working = os.path.join(error_db, f'RCL_errors_{today}')
rcls_final = os.path.join(error_db, f'RCL_errors_only_{today}')
rcls_final_name = f'RCL_errors_only_{today}'

## Make a copy of the data to work on
print(f'Copying data into {rcls_working} ...')
arcpy.management.CopyFeatures(rcls, rcls_working)
arcpy.AddField_management(rcls_working, "Error_UGRC", "TEXT", "", "", 100)

print("Time elapsed: {:.2f}s".format(time.time() - start_time)) 


rcl_count = int(arcpy.management.GetCount(rcls_working)[0])


print(f'Working on error checks for: {rcls_working}')

description = arcpy.da.Describe(rcls_working)

#: list of mandatory fields
#mandatory_fields = ['RCL_NGUID', 'FromAddr_L', 'ToAddr_L', 'Parity_L', 'Parity_R', 'FromAddr_R', 'ToAddr_R', 'State_L', 'State_R',
#                    'County_L', 'County_R', 'MSAGComm_L', 'MSAGComm_R', 'LSt_Name']
mandatory_fields = ['RCL_NGUID', 'FromAddr_L', 'ToAddr_L', 'FromAddr_R', 'ToAddr_R', 'State_L', 'State_R',
                    'County_L', 'County_R', 'MSAGComm_L', 'MSAGComm_R', 'LSt_Name']

#: add more fields needed in the analysis
fields = mandatory_fields + ['OID@', 'Error_UGRC', 'Parity_L', 'Parity_R']

empties = [None, 'none', 'null', '', ' ', '  ']

oid_index = fields.index('OID@')
notes_index = fields.index('Error_UGRC')
fromL_index = fields.index('FromAddr_L')
toL_index = fields.index('ToAddr_L')
fromR_index = fields.index('FromAddr_R')
toR_index = fields.index('ToAddr_R')
parL_index = fields.index('Parity_L')
parR_index = fields.index('Parity_R')

oids_with_issues = []

range_count = 0
parity_count = 0
required_count = 0
print("Looping through rows in FC ...")
with arcpy.da.UpdateCursor(rcls_working, fields) as update_cursor:
    mandatory_idx = [fields.index(item) for item in fields if item in mandatory_fields]
    for row in update_cursor:
        comment = None
        parity_issue = False
        fromL = row[fromL_index]
        toL = row[toL_index]
        fromR = row[fromR_index]
        toR = row[toR_index]
        parL = row[parL_index]
        parR = row[parR_index]
        object_id = row[oid_index]
        
        if object_id % 25000 == 0:
            print(f'working on OBJECTID: {object_id}')

        #: Check for low vs high range problem
        if (toL < fromL) or (toR < fromR):
            range_count += 1 
            comment = 'low vs high range'
        #: Option to automatically apply range fix
#        if toL < fromL:
#            row[fromL_index] = toL
#            row[toL_index] = fromL
#            comment = 'python range fix'
#        if toR < fromR:
#            row[fromR_index] = toR
#            row[toR_index] = fromR
#            comment = 'python range fix'
        
        #: Check for parity inconsistency
        fromL_odd = True if fromL % 2 else False
        toL_odd = True if toL % 2 else False
        fromR_odd = True if fromR % 2 else False
        toR_odd = True if toR % 2 else False
        
#        if parL == 'O' or parR == 'E':
        if parL != 'B' and parR != 'B':
            if fromL_odd != toL_odd:
                parity_issue = True
                if comment is None or comment in ('', ' '):
                    comment = 'left parity'
                else:
                    comment += ', left parity'
                
            if fromR_odd != toR_odd:
                parity_issue = True
                if comment is None or comment in ('', ' '):
                    comment = 'right parity'
                else:
                    comment += ', right parity'
                    
            if parity_issue: parity_count += 1
        
        #: Check mandatory fields (initially ignoring 'Parity_L' and 'Parity_R')
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

print(f"Total count of low vs high range issues is: {range_count} or {round((range_count/rcl_count)*100, 3)}%")
print(f"Total count of parity issues is: {parity_count} or {round((parity_count/rcl_count)*100, 3)}%")
print(f"Total count of rows missing required value: {required_count} or {round((required_count/rcl_count)*100, 3)}%")

oid_set = set(oids_with_issues)
print('\nSelect statement to view errors in ArcGIS:')
sql = f'OBJECTID IN ({", ".join([str(oid) for oid in oid_set])})'
print(sql)

# Create copy with only points containing errors
print('Exporting features with errors in separate feature class ...')
where_clause = """Error_UGRC IS NOT NULL"""
arcpy.conversion.FeatureClassToFeatureClass(rcls_working, error_db, rcls_final_name, where_clause)

##########################
#  Call Functions Below  #
##########################

print("Script shutting down ...")
# Stop timer and print end time in UTC
readable_end = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
print("The script end time is {}".format(readable_end))
print("Time elapsed: {:.2f}s".format(time.time() - start_time))    
