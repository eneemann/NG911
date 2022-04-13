# -*- coding: utf-8 -*-
"""
Created on Mon Aug 30 13:01:42 2021
@author: eneemann
Script to fix UTRANS errors where RCLs are pointed in the wrong direction
"""

import arcpy
import os
import time
import numpy as np

# Start timer and print start time in UTC
start_time = time.time()
readable_start = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
print("The script start time is {}".format(readable_start))

######################
#  Set up variables  #
######################

# Set up databases (SGID must be changed based on user's path)
#utrans_db = r"Database Connections\eneemann@UTRANS@utrans.agrc.utah.gov.sde"
utrans_db = r'C:\Users\eneemann\Desktop\Neemann\working_data.gdb'

arcpy.env.workspace = utrans_db
arcpy.env.overwriteOutput = True
arcpy.env.qualifiedFieldNames = False

today = time.strftime("%Y%m%d")

#RCLs = os.path.join(utrans_db, r'UTRANS.TRANSADMIN.Centerlines_Edit\UTRANS.TRANSADMIN.Roads_Edit')
RCLs = os.path.join(utrans_db, 'UTRANS_overlaps_export_20220413')
RCLs_backup = os.path.join(utrans_db, f'UTRANS_overlaps_20220413_{today}')

## Make a copy of the data to work on
#arcpy.management.CopyFeatures(RCLs, RCLs_backup)

# Get list of global_ids that have range overlaps
global_ids = [str(i).strip("(',)") for i in arcpy.da.SearchCursor(RCLs, 'GlobalID')]

# Get list of overlapping global_ids to compare against 'global_ids' list
overlap_ids = [str(i).strip("(',)").split() for i in arcpy.da.SearchCursor(RCLs, 'CUSTOMTAGS')]
overlap_ids_clean = []
for l in overlap_ids:
    l = list(set([str(item).strip("(',)") for item in l if '{' in item]))
    overlap_ids_clean.append(l)

# Merge lists into two-global_id combinations to check for connectivity and overlaps
combos_of_two = []
print(len(global_ids))
for i in np.arange(len(global_ids)):
    for item in overlap_ids_clean[i]:
        temp1 = [global_ids[i]]
        temp1.append(item)
        combos_of_two.append(temp1)


# Create lists to hold guids for segments than need range fixes
guids_to_fix_L = []
guids_to_fix_R = []

missing_guid = 0
# Loop through data and flag segments for toaddr_l decreases
combo_count = 0
print("Looping through combos to flag segments that need corrections...")
for combo in combos_of_two:
    combo_count += 1
    if combo_count % 1000 == 0:
        print(f'working on combo {combo_count}')
    d = {}
    query = f"GlobalID IN ('{combo[0]}', '{combo[1]}') AND CUSTOMTAGS LIKE '%overlap%' and CUSTOMTAGS NOT LIKE '%fixed%' and CUSTOMTAGS NOT LIKE '%okay%'"
    #             0           1          2             3           4             5            6
    fields = ['GlobalID', 'SHAPE@', 'CUSTOMTAGS', 'FROMADDR_L', 'TOADDR_L', 'FROMADDR_R', 'TOADDR_R']
    count = [row for row in arcpy.da.SearchCursor(RCLs, fields, query)]
    if len(count) < 2:
        print('missing a guid ...')
        missing_guid += 1
        continue
    with arcpy.da.SearchCursor(RCLs, fields, query) as search_cursor:
        seg_count = 1
        for row in search_cursor:
            shape_obj = row[1]
            d.update(
                    {f'guid_{seg_count}': row[0],
                     f'start_{seg_count}': [shape_obj.firstPoint.X, shape_obj.firstPoint.Y],
                     f'end_{seg_count}': [shape_obj.lastPoint.X, shape_obj.lastPoint.Y],
                     f'from_L_{seg_count}': row[3],
                     f'to_L_{seg_count}': row[4],
                     f'from_R_{seg_count}': row[5],
                     f'to_R_{seg_count}': row[6]}
                     )
            
            seg_count +=1
            
            
    # check that segments are touching/adjacent
    touching = False
    fix_left = False
    fix_right = False
    
    if d['end_1'] == d['start_2'] or d['start_1'] == d['end_2']:
        touching = True
    
    # Check ranges to see if start or end of ranges match, if so, segment needs fixed
    if touching:
        if d['to_L_1'] == d['from_L_2'] or d['from_L_1'] == d['to_L_2']:
            fix_left = True
        if d['to_R_1'] == d['from_R_2'] or d['from_R_1'] == d['to_R_2']:
            fix_right = True
    
    # Append guid for left side fixes, segment with lowest fromaddr will get fixed        
    if fix_left:
        if d['from_L_1'] < d['from_L_2']:
            guids_to_fix_L.append(d['guid_1'])
        elif d['from_L_2'] < d['from_L_2']:
            guids_to_fix_L.append(d['guid_2'])
    
    # Append guid for right side fixes, segment with lowest fromaddr will get fixed       
    if fix_right:
        if d['from_R_1'] < d['from_R_2']:
            guids_to_fix_R.append(d['guid_1'])
        elif d['from_R_2'] < d['from_R_2']:
            guids_to_fix_R.append(d['guid_2'])

print(f"Total number of missing guids: {missing_guid}")
                  
# Get list of all guids to update values on
all_guids_to_fix = list(set(guids_to_fix_L + guids_to_fix_R))
guids_to_fix_L = list(set(guids_to_fix_L))
guids_to_fix_R = list(set(guids_to_fix_R))    
total = len(guids_to_fix_L) + len(guids_to_fix_R)

# Time hack
print("Time elapsed identifying fixes: {:.2f}s".format(time.time() - start_time))
fix_time = time.time()


## Start an edit session
#edit = arcpy.da.Editor(utrans_db)
#edit.startEditing(False, True)
#edit.startOperation()

fixes = []
fix_count = 0
# Loop through flagged segments and apply range decreases
sql = f"""GlobalID IN ('{"', '".join([str(guid) for guid in all_guids_to_fix])}') AND CUSTOMTAGS LIKE '%overlap%' and CUSTOMTAGS NOT LIKE '%fixed%' and CUSTOMTAGS NOT LIKE '%okay%'"""
#             0            1            2             3           4             5            6
fields = ['GlobalID', 'CUSTOMTAGS', 'FROMADDR_L', 'TOADDR_L', 'FROMADDR_R', 'TOADDR_R']
with arcpy.da.UpdateCursor(RCLs, fields, sql) as update_cursor:
    print("Looping through rows in FC to fix ranges ...")
    for row in update_cursor:
        side = ''
        tags = row[1]
        gid = row[0]
        if 'fixed' in tags:
            print(f'fixes already made on {gid}')
            continue
        if gid in guids_to_fix_L:
            side = 'left'
            row[3] = row[3] - 2
        if gid in guids_to_fix_R:
            if len(side) > 3:
                side += ' right'
            else:
                side = 'right'
            row[5] = row[5] - 2

        tags = tags + f' pythonfixed {side} ' + time.strftime('%m/%d/%Y')
        row[1] = tags
        update_cursor.updateRow(row)

print(f"Total number of checks is: {total}")


print("Time elapsed applying the fixes: {:.2f}s".format(time.time() - fix_time))
    

## Stop edit session
#edit.stopOperation()
#edit.stopEditing(True)


##########################
#  Call Functions Below  #
##########################

print("Script shutting down ...")
# Stop timer and print end time in UTC
readable_end = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
print("The script end time is {}".format(readable_end))
print("Time elapsed: {:.2f}s".format(time.time() - start_time)) 
    