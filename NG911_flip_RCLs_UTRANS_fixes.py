# -*- coding: utf-8 -*-
"""
Created on Mon Aug 30 13:01:42 2021
@author: eneemann
Script to fix UTRANS errors where RCLs are pointed in the wrong direction
"""

import arcpy
import os
import time
from datetime import datetime
import pandas as pd
import math

# Start timer and print start time in UTC
start_time = time.time()
readable_start = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
print("The script start time is {}".format(readable_start))

######################
#  Set up variables  #
######################

# Set up databases (SGID must be changed based on user's path)
utrans_db = r"Database Connections\eneemann@UTRANS@utrans.agrc.utah.gov.sde"


arcpy.env.workspace = utrans_db
arcpy.env.overwriteOutput = True
arcpy.env.qualifiedFieldNames = False

today = time.strftime("%Y%m%d")

RCLs = os.path.join(utrans_db, r'UTRANS.TRANSADMIN.Centerlines_Edit\UTRANS.TRANSADMIN.Roads_Edit')

## Make a copy of the data to work on
#arcpy.management.CopyFeatures(RCLs, RCLs_working)

# Get the spatial reference for later use
sr = arcpy.Describe(RCLs).spatialReference
print(sr)


def angle_calc(line):  
    angle = math.degrees(math.atan2((line.lastPoint.X - line.firstPoint.X),(line.lastPoint.Y - line.firstPoint.Y)))  
    if angle < 0:
        angle += 360.0
        
    return angle


def reversed_check(line, predir):
    status = False
    
    # Calculate angle
    angle = angle_calc(line)
    
    # Compare angle to expected range based on predir
    # If angle doesn't align with predir, change status to True
#    if predir == 'N' and (angle > 90 and angle < 270):
#        status = True
#    elif predir == 'S' and (angle > 270 or angle < 90):
#        status = True
#    elif predir == 'E' and (angle > 180 and angle < 360):
#        status = True
#    elif predir == 'W' and (angle > 0 and angle < 180):
#        status = True
        
    if predir == 'N' and (angle > 100 and angle < 260):
        status = True
    elif predir == 'S' and (angle > 280 or angle < 80):
        status = True
    elif predir == 'E' and (angle > 190 and angle < 350):
        status = True
    elif predir == 'W' and (angle > 10 and angle < 170):
        status = True
    
#    if not status:
#        print(f'predir is {predir}, angle is {angle}')
    
    return status, angle


def reverse_line(line):
    multi = False
    pts_orig = []
    if shape_obj.partCount > 1: 
        print("Warning: multiple parts! extra parts are automatically trimmed!")
        print("Line has {} parts".format(shape_obj.partCount))
        multi = True
    # Step through and use first part of the feature
    part = line.getPart(0)
    # put the points into a list, then reverse the list
    for pnt in part:
        pts_orig.append((pnt.X, pnt.Y))
    pts_rev = pts_orig[::-1]
    
    #            print(pts_orig)
    #            print(pts_rev)
    
    # rebuild geometry of reversed line
    arc_pts = [arcpy.Point(item[0], item[1]) for item in pts_rev]
    array= arcpy.Array(arc_pts)
    geom_rev = arcpy.Polyline(array, sr)
    
    return geom_rev, multi
    

# Start an edit session
edit = arcpy.da.Editor(utrans_db)
edit.startEditing(False, True)
edit.startOperation()

# Loop through data and flip roads that have bad directions
multi_parts = []
flips = []
checks = 0
flip_count = 0
multi_count = 0

query = "TOADDR_L <> 0 AND TOADDR_R <> 0"
    #          0           1          2             3          4           5           6
fields = ['UNIQUE_ID', 'SHAPE@', 'PREDIR', 'UTRANS_NOTES', 'OBJECTID', 'TOADDR_L', 'TOADDR_R']
with arcpy.da.UpdateCursor(RCLs, fields, query) as update_cursor:
    print("Looping through rows in FC to check for flipped segments ...")
    for row in update_cursor:
        if row[4] % 10000 == 0:
            print('working on OBJECTID: {}'.format(row[4]))
#        if row[5] == 0 and row[6] == 0:
#            continue
#        if row[5] == 4:
#         if row[4] > 210000:
#             print(row[4])
        shape_obj = row[1]
        predir = row[2]
        if shape_obj.partCount > 1:
            print("OBJECTID {} has multiple parts!".format(row[4]))
            multi_parts.append(row[4])
            multi_count += 1
            continue
            
#        print(shape_obj)
#       print(f'part count: {shape_obj.partCount}')
        
        is_reversed, ang = reversed_check(shape_obj, predir)
        
        checks += 1
        if is_reversed:
#            print("flipping OBJECTID {}".format(row[0]))
            shape_rev, multipart = reverse_line(shape_obj)
#            if multipart:
#                print("OBJECTID {} has multiple parts!".format(row[4]))
#                multi_parts.append(row[4])
#                continue
#            row[1] = shape_rev
#            row[3] = 'python flip: {0} {1}'.format(predir, round(ang, 1))
            flip_count += 1
            flips.append(row[4])
#        else:
#            row[3] = f'ok {predir} {round(ang, 1)}'
        update_cursor.updateRow(row)
print("Total number of checks is: {}".format(checks))
print("Total count of flipped segments is: {}".format(flip_count))
print("Total count of multipart segments is: {}".format(len(multi_parts)))

# Stop edit session
edit.stopOperation()
edit.stopEditing(True)


##########################
#  Call Functions Below  #
##########################

print("Script shutting down ...")
# Stop timer and print end time in UTC
readable_end = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
print("The script end time is {}".format(readable_end))
print("Time elapsed: {:.2f}s".format(time.time() - start_time)) 
    