# -*- coding: utf-8 -*-
"""
Created on Mon Aug 30 13:01:42 2021
@author: eneemann
Script to fix NG911 errors where RCLs are pointed in the wrong direction

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
ng911_db = r"C:\Users\eneemann\Desktop\Neemann\NG911\911 DataMaster\NG911_Errors_20210812.gdb"


arcpy.env.workspace = ng911_db
arcpy.env.overwriteOutput = True
arcpy.env.qualifiedFieldNames = False

today = time.strftime("%Y%m%d")

#RCLs = os.path.join(ng911_db, 'Davis_Road_errors_20210815_v3')
RCLs = os.path.join(ng911_db, 'RoadCenterlines')
RCLs_working = os.path.join(ng911_db, f'RCL_flip_fixes_{today}')

# Make a copy of the data to work on
arcpy.management.CopyFeatures(RCLs, RCLs_working)

# Get the spatial reference for later use
sr = arcpy.Describe(RCLs_working).spatialReference


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
    if predir == 'NORTH' and (angle > 90 and angle < 270):
        status = True
    elif predir == 'SOUTH' and (angle > 270 or angle < 90):
        status = True
    elif predir == 'EAST' and (angle > 180 and angle < 360):
        status = True
    elif predir == 'WEST' and (angle > 0 and angle < 180):
        status = True
    
#    if not status:
#        print(f'predir is {predir}, angle is {angle}')
    
    return status, angle


def reverse_line(line):
    multi = False
    pts_orig = []
    if shape_obj.partCount > 1: 
        print("Warning: multiple parts! extra parts are automatically trimmed!")
        print(f"Line has {shape_obj.partCount} parts")
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
    

# Loop through data and flip roads that have bad directions
flip_count = 0
    #          0           1          2             3            4
fields = ['RCL_NGUID', 'SHAPE@', 'St_PreDir', 'dmNotesXML', 'OBJECTID']
with arcpy.da.UpdateCursor(RCLs_working, fields) as update_cursor:
    print("Looping through rows in FC to check for flipped segments ...")
    for row in update_cursor:
#        if row[5] == 4:
        shape_obj = row[1]
        predir = row[2]
#        print(shape_obj)
#       print(f'part count: {shape_obj.partCount}')
        
        is_reversed, ang = reversed_check(shape_obj, predir)
        
        if is_reversed:
#            print(f"flipping NGUID {row[0]}")
            shape_rev, multipart = reverse_line(shape_obj)
            if multipart:
                print(f"NGUID {row[0] has multiple parts!}")
            row[1] = shape_rev
            row[3] = f'python flipped {predir} {round(ang, 1)}'
            flip_count += 1
        else:
            row[3] = f'ok {predir} {round(ang, 1)}'
        update_cursor.updateRow(row)
print(f"Total count of flipped segments is: {flip_count}")


##########################
#  Call Functions Below  #
##########################

print("Script shutting down ...")
# Stop timer and print end time in UTC
readable_end = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
print("The script end time is {}".format(readable_end))
print("Time elapsed: {:.2f}s".format(time.time() - start_time)) 
    