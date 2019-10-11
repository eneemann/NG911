# -*- coding: utf-8 -*-
"""
Created on Tue Oct 8 13:20:42 2019
@author: eneemann
Script to build NG911 law enforecement boundaries from SGID data

"""

import arcpy
import os
import time
from datetime import datetime

# Start timer and print start time in UTC
start_time = time.time()
readable_start = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
print("The script start time is {}".format(readable_start))

######################
#  Set up variables  #
######################

SGID = r"C:\Users\eneemann\AppData\Roaming\ESRI\ArcGISPro\Favorites\sgid.agrc.utah.gov.sde"
ng911_db = r"C:\Users\eneemann\Desktop\Neemann\NG911\NG911_project\NG911_project.gdb"
#ng911_db = r"L:\agrc\data\ng911\NG911_boundary_work.gdb"

arcpy.env.workspace = ng911_db
arcpy.env.overwriteOutput = True
arcpy.env.qualifiedFieldNames = False

today = time.strftime("%Y%m%d")

counties = os.path.join(SGID, 'SGID10.BOUNDARIES.Counties')
munis = os.path.join(SGID, 'SGID10.BOUNDARIES.Municipalities')
roads = os.path.join(SGID, 'SGID10.TRANSPORTATION.Roads')
old_law = os.path.join(SGID, 'SGID10.SOCIETY.LawEnforcementBoundaries')
unique = os.path.join(ng911_db, 'NG911_Law_unique_UTM')
law_schema = os.path.join(ng911_db, 'NG911_Law_schema')
law_working = os.path.join(ng911_db, 'NG911_Law_bound_working_' + today)

# Read in text file of municipalities with PDs
print("Reading in text file to get Municipalities with Police Departments ...")
textfile_dir = r'C:\Users\eneemann\Desktop\Neemann\NG911\NG911_project'
filename = os.path.join(textfile_dir, 'Munis_with_PDs.txt')
with open(filename, 'r') as filehandle:
    muni_pd_temp = [muni.strip() for muni in filehandle.readlines()]
muni_pd = []
for item in muni_pd_temp:
    if 'SAINT ' in item:
        muni_pd.append(item.replace('SAINT ', 'ST. '))
    else:
        muni_pd.append(item)
print(muni_pd)

##################
# Basic Workflow #
##################

# Build Sheriff's Office boundaries from county boundaries (law_SOs_temp)
print("Building Sheriff's Office boundaries from counties ...")
SOs_temp = os.path.join(ng911_db, 'NG911_law_bound_SOs_temp')
arcpy.management.CopyFeatures(law_schema, SOs_temp)
if arcpy.Exists("working_lyr"):
    arcpy.management.Delete("working_lyr")
arcpy.management.MakeFeatureLayer(SOs_temp, "working_lyr")
arcpy.management.MakeFeatureLayer(counties, "county_lyr")

# Field Map county name into law schema fields
fms = arcpy.FieldMappings()

# NAME to DsplayName
fm_name = arcpy.FieldMap()
fm_name.addInputField("county_lyr", "NAME")
output = fm_name.outputField
output.name = "DsplayName"
fm_name.outputField = output
fms.addFieldMap(fm_name)

# NAME to Agency_ID
fm_agency = arcpy.FieldMap()
fm_agency.addInputField("county_lyr", "NAME")
output = fm_agency.outputField
output.name = "Agency_ID"
fm_agency.outputField = output
fms.addFieldMap(fm_agency)

# Complete the append with field mapping
arcpy.management.Append("county_lyr", "working_lyr", "NO_TEST", field_mapping=fms)

# Populate fields with information
update_count = 0
#            0           1           2          3            4
fields = ['Source', 'DateUpdate', 'State', 'Agency_ID', 'DsplayName']
with arcpy.da.UpdateCursor("working_lyr", fields) as update_cursor:
    print("Looping through rows in FC ...")
    for row in update_cursor:
        row[0] = 'AGRC'
        row[1] = datetime.now()
        row[2] = 'UT'
        row[3] = row[3] + ' COUNTY SO'
        row[4] = row[4] + ' COUNTY SHERIFFS OFFICE'
        update_count += 1
        update_cursor.updateRow(row)
print("Total count of updates is: {}".format(update_count))


# Drop in other jurisdictions
# Build Muncipality PD boundaries (law_PDs_temp)
print("Building Police Department boundaries from Municipalities ...")
PDs_temp = os.path.join(ng911_db, 'NG911_law_bound_PDs_temp')
arcpy.management.CopyFeatures(law_schema, PDs_temp)
if arcpy.Exists("working_lyr_2"):
    arcpy.management.Delete("working_lyr_2")
arcpy.management.MakeFeatureLayer(PDs_temp, "working_lyr_2")
temp_list = ",".join(f"'{item.upper()}'" for item in muni_pd)
query = f"NAME IN ({temp_list})"
print(query)
arcpy.management.MakeFeatureLayer(munis, "muni_lyr", query)

# Field Map county name into law schema fields
fms = arcpy.FieldMappings()

# NAME to DsplayName
fm_name = arcpy.FieldMap()
fm_name.addInputField("muni_lyr", "NAME")
output = fm_name.outputField
output.name = "DsplayName"
fm_name.outputField = output
fms.addFieldMap(fm_name)

# NAME to Agency_ID
fm_agency = arcpy.FieldMap()
fm_agency.addInputField("muni_lyr", "NAME")
output = fm_agency.outputField
output.name = "Agency_ID"
fm_agency.outputField = output
fms.addFieldMap(fm_agency)

# Complete the append with field mapping
arcpy.management.Append("muni_lyr", "working_lyr_2", "NO_TEST", field_mapping=fms)
# now munis are in law schema with only Agency_ID and DsplayName populated (sk_lyr_2, PDs_temp)

# Populate fields with information
update_count = 0
#            0           1           2          3            4
fields = ['Source', 'DateUpdate', 'State', 'Agency_ID', 'DsplayName']
with arcpy.da.UpdateCursor("working_lyr_2", fields) as update_cursor:
    print("Looping through rows in FC ...")
    for row in update_cursor:
        row[0] = 'AGRC'
        row[1] = datetime.now()
        row[2] = 'UT'
        row[3] = row[3].upper() + ' PD'
        row[4] = row[4].upper() + ' POLICE DEPARTMENT'
        update_count += 1
        update_cursor.updateRow(row)
print("Total count of updates is: {}".format(update_count))

# Dissolve jurisdictions with multiple polygons (Draper, Park City, Santaquin)
print("Dissolving jurisdictions that cross county boundaries ...")
PDs_diss = os.path.join(ng911_db, 'NG911_law_bound_PDs_diss')
PDs_join = os.path.join(ng911_db, 'NG911_law_bound_PDs_join')
arcpy.management.Dissolve(PDs_temp, PDs_diss, "Agency_ID")

# Attempt with attribute join
arcpy.management.CopyFeatures(PDs_diss, PDs_join)
fields_list = ['Source', 'DateUpdate', 'Effective', 'Expire', 'ES_NGUID', 'State',
               'ServiceURI', 'ServiceURN', 'ServiceNum', 'AVcard_URI', 'DsplayName']
print(fields_list)
arcpy.management.JoinField(PDs_join, "Agency_ID", PDs_temp, "Agency_ID", fields_list)

# Add back in all fields via spatial join
#arcpy.analysis.SpatialJoin(PDs_diss, PDs_temp, PDs_join, "JOIN_ONE_TO_ONE", "KEEP_ALL", "", "CLOSEST")

# Build Lone Peak & North Park jurisdictions
print("Building boundaries for PDs that cover multiple municipalities ...")
combos_temp = os.path.join(ng911_db, 'NG911_law_bound_combos_temp')
arcpy.management.CopyFeatures(law_schema, combos_temp)
if arcpy.Exists("working_lyr_3"):
    arcpy.management.Delete("working_lyr_3")
arcpy.management.MakeFeatureLayer(combos_temp, "working_lyr_3")
query = "NAME IN ('Alpine', 'Highland', 'North Logan', 'Hyde Park')"
print(query)
arcpy.management.MakeFeatureLayer(munis, "muni_lyr_3", query)

# Field Map county name into law schema fields
fms = arcpy.FieldMappings()

# NAME to DsplayName
fm_name = arcpy.FieldMap()
fm_name.addInputField("muni_lyr_3", "NAME")
output = fm_name.outputField
output.name = "DsplayName"
fm_name.outputField = output
fms.addFieldMap(fm_name)

# NAME to Agency_ID
fm_agency = arcpy.FieldMap()
fm_agency.addInputField("muni_lyr_3", "NAME")
output = fm_agency.outputField
output.name = "Agency_ID"
fm_agency.outputField = output
fms.addFieldMap(fm_agency)

# Complete the append with field mapping
arcpy.management.Append("muni_lyr_3", "working_lyr_3", "NO_TEST", field_mapping=fms)

# Populate fields with information and rename to combo jurisdiction (Lone Peak)
update_count = 0
#            0           1           2          3            4
fields = ['Source', 'DateUpdate', 'State', 'Agency_ID', 'DsplayName']
query1 = "Agency_ID IN ('Alpine', 'Highland')"
with arcpy.da.UpdateCursor("working_lyr_3", fields, query1) as update_cursor:
    print("Looping through rows in FC ...")
    for row in update_cursor:
        row[0] = 'AGRC'
        row[1] = datetime.now()
        row[2] = 'UT'
        row[3] = 'LONE PEAK PD'
        row[4] = 'LONE PEAK POLICE DEPARTMENT'
        update_count += 1
        update_cursor.updateRow(row)
print("Total count of updates is: {}".format(update_count))

# Populate fields with information and rename to combo jurisdiction (North Park)
update_count = 0
#            0           1           2          3            4
fields = ['Source', 'DateUpdate', 'State', 'Agency_ID', 'DsplayName']
query2 = "Agency_ID IN ('North Logan', 'Hyde Park')"
with arcpy.da.UpdateCursor("working_lyr_3", fields, query2) as update_cursor:
    print("Looping through rows in FC ...")
    for row in update_cursor:
        row[0] = 'AGRC'
        row[1] = datetime.now()
        row[2] = 'UT'
        row[3] = 'NORTH PARK PD'
        row[4] = 'NORTH PARK POLICE DEPARTMENT'
        update_count += 1
        update_cursor.updateRow(row)
print("Total count of updates is: {}".format(update_count))

# Dissolve jurisdictions with multiple polygons (Lone Peak, North Park)
combos_diss = os.path.join(ng911_db, 'NG911_law_bound_combos_diss')
combos_join = os.path.join(ng911_db, 'NG911_law_bound_combos_join')
arcpy.management.Dissolve(combos_temp, combos_diss, "Agency_ID")

# Attempt with attribute join
arcpy.management.CopyFeatures(combos_diss, combos_join)
arcpy.management.JoinField(combos_join, "Agency_ID", combos_temp, "Agency_ID", fields_list)

# Add back in all fields via spatial join
#arcpy.analysis.SpatialJoin(combos_diss, combos_temp, combos_join, "JOIN_ONE_TO_ONE", "KEEP_ALL", "", "CLOSEST")
# "HAVE_THEIR_CENTER_IN" - Hildale and Kanab centers don't work
# "INTERSECT" - Utah County cities get screwed up
# "ARE_IDENTICAL_TO" - Several issues - combo jurisdictions and cross counties
# "CONTAINS" - Tremonton doesn't work (two polygons?)
# "CONTAINS_CLEMENTINI" - Tremonton doesn't work (two polygons?)

# Append combo jurisdictions into PDs layer
print("Adding combo jurisdictions into PDs layer ...")
arcpy.management.Append(combos_join, PDs_join, "NO_TEST")

# Drop police departments into sheriffs offices via erase/append
print("Inserting PD boundaries into Sheriff's Office boundaries ...")
# Erase
SOs_holes = os.path.join(ng911_db, 'NG911_law_bound_SOs_holes')
arcpy.analysis.Erase(SOs_temp, PDs_join, SOs_holes)
# Append
arcpy.management.Append(PDs_join, SOs_holes, "NO_TEST")

# Drop in unique districts via erase/append (law_unique_temp) - tribal, Navajo Nation, etc.
print("Adding unique districts into SOs/PDs layer ...")
# Erase
law_final = os.path.join(ng911_db, 'NG911_law_bound_final')
arcpy.analysis.Erase(SOs_holes, unique, law_final)
# Append
arcpy.management.Append(unique, law_final, "NO_TEST")

# Ensure Unified PD is properly named (from remainder of Salt Lake County)
update_count = 0
#            0           1           2          3            4
fields = ['Source', 'DateUpdate', 'State', 'Agency_ID', 'DsplayName']
query3 = "Agency_ID = 'SALT LAKE COUNTY SO'"
with arcpy.da.UpdateCursor(law_final, fields, query3) as update_cursor:
    print("Looping through rows in FC ...")
    for row in update_cursor:
        row[3] = 'UNIFIED PD'
        row[4] = 'UNIFIED POLICE DEPARTMENT'
        update_count += 1
        update_cursor.updateRow(row)
print("Total count of updates is: {}".format(update_count))



# Project final data to WGS84
print("Projecting final law boundaries into WGS84 ...")
law_wgs84 = os.path.join(ng911_db, 'NG911_law_bound_final_WGS84')
sr = arcpy.SpatialReference("WGS 1984")
arcpy.management.Project(law_final, law_wgs84, sr, "WGS_1984_(ITRF00)_To_NAD_1983")

# Drop in UHP boundaries - buffer state/federal highways (10-30m)
# Only use buffers outside of municipalities?

###############
#  Functions  #
###############

#add_sheriff()
#add_muni_pds()
#add_lone_peak()
#add_north_park()
#add_unified_pd()
#add_unique_pds()

##########################
#  Call Functions Below  #
##########################

print("Script shutting down ...")
# Stop timer and print end time in UTC
readable_end = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
print("The script end time is {}".format(readable_end))
print("Time elapsed: {:.2f}s".format(time.time() - start_time))
