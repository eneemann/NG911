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

# Set up databases (SGID must be changed based on user's path)
SGID = r"C:\Users\eneemann\AppData\Roaming\ESRI\ArcGISPro\Favorites\internal@SGID@internal.agrc.utah.gov.sde"
ng911_db = r"\\itwfpcap2\AGRC\agrc\data\ng911\NG911_boundary_work.gdb"
#ng911_db = r"C:\Users\eneemann\Desktop\Neemann\NG911\NG911_project\NG911_project.gdb"


arcpy.env.workspace = ng911_db
arcpy.env.overwriteOutput = True
arcpy.env.qualifiedFieldNames = False

today = time.strftime("%Y%m%d")

counties = os.path.join(SGID, 'SGID.BOUNDARIES.Counties')
munis = os.path.join(SGID, 'SGID.BOUNDARIES.Municipalities')
unique = os.path.join(ng911_db, 'NG911_Law_unique_UTM')
law_schema = os.path.join(ng911_db, 'NG911_Law_schema')
law_working = os.path.join(ng911_db, 'NG911_Law_bound_working_' + today)

# Read in text file of municipalities with PDs
print("Reading in text file to get Municipalities with Police Departments ...")
textfile_dir = r'\\itwfpcap2\AGRC\agrc\data\ng911'
#textfile_dir = r'C:\Users\eneemann\Desktop\Neemann\NG911\NG911_project'
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

# Read in text file of combo PDs
combos = {}
print("Reading in text file for combo Police Departments ...")
#textfile_dir = r'C:\Users\eneemann\Desktop\Neemann\NG911\NG911_project'
filename2 = os.path.join(textfile_dir, 'Combo_PDs.txt')
with open(filename2, 'r') as filehandle:
    combo_temp = [muni.strip() for muni in filehandle.readlines()]
for item in combo_temp:
    first = item.split(":")[0].strip()
    second = item.split(":")[1].strip().replace('"', '')
    parts = second.split(',')
    parts = [part.strip() for part in parts]
    num_parts = len(parts)
    combos[first] = parts
print(combos)

# Read in text file of rename PDs
renames = {}
print("Reading in text file for renaming Police Departments ...")
#textfile_dir = r'C:\Users\eneemann\Desktop\Neemann\NG911\NG911_project'
filename3 = os.path.join(textfile_dir, 'Rename_PDs.txt')
with open(filename3, 'r') as filehandle:
    rename_temp = [word.strip() for word in filehandle.readlines()]
for item in rename_temp:
    first = item.split(":")[0].strip()
    second = item.split(":")[1].strip().replace('"', '')
    parts = second.split(',')
    parts = [part.strip() for part in parts]
    num_parts = len(parts)
    renames[first] = parts
print(renames)

# Set up more variables for intermediate and final feature classes
SOs_temp = os.path.join(ng911_db, 'NG911_law_bound_SOs_temp')
PDs_temp = os.path.join(ng911_db, 'NG911_law_bound_PDs_temp')
PDs_diss = os.path.join(ng911_db, 'NG911_law_bound_PDs_diss')
PDs_join = os.path.join(ng911_db, 'NG911_law_bound_PDs_join')
combos_temp = os.path.join(ng911_db, 'NG911_law_bound_combos_temp')
combos_diss = os.path.join(ng911_db, 'NG911_law_bound_combos_diss')
combos_join = os.path.join(ng911_db, 'NG911_law_bound_combos_join')
SOs_holes = os.path.join(ng911_db, 'NG911_law_bound_SOs_holes')
law_final = os.path.join(ng911_db, 'NG911_law_bound_final_' + today)
law_wgs84 = os.path.join(ng911_db, 'NG911_law_bound_final_WGS84_' + today)

###############
#  Functions  #
###############

def add_sheriff():
    # Build Sheriff's Office boundaries from county boundaries (law_SOs_temp)
    print("Building Sheriff's Office boundaries from counties ...")
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


def add_muni_pds():
    # Drop in other jurisdictions
    # Build Muncipality PD boundaries (law_PDs_temp)
    print("Building Police Department boundaries from Municipalities ...")
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


def add_combos():
    # Dissolve jurisdictions with multiple polygons (Draper, Park City, Santaquin)
    print("Dissolving jurisdictions that cross county boundaries ...")
    arcpy.management.Dissolve(PDs_temp, PDs_diss, "Agency_ID")
    
    # Add fields back in with attribute join
    arcpy.management.CopyFeatures(PDs_diss, PDs_join)
    fields_list = ['Source', 'DateUpdate', 'Effective', 'Expire', 'ES_NGUID', 'State',
                   'ServiceURI', 'ServiceURN', 'ServiceNum', 'AVcard_URI', 'DsplayName']
    print(fields_list)
    arcpy.management.JoinField(PDs_join, "Agency_ID", PDs_temp, "Agency_ID", fields_list)
    
    
    # Build combo jurisdictions
    print("Building boundaries for PDs that cover multiple municipalities ...")
    arcpy.management.CopyFeatures(law_schema, combos_temp)
    if arcpy.Exists("working_lyr_3"):
        arcpy.management.Delete("working_lyr_3")
    arcpy.management.MakeFeatureLayer(combos_temp, "working_lyr_3")
    
    # Build query to select combo PDs
    combo_list = [ item.title() for sublist in (combos[key] for key in combos) for item in sublist ]
    print(combo_list)
    query_test = f"NAME IN ({combo_list})".replace('[', '').replace(']', '')
    print(query_test)

    arcpy.management.MakeFeatureLayer(munis, "muni_lyr_3", query_test)
    
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
    
    # Loop through and populate fields with appropriate information and rename to combo jurisdictions (All)
    #            0           1           2          3            4   
    fields = ['Source', 'DateUpdate', 'State', 'Agency_ID', 'DsplayName']
    for key in combos:
        temp_list = [ item.title() for item in combos[key] ]
        query1 = f"Agency_ID IN ({temp_list})".replace('[', '').replace(']', '')
        print(query1)
        print(f"Updating {key} jurisdication ...")
        with arcpy.da.UpdateCursor("working_lyr_3", fields, query1) as update_cursor:
            for row in update_cursor:
                row[0] = 'AGRC'
                row[1] = datetime.now()
                row[2] = 'UT'
                row[3] = key
                row[4] = key.replace(' PD', ' POLICE DEPARTMENT')
                update_cursor.updateRow(row)
        
    print("Dissolving combo jurisdications ...")
    arcpy.management.Dissolve(combos_temp, combos_diss, "Agency_ID")
    
    # Attempt with attribute join
    arcpy.management.CopyFeatures(combos_diss, combos_join)
    arcpy.management.JoinField(combos_join, "Agency_ID", combos_temp, "Agency_ID", fields_list)
    
    # Append combo jurisdictions into PDs layer
    print("Adding combo jurisdictions into PDs layer ...")
    arcpy.management.Append(combos_join, PDs_join, "NO_TEST")
    
    # Drop police departments into sheriffs offices via erase/append
    print("Inserting PD boundaries into Sheriff's Office boundaries ...")
    # Erase
    arcpy.analysis.Erase(SOs_temp, PDs_join, SOs_holes)
    # Append
    arcpy.management.Append(PDs_join, SOs_holes, "NO_TEST")
    
    
def add_unique_pds():
    # Drop in unique districts via erase/append (law_unique_temp) - tribal, Navajo Nation, etc.
    print("Adding unique districts into SOs/PDs layer ...")
    # Erase
    arcpy.analysis.Erase(SOs_holes, unique, law_final)
    # Append
    arcpy.management.Append(unique, law_final, "NO_TEST")
    

def correct_names():
    # Ensure Unified PD is properly named (from remainder of Salt Lake County)
    #            0           1           2          3            4
    fields = ['Source', 'DateUpdate', 'State', 'Agency_ID', 'DsplayName']
    for key in renames:
        print(f"Updating {key} jurisdiction name ...")
        query2 = f"Agency_ID = '{key}'"
        print(query2)
        print(f'new Agency_ID is: {renames[key][0]}')
        print(f'new DsplayName is: {renames[key][1]}')
        with arcpy.da.UpdateCursor(law_final, fields, query2) as update_cursor:
            for row in update_cursor:
                row[3] = renames[key][0]
                row[4] = renames[key][1]
                update_cursor.updateRow(row)
       

def project_to_WGS84():
    # Project final data to WGS84
    print("Projecting final law boundaries into WGS84 ...")
    sr = arcpy.SpatialReference("WGS 1984")
    arcpy.management.Project(law_final, law_wgs84, sr, "WGS_1984_(ITRF00)_To_NAD_1983")

#----------------------------------------------------------------
# Additional enhancements for the future
# Drop in UHP boundaries - buffer state/federal highways (10-30m)
#   Only use buffers outside of municipalities?
#----------------------------------------------------------------


##########################
#  Call Functions Below  #
##########################
    
add_sheriff()
add_muni_pds()
add_combos()
add_unique_pds()
correct_names()
project_to_WGS84()

print("Script shutting down ...")
# Stop timer and print end time in UTC
readable_end = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
print("The script end time is {}".format(readable_end))
print("Time elapsed: {:.2f}s".format(time.time() - start_time))
