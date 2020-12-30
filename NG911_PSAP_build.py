# -*- coding: utf-8 -*-
"""
Created on Mon Dec 28 14:48:42 2020
@author: eneemann
Script to build NG911 PSAP boundaries from SGID data

"""

import arcpy
import os
import time
from datetime import datetime
import pandas as pd

# Start timer and print start time in UTC
start_time = time.time()
readable_start = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
print("The script start time is {}".format(readable_start))

######################
#  Set up variables  #
######################

# Set up databases (SGID must be changed based on user's path)
SGID = r"C:\Users\eneemann\AppData\Roaming\ESRI\ArcGISPro\Favorites\internal@SGID@internal.agrc.utah.gov.sde"
# ng911_db = r"\\itwfpcap2\AGRC\agrc\data\ng911\NG911_boundary_work.gdb"
# ng911_db = r"C:\Users\eneemann\Desktop\Neemann\NG911\NG911_project\NG911_project.gdb"
ng911_db = r"C:\Users\eneemann\Desktop\Neemann\NG911\NG911_project\NG911_boundary_work_testing.gdb"


arcpy.env.workspace = ng911_db
arcpy.env.overwriteOutput = True
arcpy.env.qualifiedFieldNames = False

today = time.strftime("%Y%m%d")

SGID_counties = os.path.join(SGID, 'SGID.BOUNDARIES.Counties')
counties = os.path.join(ng911_db, f'SGID_counties_{today}')
SGID_munis = os.path.join(SGID, 'SGID.BOUNDARIES.Municipalities')
munis = os.path.join(ng911_db, f'SGID_munis_{today}')
unique = os.path.join(ng911_db, 'NG911_PSAP_unique_UTM')
psap_schema = os.path.join(ng911_db, 'NG911_PSAP_schema_simple')
psap_working = os.path.join(ng911_db, f'NG911_PSAP_bound_working_{today}')

# Read in CSV of PSAP info into pandas dataframe, use df to build dictionaries
print("Reading in CSV to get PSAP info ...")
# textfile_dir = r'L:\agrc\data\ng911'
# textfile_dir = r'C:\Users\eneemann\Desktop\Neemann\NG911\NG911_project'
textfile_dir = r'C:\Users\eneemann\Desktop\Python Code\NG911'

csv = os.path.join(textfile_dir, 'PSAP_info.csv')
psap_info = pd.read_csv(csv)

# Create dictionary for single county PSAPs
single_county = psap_info[psap_info['Type'] == 'single county']
single_county.drop(['Key', 'Type', 'Munis'], axis=1, inplace=True)
single_county_dict = single_county.set_index('PSAP').to_dict()['Counties']

# Create dictionary for multi county PSAPs
multi_county = psap_info[psap_info['Type'] == 'multi county']
multi_county.drop(['Key', 'Type', 'Munis'], axis=1, inplace=True)
multi_county_dict = multi_county.set_index('PSAP').to_dict()['Counties']

# Create dictionary for single muni PSAPs
single_muni = psap_info[psap_info['Type'] == 'single muni']
single_muni.drop(['Key', 'Type', 'Counties'], axis=1, inplace=True)
single_muni_dict = single_muni.set_index('PSAP').to_dict()['Munis']

# Create dictionary for multi muni PSAPs
multi_muni = psap_info[psap_info['Type'] == 'multi muni']
multi_muni.drop(['Key', 'Type', 'Counties'], axis=1, inplace=True)
multi_muni_dict = multi_muni.set_index('PSAP').to_dict()['Munis']


# Set up more variables for intermediate and final feature classes
single_county_temp = os.path.join(ng911_db, 'NG911_psap_bound_sc_temp')
multi_county_temp = os.path.join(ng911_db, 'NG911_psap_bound_mc_temp')
all_county_temp = os.path.join(ng911_db, 'NG911_psap_bound_allc_temp')
mc_diss = os.path.join(ng911_db, 'NG911_psap_bound_mc_diss')
# combos_temp = os.path.join(ng911_db, 'NG911_psap_bound_combos_temp')
# combos_diss = os.path.join(ng911_db, 'NG911_psap_bound_combos_diss')
# combos_join = os.path.join(ng911_db, 'NG911_psap_bound_combos_join')
# SOs_holes = os.path.join(ng911_db, 'NG911_psap_bound_SOs_holes')
# psap_final = os.path.join(ng911_db, 'NG911_psap_bound_final_' + today)
# psap_wgs84 = os.path.join(ng911_db, 'NG911_psap_bound_final_WGS84_' + today)

fc_list = [counties, munis, single_county_temp, multi_county_temp, all_county_temp, mc_diss]

for fc in fc_list:
    if arcpy.Exists(fc):
        print(f'Deleting {fc} ...')
        arcpy.management.Delete(fc)
        
# Copy SGID counties and munis to local fc

arcpy.management.CopyFeatures(SGID_counties, counties)
arcpy.management.CopyFeatures(SGID_munis, munis)

###############
#  Functions  #
###############

def add_single_county():
    # Build single county PSAP boundaries from county boundaries
    print("Building single county PSAPs from county boundaries ...")
    arcpy.management.CopyFeatures(psap_schema, single_county_temp)
    if arcpy.Exists("single_county_lyr"):
        arcpy.management.Delete("single_county_lyr")
    if arcpy.Exists("county_lyr"):
        arcpy.management.Delete("county_lyr")
    arcpy.management.MakeFeatureLayer(single_county_temp, "single_county_lyr")
    arcpy.management.MakeFeatureLayer(counties, "county_lyr")
    
    # Field Map county name into psap schema fields
    fms = arcpy.FieldMappings()
    
    # NAME to DsplayName
    fm_name = arcpy.FieldMap()
    fm_name.addInputField("county_lyr", "NAME")
    output = fm_name.outputField
    output.name = "DsplayName"
    fm_name.outputField = output
    fms.addFieldMap(fm_name)
    
    # Complete the append with field mapping and query
    sc_list = list(single_county_dict.values())
    print(sc_list)
    sc_query = f"NAME IN ({sc_list})".replace('[', '').replace(']', '')
    print(sc_query)
    
    arcpy.management.Append("county_lyr", single_county_temp, "NO_TEST", field_mapping=fms, expression=sc_query)
    
    # Populate fields with correct information
    update_count = 0
    fields = ['DsplayName']
    with arcpy.da.UpdateCursor(single_county_temp, fields) as update_cursor:
        print("Looping through rows in FC ...")
        for row in update_cursor:
            for k,v in single_county_dict.items():
                if v == row[0]:
                    row[0] = k
            update_count += 1
            update_cursor.updateRow(row)
    print(f"Total count of single county updates is: {update_count}")


def add_multi_county():
    # Build multi county PSAP boundaries from county boundaries
    print("Building multi county PSAPs from county boundaries ...")
    arcpy.management.CopyFeatures(psap_schema, multi_county_temp)
    if arcpy.Exists("multi_county_lyr"):
        arcpy.management.Delete("multi_county_lyr")
    if arcpy.Exists("county_lyr"):
        arcpy.management.Delete("county_lyr")
    arcpy.management.MakeFeatureLayer(multi_county_temp, "multi_county_lyr")
    arcpy.management.MakeFeatureLayer(counties, "county_lyr")
       
    # Field Map county name into psap schema fields
    fms = arcpy.FieldMappings()
    
    # NAME to DsplayName
    fm_name = arcpy.FieldMap()
    fm_name.addInputField("county_lyr", "NAME")
    output = fm_name.outputField
    output.name = "DsplayName"
    fm_name.outputField = output
    fms.addFieldMap(fm_name)
    
    # Build query to select multi county 
    mc_list = [ item.split(',') for item in list(multi_county_dict.values())]
    mc_list = [y.strip() for x in mc_list for y in x]
    print(mc_list)
    mc_query = f"NAME IN ({mc_list})".replace('[', '').replace(']', '')
    print(mc_query)
    
    # Complete the append with field mapping and query to get all counties in group
    # arcpy.MakeFeatureLayer_management(counties, "county_lyr_query", mc_query)
    # print(f'Feature count in multi county layer: {arcpy.management.GetCount("county_lyr_query")}')
    arcpy.management.Append("county_lyr", multi_county_temp, "NO_TEST", field_mapping=fms, expression=mc_query)
    
    # Loop through and populate fields with appropriate information and rename to multi county psaps
    update_count = 0
    fields = ['DsplayName']
    with arcpy.da.UpdateCursor(multi_county_temp, fields) as update_cursor:
        print("Looping through rows in FC ...")
        for row in update_cursor:
            for k,v in multi_county_dict.items():
                print(f'key: {k}     value: {v}')
                if row[0] in v:
                    print(f'Found {row[0]} in {v} ...')
                    row[0] = k
            update_count += 1
            update_cursor.updateRow(row)
    print(f"Total count of multi county updates is: {update_count}")
        
    print("Dissolving combo jurisdications ...")
    arcpy.management.Dissolve(multi_county_temp, mc_diss, "DsplayName")
    
    # Append multi county psaps into single county psaps fc
    arcpy.management.CopyFeatures(single_county_temp, all_county_temp)
    print("Appending multi county PSAPs with single county PSAPs ...")
    arcpy.management.Append(mc_diss, all_county_temp, "NO_TEST")
    

def add_single_muni():
    # Drop in other jurisdictions
    # Build Muncipality PD boundaries (psap_PDs_temp)
    print("Building Police Department boundaries from Municipalities ...")
    arcpy.management.CopyFeatures(psap_schema, PDs_temp)
    if arcpy.Exists("working_lyr_2"):
        arcpy.management.Delete("working_lyr_2")
    arcpy.management.MakeFeatureLayer(PDs_temp, "working_lyr_2")
    temp_list = ",".join(f"'{item.upper()}'" for item in muni_pd)
    query = f"NAME IN ({temp_list})"
    print(query)
    arcpy.management.MakeFeatureLayer(munis, "muni_lyr", query)
    
    # Field Map county name into psap schema fields
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
    # now munis are in psap schema with only Agency_ID and DsplayName populated (sk_lyr_2, PDs_temp)
    
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
    
    # Drop police departments into sheriffs offices via erase/append
    print("Inserting PD boundaries into Sheriff's Office boundaries ...")
    # Erase
    arcpy.analysis.Erase(SOs_temp, PDs_join, SOs_holes)
    # Append
    arcpy.management.Append(PDs_join, SOs_holes, "NO_TEST")


def add_multi_muni():
    # Dissolve jurisdictions with multiple munis (Draper, Park City, Santaquin)
    test = None
    
    
def add_unique_psaps():
    # Drop in unique districts via erase/append (psap_unique_temp) - tribal, Navajo Nation, etc.
    print("Adding unique districts into SOs/PDs layer ...")
    # Erase
    arcpy.analysis.Erase(SOs_holes, unique, psap_final)
    # Append
    arcpy.management.Append(unique, psap_final, "NO_TEST")
    

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
        with arcpy.da.UpdateCursor(psap_final, fields, query2) as update_cursor:
            for row in update_cursor:
                row[3] = renames[key][0]
                row[4] = renames[key][1]
                update_cursor.updateRow(row)
       

def project_to_WGS84():
    # Project final data to WGS84
    print("Projecting final psap boundaries into WGS84 ...")
    sr = arcpy.SpatialReference("WGS 1984")
    arcpy.management.Project(psap_final, psap_wgs84, sr, "WGS_1984_(ITRF00)_To_NAD_1983")

#----------------------------------------------------------------
# Additional enhancements for the future
# Drop in UHP boundaries - buffer state/federal highways (10-30m)
#   Only use buffers outside of municipalities?
#----------------------------------------------------------------


##########################
#  Call Functions Below  #
##########################

function_time = time.time()    

add_single_county()
add_multi_county()
# add_single_muni()
# add_multi_muni()
# add_unique_psaps()
# correct_names()
# project_to_WGS84()

print("Time elapsed in functions: {:.2f}s".format(time.time() - function_time))

print("Script shutting down ...")
# Stop timer and print end time in UTC
readable_end = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
print("The script end time is {}".format(readable_end))
print("Time elapsed: {:.2f}s".format(time.time() - start_time))
