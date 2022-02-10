# -*- coding: utf-8 -*-
"""
Created on Fri Nov 12 11:12:44 2021
@author: eneemann

Script to download OSM places and ETL into schema for SGID

more ideas:
    - take centroid of areas to convert to a point
    - deduplicate areas/nodes (like building churches and point churches)
    - reverse geocode to get nearest address?

12 November 2021: first version of code (EMN)
"""


import os
import time
import zipfile
import wget
import arcpy

#from arcpy import env
#import pandas as pd
#import numpy as np
#from matplotlib import pyplot as plt



# Start timer and print start time in UTC
start_time = time.time()
readable_start = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
print("The script start time is {}".format(readable_start))
today = time.strftime("%Y%m%d")


# Set up directories
# base_dir = r'C:\E911\2 - OSM Data'
#base_dir = r'C:\Users\eneemann\Documents\E911\2 - OSM Data'  # Used on the Citrix machine
base_dir = r'\\itwfpcap2\AGRC\agrc\users\eneemann\Neemann\2 - OSM Data'  # Used for the L:Drive (from laptop)
work_dir = os.path.join(base_dir, f'Utah_{today}')

if os.path.isdir(work_dir) == False:
    os.mkdir(work_dir)
    
today_db_name = "OSM_Places_" + today
today_db = os.path.join(work_dir, today_db_name + ".gdb")
    
# Set up OSM data URLs
#osm_url = r'https://download.geofabrik.de/north-america/us/utah-latest-free.shp.zip'
osm_url = r'http://download.geofabrik.de/north-america/us/utah-latest-free.shp.zip'

# Set up paths for shapefiles
pois = os.path.join(work_dir, 'gis_osm_pois_free_1.shp')
poi_areas = os.path.join(work_dir, 'gis_osm_pois_a_free_1.shp')
pofw = os.path.join(work_dir, 'gis_osm_pofw_free_1.shp')
pofw_areas = os.path.join(work_dir, 'gis_osm_pofw_a_free_1.shp')
transport = os.path.join(work_dir, 'gis_osm_transport_a_free_1.shp')
buildings = os.path.join(work_dir, 'gis_osm_buildings_a_free_1.shp')

# Set up SGID paths and variables
SGID = r"C:\Users\eneemann\AppData\Roaming\ESRI\ArcGISPro\Favorites\internal@SGID@internal.agrc.utah.gov.sde"
county_path = os.path.join(SGID, 'SGID.BOUNDARIES.Counties')
city_path = os.path.join(SGID, 'SGID.LOCATION.AddressSystemQuadrants')
zip_path = os.path.join(SGID, 'SGID.BOUNDARIES.ZipCodes')
block_path = os.path.join(SGID, 'SGID.DEMOGRAPHIC.CensusBlocks2020')
addr_path = os.path.join(SGID, 'SGID.LOCATION.AddressPoints')

county = 'Counties'
city = 'AddressSystemQuadrants'
zipcode = 'ZipCodes'
block = 'CensusBlocks2020'
addr = 'AddressPoints'

county_field = 'NAME'
city_field = 'GRID_NAME'
zip_field = 'ZIP5'
block_field = 'GEOID20'

SGID_files = [county, city, zipcode, block, addr]
SGID_layers = [county_path, city_path, zip_path, block_path, addr_path]

# Set up paths for feature classes
combined_places_name_WGS84 = 'OSM_Places'
combined_places_name = 'OSM_Places_UTM'
pois_FC_name = 'POIs'
poi_areas_FC_name = 'POI_Areas'
poi_areas_centroid_name = 'POI_Area_centroids'
pofw_FC_name = 'POFW'
pofw_areas_FC_name = 'POFW_Areas'
pofw_areas_centroid_name = 'POFW_Area_centroids'
transport_FC_name = 'Transport'
transport_centroid_name = 'Transport_centroids'
buildings_FC_name = 'Buildings'
buildings_centroid_name = 'Building_centroids'

combined_places_WGS84 = os.path.join(today_db, combined_places_name_WGS84)
combined_places = os.path.join(today_db, combined_places_name)
pois_FC = os.path.join(today_db, pois_FC_name)
poi_areas_FC = os.path.join(today_db, poi_areas_FC_name)
poi_areas_centroid = os.path.join(today_db, poi_areas_centroid_name)
pofw_FC = os.path.join(today_db, pofw_FC_name)
pofw_areas_FC = os.path.join(today_db, pofw_areas_FC_name)
pofw_areas_centroid = os.path.join(today_db, pofw_areas_centroid_name)
transport_FC = os.path.join(today_db, transport_FC_name)
transport_centroid = os.path.join(today_db, transport_centroid_name)
buildings_FC = os.path.join(today_db, buildings_FC_name)
buildings_centroid = os.path.join(today_db, buildings_centroid_name)
combined_places_final = os.path.join(today_db, 'OSM_Places_final')

temp_files = ['Building_centroids_original', pois_FC, poi_areas_FC, poi_areas_centroid,
              pofw_FC, pofw_areas_FC, pofw_areas_centroid, transport_FC, transport_centroid,
              buildings_FC, buildings_centroid]

# Create unzip function
def unzip(directory, file):
    os.chdir = directory
    if file.endswith(".zip"):
        print(f"Unzipping {file} ...")
        with zipfile.ZipFile(file, "r") as zip_ref:
            zip_ref.extractall(directory)
       
# Download data from OSM via Geofabrik
def download_osm():
    osm_file = wget.download(osm_url, work_dir)
    unzip(work_dir, osm_file)


def create_gdb():
    # Create geodatabase for today's data
    if arcpy.Exists(today_db) == False:
        arcpy.CreateFileGDB_management(work_dir, today_db_name)
    
    arcpy.env.workspace = today_db


# Copy SGID data layers to local for faster processing
def export_sgid():
    export_time = time.time()
    for item in SGID_layers:
        exported = item.rsplit('.', 1)[1]
        if arcpy.Exists(os.path.join(today_db, exported)):
            arcpy.Delete_management(os.path.join(today_db, exported))
        print(f"Exporting SGID {item} to: {exported}")
        arcpy.FeatureClassToFeatureClass_conversion (item, today_db, exported)
        
    print("Time elapsed exporting SGID data: {:.2f}s".format(time.time() - export_time))
    
def create_places():
    # Add queried POIs into Geodatabase
    poi_query = "name NOT IN ('', ' ') AND (fclass IN ('archaeological', 'arts_centre', " \
                 "'attraction', 'bank', 'bakery', 'bar', 'beverages', 'cafe', 'camp_site', " \
                 "'car_dealership', 'car_wash', 'caravan_site', 'cinema', 'clothes', 'convenience', " \
                 "'dentist', 'doctors', 'doityourself', 'fast_food', 'fire_station', " \
                 "'garden_centre', 'guesthouse', 'hairdresser', 'hotel', 'jeweller', " \
                 "'kindergarten', 'laundry', 'library', 'motel', 'museum', 'nightclub', " \
                 "'optician', 'park', 'pharmacy', 'post_office', 'police', 'restaurant', " \
                 "'school', 'supermarket', 'tourist_info', 'tower', 'town_hall', 'university', " \
                 "'vending_any', 'veterinary', 'viewpoint') OR fclass LIKE '%shop%' " \
                 "OR fclass LIKE '%store%' OR fclass LIKE '%rental%')"
        
    arcpy.conversion.FeatureClassToFeatureClass(pois, today_db, pois_FC_name, poi_query)
    arcpy.conversion.FeatureClassToFeatureClass(pois_FC, today_db, combined_places_name_WGS84)
    print(f"combined_places is starting with {arcpy.management.GetCount(combined_places_WGS84)[0]} features from POIs")
    
    # Project combined_places_WGS84 into UTM 12N (26912)
    print(f"Projecting {combined_places_WGS84} to UTM 12N ...")
    sr = arcpy.SpatialReference(26912)
    arcpy.management.Project(combined_places_WGS84, combined_places, sr)
    
    # Add queried POI Areas into Geodatabase
    poi_areas_query = "name NOT IN ('', ' ') AND (fclass IN ('archaeological', 'arts_centre', " \
    "'attraction', 'bank', 'bakery', 'bar', 'beverages', 'cafe', 'camp_site', 'car_dealership', " \
    "'car_wash', 'caravan_site', 'cinema', 'clothes', 'convenience', 'dentist', 'doctors', 'doityourself', " \
    "'fast_food', 'fire_station', 'garden_centre', 'guesthouse', 'hairdresser', 'graveyard', 'hospital', " \
    "'hotel', 'jeweller', 'kindergarten', 'laundry', 'library', 'mall', 'motel', 'museum', 'nightclub', " \
    "'optician', 'park', 'playground', 'pharmacy', 'post_office', 'police', 'restaurant', 'school', " \
    "'shelter', 'stadium', 'supermarket', 'swimming_pool', 'theatre', 'tourist_info', 'tower', 'town_hall', 'university', " \
    "'vending_any', 'veterinary', 'viewpoint') OR fclass LIKE '%shop%' OR fclass LIKE '%store%' OR fclass LIKE '%rental%'" \
    "OR (fclass = 'golf_course' AND name NOT IN ('', ' ') AND name NOT LIKE '%Hole%' AND name NOT LIKE '%hole%'))"
    
    arcpy.conversion.FeatureClassToFeatureClass(poi_areas, today_db, poi_areas_FC_name, poi_areas_query)
    
    arcpy.management.MakeFeatureLayer(poi_areas_FC, "poi_areas_lyr")
    # Select those that don't intersect existing OSM places
    arcpy.management.SelectLayerByLocation("poi_areas_lyr", "INTERSECT", combined_places, "3 Meters", "NEW_SELECTION", "INVERT")
    arcpy.management.FeatureToPoint("poi_areas_lyr", poi_areas_centroid, "INSIDE")
    
    # Append queried POI_Areas into OSM_Places
    print(f"Adding {arcpy.management.GetCount(poi_areas_centroid)[0]} POI area features to combined_places")
    arcpy.management.Append(poi_areas_centroid, combined_places, "NO_TEST")


def add_pofw():
    # Add queried POFW into Geodatabase
    pofw_query = "name NOT IN ('', ' ')"
    
    arcpy.conversion.FeatureClassToFeatureClass(pofw, today_db, pofw_FC_name, pofw_query)
    print(f"Adding {arcpy.management.GetCount(pofw_FC)[0]} POFW features to combined_places")
    arcpy.management.Append(pofw_FC, combined_places, "NO_TEST")


def add_pofw_areas():
# Add queried POFW_Areas into Geodatabase
    pofw_areas_query = "name NOT IN ('', ' ')"
    
    arcpy.conversion.FeatureClassToFeatureClass(pofw_areas, today_db, pofw_areas_FC_name, pofw_areas_query)
    
    arcpy.management.MakeFeatureLayer(pofw_areas_FC, "pofw_areas_lyr")
    
    # Select those that don't intersect existing POFW points
    arcpy.management.SelectLayerByLocation("pofw_areas_lyr", "INTERSECT", pofw_FC, "3 Meters", "NEW_SELECTION", "INVERT")
    arcpy.management.FeatureToPoint("pofw_areas_lyr", pofw_areas_centroid, "INSIDE")
    print(f"Adding {arcpy.management.GetCount(pofw_areas_centroid)[0]} POFW area features to combined_places")
    arcpy.management.Append(pofw_areas_centroid, combined_places, "NO_TEST")


def add_transportation():
    # Add queried Transportation locations into Geodatabase
    transport_query = "fclass IN ('airport', 'helipad', 'railway_station') and name NOT IN ('', ' ')"
    
    arcpy.conversion.FeatureClassToFeatureClass(transport, today_db, transport_FC_name, transport_query)
    
    # Select those that don't intersect existing OSM places
    arcpy.management.FeatureToPoint(transport_FC, transport_centroid, "INSIDE")
    
    arcpy.management.MakeFeatureLayer(transport_centroid, "transport_lyr")
    arcpy.management.SelectLayerByLocation("transport_lyr", "INTERSECT", combined_places, "10 Meters", "NEW_SELECTION", "INVERT")
    print(f"Adding {arcpy.management.GetCount('transport_lyr')[0]} transportation features to combined_places")
    arcpy.management.Append("transport_lyr", combined_places, "NO_TEST")


def numeric_check(string):
    stripped = string.strip().casefold().replace('building', '')
#    print(stripped)
    numeric = 0
    total = len(stripped)
    
    for char in stripped:
        if char.isnumeric() or char in ('-'):
            numeric += 1
            
#    print(numeric)
#    print(total)
    percent = float(numeric/total)
#    print(percent)
    
    if total < 3:
        result = 'bad'
#        print('bad')
    elif percent > 0.5:
        result = 'bad'
#        print('bad')
    else:
        result = 'good'
#        print('good')
        
    return result


def add_buildings():
    # Add queried Buildings into Geodatabase
    buildings_query = "name NOT IN ('', ' ')"
    
    arcpy.conversion.FeatureClassToFeatureClass(buildings, today_db, buildings_FC_name, buildings_query)
    
    arcpy.management.MakeFeatureLayer(buildings_FC, "buildings_lyr")
    
    # Select those that don't intersect existing OSM places
    # Turn off SelectByLocation to get all buildings and filter duplicates later based on spatial index
    #arcpy.management.SelectLayerByLocation("buildings_lyr", "INTERSECT", combined_places, "3 Meters", "NEW_SELECTION", "INVERT")
    arcpy.management.FeatureToPoint("buildings_lyr", buildings_centroid, "INSIDE")
    arcpy.AddField_management(buildings_centroid, "Numeric", "TEXT", "", "", 10)
    
    arcpy.management.CopyFeatures(buildings_centroid, os.path.join(today_db, 'Building_centroids_original'))
    
    # Filter out buildings with bad/numeric names (like '12C', 'Building 15', just a house number, etc.)
        #        0        1
    good_count = 0
    bad_count = 0
    fields = ['name', 'Numeric']
    with arcpy.da.UpdateCursor(buildings_centroid, fields) as update_cursor:
        print("Looping through rows in FC to check for bad building names ...")
        for row in update_cursor:
            check = numeric_check(row[0])
            row[1] = check
            if check == 'good':
                good_count += 1
                update_cursor.updateRow(row)
            else:
                bad_count += 1
                update_cursor.deleteRow()
                
    print(f'Count of good building names found: {good_count}')
    print(f'Count of bad building names found: {bad_count}')
    
    print(f"Adding {arcpy.management.GetCount(buildings_centroid)[0]} building features to combined_places")
    arcpy.management.Append(buildings_centroid, combined_places, "NO_TEST")


def assign_poly_attr(pts, polygonDict):
    
    arcpy.env.workspace = os.path.dirname(pts)
    arcpy.env.overwriteOutput = True
    
    for lyr in polygonDict:
        # set path to polygon layer
        polyFC = polygonDict[lyr]['poly_path']
        print (polyFC)
        
        # generate near table for each polygon layer
        neartable = 'in_memory\\near_table'
        arcpy.analysis.GenerateNearTable(pts, polyFC, neartable, '1 Meters', 'NO_LOCATION', 'NO_ANGLE', 'CLOSEST')
        
        # create dictionaries to store data
        pt_poly_link = {}       # dictionary to link points and polygons by OIDs 
        poly_OID_field = {}     # dictionary to store polygon NEAR_FID as key, polygon field as value
    
        # loop through near table, store point IN_FID (key) and polygon NEAR_FID (value) in dictionary (links two features)
        with arcpy.da.SearchCursor(neartable, ['IN_FID', 'NEAR_FID', 'NEAR_DIST']) as nearCur:
            for row in nearCur:
                pt_poly_link[row[0]] = row[1]       # IN_FID will return NEAR_FID
                # add all polygon OIDs as key in dictionary
                poly_OID_field.setdefault(row[1])
        
        # loop through polygon layer, if NEAR_FID key in poly_OID_field, set value to poly field name
        with arcpy.da.SearchCursor(polyFC, ['OID@', polygonDict[lyr]['poly_field']]) as polyCur:
            for row in polyCur:
                if row[0] in poly_OID_field:
                    poly_OID_field[row[0]] = row[1]
        
        # loop through points layer, using only OID and field to be updated
        with arcpy.da.UpdateCursor(pts, ['OID@', lyr]) as uCur:
            for urow in uCur:
                try:
                    # search for corresponding polygon OID in polygon dictionay (polyDict)
                    if pt_poly_link[urow[0]] in poly_OID_field:
                        # if found, set point field equal to polygon field
                        # IN_FID in pt_poly_link returns NEAR_FID, which is key in poly_OID_field that returns value of polygon field
                        urow[1] =  poly_OID_field[pt_poly_link[urow[0]]]
                except:         # if error raised, just put a blank in the field
                    urow[1] = ''
                uCur.updateRow(urow)
    
        # Delete in memory near table
        arcpy.management.Delete(neartable)

# Add polygon attributes (County, City, Zip, block_id)
def add_attributes():
    # Add fields
    print("Adding new fields to combined_places")
    arcpy.AddField_management(combined_places, "County", "TEXT", "", "", 25)
    arcpy.AddField_management(combined_places, "City", "TEXT", "", "", 50)
    arcpy.AddField_management(combined_places, "Zip", "TEXT", "", "", 5)
    arcpy.AddField_management(combined_places, "Block_id", "TEXT", "", "", 15)
    
    poly_dict = {
            'County': {'poly_path': county, 'poly_field': county_field},
            'City': {'poly_path': city, 'poly_field': city_field},
            'Zip': {'poly_path': zipcode, 'poly_field': zip_field},
            'Block_id': {'poly_path': block, 'poly_field': block_field}
            }
    
    print("Populating new fields from polygons")
    poly_time = time.time()
    assign_poly_attr(combined_places, poly_dict)
    print("Time elapsed populating polygon attributes: {:.2f}s".format(time.time() - poly_time))


def remove_duplicates():
    # De-duplicate final data based on name and block_id
    duplicate_time = time.time()
    # Identify duplicates
    duplicate_oids = []
    string_dict = {}
    dup_fields = ['name', 'Block_id', 'OID@']
    with arcpy.da.SearchCursor(combined_places, dup_fields) as search_cursor:
        print("Looping through rows in FC to check for duplicates within a census block ...")
        for row in search_cursor:
            string_code = row[0] + ' ' + row[1]
            if string_code.casefold() in string_dict:
                duplicate_oids.append(row[2])
            
            string_dict.setdefault(string_code.casefold())
    
    # Remove duplicates
    print(f'Removing {len(duplicate_oids)} duplicates ...')
    duplicate_query = f"""OBJECTID IN ({", ".join([str(oid) for oid in duplicate_oids])}) OR county IN ('', ' ')"""
    with arcpy.da.UpdateCursor(combined_places, ['OID@'], duplicate_query) as update_cursor:
        print("Looping through rows in FC to delete duplicates ...")
        for row in update_cursor:
            update_cursor.deleteRow()
                
    print("Time elapsed finding and deleting duplicates: {:.2f}s".format(time.time() - duplicate_time))


def add_addresses():
    # Add Near_addr from address points within 25 m
    address_time = time.time()
    #addpt_path = addr    
    
    # Field Map FullAdd into your new address field
    fms = arcpy.FieldMappings()
    
    # Add all fields from original combined_places
    fms.addTable(combined_places)
    fms.addTable(addr)
    
    ## FullAdd to near_addr
    #fm_addr = arcpy.FieldMap()
    #fm_addr.addInputField(addr, "FullAdd")
    #output = fm_addr.outputField
    #output.name = "Near_addr"
    #fm_addr.outputField = output
    #fms.addFieldMap(fm_addr)
    
    # Remove unwanted fields from join
    keep_fields = ['osm_id', 'code', 'fclass', 'name', 'County', 'City', 'Zip', 'Block_id', 'FullAdd']
    for field in fms.fields:
        if field.name not in keep_fields:
            fms.removeFieldMap(fms.findFieldMapIndex(field.name))
    
    # Complete spatial join with field mapping
    arcpy.analysis.SpatialJoin(combined_places, addr, combined_places_final, 'JOIN_ONE_TO_ONE', 'KEEP_ALL', fms, 'CLOSEST', '25 Meters', 'Addr_dist')
    print("Time elapsed joining near addresses: {:.2f}s".format(time.time() - address_time))


def calc_fields():
    # Clean up schema and calculate fields
    # Delete unneeded/unwanted fields
    arcpy.management.DeleteField(combined_places_final, ['Join_Count', 'TARGET_FID', ])
    
    # Rename FullAdd field and make other fields Title case
    arcpy.management.AlterField(combined_places_final, 'FullAdd', 'Near_addr', 'Near_addr')
    arcpy.management.AlterField(combined_places_final, 'osm_id', 'OSM_id', 'OSM_id')
    arcpy.management.AlterField(combined_places_final, 'code', 'Code', 'Code')
    arcpy.management.AlterField(combined_places_final, 'fclass', 'FClass', 'FClass')
    arcpy.management.AlterField(combined_places_final, 'name', 'Name', 'Name')
    
    # Add disclaimer field
    arcpy.management.AddField(combined_places_final, "Disclaimer", "TEXT", "", "", 150)
    
    calc_time = time.time()
    #                   0            1             2         3       4   
    calc_fields = ['Near_addr', 'Addr_dist', 'Disclaimer', 'City', 'Zip']
    with arcpy.da.UpdateCursor(combined_places_final, calc_fields) as update_cursor:
        print("Looping through rows in FC to calculate fields ...")
        for row in update_cursor:
            if row[0] is None:
                row[1] = None
            else:
                row[2] = 'NOT AN OFFICIAL ADDRESS.  Address based on nearest address point (within 25m) in UGRC database, Addr_dist provides distance from OSM point.'
            if row[3] in ('', ' '):
                row[3] = None
            if row[4] in ('', ' '):
                row[4] = None
                
            update_cursor.updateRow(row)
    
    print("Time elapsed calculating fields: {:.2f}s".format(time.time() - calc_time))


def delete_files():
    ## Delete temporary and intermediate files
    #print("Deleting copied SGID files ...")
    #for file in SGID_files:
    #    if arcpy.Exists(file):
    #        print(f"Deleting {file} ...")
    #        arcpy.management.Delete(file)
    #
    #print("Deleting temporary files ...")
    #for file in temp_files:
    #    if arcpy.Exists(file):
    #        print(f"Deleting {file} ...")
    #        arcpy.management.Delete(file)
    
    print("Deleting OSM shapefiles ...")
    arcpy.env.workspace = work_dir  
    featureclasses = arcpy.ListFeatureClasses('*.shp')
    for fc in featureclasses:
        print(f"Deleting {fc} ...")
        arcpy.management.Delete(fc)


# Call functions 
download_osm()
create_gdb()
export_sgid()
create_places()
add_pofw()
add_pofw_areas()
add_transportation()
add_buildings()
add_attributes()
remove_duplicates()
add_addresses()
calc_fields()
delete_files()



print("Script shutting down ...")
# Stop timer and print end time in UTC
readable_end = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
print("The script end time is {}".format(readable_end))
print("Time elapsed: {:.2f}s".format(time.time() - start_time))

