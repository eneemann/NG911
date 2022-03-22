# -*- coding: utf-8 -*-
"""
Created on Fri Nov 12 11:12:44 2021
@author: eneemann

Script to download OSM data and ETL into Open Source Places schema for SGID

12 November 2021: first version of code (EMN)
"""

import os
import time
import zipfile
import wget
import arcpy
import requests
import pandas as pd
import numpy as np
from arcgis.features import GeoAccessor, GeoSeriesAccessor

#: Start timer and print start time in UTC
start_time = time.time()
readable_start = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
print("The script start time is {}".format(readable_start))
today = time.strftime("%Y%m%d")

#: Set up directories
#base_dir = r'C:\E911\2 - OSM Data'
base_dir = r'C:\Users\eneemann\Documents\E911\2 - OSM Data'  # Used on the Citrix machine
work_dir = os.path.join(base_dir, f'Utah_{today}')

if os.path.isdir(work_dir) == False:
    os.mkdir(work_dir)
    
today_db_name = "OSM_Places_" + today
today_db = os.path.join(work_dir, today_db_name + ".gdb")
    
#: Set up OSM data URL
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

#: Get SGID layers names from paths
county = county_path.split('.')[-1]
city = city_path.split('.')[-1]
zipcode = zip_path.split('.')[-1]
block = block_path.split('.')[-1]
addr = addr_path.split('.')[-1]

#: SGID fields to use for given layers
county_field = 'NAME'
city_field = 'GRID_NAME'
zip_field = 'ZIP5'
block_field = 'GEOID20'

SGID_files = [county, city, zipcode, block, addr]
SGID_layers = [county_path, city_path, zip_path, block_path, addr_path]

#: Set up names and paths for feature classes
combined_places_name_WGS84 = 'OS_Places'
combined_places_name = 'OS_Places_UTM'
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
combined_places_simple = os.path.join(today_db, 'OS_Places_simple')
combined_places_final = os.path.join(today_db, 'OS_Places_final')

temp_files = ['Building_centroids_original', pois_FC, poi_areas_FC, poi_areas_centroid,
              pofw_FC, pofw_areas_FC, pofw_areas_centroid, transport_FC, transport_centroid,
              buildings_FC, buildings_centroid]

#: Create unzip function
def unzip(directory, file):
    os.chdir = directory
    if file.endswith(".zip"):
        print(f"Unzipping {file} ...")
        with zipfile.ZipFile(file, "r") as zip_ref:
            zip_ref.extractall(directory)
       
#: Download data from OSM via Geofabrik
def download_osm():
    print(f"Downloading OSM data from {osm_url} ...")
    osm_file = wget.download(osm_url, work_dir)
    unzip(work_dir, osm_file)


#: Create working geodatabase with today's date
def create_gdb():
    print("Creating file geodatabase ...")
    if arcpy.Exists(today_db) == False:
        arcpy.CreateFileGDB_management(work_dir, today_db_name)
    
    arcpy.env.workspace = today_db
    arcpy.env.qualifiedFieldNames = False


#: Copy SGID data layers to local for faster processing
#: This seems to be a little faster than hitting the internal SGID for point-in-polygon analyses
def export_sgid():
    export_time = time.time()
    for item in SGID_layers:
        exported = item.rsplit('.', 1)[1]
        if arcpy.Exists(os.path.join(today_db, exported)):
            arcpy.Delete_management(os.path.join(today_db, exported))
        print(f"Exporting SGID {item} to: {exported}")
        arcpy.FeatureClassToFeatureClass_conversion (item, today_db, exported)     
    
    print("Time elapsed exporting SGID data: {:.2f}s".format(time.time() - export_time))

#: Create intial set of point features from   
def create_places():
    #: Add queried POIs into Geodatabase
    poi_query = "name NOT IN ('', ' ') AND (fclass IN ('archaeological', 'arts_centre', " \
                 "'attraction', 'bank', 'bakery', 'bar', 'beverages', 'butcher', 'cafe', 'camp_site', " \
                 "'car_dealership', 'car_wash', 'caravan_site', 'chemist', 'cinema', 'clothes', 'college', 'convenience', " \
                 "'courthouse', 'dentist', 'doctors', 'doityourself', 'embassy', 'fast_food', 'fire_station', 'florist', " \
                 "'garden_centre', 'general', 'greengrocer', 'guesthouse', 'hairdresser', 'hospital', 'hostel', 'hotel', 'jeweller', 'kindergarten', " \
                 "'laundry', 'library', 'mall', 'market_place', 'massage', 'memorial', 'monument', 'motel', 'museum', 'nightclub', " \
                 "'nursing_home', 'optician', 'park', 'pharmacy', 'picnic_site', 'post_office', 'police', 'pub', 'restaurant', 'ruins', " \
                 "'school', 'stationery', 'supermarket', 'swimming_pool', 'theatre', 'tourist_info', 'tower', 'town_hall', 'travel_agent', 'university', " \
                 "'vending_any', 'veterinary', 'viewpoint', 'zoo') OR fclass LIKE '%shop%' " \
                 "OR fclass LIKE '%store%' OR fclass LIKE '%rental%' OR fclass LIKE '%centre%')"
        
    arcpy.conversion.FeatureClassToFeatureClass(pois, today_db, pois_FC_name, poi_query)
    arcpy.conversion.FeatureClassToFeatureClass(pois_FC, today_db, combined_places_name_WGS84)
    print(f"Combined_places is starting with {arcpy.management.GetCount(combined_places_WGS84)[0]} features from POIs")
    
    #: Project combined_places_WGS84 into UTM 12N (26912)
    print(f"Projecting {combined_places_WGS84} to UTM 12N ...")
    sr = arcpy.SpatialReference(26912)
    arcpy.management.Project(combined_places_WGS84, combined_places, sr)
    
    #: Add queried POI Areas into Geodatabase
    poi_areas_query = "name NOT IN ('', ' ') AND (fclass IN ('archaeological', 'arts_centre', " \
    "'attraction', 'bank', 'bakery', 'bar', 'beverages', 'butcher', 'cafe', 'camp_site', 'car_dealership', " \
    "'car_wash', 'caravan_site', 'chemist', 'cinema', 'clothes', 'college', 'convenience', 'courthouse', 'dentist', 'doctors', 'doityourself', 'embassy', " \
    "'fast_food', 'fire_station', 'florist', 'garden_centre', 'general', 'greengrocer', 'guesthouse', 'hairdresser', 'graveyard', 'hospital', 'hostel', " \
    "'hotel', 'jeweller', 'kindergarten', 'laundry', 'library', 'mall', 'market_place', 'massage', 'memorial', 'monument', 'motel', 'museum', 'nightclub', " \
    "'nursing_home', 'optician', 'park', 'playground', 'pharmacy', 'picnic_site', 'post_office', 'police', 'pub', 'restaurant', 'ruins', 'school', " \
    "'shelter', 'stadium', 'stationery', 'supermarket', 'swimming_pool', 'theatre', 'tourist_info', 'tower', 'town_hall', 'travel_agent', 'university', " \
    "'vending_any', 'veterinary', 'viewpoint', 'zoo') OR fclass LIKE '%shop%' OR fclass LIKE '%store%' OR fclass LIKE '%rental%' OR fclass LIKE '%centre%'" \
    "OR (fclass = 'golf_course' AND name NOT IN ('', ' ') AND name NOT LIKE '%Hole%' AND name NOT LIKE '%hole%'))"
    
    arcpy.conversion.FeatureClassToFeatureClass(poi_areas, today_db, poi_areas_FC_name, poi_areas_query)
    arcpy.management.MakeFeatureLayer(poi_areas_FC, "poi_areas_lyr")
    
    #: Select those that aren't within 3m of existing OS places
    arcpy.management.SelectLayerByLocation("poi_areas_lyr", "INTERSECT", combined_places, "3 Meters", "NEW_SELECTION", "INVERT")
    arcpy.management.FeatureToPoint("poi_areas_lyr", poi_areas_centroid, "INSIDE")
    
    #: Append queried POI_Areas into OS_Places
    print(f"Adding {arcpy.management.GetCount(poi_areas_centroid)[0]} POI area features to combined_places")
    arcpy.management.Append(poi_areas_centroid, combined_places, "NO_TEST")


def add_pofw():
    #: Add queried POFW into Geodatabase
    pofw_query = "name NOT IN ('', ' ')"
    arcpy.conversion.FeatureClassToFeatureClass(pofw, today_db, pofw_FC_name, pofw_query)
    print(f"Adding {arcpy.management.GetCount(pofw_FC)[0]} POFW features to combined_places")
    arcpy.management.Append(pofw_FC, combined_places, "NO_TEST")


def add_pofw_areas():
    #: Add queried POFW_Areas into Geodatabase
    pofw_areas_query = "name NOT IN ('', ' ')"
    arcpy.conversion.FeatureClassToFeatureClass(pofw_areas, today_db, pofw_areas_FC_name, pofw_areas_query)
    arcpy.management.MakeFeatureLayer(pofw_areas_FC, "pofw_areas_lyr")
    
    #: Select those that aren't within 3m of existing POFW points
    arcpy.management.SelectLayerByLocation("pofw_areas_lyr", "INTERSECT", pofw_FC, "3 Meters", "NEW_SELECTION", "INVERT")
    #: Convert polygons to internal centroids
    arcpy.management.FeatureToPoint("pofw_areas_lyr", pofw_areas_centroid, "INSIDE")
    print(f"Adding {arcpy.management.GetCount(pofw_areas_centroid)[0]} POFW area features to combined_places")
    arcpy.management.Append(pofw_areas_centroid, combined_places, "NO_TEST")


def add_transportation():
    #: Add queried Transportation locations into Geodatabase
    transport_query = "fclass IN ('airport', 'helipad', 'railway_station') and name NOT IN ('', ' ')"
    arcpy.conversion.FeatureClassToFeatureClass(transport, today_db, transport_FC_name, transport_query)
    #: Convert polygons to internal centroids
    arcpy.management.FeatureToPoint(transport_FC, transport_centroid, "INSIDE")
    arcpy.management.MakeFeatureLayer(transport_centroid, "transport_lyr")
    #: Select those that aren't within 10m of existing OS places
    arcpy.management.SelectLayerByLocation("transport_lyr", "INTERSECT", combined_places, "10 Meters", "NEW_SELECTION", "INVERT")
    print(f"Adding {arcpy.management.GetCount('transport_lyr')[0]} transportation features to combined_places")
    arcpy.management.Append("transport_lyr", combined_places, "NO_TEST")


def numeric_check(string):
    #: Function to check for short names or names dominated by numeric characters (1234, 750A, etc.)
    stripped = string.strip().casefold().replace('building', '')
    numeric = 0
    total = len(stripped)
    
    for char in stripped:
        if char.isnumeric() or char in ('-'):
            numeric += 1
            
    percent = float(numeric/total)
    
    if total < 3:
        result = 'bad'
    elif percent > 0.5:
        result = 'bad'
    else:
        result = 'good'
        
    return result


def add_buildings():
    #: Add queried Buildings into Geodatabase
    buildings_query = "name NOT IN ('', ' ')"
    arcpy.conversion.FeatureClassToFeatureClass(buildings, today_db, buildings_FC_name, buildings_query)
    arcpy.management.MakeFeatureLayer(buildings_FC, "buildings_lyr")
    
    #: Not using SelectByLocation, will use all buildings and filter duplicates later based on spatial index
    #: Convert polygons to internal centroids
    arcpy.management.FeatureToPoint("buildings_lyr", buildings_centroid, "INSIDE")
    arcpy.management.AddField(buildings_centroid, "Numeric", "TEXT", "", "", 10)
    arcpy.management.CopyFeatures(buildings_centroid, os.path.join(today_db, 'Building_centroids_original'))
    
    #: Filter out buildings with bad/numeric names (like '12C', 'Building 15', just a house number, etc.)
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
    #: Assing polygon attributes to poins using near tables to get the nearsest polygon info
    arcpy.env.workspace = os.path.dirname(pts)
    arcpy.env.overwriteOutput = True
    
    for lyr in polygonDict:
        #: Set path to polygon layer
        polyFC = polygonDict[lyr]['poly_path']
        print (polyFC)
        
        #: Generate near table for each polygon layer
        neartable = 'in_memory\\near_table'
        arcpy.analysis.GenerateNearTable(pts, polyFC, neartable, '1 Meters', 'NO_LOCATION', 'NO_ANGLE', 'CLOSEST')
        
        #: Create dictionaries to store data
        pt_poly_link = {}       #: Dictionary to link points and polygons by OIDs 
        poly_OID_field = {}     #: Dictionary to store polygon NEAR_FID as key, polygon field as value
    
        #: Loop through near table, store point IN_FID (key) and polygon NEAR_FID (value) in dictionary (links two features)
        with arcpy.da.SearchCursor(neartable, ['IN_FID', 'NEAR_FID', 'NEAR_DIST']) as nearCur:
            for row in nearCur:
                pt_poly_link[row[0]] = row[1]       # IN_FID will return NEAR_FID
                #: Add all polygon OIDs as key in dictionary
                poly_OID_field.setdefault(row[1])
        
        #: Loop through polygon layer, if NEAR_FID key in poly_OID_field, set value to poly field name
        with arcpy.da.SearchCursor(polyFC, ['OID@', polygonDict[lyr]['poly_field']]) as polyCur:
            for row in polyCur:
                if row[0] in poly_OID_field:
                    poly_OID_field[row[0]] = row[1]
        
        #: Loop through points layer, using only OID and field to be updated
        with arcpy.da.UpdateCursor(pts, ['OID@', lyr]) as uCur:
            for urow in uCur:
                try:
                    #: Search for corresponding polygon OID in polygon dictionay (polyDict)
                    if pt_poly_link[urow[0]] in poly_OID_field:
                        #: If found, set point field equal to polygon field
                        #: IN_FID in pt_poly_link returns NEAR_FID, which is key in poly_OID_field that returns value of polygon field
                        urow[1] =  poly_OID_field[pt_poly_link[urow[0]]]
                except:         #: If error raised, just put a blank in the field
                    urow[1] = ''
                uCur.updateRow(urow)
    
        #: Delete in memory near table
        arcpy.management.Delete(neartable)

#: Add polygon attributes (county, city, zip, block_id)
def add_attributes():
    #: Add fields to feature class
    print("Adding new fields to combined_places")
    arcpy.management.AddField(combined_places, "county", "TEXT", "", "", 25, field_alias="County")
    arcpy.management.AddField(combined_places, "city", "TEXT", "", "", 50, field_alias="City")
    arcpy.management.AddField(combined_places, "zip", "TEXT", "", "", 5, field_alias="Zip Code")
    arcpy.management.AddField(combined_places, "block_id", "TEXT", "", "", 15, field_alias="Census Block ID")
    
    #: Set up path and field of interest for each polygon layer
    poly_dict = {
            'county': {'poly_path': county, 'poly_field': county_field},
            'city': {'poly_path': city, 'poly_field': city_field},
            'zip': {'poly_path': zipcode, 'poly_field': zip_field},
            'block_id': {'poly_path': block, 'poly_field': block_field}
            }
    
    print("Populating new fields from polygons")
    poly_time = time.time()
    assign_poly_attr(combined_places, poly_dict)
    print("Time elapsed populating polygon attributes: {:.2f}s".format(time.time() - poly_time))


def remove_duplicates():
    #: De-duplicate final data based on concatening name and block_id as a pseudo spatial index
    duplicate_time = time.time()
    #: First, identify duplicates
    duplicate_oids = []
    string_dict = {}
    dup_fields = ['name', 'block_id', 'OID@']
    with arcpy.da.SearchCursor(combined_places, dup_fields) as search_cursor:
        print("Looping through rows in FC to check for duplicates within a census block ...")
        for row in search_cursor:
            string_code = row[0] + ' ' + row[1]
            if string_code.casefold() in string_dict:
                duplicate_oids.append(row[2])
            
            string_dict.setdefault(string_code.casefold())
    
    #: Second, remove duplicates
    print(f'Removing {len(duplicate_oids)} duplicates ...')
    duplicate_query = f"""OBJECTID IN ({", ".join([str(oid) for oid in duplicate_oids])}) OR county IN ('', ' ')"""
    with arcpy.da.UpdateCursor(combined_places, ['OID@'], duplicate_query) as update_cursor:
        print("Looping through rows in FC to delete duplicates ...")
        for row in update_cursor:
            update_cursor.deleteRow()
                
    print("Time elapsed finding and deleting duplicates: {:.2f}s".format(time.time() - duplicate_time))


def add_addresses():
    #: Add ugrc_addr field from address points within 25m
    address_time = time.time()  
    
    #: Field Map FullAdd into the new address field
    fms = arcpy.FieldMappings()
    #: Add all fields from the original combined_places
    fms.addTable(combined_places)
    fms.addTable(addr)
        
    #: Remove unwanted fields from the join list
    keep_fields = ['osm_id', 'code', 'fclass', 'name', 'county', 'city', 'zip', 'block_id', 'FullAdd']
    for field in fms.fields:
        if field.name not in keep_fields:
            fms.removeFieldMap(fms.findFieldMapIndex(field.name))
    
    #: Complete spatial join with field mapping
    arcpy.analysis.SpatialJoin(combined_places, addr, combined_places_simple, 'JOIN_ONE_TO_ONE', 'KEEP_ALL', fms, 'CLOSEST', '25 Meters', 'addr_dist')
    print("Time elapsed joining near addresses: {:.2f}s".format(time.time() - address_time))


def calc_fields():
    #: Clean up schema and calculate fields
    #: Delete unneeded/unwanted fields
    arcpy.management.DeleteField(combined_places_simple, ['Join_Count', 'TARGET_FID', ])
    
    #: Rename FullAdd field to ugrc_addr
    arcpy.management.AlterField(combined_places_simple, 'FullAdd', 'ugrc_addr', 'ugrc_addr')
    
    #: Add disclaimer field
    arcpy.management.AddField(combined_places_simple, "disclaimer", "TEXT", "", "", 150, field_alias="Disclaimer")
    
    #: Calculate fields (disclaimer, replace blanks with NULLs)
    calc_time = time.time()
    #                   0            1             2         3       4   
    calc_fields = ['ugrc_addr', 'addr_dist', 'disclaimer', 'city', 'zip']
    with arcpy.da.UpdateCursor(combined_places_simple, calc_fields) as update_cursor:
        print("Looping through rows in FC to calculate fields ...")
        for row in update_cursor:
            if row[0] is None:
                row[1] = None
            else:
                row[2] = 'ugrc_addr is NOT official, it is based on the nearest address point (within 25m) in UGRC database, addr_dist provides distance from actual point'
            if row[3] in ('', ' '):
                row[3] = None
            if row[4] in ('', ' '):
                row[4] = None
                
            update_cursor.updateRow(row)
    
    #: Calculate lon/lat values for all points (WGS84 coords)
    arcpy.management.AddField(combined_places_simple, 'lon', 'FLOAT', field_scale="6", field_alias="Longitude")
    arcpy.management.AddField(combined_places_simple, 'lat', 'FLOAT', field_scale="6", field_alias="Latitude")
    arcpy.management.CalculateGeometryAttributes(combined_places_simple, [['lon', 'POINT_X'], ['lat', 'POINT_Y']], coordinate_format='DD')
        
    print("Time elapsed calculating fields: {:.2f}s".format(time.time() - calc_time))


def final_numeric_check():
    arcpy.management.AddField(combined_places_simple, "NUM_CHECK", "TEXT", "", "", 10)
    
    #: Filter out place names with bad/numeric names (like '12C', 'Building 15', just a house number, etc.)
    good_count = 0
    bad_count = 0
    fields = ['name', 'NUM_CHECK']
    with arcpy.da.UpdateCursor(combined_places_simple, fields) as update_cursor:
        print("Looping through rows in FC to check for numeric place names ...")
        for row in update_cursor:
            check = numeric_check(row[0])
            row[1] = check
            if check == 'good':
                good_count += 1
                update_cursor.updateRow(row)
            else:
                bad_count += 1
                update_cursor.deleteRow()
                
    print(f'Count of good place names found: {good_count}')
    print(f'Count of bad (numeric) place names found: {bad_count}')
    
    arcpy.management.DeleteField(combined_places_simple, ['NUM_CHECK'])


def delete_files():
    #: Delete temporary and intermediate files
    print("Deleting copied SGID files ...")
    for file in SGID_files:
        if arcpy.Exists(file):
            print(f"Deleting {file} ...")
            arcpy.management.Delete(file)
    
    print("Deleting temporary files ...")
    for file in temp_files:
        if arcpy.Exists(file):
            print(f"Deleting {file} ...")
            arcpy.management.Delete(file)
    
    print("Deleting OSM shapefiles ...")
    arcpy.env.workspace = work_dir  
    featureclasses = arcpy.ListFeatureClasses('*.shp')
    for fc in featureclasses:
        print(f"Deleting {fc} ...")
        arcpy.management.Delete(fc)


#: Retrieve Overpass API data with requests, convert to dataframe
def get_overpass_df(query_string):
    r = requests.get(query_string)
    df = pd.DataFrame(r.json()['elements'])

    return df


#: Function to calculater full OSM address
def calc_address(row):
    #: Concatenate address parts into a single field
    addr = ' '.join([str(row['addr:housenumber']), str(row['addr:street']), str(row['addr:city']), str(row['addr:postcode'])])
    addr = addr.replace('nan', ' ')
    addr = ' '.join(addr.split())
    row['OSM_addr'] = addr
    
    return row


def add_overpass_fields():
    #: Add attributes from Overpass API using spatial dataframes
    
    overpass_start_time = time.time()
    #: Get data from Overpass query
    print("Pulling additional data from Overpass API ...")
    query_string = 'http://overpass-api.de/api/interpreter?data=[out:json];area[name="Utah"]->.utah;nwr[!highway][name](area.utah);out center;'
    overpass = get_overpass_df(query_string)
    #: Pare down to necessary fields and ignore 'relations'
    overpass_small = overpass[['id', 'type', 'tags']]
    overpass_small = overpass_small[overpass_small['type'] != 'relation']

    #: Normalize the tags field (dictionary) into separate columns
    print("Normalizing Overpass dataframe ...")
    temp = pd.json_normalize(overpass_small['tags'])
    overpass_normal = pd.concat([overpass_small.drop('tags', axis=1), temp], axis=1)
    #: Only keep the first 300 columns (most common) to save memory
    overpass_normal_300 = overpass_normal.iloc[:, : 300]

    #: Filter data down to columns that will be kept
    keep_cols = ['id', 'name', 'amenity', 'cuisine',
            'tourism', 'shop', 'website', 'phone', 'opening_hours', 'type',
            'addr:housenumber', 'addr:street', 'addr:city', 'addr:postcode']
    overpass_normal_slim = overpass_normal_300[keep_cols]

    #: Filter Overpass data down to rows iwth OSM_ids found in the Geofabrik data
    print("Filtering Overpass data to OSM IDs in working data ...")
    #: Get list of ids in the geofabrik data
    geofabrik_place_ids = [str(i).strip("(',)") for i in arcpy.da.SearchCursor(combined_places_simple, 'osm_id')]
    #: Convert id field to string data type
    overpass_normal_slim[['id']] = overpass_normal_slim[['id']].astype(str)
    #: Filter the dataframe
    overpass_ids = overpass_normal_slim[overpass_normal_slim['id'].isin(geofabrik_place_ids)]

    #: Calculate OSM full addresses using calc_address function
    print("Calculating OSM addresses ...")
    overpass_ids = overpass_ids.apply(calc_address, axis=1)

    #: Export Overpass data to CSV
    print("Writing Overpass data to CSV ...")
    overpass_csv = os.path.join(work_dir, 'overpass_data.csv')
    overpass_ids.to_csv(overpass_csv)

    #: Pare columns down to only those that will be joined
    join_cols = ['id', 'amenity', 'cuisine', 'tourism', 'shop', 'website',
                  'phone', 'opening_hours', 'OSM_addr']
    overpass_to_join = overpass_ids[join_cols]

    #: Convert working feature class to spatial data frame
    print("Converting working data to spatial dataframe ...")
    places_sdf = pd.DataFrame.spatial.from_featureclass(combined_places_simple)
    
    #: Join data from Overpass dataframe to working dataframe
    print("Joining Overpass data and exporting to FC ...")
    places_sdf = places_sdf.merge(overpass_to_join, how='left', left_on='osm_id', right_on='id')
    
    #: Final cleanup of columns and names, replace blanks in OSM_addr with NaNs/nulls, 
    places_sdf.drop(['code', 'id'], axis=1, inplace=True)
    places_sdf['OSM_addr'].replace(r'^\s*$', np.nan, regex=True, inplace=True)
    places_sdf.rename(columns={'fclass': 'category', 'opening_hours': 'open_hours'}, inplace=True)
    
    #: Export the final spatial dataframe to a feature class
    places_sdf.spatial.to_featureclass(location=combined_places_final)

    print("Time elapsed in Overpass function: {:.2f}s".format(time.time() - overpass_start_time))
    

#: Call functions 
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
final_numeric_check()
print("Time elapsed before Overpass function: {:.2f}s".format(time.time() - start_time))
add_overpass_fields()
delete_files()


#: Stop timer and print end time in UTC
print("Script shutting down ...")
readable_end = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
print("The script end time is {}".format(readable_end))
print("Time elapsed: {:.2f}s".format(time.time() - start_time))
