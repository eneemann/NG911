# -*- coding: utf-8 -*-
"""
Created on Tue Oct 8 13:20:42 2019
@author: eneemann
Script to build list of municipalities that have police departments and export
list to text file.  This list will be used in building NG911 law enforecement
boundaries from SGID data

"""

import arcpy
from arcpy import env
import os
import time

# Start timer and print start time in UTC
start_time = time.time()
readable_start = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
print("The script start time is {}".format(readable_start))

######################
#  Set up variables  #
######################

SGID = r"C:\Users\eneemann\AppData\Roaming\ESRI\ArcGISPro\Favorites\sgid.agrc.utah.gov.sde"
ng911_db = r"L:\agrc\data\ng911\NG911_boundary_work.gdb"

env.workspace = ng911_db
env.overwriteOutput = True

counties = os.path.join(SGID, 'SGID10.BOUNDARIES.Counties')
munis = os.path.join(SGID, 'SGID10.BOUNDARIES.Municipalities')
old_law = os.path.join(SGID, 'SGID10.SOCIETY.LawEnforcementBoundaries')
unique = os.path.join(ng911_db, 'NG911_Law_unique_UTM')

temp_muni_names = []
muni_pd = []
pds = []

# Create list of municipality names
fields = ['NAME']
with arcpy.da.SearchCursor(munis, fields) as search_cursor:
    print("Looping through rows in FC ...")
    for row in search_cursor:
        temp_muni_names.append(row[0].upper())

# Sort list and distill to unique with dict keys
temp_muni_names = sorted(temp_muni_names)
temp_muni_names = list(dict.fromkeys(temp_muni_names))
print('Initial temp_muni_names:')
print(temp_muni_names)

muni_names = []
# Account for spelling of St George
for item in temp_muni_names:
    if 'ST. ' in item:
        muni_names.append(item.replace('ST. ', 'SAINT '))
    else:
        muni_names.append(item)

print('Corrected muni_names:')
print(muni_names)
print(f'number of municipalities: {len(muni_names)}')

# Create list of PDs from current SGID law boundaries layer
fields = ['NAME']
query = "NAME LIKE '% PD%'"
with arcpy.da.SearchCursor(old_law, fields, query) as search_cursor:
    print("Looping through rows in FC ...")
    for row in search_cursor:
        pds.append(row[0].upper())

# Sort list and distill to unique with dict keys
pds = sorted(pds)
pds = list(dict.fromkeys(pds))
print(pds)
print(f'number of police departments: {len(pds)}')

# Remove 'PD' and chop down to district name
pds_split = [item.rsplit(' ', 1)[0] for item in pds]
print(pds_split)

for item in muni_names:
    if item in pds_split:
        muni_pd.append(item)

print('final municipalities with police departments:')
print(muni_pd)
print(f'number of munis with police departments: {len(muni_pd)}')

# Find PDs that may be unaccounted for
# Examine difference between pds_split and muni_pd
s = set(muni_pd)
diff = [x for x in pds_split if x not in s]
print(diff)

# Write out list of municipalities with PDs to a text file
out_dir = r'C:\Users\eneemann\Desktop\Neemann\NG911\NG911_project'
filename = os.path.join(out_dir, 'Munis_with_PDs.txt')
with open(filename, 'w') as filehandle:
    filehandle.writelines(f'{muni}\n' for muni in muni_pd)

print("Script shutting down ...")
# Stop timer and print end time in UTC
readable_end = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
print("The script end time is {}".format(readable_end))
print("Time elapsed: {:.2f}s".format(time.time() - start_time))
