import json
import requests
import os
import hashlib
import shapely
from shapely.geometry import shape, GeometryCollection
import fiona
import re
from shapely.ops import unary_union
import pandas as pd
import pickle

with open('buildConfig.json') as json_file:
  config = json.load(json_file)


globalHashId = hashlib.md5(str(config["globalBoundaryURL"]).encode()).hexdigest()
print(globalHashId)

globalJson = './rawData/' +  globalHashId + ".geojson"

with open(globalJson) as f:
  dta = json.load(f)

  
  
  
  
#We are normalizing to the Department of State boundaries.
#Process is:
#(1) Load in the DoS boundaries (US)
#(2) Identify if there is anything unusual about the boundary.
#In the dataset, if it has parentheses after the name, it indicates
#It is either for display only, or part of a sovereign domain (i.e.,)
#the united kingdom.
#Our first loop will clean all of this up, and build one boundary for
#each country.
#(3) For each country, load in the geoBoundaries data for the appropriate
#ADM level.
#(4) For each country, clip the geoBoundaries ADMs to the US country border.
#(5) Save each files outputs.
#(6) After all countries are done, add them all into one large shapefile.

  
##########
##LOAD Department of State Data, Merge to a single Set.
##########

countryDB = {}
for i in range(0, len(dta['features'])):
  if("(disp)" in dta['features'][i]['properties']['COUNTRY_NA']):
    continue
  
  if("(UK)" in dta['features'][i]['properties']['COUNTRY_NA']):
    dta['features'][i]['properties']['COUNTRY_NA'] = "United Kingdom"
  
  if("(US)" in dta['features'][i]['properties']['COUNTRY_NA']):
    dta['features'][i]['properties']['COUNTRY_NA'] = "United States"
  
  if("(Aus)" in dta['features'][i]['properties']['COUNTRY_NA']):
    dta['features'][i]['properties']['COUNTRY_NA'] = "Australia"
  
  if("(Den)" in dta['features'][i]['properties']['COUNTRY_NA']):
    dta['features'][i]['properties']['COUNTRY_NA'] = "Denmark"
    
  if("(Fr)" in dta['features'][i]['properties']['COUNTRY_NA']):
    dta['features'][i]['properties']['COUNTRY_NA'] = "France"
  
  if("(Ch)" in dta['features'][i]['properties']['COUNTRY_NA']):
    dta['features'][i]['properties']['COUNTRY_NA'] = "China"
  
  if("(Nor)" in dta['features'][i]['properties']['COUNTRY_NA']):
    dta['features'][i]['properties']['COUNTRY_NA'] = "Norway"
  
  if("(NZ)" in dta['features'][i]['properties']['COUNTRY_NA']):
    dta['features'][i]['properties']['COUNTRY_NA'] = "New Zealand"
  
  if("Netherlands [Caribbean]" in dta['features'][i]['properties']['COUNTRY_NA']):
    dta['features'][i]['properties']['COUNTRY_NA'] = "Netherlands"
    
  if("(Neth)" in dta['features'][i]['properties']['COUNTRY_NA']):
    dta['features'][i]['properties']['COUNTRY_NA'] = "Netherlands"
    
  if("Portugal [" in dta['features'][i]['properties']['COUNTRY_NA']):
    dta['features'][i]['properties']['COUNTRY_NA'] = "Portugal"
  
  if("Spain [" in dta['features'][i]['properties']['COUNTRY_NA']):
    dta['features'][i]['properties']['COUNTRY_NA'] = "Spain"
    
  cName = dta['features'][i]['properties']['COUNTRY_NA']
  if(cName in countryDB.keys()):
    countryDB[cName].append(dta['features'][i])
  else:
    countryDB[cName] = []
    countryDB[cName].append(dta['features'][i])


##########
#Create a master geometry for every country:
##########
pickle_file = "./rawData/unionGeoms_"+  globalHashId + ".pickle"
if(os.path.exists(pickle_file)):
  print("Union geometries already built; skipping.")
else:
  countryGeoms = {}
  for country in countryDB.keys():

    shapes = []
    for j in range(0,len(countryDB[country])):
      shapes.append(shape(countryDB[country][j]["geometry"]))

    countryGeoms[country] = unary_union(shapes)

  pickle.dump(countryGeoms, open(pickle_file, "wb"))

#Match each country
#No fancy matching here - just no spaces and caps.
#We manually fix what's missing to be sure.
isoCSV = pd.read_csv("./rawData/ISO_3166_1_Alpha_3.csv")
isoCSV["matchCountryCSV"] = isoCSV["Country"].str.upper().replace(" ","",regex=True)
finalCountryDB = {}
for country in countryGeoms.keys():
  matchCountryArray = str(country).upper().replace(" ","")
  matches = isoCSV[isoCSV["matchCountryCSV"] == matchCountryArray]
  countryISO = "No ISO Found"
  if(len(matches) > 0):
    countryISO = matches.reset_index()["Alpha-3code"][0]
  else:
    #Manual adjustments:
    if(country == "Antigua & Barbuda"):
      countryISO = "ATG"
    if(country == "Bahamas, The"):
      countryISO = "BHS"
    if(country == "Bosnia & Herzegovina"):
      countryISO = "BIH"
    if(country == "Congo, Dem Rep of the"):
      countryISO = "COD"
    if(country == "Congo, Rep of the"):
      countryISO = "COG"
    if(country == "Cabo Verde"):
      countryISO = "CPV"
    if(country == "Cote d'Ivoire"):
      countryISO = "CIV"
    if(country == "Central African Rep"):
      countryISO = "CAF"
    if(country == "Czechia"):
      countryISO = "CZE"
    if(country == "Gambia, The"):
      countryISO = "GMB"
    if(country == "Iran"):
      countryISO = "IRN"
    if(country == "Korea, North"):
      countryISO = "PRK"
    if(country == "Korea, South"):
      countryISO = "KOR"
    if(country == "Laos"):
      countryISO = "LAO"
    if(country == "Macedonia"):
      countryISO = "MKD"
    if(country == "Marshall Is"):
      countryISO = "MHL"
    if(country == "Micronesia, Fed States of"):
      countryISO = "FSM"
    if(country == "Moldova"):
      countryISO = "MDA"
    if(country == "Sao Tome & Principe"):
      countryISO = "STP"
    if(country == "Solomon Is"):
      countryISO = "SLB"
    if(country == "St Kitts & Nevis"):
      countryISO = "KNA"
    if(country == "St Lucia"):
      countryISO = "LCA"
    if(country == "St Vincent & the Grenadines"):
      countryISO = "VCT"
    if(country == "Syria"):
      countryISO = "SYR"
    if(country == "Tanzania"):
      countryISO = "TZA"
    if(country == "Vatican City"):
      countryISO = "VAT"
  if(countryISO == "No ISO Found"):
    print(country)
    print("No ISO was found for at least one country.  Please correct this.")
  else:
    finalCountryDB[countryISO] = countryGeoms[country]
  

finalPickleFile = "./rawData/countryISOMatchesGeoms_"+  globalHashId + ".pickle"
if(os.path.exists(finalPickleFile)):
  print("Final geometries already built; skipping.")
else:
  pickle.dump(finalCountryDB, open(finalPickleFile, "wb")) 
  