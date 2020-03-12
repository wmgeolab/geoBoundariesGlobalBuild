import json
import requests
import os
import hashlib
import shapely
from shapely.geometry import shape, GeometryCollection, MultiPolygon, mapping
import fiona
import re
from shapely.ops import unary_union
import pandas as pd
import pickle
import numpy as np

with open('buildConfig.json') as json_file:
  config = json.load(json_file)

print(config)


globalHashId = hashlib.md5(str(config["globalBoundaryURL"]).encode()).hexdigest()
print(globalHashId)



def alignGeoBoundaries(i, config, officialBoundary):
  pathToGeoboundary =  ("./rawData/geoBoundaries/" + config["geoBoundariesVersion"] + 
       "/" + config["admLevel"] + "/" 
       + i + ".geojson")
  
  with open(pathToGeoboundary, "rb") as f:
    gbGJ = json.load(f)
    
  #Iterate over shapes
  areaDiff = 0
  for b in range(0, len(gbGJ["features"])):
    #gb Shape:
    gb = shape(gbGJ["features"][b]["geometry"])

    #Save the overall difference in area:
    areaDiff = areaDiff + gb.difference(officialBoundary).area

    gbGJ["features"][b]["geometry"] = mapping(gb.intersection(officialBoundary))
    
    json.dump(gbGJ, open("./processedData/geoBoundaries/" + config["geoBoundariesVersion"] + "/" + 
                        config["admLevel"] + "/" + i + "_" + config["admLevel"] + ".geojson", "w"))
  
  return([areaDiff, i])


#Load our pickle..
finalPickleFile = "./rawData/countryISOMatchesGeoms_"+  globalHashId + ".pickle"
officialBoundaries = pickle.load(open(finalPickleFile, "rb")) 

#Loop over all the official boundaries,
#Loading the geoBoundaries file,
#clipping, and then saving.

clippedCountries = []
for i in officialBoundaries.keys():
  clippedCountries.append(alignGeoBoundaries(i, config, officialBoundaries[i]))
  
  break
#Save a report
np.savetxt("./processedData/geoBoundaries/" + config["geoBoundariesVersion"] + "/" + 
                        config["admLevel"] + "/areaDifference.csv", clippedCountries, delimiter=",", fmt="%s", header="ISO, Difference between geoBoundaries and US Department of State ADM0 Boundary")

  

  
